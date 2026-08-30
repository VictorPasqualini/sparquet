from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter, is_table_name
from sparquet.io.merge import MERGE_OPTIONS as _MERGE_OPTIONS, merge_sql


class IcebergReader(BaseReader):
    """Lê tabelas Iceberg.

    Aceita as duas formas de referência em 'path':
      - Tabela de catálogo: "catalogo.schema.tabela" (forma normal)
      - Caminho físico da tabela: "/warehouse/db/tabela", "s3://bucket/db/tabela"

    Opções de time travel via 'options': 'snapshot-id', 'as-of-timestamp'.
    """

    def read(self) -> DataFrame:
        reader = self.spark.read.format("iceberg")
        for key, value in self.config.options.items():
            reader = reader.option(key, value)
        return reader.load(self.config.path)


class IcebergWriter(BaseWriter):
    """Escreve em tabelas Iceberg.

    Modos via 'mode': overwrite, append e merge (upsert; requer 'merge_keys').

    No merge, além de 'merge_keys' e 'merge_condition', duas opções apagam:
    'delete_when' (condição sobre a origem; vira WHEN MATCHED AND <cond> THEN
    DELETE) e 'delete_not_matched_by_source' (true ou condição sobre T; apaga o
    que a origem não trouxe — só correto quando a origem é um snapshot completo).
    Para o que essas opções não expressam, 'on' recebe a condição inteira e
    'actions' a lista de cláusulas "WHEN ..." escritas à mão, na ordem dada.
    Ver `sparquet/io/merge.py`.

    Quando 'path' é um identificador de catálogo ("catalogo.schema.tabela"), a
    escrita usa `saveAsTable`, que **cria a tabela se ela ainda não existir** —
    incluindo o particionamento de 'partition_by'. Isso importa porque o caminho
    alternativo, `save`, exige tabela pré-existente: no Spark 4 apontar um output
    para uma tabela nova falhava com `[TABLE_OR_VIEW_NOT_FOUND]`, e o primeiro
    carregamento de qualquer pipeline só funcionava com um DDL feito à mão fora
    do framework. Para caminho físico (tem '/' ou ':') a escrita continua em
    `save`, que é a única forma que aceita path.
    """

    def write(self, df: DataFrame) -> None:
        if self.config.mode == "merge":
            self._merge(df)
            return

        writer = df.write.format("iceberg").mode(self.config.mode)
        for key, value in self.config.options.items():
            if key in _MERGE_OPTIONS:
                continue
            writer = writer.option(key, value)
        if self.config.partition_by:
            writer = writer.partitionBy(*self.config.partition_by)

        if is_table_name(self.config.path):
            writer.saveAsTable(self.config.path)
        else:
            writer.save(self.config.path)

    def _merge(self, df: DataFrame) -> None:
        options = self.config.options
        target = self.config.path
        # MERGE INTO só existe sobre tabela do catálogo, e só depois que ela
        # existe. Na primeira carga não há o que atualizar: gravar tudo cria a
        # tabela e deixa o mesmo estado que um merge contra tabela vazia deixaria.
        if is_table_name(target) and not self.spark.catalog.tableExists(target):
            df.write.format("iceberg").mode("append").saveAsTable(target)
            return

        view = "_spark_fw_merge_src"
        df.createOrReplaceTempView(view)

        # `UPDATE SET *` / `INSERT *` casam as colunas por nome e toleram uma
        # coluna a mais na origem — o oposto do Delta, que precisa listá-las.
        defaults = [
            "WHEN MATCHED THEN UPDATE SET *",
            "WHEN NOT MATCHED THEN INSERT *",
        ]
        self.spark.sql(merge_sql(target, view, options, defaults, "IcebergWriter"))
