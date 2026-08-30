from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter, is_table_name


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
            writer = writer.option(key, value)
        if self.config.partition_by:
            writer = writer.partitionBy(*self.config.partition_by)

        if is_table_name(self.config.path):
            writer.saveAsTable(self.config.path)
        else:
            writer.save(self.config.path)

    def _merge(self, df: DataFrame) -> None:
        merge_keys: list[str] = self.config.options.get("merge_keys", [])
        if not merge_keys:
            raise ValueError(
                "Iceberg merge mode requires 'merge_keys' inside output.options"
            )

        target = self.config.path
        # MERGE INTO só existe sobre tabela do catálogo, e só depois que ela
        # existe. Na primeira carga não há o que atualizar: gravar tudo cria a
        # tabela e deixa o mesmo estado que um merge contra tabela vazia deixaria.
        if is_table_name(target) and not self.spark.catalog.tableExists(target):
            df.write.format("iceberg").mode("append").saveAsTable(target)
            return

        df.createOrReplaceTempView("_spark_fw_merge_src")
        join_cond = " AND ".join(f"T.{k} = S.{k}" for k in merge_keys)
        extra = self.config.options.get("merge_condition", "")
        if extra:
            join_cond = f"({join_cond}) AND ({extra})"

        self.spark.sql(f"""
            MERGE INTO {target} AS T
            USING _spark_fw_merge_src AS S
            ON {join_cond}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
