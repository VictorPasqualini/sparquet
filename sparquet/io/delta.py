from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter, is_table_name as _is_table_name
from sparquet.io.merge import MERGE_OPTIONS as _MERGE_OPTIONS, merge_sql


class DeltaReader(BaseReader):
    """Lê tabelas Delta Lake.

    Suporta dois modos de referência via 'path' no JSON:
      - Caminho físico:  "/mnt/datalake/tabela" ou "s3://bucket/prefix"
      - Tabela Unity Catalog / Hive Metastore: "catalog.schema.tabela" ou "schema.tabela"

    O framework detecta automaticamente: se 'path' contiver '.' e não começar com
    '/' ou um scheme de storage, é tratado como nome de tabela; caso contrário, como path.

    Opções suportadas via 'options':
      versionAsOf   – lê uma versão específica (time travel)
      timestampAsOf – lê o estado em um timestamp (time travel)

    Exemplos de JSON:
      { "format": "delta", "path": "catalog.schema.clientes" }
      { "format": "delta", "path": "/mnt/raw/clientes", "options": { "versionAsOf": "5" } }
    """

    def read(self) -> DataFrame:
        path = self.config.path
        reader = self.spark.read.format("delta").options(**self.config.options)

        if _is_table_name(path):
            return reader.table(path)
        return reader.load(path)


class DeltaWriter(BaseWriter):
    """Escreve dados em tabelas Delta Lake.

    Modos suportados via 'mode' no JSON:
      overwrite – substitui toda a tabela / partição
      append    – adiciona linhas
      merge     – MERGE INTO (upsert). Requer 'on' e 'actions' em 'options'.

    Opções do merge em 'options', as duas obrigatórias:
      on               – a condição inteira do MERGE, escrita à mão (como o 'on'
                         do join), sobre T = target (tabela destino) e
                         S = source (o DataFrame que está sendo gravado).
      actions          – lista de cláusulas "WHEN ..." escritas à mão, emitidas
                         na ordem dada — e em MERGE INTO a primeira cláusula que
                         casa é a que vale. Ver `sparquet/io/merge.py`.

    `UPDATE SET *` / `INSERT *` funcionam aqui, mas só quando origem e destino
    têm as mesmas colunas: uma origem de CDC que traz `op` falha ao resolver a
    coluna extra no destino, e aí as colunas entram listadas à mão.

    Exemplos de JSON:
      { "format": "delta", "path": "catalog.schema.clientes", "mode": "overwrite" }
      {
        "format": "delta",
        "path": "catalog.schema.pedidos",
        "mode": "merge",
        "options": {
          "on": "T.pedido_id = S.pedido_id",
          "actions": [
            "WHEN MATCHED THEN UPDATE SET *",
            "WHEN NOT MATCHED THEN INSERT *"
          ]
        }
      }
      {
        "format": "delta",
        "path": "catalog.schema.clientes",
        "mode": "merge",
        "options": {
          "on": "T.cliente_id = S.cliente_id AND T.loja = S.loja",
          "actions": [
            "WHEN MATCHED AND S.op = 'D' THEN DELETE",
            "WHEN MATCHED THEN UPDATE SET T.nome = S.nome",
            "WHEN NOT MATCHED THEN INSERT (cliente_id, loja, nome) VALUES (S.cliente_id, S.loja, S.nome)"
          ]
        }
      }
    """

    def write(self, df: DataFrame) -> None:
        mode = self.config.mode.lower()

        if mode == "merge":
            self._merge(df)
        else:
            self._standard_write(df, mode)

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _standard_write(self, df: DataFrame, mode: str) -> None:
        writer = df.write.format("delta").mode(mode)
        opts = {k: v for k, v in self.config.options.items() if k not in _MERGE_OPTIONS}
        writer = writer.options(**opts)
        if self.config.partition_by:
            writer = writer.partitionBy(*self.config.partition_by)

        path = self.config.path
        if _is_table_name(path):
            writer.saveAsTable(path)
        else:
            writer.save(path)

    def _merge(self, df: DataFrame) -> None:
        options = self.config.options
        path = self.config.path
        target = f"delta.`{path}`" if not _is_table_name(path) else path

        view = "_spark_fw_merge_src"
        df.createOrReplaceTempView(view)

        df.sparkSession.sql(merge_sql(target, view, options, "DeltaWriter"))
