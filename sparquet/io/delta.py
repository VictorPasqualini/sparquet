from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter, is_table_name as _is_table_name


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
      merge     – MERGE INTO (upsert). Requer 'merge_keys' em 'options'.

    Opções relevantes em 'options':
      merge_keys       – lista de colunas que identificam o registro (para merge)
      merge_condition  – condição SQL extra para o MERGE usando T.campo / S.campo
                         onde T = target (tabela destino) e S = source (DataFrame)

    Exemplos de JSON:
      { "format": "delta", "path": "catalog.schema.clientes", "mode": "overwrite" }
      {
        "format": "delta",
        "path": "catalog.schema.pedidos",
        "mode": "merge",
        "options": { "merge_keys": ["pedido_id"] }
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
        opts = {k: v for k, v in self.config.options.items() if k not in ("merge_keys", "merge_condition")}
        writer = writer.options(**opts)
        if self.config.partition_by:
            writer = writer.partitionBy(*self.config.partition_by)

        path = self.config.path
        if _is_table_name(path):
            writer.saveAsTable(path)
        else:
            writer.save(path)

    def _merge(self, df: DataFrame) -> None:
        merge_keys: list[str] = self.config.options.get("merge_keys", [])
        if not merge_keys:
            raise ValueError(
                "DeltaWriter mode='merge' requer 'merge_keys' em options. "
                "Ex: { \"merge_keys\": [\"id\"] }"
            )

        path = self.config.path
        target = (
            f"delta.`{path}`" if not _is_table_name(path) else path
        )

        view = "_spark_fw_merge_src"
        df.createOrReplaceTempView(view)

        join_cond = " AND ".join(f"T.{k} = S.{k}" for k in merge_keys)
        extra = self.config.options.get("merge_condition", "")
        if extra:
            join_cond = f"({join_cond}) AND ({extra})"

        update_set = ", ".join(
            f"T.{c} = S.{c}" for c in df.columns if c not in merge_keys
        )
        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join(f"S.{c}" for c in df.columns)

        sql = f"""
            MERGE INTO {target} AS T
            USING {view} AS S
            ON {join_cond}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        df.sparkSession.sql(sql)
