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
      merge     – MERGE INTO (upsert). Requer 'merge_keys' em 'options'.

    Opções relevantes em 'options':
      merge_keys       – lista de colunas que identificam o registro (para merge)
      merge_condition  – condição SQL extra para o MERGE usando T.campo / S.campo
                         onde T = target (tabela destino) e S = source (DataFrame)
      delete_when      – condição SQL que marca a linha como excluída na origem;
                         vira WHEN MATCHED AND <cond> THEN DELETE, avaliada ANTES
                         do UPDATE. É o CDC de exclusão: a origem traz a linha com
                         uma marca (op = 'D', deleted = true) em vez de omiti-la.
      delete_not_matched_by_source
                       – true, ou uma condição SQL sobre T. Apaga do destino o que
                         não veio na origem: WHEN NOT MATCHED BY SOURCE THEN DELETE.
                         Só faz sentido quando a origem é um snapshot COMPLETO —
                         com uma carga incremental isto apaga o histórico.
      on               – a condição inteira do MERGE, escrita à mão (como o 'on'
                         do join). Substitui merge_keys/merge_condition.
      actions          – lista de cláusulas "WHEN ..." escritas à mão, emitidas
                         na ordem dada. Substitui o UPDATE/INSERT default e as
                         duas opções de delete. Ver `sparquet/io/merge.py`.

    Exemplos de JSON:
      { "format": "delta", "path": "catalog.schema.clientes", "mode": "overwrite" }
      {
        "format": "delta",
        "path": "catalog.schema.pedidos",
        "mode": "merge",
        "options": { "merge_keys": ["pedido_id"] }
      }
      {
        "format": "delta",
        "path": "catalog.schema.clientes",
        "mode": "merge",
        "options": {
          "merge_keys": ["cliente_id"],
          "delete_when": "S.op = 'D'"
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

        sql = merge_sql(
            target, view, options, self._default_actions(df, options), "DeltaWriter"
        )
        df.sparkSession.sql(sql)

    def _default_actions(self, df: DataFrame, options: dict) -> list[str]:
        """As cláusulas `UPDATE`/`INSERT` montadas a partir das colunas.

        Não são calculadas quando `actions` foi escrito à mão — nesse caso ler o
        schema do destino seria trabalho jogado fora, e um destino inexistente
        deixaria de ser um problema de quem escreveu o SQL.
        """
        if options.get("actions") is not None:
            return []

        merge_keys: list[str] = options.get("merge_keys") or []

        # Só as colunas que existem NOS DOIS lados entram no UPDATE e no INSERT.
        # Uma origem de CDC costuma trazer colunas de controle (`op`, `_ts`) que o
        # destino não tem: elas decidem o que fazer com a linha (é o que
        # `delete_when` lê) e não são dado a gravar. Sem este filtro o MERGE
        # falhava ao resolver `T.op`, com um erro que não diz isso.
        alvo = self._target_columns()
        gravaveis = [c for c in df.columns if alvo is None or c in alvo]

        update_set = ", ".join(
            f"T.{c} = S.{c}" for c in gravaveis if c not in merge_keys
        )
        insert_cols = ", ".join(gravaveis)
        insert_vals = ", ".join(f"S.{c}" for c in gravaveis)
        return [
            f"WHEN MATCHED THEN UPDATE SET {update_set}",
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})",
        ]

    def _target_columns(self) -> list[str] | None:
        """As colunas da tabela de destino, ou None quando não dá para saber.

        `None` faz o merge usar todas as colunas da origem, que é o
        comportamento de sempre — nunca menos do que antes por causa de uma
        leitura de schema que não deu certo.
        """
        path = self.config.path
        try:
            if _is_table_name(path):
                return self.spark.table(path).columns
            return self.spark.read.format("delta").load(path).columns
        except Exception:
            return None
