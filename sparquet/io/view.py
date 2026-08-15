from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter


class ViewReader(BaseReader):
    """Lê de uma Spark temp view ou tabela registrada no catálogo.

    path = nome da view ou 'catalog.schema.tabela'

    Útil para ler o resultado de um pipeline anterior que escreveu via ViewWriter,
    evitando reler a fonte original a cada iteração de um loop.

    Exemplo JSON:
      { "format": "view", "path": "cessoes_para_processar" }
    """

    def read(self) -> DataFrame:
        return self.spark.table(self.config.path)


class ViewWriter(BaseWriter):
    """Registra o DataFrame como Spark temp view (em memória, escopo da sessão).

    path = nome da temp view

    Por padrão faz cache() antes de registrar — isso é importante quando a view
    será lida múltiplas vezes (ex: loop por tipo de ativo). Para desativar:
      "options": { "cache": "false" }

    Exemplo JSON:
      {
        "format": "view",
        "path": "cessoes_para_processar",
        "mode": "overwrite",
        "options": { "cache": "true" }
      }
    """

    def write(self, df: DataFrame) -> None:
        if self.config.options.get("cache", "true").lower() != "false":
            df.cache()
            df.count()  # materializa o cache imediatamente

        df.createOrReplaceTempView(self.config.path)
