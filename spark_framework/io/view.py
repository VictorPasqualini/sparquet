from __future__ import annotations

from pyspark.sql import DataFrame

from spark_framework.io.base import BaseReader, BaseWriter


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

    Opções (em 'options'):
      cache      – "true" (default) faz df.cache() + df.count() antes de
                   registrar a view. Necessário quando a view será lida
                   múltiplas vezes. "false" desativa.
      checkpoint – "true" faz df.localCheckpoint() em vez de cache. Mais
                   agressivo: materializa em disco local e QUEBRA a lineage
                   do Catalyst — útil quando o DAG está crescendo demais
                   (otimizador lento) ou quando há risco de recomputo caro
                   em re-acessos da view. Quando checkpoint=true, o cache
                   é ignorado (são abordagens mutuamente exclusivas).
                   Default: "false".

    Exemplo JSON:
      {
        "format": "view",
        "path": "cessoes_base",
        "mode": "overwrite",
        "options": { "checkpoint": "true" }
      }
    """

    def write(self, df: DataFrame) -> None:
        opts = self.config.options
        use_checkpoint = opts.get("checkpoint", "false").lower() == "true"
        use_cache = opts.get("cache", "true").lower() != "false"

        if use_checkpoint:
            # eager=True (default): materializa imediatamente em disco local
            # e quebra a lineage do Catalyst.
            df = df.localCheckpoint()
        elif use_cache:
            df.cache()
            df.count()  # materializa o cache imediatamente

        df.createOrReplaceTempView(self.config.path)
