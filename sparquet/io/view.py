from __future__ import annotations

from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter


class ViewReader(BaseReader):
    """Lê de uma Spark temp view ou tabela registrada no catálogo.

    path = nome da view ou 'catalog.schema.tabela'

    options.scope:
      "session" (default) – temp view da sessão atual.
      "global"            – global temp view: lida como `global_temp.<nome>`
                            (a MESMA app, qualquer sessão). Quando o path não tem
                            ponto, o prefixo `global_temp.` é adicionado automaticamente.

    Útil para ler o resultado de um pipeline anterior que escreveu via ViewWriter,
    evitando reler a fonte original a cada iteração de um loop.

    Exemplo JSON:
      { "format": "view", "path": "cessoes_para_processar" }
      { "format": "view", "path": "cessoes_para_processar", "options": { "scope": "global" } }
    """

    def read(self) -> DataFrame:
        path = self.config.path
        scope = str(self.config.options.get("scope", "session")).lower()
        if scope == "global" and "." not in path:
            path = f"global_temp.{path}"
        return self.spark.table(path)


class ViewWriter(BaseWriter):
    """Registra o DataFrame como Spark temp view (em memória).

    path = nome da temp view

    options.cache: por padrão faz cache() + count() antes de registrar — importante
      quando a view é lida múltiplas vezes (ex: loop). Desative com "cache": "false".

    options.scope:
      "session" (default) – `createOrReplaceTempView`: visível só na sessão atual.
      "global"            – `createOrReplaceGlobalTempView`: visível a TODAS as
                            sessões da mesma aplicação Spark (database `global_temp`);
                            leia via `global_temp.<nome>` ou com scope="global".

    Exemplo JSON:
      { "format": "view", "path": "staging", "options": { "cache": "true" } }
      { "format": "view", "path": "staging", "options": { "scope": "global" } }
    """

    def write(self, df: DataFrame) -> None:
        if self.config.options.get("cache", "true").lower() != "false":
            df.cache()
            df.count()  # materializa o cache imediatamente

        scope = str(self.config.options.get("scope", "session")).lower()
        if scope == "global":
            df.createOrReplaceGlobalTempView(self.config.path)
        else:
            df.createOrReplaceTempView(self.config.path)
