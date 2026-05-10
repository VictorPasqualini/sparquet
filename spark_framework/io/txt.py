from pyspark.sql import DataFrame

from spark_framework.io.base import BaseReader, BaseWriter


class TxtReader(BaseReader):
    """Lê arquivos de texto puro.

    Cada linha vira uma linha do DataFrame com uma coluna chamada 'value' (string).
    Use transformações subsequentes (sql, with_column, etc.) para parsear o conteúdo.

    Opções suportadas via 'options' no JSON:
      wholetext – "true" para ler cada arquivo inteiro como uma única string (default: false)
      lineSep   – separador de linha customizado (default: \\n)
      pathGlobFilter – filtro glob para múltiplos arquivos
    """

    def read(self) -> DataFrame:
        return self.spark.read.options(**self.config.options).text(self.config.path)


class TxtWriter(BaseWriter):
    """Escreve um DataFrame como arquivo de texto puro.

    O DataFrame deve ter exatamente UMA coluna do tipo string.
    Se tiver mais de uma coluna, use 'columns' no output para projetar antes de gravar,
    ou concatene as colunas com a transformação with_column + concat_ws().

    Opções suportadas via 'options' no JSON:
      lineSep – separador de linha customizado (default: \\n)
      compression – none | bzip2 | gzip | lz4 | snappy | deflate
    """

    def write(self, df: DataFrame) -> None:
        writer = df.write.mode(self.config.mode).options(**self.config.options)
        if self.config.partition_by:
            writer = writer.partitionBy(*self.config.partition_by)
        writer.text(self.config.path)
