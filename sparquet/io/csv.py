from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter

_CSV_READ_DEFAULTS = {
    "header": "true",
    "inferSchema": "true",
    "encoding": "UTF-8",
    # O mesmo dialeto RFC 4180 do writer (ver _CSV_WRITE_DEFAULTS): sem isto o
    # framework escreveria um CSV que ele proprio nao rele — as aspas dobradas
    # voltariam partidas no meio do campo.
    "escape": '"',
}

_CSV_WRITE_DEFAULTS = {
    "header": "true",
    "encoding": "UTF-8",
    # RFC 4180: aspas dentro de um campo sao DOBRADAS (""), nao escapadas com "\\".
    # O default do Spark escreve `\\"`, que ele proprio rele — mas nao o csv do
    # Python, nem o pandas, nem o Excel. Isso quebrava na pratica o
    # `validations.report`, cuja coluna `rule_params` e um JSON cheio de aspas: o
    # relatorio saia ilegivel justamente na ferramenta em que ele e analisado.
    # Sobrescrevivel via `options` para quem precisa do dialeto antigo.
    "escape": '"',
}


class CsvReader(BaseReader):
    def read(self) -> DataFrame:
        options = {**_CSV_READ_DEFAULTS, **self.config.options}
        return self.spark.read.options(**options).csv(self.config.path)


class CsvWriter(BaseWriter):
    def write(self, df: DataFrame) -> None:
        options = {**_CSV_WRITE_DEFAULTS, **self.config.options}
        writer = df.write.mode(self.config.mode).options(**options)
        if self.config.partition_by:
            writer = writer.partitionBy(*self.config.partition_by)
        writer.csv(self.config.path)
