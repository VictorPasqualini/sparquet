from pyspark.sql import DataFrame

from sparquet.io.base import BaseReader, BaseWriter

_CSV_READ_DEFAULTS = {
    "header": "true",
    "inferSchema": "true",
    "encoding": "UTF-8",
}

_CSV_WRITE_DEFAULTS = {
    "header": "true",
    "encoding": "UTF-8",
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
