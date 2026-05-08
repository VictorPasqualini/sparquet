from pyspark.sql import DataFrame

from spark_framework.io.base import BaseReader, BaseWriter


class ParquetReader(BaseReader):
    def read(self) -> DataFrame:
        reader = self.spark.read.format("parquet")
        for key, value in self.config.options.items():
            reader = reader.option(key, value)
        return reader.load(self.config.path)


class ParquetWriter(BaseWriter):
    def write(self, df: DataFrame) -> None:
        writer = df.write.format("parquet").mode(self.config.mode)
        for key, value in self.config.options.items():
            writer = writer.option(key, value)
        if self.config.partition_by:
            writer = writer.partitionBy(*self.config.partition_by)
        writer.save(self.config.path)
