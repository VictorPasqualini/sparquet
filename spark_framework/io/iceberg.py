from pyspark.sql import DataFrame

from spark_framework.io.base import BaseReader, BaseWriter


class IcebergReader(BaseReader):
    def read(self) -> DataFrame:
        reader = self.spark.read.format("iceberg")
        for key, value in self.config.options.items():
            reader = reader.option(key, value)
        return reader.load(self.config.path)


class IcebergWriter(BaseWriter):
    def write(self, df: DataFrame) -> None:
        if self.config.mode == "merge":
            self._merge(df)
        else:
            writer = df.write.format("iceberg").mode(self.config.mode)
            for key, value in self.config.options.items():
                writer = writer.option(key, value)
            if self.config.partition_by:
                writer = writer.partitionBy(*self.config.partition_by)
            writer.save(self.config.path)

    def _merge(self, df: DataFrame) -> None:
        merge_keys: list[str] = self.config.options.get("merge_keys", [])
        if not merge_keys:
            raise ValueError(
                "Iceberg merge mode requires 'merge_keys' inside output.options"
            )

        df.createOrReplaceTempView("_merge_source")
        join_condition = " AND ".join(
            f"target.{k} = source.{k}" for k in merge_keys
        )
        self.spark.sql(f"""
            MERGE INTO {self.config.path} AS target
            USING _merge_source AS source
            ON {join_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
