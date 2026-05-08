from __future__ import annotations

from pyspark.sql import SparkSession

from spark_framework.core.config import SparkConfig


class SparkContextManager:
    """Manages a singleton SparkSession for the pipeline lifetime."""

    _session: SparkSession | None = None

    @classmethod
    def get_or_create(cls, config: SparkConfig) -> SparkSession:
        if cls._session is None:
            builder = (
                SparkSession.builder
                .appName(config.app_name)
                .master(config.master)
            )
            for key, value in config.configs.items():
                builder = builder.config(key, value)

            cls._session = builder.getOrCreate()
            cls._session.sparkContext.setLogLevel("WARN")

        return cls._session

    @classmethod
    def stop(cls) -> None:
        if cls._session is not None:
            cls._session.stop()
            cls._session = None
