from __future__ import annotations

import os

from pyspark.sql import SparkSession

from sparquet.core.config import SparkConfig


def _detect_environment() -> str:
    """Detecta o ambiente de execução atual.

    Returns:
        "databricks" | "emr" | "dataproc" | "synapse" | "local"
    """
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return "databricks"
    if os.environ.get("EMR_CLUSTER_ID") or os.path.exists("/mnt/var/lib/info/job-flow.json"):
        return "emr"
    if os.environ.get("DATAPROC_IMAGE_VERSION") or os.environ.get("DATAPROC_CLUSTER_NAME"):
        return "dataproc"
    if os.environ.get("SYNAPSE_WORKSPACE_NAME") or os.environ.get("AZURE_DATABRICKS_ORG_ID"):
        return "synapse"
    return "local"


class SparkContextManager:
    """Gerencia um singleton de SparkSession para toda a vida do pipeline.

    Em Databricks reutiliza a sessão ativa do runtime — nunca cria uma nova.
    Em outros ambientes (EMR, Dataproc, Synapse, local) cria/reutiliza via builder.
    """

    _session: SparkSession | None = None

    @classmethod
    def get_or_create(cls, config: SparkConfig) -> SparkSession:
        if cls._session is not None:
            return cls._session

        env = _detect_environment()

        if env == "databricks":
            # No Databricks a sessão já existe; criar uma nova causaria erro.
            cls._session = SparkSession.getActiveSession()
            if cls._session is None:
                # Fallback improvável, mas seguro
                cls._session = SparkSession.builder.getOrCreate()
        else:
            builder = SparkSession.builder.appName(config.app_name)

            # Master só faz sentido fora de clusters gerenciados
            if env == "local":
                builder = builder.master(config.master)

            for key, value in config.configs.items():
                builder = builder.config(key, value)

            cls._session = builder.getOrCreate()
            cls._session.sparkContext.setLogLevel("WARN")

        return cls._session

    @classmethod
    def stop(cls) -> None:
        env = _detect_environment()
        if cls._session is not None and env != "databricks":
            cls._session.stop()
        cls._session = None

    @classmethod
    def current_environment(cls) -> str:
        """Retorna o ambiente detectado (útil para logs e diagnóstico)."""
        return _detect_environment()
