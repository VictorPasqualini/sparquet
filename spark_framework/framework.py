from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from spark_framework.core.config import PipelineConfig, SparkConfig
from spark_framework.core.context import SparkContextManager
from spark_framework.core.pipeline import Pipeline, PipelineResult
from spark_framework.io.factory import ReaderFactory, WriterFactory
from spark_framework.io.base import BaseReader, BaseWriter
from spark_framework.transform.base import BaseTransformation
from spark_framework.transform.engine import TransformationEngine
from spark_framework.utils.logger import logger
from spark_framework.validation.base import BaseValidator
from spark_framework.validation.engine import ValidationEngine


class SparkFramework:
    """Ponto de entrada principal para uso do framework como biblioteca.

    Gerencia uma única SparkSession compartilhada entre múltiplas execuções.
    Permite registrar formatos, transformações e validators customizados que
    ficam disponíveis em todos os pipelines executados pela mesma instância.

    Uso básico:
        fw = SparkFramework()
        fw.run("pipeline_clientes.json")
        fw.run("pipeline_pedidos.json")
        fw.stop()

    Com configurações Spark personalizadas:
        fw = SparkFramework(spark={"master": "yarn", "app_name": "MeuJob"})

    Com extensões customizadas:
        fw = SparkFramework()
        fw.register_reader("delta", DeltaReader)
        fw.register_transformation("normalize", NormalizeTransformation)
        fw.run("config.json")
    """

    def __init__(self, spark: Optional[Dict[str, Any]] = None) -> None:
        self._spark_config = SparkConfig.from_dict(spark or {})
        self._transform_engine = TransformationEngine()
        self._validation_engine = ValidationEngine()
        SparkContextManager.get_or_create(self._spark_config)

    # ------------------------------------------------------------------
    # Execução de pipelines
    # ------------------------------------------------------------------

    def run(
        self,
        config_path: str,
        columns: Optional[Dict[str, Any]] = None,
    ) -> PipelineResult:
        """Executa um pipeline a partir de um arquivo JSON.

        Args:
            config_path: caminho para o JSON de configuração.
            columns:     Valores literais de runtime injetados como colunas no df
                         logo após a leitura, antes das transformações.
                         Escalares viram F.lit; listas viram F.array(F.lit, ...).
                         Valores None/[] não são injetados — transformações que
                         dependem deles devem usar "skip_if_null".
                         Ex: {"param_tipo_ativo": "NC", "param_lista_cessoes": ["a","b"]}.
        """
        config = PipelineConfig.from_file(config_path)
        self._apply_spark_override(config)
        return self._execute(config, columns=columns)

    def run_from_dict(
        self,
        config: Dict[str, Any],
        columns: Optional[Dict[str, Any]] = None,
    ) -> PipelineResult:
        """Executa um pipeline a partir de um dicionário Python."""
        pipeline_config = PipelineConfig.from_dict(config)
        self._apply_spark_override(pipeline_config)
        return self._execute(pipeline_config, columns=columns)

    # ------------------------------------------------------------------
    # Registro de extensões
    # ------------------------------------------------------------------

    def register_reader(self, format_name: str, reader_cls: type[BaseReader]) -> None:
        """Registra um leitor customizado para um novo formato."""
        ReaderFactory.register(format_name, reader_cls)

    def register_writer(self, format_name: str, writer_cls: type[BaseWriter]) -> None:
        """Registra um escritor customizado para um novo formato."""
        WriterFactory.register(format_name, writer_cls)

    def register_transformation(
        self, name: str, transformation_cls: type[BaseTransformation]
    ) -> None:
        """Registra uma transformação customizada disponível via JSON."""
        self._transform_engine.register(name, transformation_cls)

    def register_validator(
        self, name: str, validator_cls: type[BaseValidator]
    ) -> None:
        """Registra um validator customizado disponível via JSON."""
        self._validation_engine.register(name, validator_cls)

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    def drop_views(self, names: Iterable[str]) -> List[str]:
        """Remove temp views da sessão Spark e libera o cache associado.

        Útil ao final de um orquestrador para limpar as views intermediárias
        criadas por ViewWriter (que faz cache + createOrReplaceTempView).

        Args:
            names: nomes das temp views a remover.

        Returns:
            Lista das views efetivamente removidas (as inexistentes são ignoradas).
        """
        spark = SparkContextManager.get_or_create(self._spark_config)
        removed: List[str] = []
        for name in names:
            try:
                # unpersist (caso a view esteja em cache via ViewWriter)
                try:
                    spark.table(name).unpersist()
                except Exception:
                    pass
                spark.catalog.dropTempView(name)
                removed.append(name)
            except Exception as exc:
                logger.warn("Falha ao remover temp view", view=name, error=str(exc))
        if removed:
            logger.info("Temp views removidas", views=removed)
        return removed

    def stop(self) -> None:
        """Encerra a SparkSession."""
        SparkContextManager.stop()

    # ------------------------------------------------------------------
    # Interno
    # ------------------------------------------------------------------

    def _execute(
        self,
        config: PipelineConfig,
        columns: Optional[Dict[str, Any]] = None,
    ) -> PipelineResult:
        pipeline = Pipeline(
            config,
            transform_engine=self._transform_engine,
            validation_engine=self._validation_engine,
            columns=columns,
        )
        return pipeline.run()

    def _apply_spark_override(self, config: PipelineConfig) -> None:
        """Garante que configs Spark do framework prevalecem sobre o JSON."""
        if self._spark_config.app_name != "SparkFramework":
            config.spark.app_name = self._spark_config.app_name
        if self._spark_config.master != "local[*]":
            config.spark.master = self._spark_config.master
        config.spark.configs.update(self._spark_config.configs)
