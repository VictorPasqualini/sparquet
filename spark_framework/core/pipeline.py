from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_framework.core.config import OutputConfig, PipelineConfig
from spark_framework.core.context import SparkContextManager
from spark_framework.io.factory import ReaderFactory, WriterFactory
from spark_framework.transform.engine import TransformationEngine
from spark_framework.validation.base import ValidationResult
from spark_framework.validation.engine import ValidationEngine
from spark_framework.utils.logger import flush_deferred_warnings, logger


@dataclass
class PipelineResult:
    pipeline_name: str
    success: bool
    rows_read: int = 0
    rows_written: int = 0
    validation_results: List[ValidationResult] = field(default_factory=list)
    error: Optional[str] = None
    output_df: Optional[DataFrame] = None  # df após transformações; disponível quando input_df é injetado

    def summary(self) -> str:
        if not self.success:
            return f"[FAIL] '{self.pipeline_name}': {self.error}"
        passed = sum(1 for r in self.validation_results if r.passed)
        total = len(self.validation_results)
        return (
            f"[OK] '{self.pipeline_name}' | "
            f"lidos={self.rows_read} | "
            f"escritos={self.rows_written} | "
            f"validacoes={passed}/{total}"
        )


class Pipeline:
    """Orquestra o fluxo: leitura → transformação → validação → escrita."""

    def __init__(
        self,
        config: PipelineConfig,
        transform_engine: Optional[TransformationEngine] = None,
        validation_engine: Optional[ValidationEngine] = None,
        input_df: Optional[DataFrame] = None,
        columns: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.config = config
        self._transform_engine = transform_engine or TransformationEngine()
        self._validation_engine = validation_engine or ValidationEngine()
        self._input_df = input_df
        self._columns: Dict[str, Any] = columns or {}

    @classmethod
    def from_file(cls, path: str) -> Pipeline:
        return cls(PipelineConfig.from_file(path))

    @classmethod
    def from_dict(cls, data: dict) -> Pipeline:
        return cls(PipelineConfig.from_dict(data))

    def run(self) -> PipelineResult:
        log = logger.bind(pipeline=self.config.name)
        log.info("Pipeline iniciado")

        try:
            spark = SparkContextManager.get_or_create(self.config.spark)

            if self._input_df is not None:
                df = self._input_df
                rows_read = 0
                log.info("Input df injetado externamente", colunas=len(df.columns))
            else:
                df = ReaderFactory.create(spark, self.config.input).read()
                df = df.withColumn("ingestion_ts", F.current_timestamp())
                rows_read = df.count()
                log.info(
                    "Leitura concluida",
                    linhas=rows_read,
                    formato=self.config.input.format,
                )

            for col_name, value in self._columns.items():
                df = df.withColumn(col_name, F.lit(value))
            if self._columns:
                log.info("Colunas injetadas", colunas=list(self._columns))

            df = self._transform_engine.apply(df, self.config.transformations)
            log.info("Transformacoes aplicadas")

            validation_results = self._validation_engine.validate(
                df, self.config.validations
            )

            rows_written = df.count()
            self._write_outputs(spark, df, log)
            flush_deferred_warnings(log)
            log.info("Pipeline concluido", linhas_escritas=rows_written)

            return PipelineResult(
                pipeline_name=self.config.name,
                success=True,
                rows_read=rows_read,
                rows_written=rows_written,
                validation_results=validation_results,
                output_df=df,
            )

        except Exception as exc:
            flush_deferred_warnings(log)
            log.error("Pipeline falhou", error=str(exc))
            return PipelineResult(
                pipeline_name=self.config.name,
                success=False,
                error=str(exc),
            )

    def _write_outputs(
        self, spark: SparkSession, df: DataFrame, log
    ) -> None:
        for output in self.config.outputs:
            output_df = self._project_columns(df, output)
            log.info(
                "Escrevendo output",
                formato=output.format,
                path=output.path,
                modo=output.mode,
                colunas=output.columns or "todas",
            )
            WriterFactory.create(spark, output).write(output_df)

    @staticmethod
    def _project_columns(df: DataFrame, output: OutputConfig) -> DataFrame:
        """Aplica seleção de colunas se o output tiver 'columns' definido."""
        if not output.columns:
            return df
        missing = [c for c in output.columns if c not in df.columns]
        if missing:
            raise ValueError(
                f"Colunas inexistentes no output '{output.path}': {missing}. "
                f"Colunas disponiveis: {df.columns}"
            )
        return df.select(*output.columns)
