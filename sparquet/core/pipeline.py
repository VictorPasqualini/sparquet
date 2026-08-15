from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sparquet.core.config import OutputConfig, PipelineConfig
from sparquet.core.context import SparkContextManager
from sparquet.io.factory import ReaderFactory, WriterFactory
from sparquet.transform.base import PipelineStop
from sparquet.transform.engine import TransformationEngine
from sparquet.validation.base import ValidationResult
from sparquet.validation.engine import ValidationEngine
from sparquet.utils.logger import flush_deferred_warnings, logger


@dataclass
class OutputMetrics:
    """Métricas de escrita de um destino individual.

    `rows_written` é contado no df **já transformado e projetado** de cada output,
    logo antes da escrita — reflete o que aquele destino realmente recebeu (e não o
    df principal), então é exato mesmo quando o output tem `transformations` que
    mudam o número de linhas.
    """

    format: str
    path: str
    mode: str
    rows_written: int


@dataclass
class PipelineResult:
    pipeline_name: str
    success: bool
    rows_read: int = 0
    rows_written: int = 0  # soma das linhas escritas em todos os destinos
    validation_results: List[ValidationResult] = field(default_factory=list)
    output_metrics: List[OutputMetrics] = field(default_factory=list)  # uma entrada por destino
    error: Optional[str] = None
    output_df: Optional[DataFrame] = None  # df após transformações; disponível quando input_df é injetado
    skipped: bool = False  # True quando o pipeline foi encerrado por stop_if_empty (sem dados)

    def summary(self) -> str:
        if not self.success:
            return f"[FAIL] '{self.pipeline_name}': {self.error}"
        if self.skipped:
            return f"[SKIP] '{self.pipeline_name}': sem dados a processar"
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

        rows_read = 0
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

            # O engine é reusado entre execuções no Sparquet; zera o store
            # de runtime para não vazar variáveis coletadas de um run anterior.
            self._transform_engine.reset_runtime()
            df = self._transform_engine.apply(df, self.config.transformations)
            log.info("Transformacoes aplicadas")

            validation_results = self._validation_engine.validate(
                df, self.config.validations
            )
            self._write_validation_report(spark, validation_results, log)

            output_metrics = self._write_outputs(spark, df, log)
            rows_written = sum(m.rows_written for m in output_metrics)
            flush_deferred_warnings(log)
            log.info("Pipeline concluido", linhas_escritas=rows_written)

            return PipelineResult(
                pipeline_name=self.config.name,
                success=True,
                rows_read=rows_read,
                rows_written=rows_written,
                validation_results=validation_results,
                output_metrics=output_metrics,
                output_df=df,
            )

        except PipelineStop as stop:
            flush_deferred_warnings(log)
            log.info("Pipeline encerrado sem processamento", motivo=str(stop))
            return PipelineResult(
                pipeline_name=self.config.name,
                success=True,
                rows_read=rows_read,
                rows_written=0,
                skipped=True,
            )

        except Exception as exc:
            flush_deferred_warnings(log)
            log.error("Pipeline falhou", error=str(exc))
            return PipelineResult(
                pipeline_name=self.config.name,
                success=False,
                error=str(exc),
            )

    def _write_validation_report(
        self, spark: SparkSession, results: List[ValidationResult], log
    ) -> None:
        """Grava o resultado das validações no destino de `validations.report`,
        se configurado — uma linha por regra, para análise de qualidade.

        Observação: em `on_failure="fail"` com violações, o ValidationEngine
        interrompe antes daqui, então o relatório é gerado nos modos que não
        abortam (`warn`/`skip`) ou quando todas as regras passam.
        """
        report = self.config.validations.report
        if report is None:
            return

        from pyspark.sql.types import (
            BooleanType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        schema = StructType([
            StructField("pipeline", StringType()),
            StructField("rule_type", StringType()),
            StructField("passed", BooleanType()),
            StructField("failed_count", LongType()),
            StructField("message", StringType()),
        ])
        rows = [
            (self.config.name, r.rule_type, r.passed, int(r.failed_count), r.message)
            for r in results
        ]
        report_df = spark.createDataFrame(rows, schema).withColumn(
            "validated_at", F.current_timestamp()
        )
        log.info(
            "Escrevendo relatorio de validacoes",
            formato=report.format,
            path=report.path,
            regras=len(results),
        )
        WriterFactory.create(spark, report).write(report_df)

    def _write_outputs(
        self, spark: SparkSession, df: DataFrame, log
    ) -> List[OutputMetrics]:
        metrics: List[OutputMetrics] = []
        for output in self.config.outputs:
            # Transformações próprias do destino (ex: explode, to_json, join),
            # aplicadas sobre o df principal sem afetar as demais saídas.
            output_df = df
            if output.transformations:
                output_df = self._transform_engine.apply(
                    output_df, output.transformations
                )
            output_df = self._project_columns(output_df, output)
            # Conta o df final deste destino ANTES de escrever — reflete exatamente
            # o que é gravado aqui, mesmo com transformações de output que mudam linhas.
            rows_written = output_df.count()
            log.info(
                "Escrevendo output",
                formato=output.format,
                path=output.path,
                modo=output.mode,
                colunas=output.columns or "todas",
                transformacoes=len(output.transformations),
                linhas=rows_written,
            )
            WriterFactory.create(spark, output).write(output_df)
            metrics.append(
                OutputMetrics(
                    format=output.format,
                    path=output.path,
                    mode=output.mode,
                    rows_written=rows_written,
                )
            )
        return metrics

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
