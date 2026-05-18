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
from spark_framework.utils.logger import logger


@dataclass
class PipelineResult:
    pipeline_name: str
    success: bool
    rows_read: int = 0
    rows_written: int = 0
    validation_results: List[ValidationResult] = field(default_factory=list)
    error: Optional[str] = None

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


def _is_empty_param(value: Any) -> bool:
    """Determina se um parâmetro de runtime deve ser tratado como vazio.

    Usado por `skip_if_null` nas transformações e pela injeção de colunas
    (valores vazios não viram coluna literal — a transformação dependente skipa).
    """
    if value is None:
        return True
    if isinstance(value, (list, tuple, dict, str)) and len(value) == 0:
        return True
    return False


def _inject_column(df: DataFrame, name: str, value: Any) -> DataFrame:
    """Injeta um valor de runtime como coluna literal.

    Escalares → F.lit(value). Listas/tuplas → F.array(F.lit(...), ...).
    """
    if isinstance(value, (list, tuple)):
        return df.withColumn(name, F.array(*[F.lit(v) for v in value]))
    return df.withColumn(name, F.lit(value))


class Pipeline:
    """Orquestra o fluxo: leitura → transformação → validação → escrita."""

    def __init__(
        self,
        config: PipelineConfig,
        transform_engine: Optional[TransformationEngine] = None,
        validation_engine: Optional[ValidationEngine] = None,
        columns: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.config = config
        self._transform_engine = transform_engine or TransformationEngine()
        self._validation_engine = validation_engine or ValidationEngine()
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

            # Validação declarativa de params: warning quando faltam params
            # declarados no campo "params" da conf
            if self.config.params:
                missing = [
                    p for p in self.config.params
                    if p not in self._columns or _is_empty_param(self._columns.get(p))
                ]
                if missing:
                    log.warning("Params declarados sem valor", faltando=missing)

            df = ReaderFactory.create(spark, self.config.input).read()
            df = df.withColumn("ingestion_ts", F.current_timestamp())
            rows_read = df.count()
            log.info(
                "Leitura concluida",
                linhas=rows_read,
                formato=self.config.input.format,
            )

            injected: List[str] = []
            for col_name, value in self._columns.items():
                if _is_empty_param(value):
                    continue  # transformações com skip_if_null cuidam disso
                df = _inject_column(df, col_name, value)
                injected.append(col_name)
            if injected:
                log.info("Colunas injetadas", colunas=injected)

            df = self._transform_engine.apply(
                df, self.config.transformations, columns=self._columns
            )
            log.info("Transformacoes aplicadas")

            validation_results = self._validation_engine.validate(
                df, self.config.validations
            )

            rows_written = df.count()
            self._write_outputs(spark, df, log)
            log.info("Pipeline concluido", linhas_escritas=rows_written)

            return PipelineResult(
                pipeline_name=self.config.name,
                success=True,
                rows_read=rows_read,
                rows_written=rows_written,
                validation_results=validation_results,
            )

        except Exception as exc:
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
            output_df = df
            if output.transformations:
                # Aplica transformações específicas do output (não afeta os demais)
                # — mesmo dicionário de columns vale aqui (skip_if_null funciona).
                output_df = self._transform_engine.apply(
                    output_df, output.transformations, columns=self._columns
                )
            output_df = self._project_columns(output_df, output)
            log.info(
                "Escrevendo output",
                formato=output.format,
                path=output.path,
                modo=output.mode,
                colunas=output.columns or "todas",
                transformacoes=len(output.transformations),
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
