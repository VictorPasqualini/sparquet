from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sparquet.core.config import OutputConfig, PipelineConfig
from sparquet.core.context import SparkContextManager
from sparquet.core.write_metrics import WriteMetrics
from sparquet.observability import history
from sparquet.io.factory import ReaderFactory, WriterFactory
from sparquet.transform.base import PipelineStop
from sparquet.transform.engine import TransformationEngine
from sparquet.validation.base import ValidationResult
from sparquet.validation.engine import ValidationEngine
from sparquet.utils.logger import flush_deferred_warnings, logger


@dataclass
class OutputMetrics:
    """Métricas de escrita de um destino individual.

    `rows_written` é o que **aquele destino** recebeu (e não o df principal), então é
    exato mesmo quando o output tem `transformations` próprias que mudam o número de
    linhas.

    O número vem do job da escrita, que já conta as linhas enquanto grava
    (`rows_from="write_metrics"`, ver `sparquet.core.write_metrics`). Quando o
    formato não publica essa métrica — JDBC, por exemplo — cai para um `count()`
    explícito no df projetado, antes da escrita (`rows_from="count"`). Os dois
    caminhos dão o mesmo número; o primeiro não custa uma action a mais.
    """

    format: str
    path: str
    mode: str
    rows_written: int
    rows_from: str = "write_metrics"


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
            return f"[SKIP] '{self.pipeline_name}': no data to process"
        passed = sum(1 for r in self.validation_results if r.passed)
        total = len(self.validation_results)
        return (
            f"[OK] '{self.pipeline_name}' | "
            f"read={self.rows_read} | "
            f"written={self.rows_written} | "
            f"validations={passed}/{total}"
        )


def _rule_target(params: Dict[str, Any]) -> str:
    """Coluna(s) que a regra checou, como texto — `column`, `columns`, ou vazio.

    Um relatório que não diz o alvo obriga a reler o JSON do pipeline para
    interpretar a linha.
    """
    if "columns" in params:
        value = params["columns"]
        return ", ".join(str(v) for v in value) if isinstance(value, list) else str(value)
    if "column" in params:
        return str(params["column"])
    return ""


def _rule_params_json(params: Dict[str, Any]) -> str:
    """Parâmetros da regra em JSON, para o relatório registrar o que foi afirmado
    (min/max, pattern, metric, must_be…). `output`/`failed_rows` ficam de fora: são
    destino e query, não critério."""
    import json as _json

    slim = {
        k: v
        for k, v in params.items()
        if k not in ("output", "column", "columns", "name") and not isinstance(v, (dict, list))
        or k in ("valid_values", "invalid_values")
    }
    try:
        return _json.dumps(slim, ensure_ascii=False, default=str, sort_keys=True)
    except (TypeError, ValueError):
        return ""


class Pipeline:
    """Orquestra o fluxo: leitura → transformação → validação → escrita."""

    def __init__(
        self,
        config: PipelineConfig,
        transform_engine: Optional[TransformationEngine] = None,
        validation_engine: Optional[ValidationEngine] = None,
        input_df: Optional[DataFrame] = None,
        columns: Optional[Dict[str, Any]] = None,
        input_view: Optional[Union[str, Dict[str, Any]]] = None,
    ) -> None:
        self.config = config
        self._transform_engine = transform_engine or TransformationEngine()
        self._validation_engine = validation_engine or ValidationEngine()
        self._input_df = input_df
        self._columns: Dict[str, Any] = columns or {}
        # input_view aceita uma string (nome, escopo "session") ou um dict
        # {"name": ..., "type": "session"|"global"}. Normaliza para nome + escopo.
        if isinstance(input_view, dict):
            self._input_view = input_view.get("name")
            self._input_view_scope = input_view.get("type", "session")
        else:
            self._input_view = input_view
            self._input_view_scope = "session"

    @classmethod
    def from_file(cls, path: str) -> Pipeline:
        return cls(PipelineConfig.from_file(path))

    @classmethod
    def from_dict(cls, data: dict) -> Pipeline:
        return cls(PipelineConfig.from_dict(data))

    def run(self) -> PipelineResult:
        """Executa o pipeline e devolve o resultado. Nunca levanta exceção.

        Quando há histórico externo configurado (ver `sparquet.observability`), a
        execução é recolhida e publicada aqui, depois de o resultado estar pronto —
        inclusive quando ela falha, que é justamente o caso que interessa observar.
        Sem histórico configurado, `recorder_for` devolve `None` e este método é o
        `_execute` de sempre.
        """
        recorder = history.recorder_for(self.config.name)
        if recorder is None:
            return self._execute()
        with recorder:
            result = self._execute()
        recorder.publish(result, datetime.now(timezone.utc).isoformat())
        return result

    def _execute(self) -> PipelineResult:
        log = logger.bind(pipeline=self.config.name)
        log.info("Pipeline started")

        rows_read = 0
        try:
            spark = SparkContextManager.get_or_create(self.config.spark)

            if self._input_df is not None:
                df = self._input_df
                rows_read = 0
                log.info("Input DataFrame injected externally", columns=len(df.columns))
            else:
                # Marcadores de etapa (scope="input") para o status do nó de origem.
                log.info(
                    "Input started",
                    scope="input", index=0, total=1, step=True,
                    format=self.config.input.format, path=self.config.input.path,
                )
                df = ReaderFactory.create(spark, self.config.input).read()
                df = df.withColumn("ingestion_ts", F.current_timestamp())
                rows_read = df.count()
                log.info(
                    "Input read",
                    rows=rows_read,
                    format=self.config.input.format,
                    scope="input", index=0, total=1, step=True,
                )

            for col_name, value in self._columns.items():
                df = df.withColumn(col_name, F.lit(value))
            if self._columns:
                log.info("Columns injected", columns=list(self._columns))

            # Registra (e cacheia) a entrada como temp view, se pedido — permite
            # self-join / SQL sobre a entrada sem reler a base (o cache evita
            # recomputar a linhagem da fonte a cada leitura da view).
            if self._input_view:
                df.cache()
                if self._input_view_scope == "global":
                    df.createOrReplaceGlobalTempView(self._input_view)
                else:
                    df.createOrReplaceTempView(self._input_view)
                log.info(
                    "Input registered as a temp view",
                    view=self._input_view,
                    scope=self._input_view_scope,
                )

            # O engine é reusado entre execuções no Sparquet; zera o store
            # de runtime para não vazar variáveis coletadas de um run anterior.
            self._transform_engine.reset_runtime()
            df = self._transform_engine.apply(
                df, self.config.transformations, top_level=True
            )
            log.info("Transformations applied")

            # Materializa o df antes de validar. Só quando o JSON pede: as regras
            # agregáveis são medidas numa passada só (ver `ValidationEngine`), então
            # o cache deixou de trocar N passes por um — ele troca uma releitura por
            # uma materialização, que na medição saiu mais cara. `cached` continua
            # atrelado a haver regras: sem elas não há nada a reaproveitar.
            cached = bool(self.config.validations.rules) and self.config.validations.cache
            if cached:
                df.cache()

            validation_results = self._validation_engine.validate(
                df, self.config.validations
            )
            self._write_validation_report(spark, validation_results, log, rows_read)
            self._write_validation_outputs(spark, df, validation_results, log)

            output_metrics = self._write_outputs(spark, df, log)
            rows_written = sum(m.rows_written for m in output_metrics)
            if cached:
                # A sessão é reusada entre execuções no Sparquet: um df cacheado e
                # nunca liberado ocuparia memória até o fim do processo.
                df.unpersist()
            flush_deferred_warnings(log)
            log.info("Pipeline finished", rows_written=rows_written)

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
            log.info("Pipeline stopped without processing", reason=str(stop))
            return PipelineResult(
                pipeline_name=self.config.name,
                success=True,
                rows_read=rows_read,
                rows_written=0,
                skipped=True,
            )

        except Exception as exc:
            flush_deferred_warnings(log)
            log.error("Pipeline failed", error=str(exc))
            return PipelineResult(
                pipeline_name=self.config.name,
                success=False,
                error=str(exc),
            )

    def _write_validation_report(
        self,
        spark: SparkSession,
        results: List[ValidationResult],
        log,
        rows_read: int = 0,
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
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        schema = StructType([
            StructField("pipeline", StringType()),
            StructField("rule_type", StringType()),
            StructField("check_name", StringType()),
            # Colunas alvo e parâmetros da regra: sem eles o relatório diz que um
            # `range` falhou, mas não em qual coluna nem contra quais limites — a
            # informação existia só no texto livre de `message`, e apenas quando
            # falhava. Estruturados aqui, dão para filtrar e agrupar.
            StructField("target", StringType()),
            StructField("rule_params", StringType()),
            StructField("severity", StringType()),
            StructField("passed", BooleanType()),
            StructField("failed_count", LongType()),
            # Denominador de `failed_count`. É a contagem da LEITURA, não do df
            # validado: contar o df validado custaria uma action extra a cada run.
            # Se houver `filter` antes das validações, os dois diferem — por isso o
            # nome é explícito e nenhum percentual é derivado aqui.
            StructField("rows_read", LongType()),
            StructField("metric_value", DoubleType()),
            StructField("message", StringType()),
        ])
        configured = self.config.validations.rules
        rows = [
            (
                self.config.name,
                r.rule_type,
                r.check_name,
                _rule_target(configured[i].params if i < len(configured) else {}),
                _rule_params_json(configured[i].params if i < len(configured) else {}),
                r.severity,
                r.passed,
                int(r.failed_count),
                int(rows_read),
                float(r.metric_value) if r.metric_value is not None else None,
                r.message,
            )
            for i, r in enumerate(results)
        ]
        # coalesce(1): o relatório tem UMA linha por regra — dezenas, no máximo. Com o
        # paralelismo default o Spark abre um part file por partição, quase todos com
        # apenas o cabeçalho, mais `_SUCCESS` e `.crc`. Um arquivo é o que se lê.
        report_df = (
            _rows_df(spark, rows, schema)
            .withColumn("validated_at", F.current_timestamp())
            .coalesce(1)
        )
        log.info(
            "Writing validation report",
            format=report.format,
            path=report.path,
            regras=len(results),
        )
        # Marcadores de etapa das saidas de validacao. Ao contrario de
        # input/transformation/output, a chave aqui e o PAPEL (`role`) e nao um
        # `index`: no Studio esses destinos sao declaracoes soltas, sem ligacao e
        # sem posicao numa lista, logo um indice nao teria a que se referir.
        # Diferente das transformacoes (lazy), o par started/finished delimita uma
        # escrita real — o tempo entre eles e trabalho de verdade.
        log.info(
            "Validation output started",
            scope="validation_sink", role="report", step=True,
            format=report.format, path=report.path,
        )
        WriterFactory.create(spark, report).write(report_df)
        log.info(
            "Validation output written",
            scope="validation_sink", role="report", step=True,
            format=report.format, path=report.path, rows=len(results),
        )

    def _write_validation_outputs(
        self, spark: SparkSession, df: DataFrame, results, log
    ) -> None:
        """Roteia LINHAS a partir das validações — apartado das saídas principais.

        1. `validations.outputs.{valid,invalid}` — split de quarentena por linha
           (bronze → silver_ok / silver_quarentena), baseado nos checks row-level.
           Cada lado pode ser **escopado** a algumas regras (`rules`, por código) e o
           `invalid` pode ser **rotulado** (`annotate`) com os códigos que rejeitaram
           cada linha.
        2. Destino próprio de um check `sql` com `failed_rows` (`rule.output`) — grava
           exatamente as linhas que a query marcou como ruins.

        Em `on_failure="fail"` com violação, o engine aborta antes daqui (nada é escrito).
        """
        validations = self.config.validations

        if validations.outputs:
            declared = self._validation_engine.codes(validations)
            # Um split por ESCOPO, não por destino: sem `rules`/`annotate` diferentes,
            # `valid` e `invalid` saem do mesmo split (uma passada, como antes).
            splits: Dict[Tuple[Optional[Tuple[str, ...]], str], Any] = {}
            for key, output in validations.outputs.items():
                if key not in ("valid", "invalid"):
                    log.warning(
                        "validations.outputs: unknown key (use 'valid'/'invalid')",
                        key=key,
                    )
                    continue
                only = output.rules
                # A coluna de códigos existe só onde há linha rejeitada (a config já
                # recusa `annotate` em qualquer outro destino).
                annotate = output.annotate if key == "invalid" else None
                self._check_quarantine_scope(output, declared, key, log)
                # None (sem escopo) e [] (escopo vazio) são splits DIFERENTES, então
                # não podem cair na mesma entrada do cache.
                scope = (None if only is None else tuple(only), annotate or "")
                split = splits.get(scope)
                if split is None:
                    split = self._validation_engine.split(
                        df, validations, annotate=annotate, only=only
                    )
                    splits[scope] = split
                target = split.valid if key == "valid" else split.invalid
                projected = self._project_columns(target, output)
                log.info(
                    "Writing validation quarantine",
                    tipo=key,
                    format=output.format,
                    path=output.path,
                )
                # `role` = "valid" | "invalid" (chaves desconhecidas ja sairam no
                # `continue` acima). Ver o comentario em _write_validation_report
                # sobre por que a chave e o papel e nao um indice.
                log.info(
                    "Validation output started",
                    scope="validation_sink", role=key, step=True,
                    format=output.format, path=output.path,
                )
                WriterFactory.create(spark, output).write(projected)
                log.info(
                    "Validation output written",
                    scope="validation_sink", role=key, step=True,
                    format=output.format, path=output.path,
                )

        # Destino por check (ex.: sql failed_rows com output próprio).
        for rule, result in zip(validations.rules, results):
            raw_output = rule.params.get("output")
            failed = getattr(result, "failed_rows", None)
            if raw_output and failed is not None:
                out_cfg = OutputConfig.from_dict(raw_output)
                projected = self._project_columns(failed, out_cfg)
                log.info(
                    "Writing failed rows",
                    rule=result.rule_type,
                    path=out_cfg.path,
                )
                WriterFactory.create(spark, out_cfg).write(projected)

    @staticmethod
    def _check_quarantine_scope(
        output: OutputConfig, declared: List[str], role: str, log
    ) -> None:
        """Avisa sobre um escopo/rótulo de quarentena que não vai chegar aos dados.

        Nada aqui aborta: um código com typo escreveria a quarentena vazia sem dizer
        por quê, e uma projeção que não pede a coluna de códigos gravaria a linha sem o
        motivo dela — os dois casos são silenciosos, então ganham um aviso.
        """
        for code in output.rules or []:
            if code in declared:
                continue
            log.warning(
                "validations.outputs: scoped to a code no rule declares",
                role=role, code=code, declared=declared,
            )
        if output.annotate and output.columns and output.annotate not in output.columns:
            log.warning(
                "validations.outputs.invalid: the column projection drops the "
                "annotate column",
                role=role, annotate=output.annotate, columns=output.columns,
            )

    def _write_outputs(
        self, spark: SparkSession, df: DataFrame, log
    ) -> List[OutputMetrics]:
        metrics: List[OutputMetrics] = []
        total = len(self.config.outputs)
        # Lê o contador que a própria escrita apura, em vez de gastar um count()
        # por destino. Ver sparquet/core/write_metrics.py.
        write_metrics = WriteMetrics(spark)
        for index, output in enumerate(self.config.outputs):
            # Marcador de etapa (scope="output") para o Studio pintar o status do
            # nó de destino ao vivo. Ver TransformationEngine.apply(top_level=True).
            log.info(
                "Output started",
                scope="output", index=index, total=total, step=True,
                format=output.format, path=output.path,
            )
            # Transformações próprias do destino (ex: explode, to_json, join),
            # aplicadas sobre o df principal sem afetar as demais saídas.
            output_df = df
            if output.transformations:
                output_df = self._transform_engine.apply(
                    output_df, output.transformations
                )
            output_df = self._project_columns(output_df, output)
            log.info(
                "Writing output",
                format=output.format,
                path=output.path,
                mode=output.mode,
                columns=output.columns or "todas",
                transformations=len(output.transformations),
            )
            writer = WriterFactory.create(spark, output)
            rows_written = write_metrics.measure(lambda: writer.write(output_df))
            rows_from = "write_metrics"
            if rows_written is None:
                # O formato não publicou métrica de escrita. Conta explicitamente:
                # um número errado aqui contaminaria relatório, histórico e cobrança,
                # e o df já está materializado pela escrita que acabou de rodar.
                rows_written = output_df.count()
                rows_from = "count"
            log.info(
                "Output written",
                scope="output", index=index, total=total, step=True,
                format=output.format, path=output.path, rows=rows_written,
                rows_from=rows_from,
            )
            metrics.append(
                OutputMetrics(
                    format=output.format,
                    path=output.path,
                    mode=output.mode,
                    rows_written=rows_written,
                    rows_from=rows_from,
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


def _rows_df(spark: SparkSession, rows: List[tuple], schema) -> DataFrame:
    """DataFrame das linhas montadas no driver, **sem abrir worker Python**.

    `spark.createDataFrame(lista)` paraleliza a lista em `defaultParallelism`
    partições e cada tarefa desserializa a sua fatia num worker Python — 16
    processos para as poucas dezenas de linhas do relatório de validação, pagos
    de novo a cada action. Em master local no Windows isso custou ~20s por
    action (medido: 21,6s só para contar 5 linhas, e outro tanto para gravá-las).
    Montadas como literais, as linhas viram um `explode` de um array de structs:
    o plano inteiro fica na JVM, nenhum worker sobe, e o mesmo relatório sai em
    menos de 1s.

    É também o que tira do caminho o `Python worker exited unexpectedly` que
    aparecia aqui quando o worker subia com outro interpretador (ver
    `core/context.py`): esta escrita deixa de criar worker.
    """
    if not rows:
        return spark.createDataFrame([], schema)
    linhas = F.array(*[
        F.struct(*[
            F.lit(valor).cast(campo.dataType).alias(campo.name)
            for valor, campo in zip(row, schema.fields)
        ])
        for row in rows
    ])
    return spark.range(1).select(F.explode(linhas).alias("_r")).select("_r.*")
