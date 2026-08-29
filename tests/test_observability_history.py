"""Histórico de execução para quem roda o sparquet fora do Studio.

O que estes testes protegem, em ordem de gravidade:

1. **Desligado por padrão.** Sem `SPARQUET_HISTORY_URL` e sem sink registrado,
   nada é instanciado e `Pipeline.run` não muda de comportamento.
2. **Observar não derruba.** Sink que explode, rede caída, HTTP 401: o
   `PipelineResult` sai igual ao que sairia sem histórico nenhum.
3. **Uma execução não contamina outra.** Dois pipelines na mesma JVM, cada um com
   seus registros.
4. **A falha é justamente o que interessa gravar** — o documento sai também quando
   o pipeline falha, com o erro dentro.

Não precisa de Spark: o que está sob teste é a coleta e o envio, não o motor.

    PYTHONPATH=. python tests/test_observability_history.py
"""
from __future__ import annotations

import json
import os
import sys
import unittest
import urllib.error
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sparquet.observability import history  # noqa: E402
from sparquet.observability.history import (  # noqa: E402
    HistorySink,
    HttpHistorySink,
    RunRecorder,
    identity_from_env,
    sink_from_env,
)
from sparquet.utils.logger import logger  # noqa: E402


# --------------------------------------------------------------------- dublês


class CaptureSink(HistorySink):
    """Guarda o que receberia a rede."""

    def __init__(self) -> None:
        self.documents: List[Dict[str, Any]] = []

    def send(self, document: Dict[str, Any]) -> None:
        self.documents.append(document)


class ExplodingSink(HistorySink):
    def send(self, document: Dict[str, Any]) -> None:
        raise RuntimeError("runner fora do ar")


@dataclass
class FakeMetric:
    format: str = "parquet"
    path: str = "/tmp/out"
    mode: str = "overwrite"
    rows_written: int = 10
    rows_from: str = "write_metrics"


@dataclass
class FakeValidation:
    rule_type: str = "not_null"
    passed: bool = False
    message: str = "3 nulos em id"
    failed_count: int = 3
    severity: str = "error"


@dataclass
class FakeResult:
    success: bool = True
    skipped: bool = False
    error: Optional[str] = None
    rows_read: int = 100
    rows_written: int = 10
    output_metrics: List[FakeMetric] = field(default_factory=list)
    validation_results: List[FakeValidation] = field(default_factory=list)


def recorder(name: str = "vendas", **kwargs: Any) -> RunRecorder:
    kwargs.setdefault("identity", {})
    return RunRecorder(name, [CaptureSink()], **kwargs)


# --------------------------------------------------------------------- coleta


class TestRecorderCollects(unittest.TestCase):
    """O que entra no documento enquanto a execução acontece."""

    def test_separates_message_from_context(self):
        rec = recorder()
        with rec:
            logger.bind(pipeline="vendas").info("Input read", rows=42, scope="input")

        entry = rec.records[-1]
        self.assertEqual(entry["message"], "Input read")
        self.assertEqual(entry["level"], "INFO")
        self.assertEqual(entry["context"]["rows"], 42)
        self.assertEqual(entry["context"]["scope"], "input")
        # `pipeline` fica no contexto: é o que amarra o registro à execução.
        self.assertEqual(entry["context"]["pipeline"], "vendas")
        self.assertTrue(entry["timestamp"])

    def test_ignores_other_pipelines(self):
        """Duas execuções na mesma JVM não podem se misturar."""
        rec = recorder("vendas")
        with rec:
            logger.bind(pipeline="vendas").info("minha")
            logger.bind(pipeline="estoque").info("da outra")
            logger.info("sem pipeline nenhum")

        self.assertEqual([r["message"] for r in rec.records], ["minha"])

    def test_stops_collecting_after_exit(self):
        rec = recorder()
        with rec:
            logger.bind(pipeline="vendas").info("dentro")
        logger.bind(pipeline="vendas").info("fora")

        self.assertEqual([r["message"] for r in rec.records], ["dentro"])

    def test_started_at_is_the_first_record(self):
        rec = recorder()
        with rec:
            logger.bind(pipeline="vendas").info("primeira")
            logger.bind(pipeline="vendas").info("segunda")

        self.assertEqual(rec.started_at, rec.records[0]["timestamp"])

    def test_logs_off_keeps_step_markers(self):
        """`SPARQUET_HISTORY_LOGS=off`: sem as linhas, mas ainda com as etapas."""
        rec = recorder(include_logs=False)
        with rec:
            log = logger.bind(pipeline="vendas")
            log.info("linha solta")
            log.info("Output written", scope="output", index=0, step=True)

        self.assertEqual([r["message"] for r in rec.records], ["Output written"])

    def test_cap_drops_logs_but_never_steps(self):
        """Job que imprime em laço não vira um POST de centenas de MB."""
        rec = recorder(max_records=3)
        with rec:
            log = logger.bind(pipeline="vendas")
            for index in range(10):
                log.info(f"linha {index}")
            log.info("Output written", scope="output", index=0, step=True)

        self.assertEqual(len(rec.records), 4)
        self.assertEqual(rec.dropped, 7)
        self.assertEqual(rec.records[-1]["message"], "Output written")

    def test_a_broken_sink_never_reaches_the_logger(self):
        """Erro dentro do coletor não pode virar exceção em quem loga."""
        rec = recorder()
        with mock.patch.object(rec, "_collect", side_effect=RuntimeError("boom")):
            with rec:
                logger.bind(pipeline="vendas").info("segue vivo")


# ----------------------------------------------------------------- documento


class TestDocument(unittest.TestCase):
    def test_success(self):
        rec = recorder()
        doc = rec.document(FakeResult(), "2026-08-29T10:00:00+00:00")

        self.assertEqual(doc["schema"], "sparquet.run/1")
        self.assertEqual(doc["run"]["status"], "success")
        self.assertEqual(doc["run"]["name"], "vendas")
        self.assertEqual(doc["run"]["rows_read"], 100)
        self.assertEqual(doc["run"]["rows_written"], 10)
        self.assertEqual(doc["run"]["launched"], "external")
        self.assertIsNone(doc["run"]["error"])
        self.assertTrue(doc["sparquet_version"])

    def test_failure_carries_the_error(self):
        result = FakeResult(success=False, error="Path does not exist: /raw/x")
        doc = recorder().document(result, "2026-08-29T10:00:00+00:00")

        self.assertEqual(doc["run"]["status"], "failed")
        self.assertEqual(doc["run"]["error"], "Path does not exist: /raw/x")

    def test_skipped_is_not_failure(self):
        """`stop_if_empty` é encerramento gracioso, não erro."""
        doc = recorder().document(FakeResult(skipped=True), "2026-08-29T10:00:00+00:00")

        self.assertEqual(doc["run"]["status"], "skipped")

    def test_outputs_and_validations(self):
        result = FakeResult(
            output_metrics=[FakeMetric(path="/lake/vendas", rows_written=7)],
            validation_results=[FakeValidation()],
        )
        doc = recorder().document(result, "2026-08-29T10:00:00+00:00")

        self.assertEqual(doc["outputs"][0]["path"], "/lake/vendas")
        self.assertEqual(doc["outputs"][0]["rows_written"], 7)
        self.assertEqual(doc["outputs"][0]["rows_from"], "write_metrics")
        self.assertEqual(doc["validations"][0]["rule_type"], "not_null")
        self.assertFalse(doc["validations"][0]["passed"])
        self.assertEqual(doc["validations"][0]["failed_count"], 3)

    def test_identity_goes_into_the_run(self):
        rec = RunRecorder(
            "vendas", [CaptureSink()],
            identity={"job_id": "job-1", "tags": ["financeiro"]},
        )
        doc = rec.document(FakeResult(), "2026-08-29T10:00:00+00:00")

        self.assertEqual(doc["run"]["job_id"], "job-1")
        self.assertEqual(doc["run"]["tags"], ["financeiro"])

    def test_is_json_serializable(self):
        """O documento vai para a rede: nada dentro pode escapar do json."""
        rec = recorder()
        with rec:
            logger.bind(pipeline="vendas").info("Input read", rows=1)
        json.dumps(rec.document(FakeResult(), "2026-08-29T10:00:00+00:00"))


# ----------------------------------------------------------------- publicação


class TestPublish(unittest.TestCase):
    def test_reaches_every_sink(self):
        first, second = CaptureSink(), CaptureSink()
        rec = RunRecorder("vendas", [first, second], identity={})
        rec.publish(FakeResult(), "2026-08-29T10:00:00+00:00")

        self.assertEqual(len(first.documents), 1)
        self.assertEqual(len(second.documents), 1)

    def test_a_failing_sink_does_not_stop_the_others(self):
        good = CaptureSink()
        rec = RunRecorder("vendas", [ExplodingSink(), good], identity={})
        rec.publish(FakeResult(), "2026-08-29T10:00:00+00:00")

        self.assertEqual(len(good.documents), 1)

    def test_network_failure_is_a_warning_not_an_exception(self):
        rec = RunRecorder("vendas", [ExplodingSink()], identity={})
        with mock.patch.object(history.logger, "warning") as warning:
            rec.publish(FakeResult(), "2026-08-29T10:00:00+00:00")

        warning.assert_called_once()
        self.assertIn("runner fora do ar", str(warning.call_args))

    def test_rejected_by_the_receiver_is_a_warning_too(self):
        class Refusing(HistorySink):
            def send(self, document):
                raise urllib.error.HTTPError("http://x", 401, "Unauthorized", {}, None)

        rec = RunRecorder("vendas", [Refusing()], identity={})
        with mock.patch.object(history.logger, "warning") as warning:
            rec.publish(FakeResult(), "2026-08-29T10:00:00+00:00")

        self.assertEqual(warning.call_args.kwargs["status"], 401)


# ------------------------------------------------------------------- HTTP


class TestHttpSink(unittest.TestCase):
    def send(self, sink: HttpHistorySink, document: Dict[str, Any]):
        with mock.patch("urllib.request.urlopen") as urlopen:
            urlopen.return_value.__enter__.return_value.read.return_value = b"{}"
            sink.send(document)
        return urlopen.call_args

    def test_posts_json(self):
        call = self.send(HttpHistorySink("http://127.0.0.1:8765/runs/ingest"), {"a": 1})
        request = call.args[0]

        self.assertEqual(request.method, "POST")
        self.assertEqual(request.full_url, "http://127.0.0.1:8765/runs/ingest")
        self.assertEqual(json.loads(request.data), {"a": 1})
        self.assertEqual(request.get_header("Content-type"), "application/json")

    def test_sends_the_token(self):
        call = self.send(HttpHistorySink("http://x", token="s3cr3t"), {})

        self.assertEqual(call.args[0].get_header("X-sparquet-token"), "s3cr3t")

    def test_no_token_no_header(self):
        call = self.send(HttpHistorySink("http://x"), {})

        self.assertIsNone(call.args[0].get_header("X-sparquet-token"))

    def test_honours_the_timeout(self):
        """Histórico não pode segurar o processo de um job em batch."""
        call = self.send(HttpHistorySink("http://x", timeout=3.0), {})

        self.assertEqual(call.kwargs["timeout"], 3.0)


# ---------------------------------------------------------------- ambiente


class TestEnvironment(unittest.TestCase):
    def test_off_by_default(self):
        self.assertIsNone(sink_from_env({}))
        self.assertIsNone(sink_from_env({"SPARQUET_HISTORY_URL": "   "}))

    def test_url_turns_it_on(self):
        sink = sink_from_env({
            "SPARQUET_HISTORY_URL": "https://studio/runs/ingest",
            "SPARQUET_HISTORY_TOKEN": "t",
            "SPARQUET_HISTORY_TIMEOUT": "2.5",
        })

        self.assertIsInstance(sink, HttpHistorySink)
        self.assertEqual(sink.url, "https://studio/runs/ingest")
        self.assertEqual(sink._timeout, 2.5)

    def test_a_bad_timeout_does_not_break_the_run(self):
        sink = sink_from_env({
            "SPARQUET_HISTORY_URL": "https://studio",
            "SPARQUET_HISTORY_TIMEOUT": "depressa",
        })

        self.assertEqual(sink._timeout, 10.0)

    def test_identity(self):
        identity = identity_from_env({
            "SPARQUET_HISTORY_JOB_ID": "job-1",
            "SPARQUET_HISTORY_WORKFLOW_ID": "wf-1",
            "SPARQUET_HISTORY_RUN_AS": "airflow",
            "SPARQUET_HISTORY_TAGS": "financeiro, diario ,,",
        })

        self.assertEqual(identity["job_id"], "job-1")
        self.assertEqual(identity["workflow_id"], "wf-1")
        self.assertEqual(identity["run_as"], "airflow")
        self.assertEqual(identity["tags"], ["financeiro", "diario"])
        self.assertIsNone(identity["pipeline_id"])

    def test_empty_identity(self):
        identity = identity_from_env({})

        self.assertEqual(identity["tags"], [])
        self.assertIsNone(identity["job_id"])


# ------------------------------------------------------------------ registro


class TestRegistry(unittest.TestCase):
    def setUp(self):
        history.clear_sinks()
        self.addCleanup(history.clear_sinks)

    def test_no_sink_no_recorder(self):
        """O caminho normal: nada configurado, nada instanciado."""
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SPARQUET_HISTORY_URL", None)
            self.assertIsNone(history.recorder_for("vendas"))

    def test_registered_sink_creates_a_recorder(self):
        history.register_sink(CaptureSink())

        self.assertIsInstance(history.recorder_for("vendas"), RunRecorder)

    def test_environment_creates_a_recorder(self):
        with mock.patch.dict(os.environ, {"SPARQUET_HISTORY_URL": "http://x"}):
            self.assertIsInstance(history.recorder_for("vendas"), RunRecorder)

    def test_the_environment_is_read_once(self):
        with mock.patch.dict(os.environ, {"SPARQUET_HISTORY_URL": "http://x"}):
            history.active_sinks()
        # Já resolvido: tirar a variável depois não reconfigura no meio do processo.
        self.assertEqual(len(history.active_sinks()), 1)


# ------------------------------------------------------------------ pipeline


class TestPipelineWiring(unittest.TestCase):
    """A ponta que importa: `Pipeline.run` publica sem mudar o resultado."""

    def setUp(self):
        history.clear_sinks()
        self.addCleanup(history.clear_sinks)
        os.environ.pop("SPARQUET_HISTORY_URL", None)

    def pipeline(self, result: FakeResult):
        from sparquet.core.pipeline import Pipeline

        pipe = Pipeline.__new__(Pipeline)
        pipe.config = mock.Mock(name="config")
        pipe.config.name = "vendas"
        pipe._execute = mock.Mock(return_value=result)
        return pipe

    def test_publishes_the_run(self):
        sink = CaptureSink()
        history.register_sink(sink)
        result = FakeResult(rows_written=7)

        self.assertIs(self.pipeline(result).run(), result)
        self.assertEqual(sink.documents[0]["run"]["rows_written"], 7)
        self.assertTrue(sink.documents[0]["run"]["finished_at"])

    def test_publishes_failures_too(self):
        sink = CaptureSink()
        history.register_sink(sink)
        result = FakeResult(success=False, error="Path does not exist")

        self.pipeline(result).run()

        self.assertEqual(sink.documents[0]["run"]["status"], "failed")

    def test_a_dead_receiver_does_not_change_the_result(self):
        history.register_sink(ExplodingSink())
        result = FakeResult()

        self.assertIs(self.pipeline(result).run(), result)

    def test_without_sinks_nothing_is_recorded(self):
        pipe = self.pipeline(FakeResult())
        with mock.patch.object(history, "RunRecorder") as recorder_cls:
            pipe.run()

        recorder_cls.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
