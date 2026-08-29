"""Histórico de execução para pipelines que rodam fora do Studio.

O problema
----------
O framework roda em qualquer lugar — `sparquet.cli`, Airflow, Databricks, EMR, um
`python job.py` numa VM — e não depende de nada para isso. O histórico, porém, só
existia quando o runner do Studio era quem executava: era ele que gravava as
execuções, as etapas e os logs. Ou seja, justamente em produção não havia
monitoramento nenhum.

A forma
-------
Um **sink opcional**, desligado por padrão. Sem `SPARQUET_HISTORY_URL` (e sem sink
registrado à mão) nada disto é instanciado e a execução não paga nada: nem um
import a mais no caminho quente, nem uma linha de log a mais.

Ligado, a execução é recolhida enquanto acontece — pelos registros estruturados
que o framework **já emite** (marcadores de etapa, contagens, erros; ver
`sparquet.utils.logger.add_sink`) — e enviada **uma vez, no fim**, como um único
documento JSON. Uma requisição por execução, não uma por etapa: é histórico, não
streaming ao vivo. Um job de 4 horas no Databricks aparece no Studio quando
termina, com todas as etapas e todos os logs.

O envio nunca derruba o pipeline. Falha de rede, runner fora do ar, token errado:
tudo vira um `warning` no log e o `PipelineResult` segue exatamente o mesmo.

Configuração (tudo por ambiente, para caber em qualquer orquestrador)
--------------------------------------------------------------------
| Variável | Efeito |
|---|---|
| `SPARQUET_HISTORY_URL` | Endpoint que recebe o documento. **Vazio = desligado.** |
| `SPARQUET_HISTORY_TOKEN` | Segredo enviado em `X-Sparquet-Token`. |
| `SPARQUET_HISTORY_TIMEOUT` | Segundos de espera pelo envio (padrão 10). |
| `SPARQUET_HISTORY_JOB_ID` | Job do Studio a que esta execução pertence. |
| `SPARQUET_HISTORY_WORKFLOW_ID` | Workflow do Studio. |
| `SPARQUET_HISTORY_PIPELINE_ID` | Pipeline do Studio, quando a execução é de uma etapa dele. |
| `SPARQUET_HISTORY_RUN_AS` | Quem/o quê executou (usuário, service account). |
| `SPARQUET_HISTORY_TAGS` | Tags separadas por vírgula, para agregar custo e uso. |
| `SPARQUET_HISTORY_LOGS` | `off` envia só etapas e totais, sem as linhas de log. |

Segurança
---------
O token é uma senha: quem o tem escreve no histórico de quem o recebe. Trate-o
como credencial (variável de ambiente ou cofre, nunca no JSON do pipeline nem no
repositório) e prefira `https://` — em `http://` ele viaja em claro.

O destino deste envio costuma ser o runner do Studio, que **executa Spark
arbitrário** e por isso escuta em `127.0.0.1` de propósito. Publicá-lo numa rede
para receber histórico expõe junto tudo o que ele sabe executar. Se precisar
receber execuções de outras máquinas, coloque um proxy reverso na frente que
aceite **apenas** a rota de ingestão, com TLS, e mantenha o runner fechado.
"""
from __future__ import annotations

import json
import os
import platform
import socket
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

from sparquet.utils.logger import add_sink, logger, remove_sink

#: Versão do formato do documento. O receptor rejeita o que não souber ler.
SCHEMA = "sparquet.run/1"

#: Teto de linhas de log por execução, igual ao que o runner já guarda. Um job que
#: imprime em laço não pode virar um POST de centenas de MB.
MAX_RECORDS = 3000

#: Chaves que já viram campo de primeira classe no registro; o resto vira contexto.
_RECORD_KEYS = ("timestamp", "level", "message")

_DEFAULT_TIMEOUT = 10.0


def _flag(value: Optional[str], fallback: bool) -> bool:
    if value is None or value.strip() == "":
        return fallback
    return value.strip().lower() in {"1", "on", "true", "yes"}


class HistorySink:
    """Para onde vai o documento de uma execução.

    Implementar é uma função só. Um sink que grave em arquivo, publique numa fila
    ou chame outra API entra sem tocar no framework.
    """

    def send(self, document: Dict[str, Any]) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class HttpHistorySink(HistorySink):
    """POST do documento, com timeout e sem levantar exceção.

    Usa `urllib` da biblioteca padrão de propósito: observabilidade opcional não
    justifica uma dependência nova num framework que roda em cluster alheio.
    """

    def __init__(
        self,
        url: str,
        token: Optional[str] = None,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> None:
        self.url = url
        self._token = token
        self._timeout = timeout

    def send(self, document: Dict[str, Any]) -> None:
        body = json.dumps(document, default=str).encode("utf-8")
        request = urllib.request.Request(self.url, data=body, method="POST")
        request.add_header("content-type", "application/json")
        if self._token:
            request.add_header("X-Sparquet-Token", self._token)
        with urllib.request.urlopen(request, timeout=self._timeout) as response:
            response.read()


def sink_from_env(env: Optional[Dict[str, str]] = None) -> Optional[HistorySink]:
    """O sink que o ambiente pede, ou `None` — que é o padrão."""
    env = os.environ if env is None else env
    url = (env.get("SPARQUET_HISTORY_URL") or "").strip()
    if not url:
        return None
    try:
        timeout = float(env.get("SPARQUET_HISTORY_TIMEOUT") or _DEFAULT_TIMEOUT)
    except ValueError:
        timeout = _DEFAULT_TIMEOUT
    return HttpHistorySink(url, env.get("SPARQUET_HISTORY_TOKEN") or None, timeout)


def identity_from_env(env: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """A que Job/Workflow/Pipeline do Studio esta execução pertence.

    Sem isso a execução ainda é gravada, mas fica solta: não entra na série
    histórica de nenhum Job nem no rateio de nenhum Workflow.
    """
    env = os.environ if env is None else env
    tags = [t.strip() for t in (env.get("SPARQUET_HISTORY_TAGS") or "").split(",")]
    return {
        "workflow_id": env.get("SPARQUET_HISTORY_WORKFLOW_ID") or None,
        "job_id": env.get("SPARQUET_HISTORY_JOB_ID") or None,
        "pipeline_id": env.get("SPARQUET_HISTORY_PIPELINE_ID") or None,
        "run_as": env.get("SPARQUET_HISTORY_RUN_AS") or None,
        "tags": [t for t in tags if t],
        "host": _host(),
    }


def _host() -> Optional[str]:
    try:
        return socket.gethostname()
    except Exception:  # pragma: no cover - defensivo
        return None


class RunRecorder:
    """Recolhe uma execução e a entrega aos sinks.

    Enquanto está ativo, assina o logger e guarda cada registro na forma
    `{"timestamp", "level", "message", "context"}` — a mesma que o runner do
    Studio já sabe transformar em etapas e linhas de log, para que uma execução
    externa e uma execução no Studio virem exatamente o mesmo registro.

    Só guarda o que pertence a esta execução: os registros trazem `pipeline` no
    contexto, e dois pipelines rodando na mesma JVM não podem se misturar.
    """

    def __init__(
        self,
        pipeline_name: str,
        sinks: List[HistorySink],
        *,
        include_logs: bool = True,
        identity: Optional[Dict[str, Any]] = None,
        max_records: int = MAX_RECORDS,
    ) -> None:
        self.pipeline_name = pipeline_name
        self._sinks = sinks
        self._include_logs = include_logs
        self._identity = identity if identity is not None else identity_from_env()
        self._max_records = max_records
        self.records: List[Dict[str, Any]] = []
        self.dropped = 0
        self.started_at: Optional[str] = None

    # ------------------------------------------------------------- assinatura

    def __enter__(self) -> "RunRecorder":
        add_sink(self._collect)
        return self

    def __exit__(self, *_exc: Any) -> None:
        remove_sink(self._collect)

    def _collect(self, record: Dict[str, Any]) -> None:
        if record.get("pipeline") != self.pipeline_name:
            return
        context = {k: v for k, v in record.items() if k not in _RECORD_KEYS}
        if self.started_at is None:
            self.started_at = str(record.get("timestamp"))
        # Um marcador de etapa vale mais que uma linha de log: com os logs
        # desligados, ou com o teto estourado, as etapas continuam entrando.
        is_step = bool(context.get("step"))
        if not is_step and not self._include_logs:
            return
        if len(self.records) >= self._max_records and not is_step:
            self.dropped += 1
            return
        self.records.append(
            {
                "timestamp": record.get("timestamp"),
                "level": record.get("level"),
                "message": record.get("message"),
                "context": context,
            }
        )

    # ------------------------------------------------------------- publicação

    def document(self, result: Any, finished_at: str) -> Dict[str, Any]:
        from sparquet import __version__

        status = "failed"
        if getattr(result, "success", False):
            status = "skipped" if getattr(result, "skipped", False) else "success"

        return {
            "schema": SCHEMA,
            "sparquet_version": __version__,
            "python": platform.python_version(),
            "run": {
                "name": self.pipeline_name,
                "status": status,
                "started_at": self.started_at,
                "finished_at": finished_at,
                "error": getattr(result, "error", None),
                "rows_read": getattr(result, "rows_read", 0),
                "rows_written": getattr(result, "rows_written", 0),
                "launched": "external",
                **self._identity,
            },
            "outputs": [
                {
                    "format": metric.format,
                    "path": metric.path,
                    "mode": metric.mode,
                    "rows_written": metric.rows_written,
                    "rows_from": getattr(metric, "rows_from", None),
                }
                for metric in getattr(result, "output_metrics", []) or []
            ],
            "validations": [
                {
                    "rule_type": item.rule_type,
                    "passed": item.passed,
                    "message": item.message,
                    "failed_count": getattr(item, "failed_count", None),
                    "severity": getattr(item, "severity", None),
                }
                for item in getattr(result, "validation_results", []) or []
            ],
            "records": self.records,
            "records_dropped": self.dropped,
        }

    def publish(self, result: Any, finished_at: str) -> None:
        """Entrega o documento a cada sink. Nenhuma falha aqui chega ao chamador."""
        document = self.document(result, finished_at)
        for sink in self._sinks:
            try:
                sink.send(document)
            except urllib.error.HTTPError as exc:
                logger.warning(
                    "Execution history was refused",
                    status=exc.code, reason=str(exc.reason),
                )
            except Exception as exc:
                logger.warning("Execution history could not be sent", error=str(exc))


# --------------------------------------------------------------------- registro
#
# Class-level como os demais registries do framework: um sink registrado vale para
# todas as execuções do processo.

_registered: List[HistorySink] = []
_env_sink_resolved = False
_env_sink: Optional[HistorySink] = None


def register_sink(sink: HistorySink) -> None:
    """Passa a receber o documento de toda execução deste processo."""
    _registered.append(sink)


def clear_sinks() -> None:
    """Esquece os sinks registrados e a leitura do ambiente. Usado em teste."""
    global _env_sink_resolved, _env_sink
    _registered.clear()
    _env_sink_resolved = False
    _env_sink = None


def active_sinks() -> List[HistorySink]:
    """Os sinks em vigor: os registrados mais o que o ambiente pediu.

    O ambiente é lido uma vez só. Mudar `SPARQUET_HISTORY_URL` no meio do processo
    não reconfigura nada — o que vale é o ambiente de quando a primeira execução
    começou.
    """
    global _env_sink_resolved, _env_sink
    if not _env_sink_resolved:
        _env_sink = sink_from_env()
        _env_sink_resolved = True
    return [*_registered, *([_env_sink] if _env_sink else [])]


def recorder_for(pipeline_name: str) -> Optional[RunRecorder]:
    """Um recorder, ou `None` quando ninguém está ouvindo.

    `None` é o caminho normal: sem sink configurado, `Pipeline.run` não paga nada.
    """
    sinks = active_sinks()
    if not sinks:
        return None
    return RunRecorder(
        pipeline_name,
        sinks,
        include_logs=_flag(os.environ.get("SPARQUET_HISTORY_LOGS"), True),
    )
