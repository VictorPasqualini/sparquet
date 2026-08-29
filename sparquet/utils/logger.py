from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List


#: Assinantes que recebem cada registro emitido, já como dicionário.
#:
#: Existe para que a observabilidade fora do Studio não precise reimplementar a
#: instrumentação: os marcadores de etapa, as contagens e os erros já passam por
#: aqui. Ver `sparquet.observability`.
_sinks: List[Callable[[Dict[str, Any]], None]] = []


def add_sink(sink: Callable[[Dict[str, Any]], None]) -> None:
    """Passa a receber uma cópia de cada registro estruturado.

    O sink roda dentro da chamada de log, então tem de ser rápido e não pode
    levantar exceção — uma falha ali é engolida, porque observabilidade nunca
    derruba a execução que ela observa.
    """
    _sinks.append(sink)


def remove_sink(sink: Callable[[Dict[str, Any]], None]) -> None:
    """Cancela a assinatura. Ignora um sink que já não esteja registrado."""
    try:
        _sinks.remove(sink)
    except ValueError:
        pass


def _publish(record: Dict[str, Any]) -> None:
    for sink in list(_sinks):
        try:
            sink(record)
        except Exception:  # pragma: no cover - defensivo
            pass


class StructuredLogger:
    """Structured JSON logger with contextual binding."""

    def __init__(self, name: str, context: Dict[str, Any] | None = None):
        self._logger = logging.getLogger(name)
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(message)s"))
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.INFO)
        self._context: Dict[str, Any] = context or {}

    def bind(self, **kwargs: Any) -> StructuredLogger:
        return StructuredLogger(self._logger.name, {**self._context, **kwargs})

    def _build_record(self, level: str, message: str, **kwargs: Any) -> str:
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "message": message,
            **self._context,
            **kwargs,
        }
        _publish(record)
        return json.dumps(record, default=str)

    def info(self, message: str, **kwargs: Any) -> None:
        self._logger.info(self._build_record("INFO", message, **kwargs))

    def warning(self, message: str, **kwargs: Any) -> None:
        self._logger.warning(self._build_record("WARNING", message, **kwargs))

    def error(self, message: str, **kwargs: Any) -> None:
        self._logger.error(self._build_record("ERROR", message, **kwargs))

    def debug(self, message: str, **kwargs: Any) -> None:
        self._logger.debug(self._build_record("DEBUG", message, **kwargs))


logger = StructuredLogger("sparquet")


# ---------------------------------------------------------------------------
# Warnings adiados — coletados durante a execução e emitidos no fim do pipeline
# ---------------------------------------------------------------------------
_deferred_warnings: list[tuple[str, Dict[str, Any]]] = []


def defer_warning(message: str, **context: Any) -> None:
    """Registra um warning para ser emitido apenas no fim do processo.

    Útil para sinalizar etapas ignoradas (ex: parâmetro inválido que fez a
    transformação ser pulada) sem interromper o pipeline nem poluir o meio do log.
    """
    _deferred_warnings.append((message, context))


def flush_deferred_warnings(log: StructuredLogger | None = None) -> None:
    """Emite e limpa todos os warnings adiados. Chamado ao final do pipeline."""
    global _deferred_warnings
    if not _deferred_warnings:
        return
    target = log or logger
    for message, context in _deferred_warnings:
        target.warning(message, **context)
    _deferred_warnings = []
