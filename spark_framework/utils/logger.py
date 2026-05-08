from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict


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
        return json.dumps(record, default=str)

    def info(self, message: str, **kwargs: Any) -> None:
        self._logger.info(self._build_record("INFO", message, **kwargs))

    def warning(self, message: str, **kwargs: Any) -> None:
        self._logger.warning(self._build_record("WARNING", message, **kwargs))

    def error(self, message: str, **kwargs: Any) -> None:
        self._logger.error(self._build_record("ERROR", message, **kwargs))

    def debug(self, message: str, **kwargs: Any) -> None:
        self._logger.debug(self._build_record("DEBUG", message, **kwargs))


logger = StructuredLogger("spark_framework")
