"""Sparquet Studio — local execution bridge.

Runs a pipeline described by an HTTP body through the real SparkFramework and
returns counters, validations, a small data preview and the framework's own
structured logs.

SECURITY WARNING
================
Every request executes arbitrary Spark work: arbitrary SQL, arbitrary reads and
arbitrary writes on the machine (and on any warehouse this machine can reach).

`/run` and `/validate` therefore require a shared secret in the `X-Sparquet-Token`
header, printed on startup (or taken from `SPARQUET_STUDIO_TOKEN`), and reject any
request whose `Origin` is outside the allow-list. Without that, any web page the
developer happens to visit could drive this runner: CORS withholds the *response*
from the attacker but never stops the request from executing. `/health` stays open
so Studio can detect the runner and prompt for the token.

This is still a single-developer tool: keep it bound to 127.0.0.1 and never expose
it to a network or the public internet.

Run it from the `sparquet-studio` directory; this module inserts the repository
root into sys.path so `spark_framework` is importable:

    uvicorn server.main:app --port 8787
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import logging
import math
import os
import re
import secrets
import sys
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone
from datetime import time as clock_time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

SERVICE_VERSION = "0.2.0"
FRAMEWORK_LOGGER = "spark_framework"
DEFAULT_ORIGINS = ("http://localhost:5273", "http://127.0.0.1:5273")
DEFAULT_PREVIEW_LIMIT = 50
MAX_PREVIEW_LIMIT = 1000
TOKEN_HEADER = "x-sparquet-token"

_VERSION_PATTERN = re.compile(r"""__version__\s*=\s*["']([^"']+)["']""")


# ---------------------------------------------------------------- bootstrap


def _framework_root() -> Path:
    override = os.getenv("SPARQUET_FRAMEWORK_PATH")
    if override:
        return Path(override).expanduser().resolve()
    # server/main.py -> sparquet-studio/server -> sparquet-studio -> repo root
    return Path(__file__).resolve().parents[2]


def _bootstrap_sys_path() -> None:
    root = _framework_root()
    if (root / "spark_framework" / "__init__.py").exists() and str(root) not in sys.path:
        sys.path.insert(0, str(root))


_bootstrap_sys_path()


# ------------------------------------------------------------------ models


class RunRequest(BaseModel):
    pipeline: Dict[str, Any]
    params: Optional[Dict[str, Any]] = None
    limit: int = Field(default=DEFAULT_PREVIEW_LIMIT, ge=1, le=MAX_PREVIEW_LIMIT)
    dry_run: bool = False


class ValidateRequest(BaseModel):
    pipeline: Dict[str, Any]
    params: Optional[Dict[str, Any]] = None


class ValidationOut(BaseModel):
    type: str
    passed: bool
    message: str = ""
    failed_count: int = 0


class PreviewOut(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    truncated: bool


class LogOut(BaseModel):
    timestamp: str
    level: str
    message: str
    context: Dict[str, Any] = Field(default_factory=dict)


class RunResponse(BaseModel):
    success: bool
    skipped: bool = False
    pipeline_name: Optional[str] = None
    rows_read: int = 0
    rows_written: int = 0
    duration_ms: int = 0
    error: Optional[str] = None
    validations: List[ValidationOut] = Field(default_factory=list)
    preview: Optional[PreviewOut] = None
    logs: List[LogOut] = Field(default_factory=list)


class ValidateResponse(BaseModel):
    valid: bool
    error: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    version: str
    spark_available: bool
    framework_version: Optional[str] = None
    # Lets Studio tell this build apart from an older, unauthenticated runner.
    auth_required: bool = True


class CapabilitiesResponse(BaseModel):
    transformations: List[str]
    readers: List[str]
    writers: List[str]
    validators: List[str]


# ------------------------------------------------------------- json safety


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_safe(value: Any) -> Any:
    """Converts a Spark/Python value into something json.dumps can emit and
    JSON.parse can read back (NaN/Infinity are valid Python JSON but not JS)."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, clock_time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    as_dict = getattr(value, "asDict", None)
    if callable(as_dict):
        # pyspark Row subclasses tuple, so this must precede the sequence branch
        try:
            return _json_safe(as_dict(recursive=True))
        except TypeError:
            return _json_safe(as_dict())
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(item) for item in value]
    return str(value)


# ----------------------------------------------------------- log capturing


class _LogCollector(logging.Handler):
    """Collects the framework's structured JSON log lines emitted during a run."""

    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.records: List[Dict[str, Any]] = []

    def emit(self, record: logging.LogRecord) -> None:
        raw = record.getMessage()
        payload: Any = None
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            payload = None

        if isinstance(payload, dict):
            context = {
                key: _json_safe(item)
                for key, item in payload.items()
                if key not in ("timestamp", "level", "message")
            }
            self.append(
                str(payload.get("timestamp") or _now_iso()),
                str(payload.get("level") or record.levelname),
                str(payload.get("message") or ""),
                context,
            )
            return

        self.append(_now_iso(), record.levelname, raw, {})

    def append(
        self, timestamp: str, level: str, message: str, context: Dict[str, Any]
    ) -> None:
        self.records.append(
            {
                "timestamp": timestamp,
                "level": level,
                "message": message,
                "context": context,
            }
        )


@contextmanager
def _capture_logs() -> Iterator[_LogCollector]:
    collector = _LogCollector()
    log = logging.getLogger(FRAMEWORK_LOGGER)
    previous_level = log.level
    if log.getEffectiveLevel() > logging.INFO:
        log.setLevel(logging.INFO)
    log.addHandler(collector)
    try:
        yield collector
    finally:
        log.removeHandler(collector)
        log.setLevel(previous_level)


# -------------------------------------------------------------- framework


_RUN_LOCK = threading.Lock()
_framework: Any = None


def _spark_available() -> bool:
    try:
        return importlib.util.find_spec("pyspark") is not None
    except (ImportError, ValueError):
        return False


def _framework_version() -> Optional[str]:
    module = sys.modules.get("spark_framework")
    if module is not None:
        version = getattr(module, "__version__", None)
        return str(version) if version else None
    # Reading the source keeps /health from importing pyspark
    init_file = _framework_root() / "spark_framework" / "__init__.py"
    try:
        match = _VERSION_PATTERN.search(init_file.read_text(encoding="utf-8"))
    except OSError:
        return None
    return match.group(1) if match else None


def _import(module_name: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except Exception as exc:  # ImportError, but pyspark can fail in other ways
        raise HTTPException(
            status_code=503,
            detail=(
                f"Cannot import '{module_name}': {exc}. Start the runner from the "
                "sparquet-studio directory inside the Sparquet repository, or "
                "install pyspark and the sparquet framework in this environment."
            ),
        ) from exc


def _get_framework() -> Any:
    """One SparkFramework per process — the SparkSession is a process-global
    singleton, so recreating it per request would be both slow and useless."""
    global _framework
    if _framework is None:
        _framework = _import("spark_framework").SparkFramework()
    return _framework


def _apply_params(pipeline: Dict[str, Any], params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not params:
        return pipeline
    template = _import("spark_framework.utils.template")
    rendered = template.apply_template(json.dumps(pipeline), params)
    parsed = json.loads(rendered)
    if not isinstance(parsed, dict):
        raise ValueError("Template substitution produced a non-object pipeline.")
    return parsed


def _parse_config_error(
    pipeline: Dict[str, Any], params: Optional[Dict[str, Any]]
) -> Optional[str]:
    config_cls = _import("spark_framework").PipelineConfig
    try:
        config_cls.from_dict(_apply_params(pipeline, params))
    except Exception as exc:
        return _describe(exc)
    return None


def _describe(exc: Exception) -> str:
    text = str(exc).strip()
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def _build_preview(df: Any, limit: int, collector: _LogCollector) -> Optional[PreviewOut]:
    try:
        columns = [str(name) for name in df.columns]
        # limit + 1 tells us whether more rows exist without a second action
        rows = df.limit(limit + 1).collect()
    except Exception as exc:
        collector.append(
            _now_iso(), "WARNING", "Preview unavailable", {"error": _describe(exc)}
        )
        return None

    return PreviewOut(
        columns=columns,
        rows=[[_json_safe(value) for value in row] for row in rows[:limit]],
        truncated=len(rows) > limit,
    )


def _map_validations(results: Any) -> List[ValidationOut]:
    out: List[ValidationOut] = []
    for item in results or []:
        out.append(
            ValidationOut(
                type=str(getattr(item, "rule_type", "unknown")),
                passed=bool(getattr(item, "passed", False)),
                message=str(getattr(item, "message", "") or ""),
                failed_count=int(getattr(item, "failed_count", 0) or 0),
            )
        )
    return out


# ------------------------------------------------------------------- access


def _allowed_origins() -> List[str]:
    raw = os.getenv("SPARQUET_STUDIO_ORIGINS", "")
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or list(DEFAULT_ORIGINS)


def _load_token() -> tuple[str, bool]:
    configured = os.getenv("SPARQUET_STUDIO_TOKEN", "").strip()
    return (configured, True) if configured else (secrets.token_urlsafe(24), False)


AUTH_TOKEN, TOKEN_FROM_ENV = _load_token()

UNAUTHORIZED_HELP = (
    f"Missing or invalid '{TOKEN_HEADER}' header. This runner executes arbitrary "
    "Spark jobs, so /run and /validate require the token printed in the runner's "
    "terminal when it started. Paste that token into Studio (Settings -> Local "
    "runner) so every request carries it, or start the runner with "
    "SPARQUET_STUDIO_TOKEN=<value> to pin a token you already know."
)


def _announce_token() -> None:
    if TOKEN_FROM_ENV:
        print("Sparquet Studio runner: using the token from SPARQUET_STUDIO_TOKEN.")
        return
    print("=" * 72)
    print("Sparquet Studio runner token (this session only):")
    print(f"    {AUTH_TOKEN}")
    print(f"Send it as the '{TOKEN_HEADER}' header on /run and /validate, or set")
    print("SPARQUET_STUDIO_TOKEN to keep the same token across restarts.")
    print("=" * 72)


_announce_token()


def require_token(request: Request) -> None:
    """Blocks drive-by requests from any page the developer happens to visit.

    CORS cannot do this: for a request with no custom header and no JSON content
    type the browser skips the preflight, the app runs, and only the *response*
    is withheld from the attacker. A header the browser refuses to attach
    cross-origin without a preflight, plus a server-side Origin check, do.
    """
    origin = request.headers.get("origin")
    if origin is not None and origin not in _allowed_origins():
        raise HTTPException(
            status_code=403,
            detail=(
                f"Origin '{origin}' is not allowed to use this runner. Set "
                "SPARQUET_STUDIO_ORIGINS if Studio is served from another origin."
            ),
        )

    if not secrets.compare_digest(request.headers.get(TOKEN_HEADER, ""), AUTH_TOKEN):
        raise HTTPException(status_code=401, detail=UNAUTHORIZED_HELP)


# --------------------------------------------------------------------- app


app = FastAPI(
    title="Sparquet Studio local runner",
    version=SERVICE_VERSION,
    description="Executes Sparquet pipelines locally. Never expose this publicly.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins(),
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    available = _spark_available()
    version = _framework_version()
    return HealthResponse(
        status="ok" if available and version else "degraded",
        version=SERVICE_VERSION,
        spark_available=available,
        framework_version=version,
    )


@app.post("/validate", response_model=ValidateResponse, dependencies=[Depends(require_token)])
def validate(body: ValidateRequest) -> ValidateResponse:
    error = _parse_config_error(body.pipeline, body.params)
    return ValidateResponse(valid=error is None, error=error)


def _engine_registry(engine_attr: str, module_name: str, class_name: str) -> Dict[str, Any]:
    """Prefers the live engine of the running framework instance: transformation
    and validator registrations are instance-scoped, unlike reader/writer ones."""
    engine = getattr(_framework, engine_attr, None) if _framework is not None else None
    if engine is None:
        engine = getattr(_import(module_name), class_name)()
    registry = getattr(engine, "_registry", None)
    return registry if isinstance(registry, dict) else {}


@app.get("/capabilities", response_model=CapabilitiesResponse)
def capabilities() -> CapabilitiesResponse:
    factory = _import("spark_framework.io.factory")
    return CapabilitiesResponse(
        transformations=sorted(
            _engine_registry(
                "_transform_engine", "spark_framework.transform.engine", "TransformationEngine"
            )
        ),
        readers=sorted(factory.ReaderFactory._registry),
        writers=sorted(factory.WriterFactory._registry),
        validators=sorted(
            _engine_registry(
                "_validation_engine", "spark_framework.validation.engine", "ValidationEngine"
            )
        ),
    )


@app.post("/run", response_model=RunResponse, dependencies=[Depends(require_token)])
def run(body: RunRequest) -> RunResponse:
    started = time.perf_counter()
    pipeline_name = body.pipeline.get("name")
    name = str(pipeline_name) if isinstance(pipeline_name, str) else None

    if body.dry_run:
        error = _parse_config_error(body.pipeline, body.params)
        return RunResponse(
            success=error is None,
            pipeline_name=name,
            duration_ms=_elapsed_ms(started),
            error=error,
            logs=[
                LogOut(
                    timestamp=_now_iso(),
                    level="INFO",
                    message="Dry run: configuration parsed, nothing executed",
                    context={},
                )
            ],
        )

    # Runs share one SparkSession, one runtime-variable store and one global
    # deferred-warning buffer, so they must never overlap.
    if not _RUN_LOCK.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail="A pipeline run is already in progress on this runner.",
        )

    try:
        with _capture_logs() as collector:
            framework = _get_framework()
            try:
                result = framework.run_from_dict(body.pipeline, params=body.params or None)
            except Exception as exc:
                # Config loading (missing keys, bad $include) raises outside the
                # pipeline's own try block and never reaches PipelineResult.
                return RunResponse(
                    success=False,
                    pipeline_name=name,
                    duration_ms=_elapsed_ms(started),
                    error=_describe(exc),
                    logs=[LogOut(**entry) for entry in collector.records],
                )

            output_df = getattr(result, "output_df", None)
            preview = (
                _build_preview(output_df, body.limit, collector)
                if output_df is not None
                else None
            )
            logs = [LogOut(**entry) for entry in collector.records]

        return RunResponse(
            success=bool(result.success),
            skipped=bool(getattr(result, "skipped", False)),
            pipeline_name=str(getattr(result, "pipeline_name", None) or name or ""),
            rows_read=int(getattr(result, "rows_read", 0) or 0),
            rows_written=int(getattr(result, "rows_written", 0) or 0),
            duration_ms=_elapsed_ms(started),
            error=getattr(result, "error", None),
            validations=_map_validations(getattr(result, "validation_results", [])),
            preview=preview,
            logs=logs,
        )
    finally:
        _RUN_LOCK.release()


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=os.getenv("SPARQUET_STUDIO_HOST", "127.0.0.1"),
        port=int(os.getenv("SPARQUET_STUDIO_PORT", "8787")),
    )
