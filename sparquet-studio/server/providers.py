"""Where the runner's replaceable pieces come from.

Three things this service does are *policy*, not *mechanism*: where credits are
kept, where identity is kept, and where the library is kept. The open runner
answers all three with SQLite and files on the operator's own disk, which is the
right answer for one team on one machine and the wrong answer for a hosted
service — a tenant's ledger cannot live in a file another tenant's process can
open, and identity cannot be a password column when the customer arrives with an
identity provider of their own.

The wrong fix is a fork. A fork of a running service diverges on the first bug
fixed on only one side, and from then on every feature is written twice. So the
three become **slots**: the surface is a `Protocol` in the module that owns it,
the local implementation is the default, and a deployment names a factory to use
instead.

    export SPARQUET_STUDIO_CREDITS_PROVIDER="sparquet_cloud.credits:build"
    export SPARQUET_STUDIO_AUTH_PROVIDER="sparquet_cloud.identity:build"
    export SPARQUET_STUDIO_WORKSPACE_PROVIDER="sparquet_cloud.library:build"

A factory takes no arguments and returns something satisfying the slot's
protocol. Anything importable works — the hosted service is one package on the
`PYTHONPATH`, not a patched copy of this file.

**A configured provider that fails to load stops the process.** It is tempting to
log the failure and carry on with the local default, and it is exactly the wrong
thing: a hosted runner that quietly falls back to SQLite puts every tenant's
ledger and every tenant's identity in one file on one disk. That is a data
isolation failure, not a degraded mode, and it must never be something an
operator has to notice in a log.
"""
from __future__ import annotations

import importlib
import logging
import os
from typing import Any, Callable, Dict, Optional, TypeVar

_log = logging.getLogger("sparquet_studio.server")

T = TypeVar("T")

#: Slot name → environment variable naming the factory for it.
_VARIABLE = {
    "credits": "SPARQUET_STUDIO_CREDITS_PROVIDER",
    "auth": "SPARQUET_STUDIO_AUTH_PROVIDER",
    "workspace": "SPARQUET_STUDIO_WORKSPACE_PROVIDER",
}

#: What was actually loaded, per slot — read by `/health` so an operator can see
#: which implementation is answering without guessing from behaviour.
_loaded: Dict[str, str] = {}


class ProviderError(RuntimeError):
    """A slot was configured and could not be honoured."""


def variable_for(slot: str) -> str:
    """The environment variable that names this slot's factory."""
    try:
        return _VARIABLE[slot]
    except KeyError:  # pragma: no cover - programming error, not configuration
        raise ProviderError(f"Unknown provider slot: {slot!r}") from None


def configured(slot: str) -> Optional[str]:
    """The `module:factory` asked for, or `None` when the local default stands."""
    value = os.getenv(variable_for(slot), "").strip()
    return value or None


def describe() -> Dict[str, str]:
    """Which implementation is answering each slot. `"local"` for the default."""
    return {slot: _loaded.get(slot, "local") for slot in _VARIABLE}


def load(slot: str, default: Callable[[], T]) -> T:
    """Builds this slot: the configured factory, or `default()`.

    Raises `ProviderError` when a factory was named and could not be imported or
    called. Falling back would be silent tenant-mixing; see the module docstring.
    """
    reference = configured(slot)
    if reference is None:
        _loaded[slot] = "local"
        return default()

    factory = _resolve(slot, reference)
    try:
        built = factory()
    except Exception as error:  # pragma: no cover - depends on the provider
        raise ProviderError(
            f"{variable_for(slot)}={reference!r} was loaded but raised while "
            f"building: {error}"
        ) from error

    _loaded[slot] = reference
    _log.info("Provider for %s: %s", slot, reference)
    return built


def _resolve(slot: str, reference: str) -> Callable[[], Any]:
    """`"package.module:factory"` → the callable, or a readable refusal."""
    module_name, separator, attribute = reference.partition(":")
    if not separator or not module_name or not attribute:
        raise ProviderError(
            f"{variable_for(slot)}={reference!r} is not in `module:factory` form, "
            'for example "sparquet_cloud.credits:build".'
        )
    try:
        module = importlib.import_module(module_name)
    except Exception as error:
        raise ProviderError(
            f"{variable_for(slot)}={reference!r}: cannot import {module_name!r} "
            f"({error}). The provider package has to be on the PYTHONPATH of the "
            f"process running the runner."
        ) from error
    try:
        factory = getattr(module, attribute)
    except AttributeError:
        raise ProviderError(
            f"{variable_for(slot)}={reference!r}: {module_name!r} has no "
            f"{attribute!r}."
        ) from None
    if not callable(factory):
        raise ProviderError(
            f"{variable_for(slot)}={reference!r}: {attribute!r} is not callable."
        )
    return factory
