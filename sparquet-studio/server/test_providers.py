"""Tests for the provider slots (`providers.py`).

Stdlib only — no pytest — so it runs the same way the rest of the runner's tests do:

    python sparquet-studio/server/test_providers.py

What is being protected here is the rule the module states and the hosted service
depends on: a slot that was *configured* and cannot be honoured stops the process.
Falling back to the local SQLite default would put every tenant's ledger and every
tenant's identity in one file on one disk, and it would do it quietly.

The factories below are referenced by name (`test_providers:build_*`), which is the
same `module:factory` string a deployment would put in the environment. Note that
under `python test_providers.py` this module is `__main__`, so `import_module` loads a
second copy of it — the assertions therefore look at what a factory *returned*, never
at module-level state shared with the copy.
"""
from __future__ import annotations

import os
import unittest

import providers


class Built:
    """What a provider factory hands back, tagged so a test can tell them apart."""

    def __init__(self, tag: str) -> None:
        self.tag = tag


def build_credits() -> Built:
    return Built("configured-credits")


def build_auth() -> Built:
    return Built("configured-auth")


def build_failing() -> Built:
    raise RuntimeError("the database is not reachable")


NOT_CALLABLE = "this is a string, not a factory"


def local() -> Built:
    return Built("local")


class _Env:
    """Sets provider variables for the length of a test and puts them back."""

    def __init__(self, **values: object) -> None:
        self._values = values
        self._previous: dict[str, str | None] = {}

    def __enter__(self) -> "_Env":
        for slot, value in self._values.items():
            name = providers.variable_for(slot)
            self._previous[name] = os.environ.get(name)
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = str(value)
        return self

    def __exit__(self, *_: object) -> None:
        for name, previous in self._previous.items():
            if previous is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous


class ProviderSlotTests(unittest.TestCase):
    def setUp(self) -> None:
        providers._loaded.clear()

    def tearDown(self) -> None:
        providers._loaded.clear()

    def test_local_default_when_nothing_is_configured(self) -> None:
        with _Env(credits=None):
            built = providers.load("credits", local)
        self.assertEqual(built.tag, "local")
        self.assertEqual(providers.describe()["credits"], "local")

    def test_configured_factory_replaces_the_default(self) -> None:
        with _Env(credits="test_providers:build_credits"):
            built = providers.load("credits", local)
        self.assertEqual(built.tag, "configured-credits")
        self.assertEqual(providers.describe()["credits"], "test_providers:build_credits")

    def test_each_slot_is_answered_independently(self) -> None:
        with _Env(credits="test_providers:build_credits", auth=None):
            credits_impl = providers.load("credits", local)
            auth_impl = providers.load("auth", local)
        self.assertEqual(credits_impl.tag, "configured-credits")
        self.assertEqual(auth_impl.tag, "local")
        self.assertEqual(
            providers.describe(),
            {
                "credits": "test_providers:build_credits",
                "auth": "local",
                "workspace": "local",
            },
        )

    def test_blank_variable_reads_as_unconfigured(self) -> None:
        with _Env(auth="   "):
            self.assertIsNone(providers.configured("auth"))
            self.assertEqual(providers.load("auth", local).tag, "local")

    def test_reference_without_a_colon_is_refused(self) -> None:
        with _Env(auth="sparquet_cloud.identity"):
            with self.assertRaises(providers.ProviderError) as raised:
                providers.load("auth", local)
        self.assertIn("module:factory", str(raised.exception))

    def test_unimportable_module_is_refused(self) -> None:
        with _Env(workspace="sparquet_cloud_absent.library:build"):
            with self.assertRaises(providers.ProviderError) as raised:
                providers.load("workspace", local)
        self.assertIn("sparquet_cloud_absent", str(raised.exception))

    def test_missing_attribute_is_refused(self) -> None:
        with _Env(credits="test_providers:build_nothing"):
            with self.assertRaises(providers.ProviderError) as raised:
                providers.load("credits", local)
        self.assertIn("build_nothing", str(raised.exception))

    def test_attribute_that_is_not_callable_is_refused(self) -> None:
        with _Env(credits="test_providers:NOT_CALLABLE"):
            with self.assertRaises(providers.ProviderError):
                providers.load("credits", local)

    def test_factory_that_raises_is_reported_not_swallowed(self) -> None:
        with _Env(auth="test_providers:build_failing"):
            with self.assertRaises(providers.ProviderError) as raised:
                providers.load("auth", local)
        self.assertIn("the database is not reachable", str(raised.exception))

    def test_a_refused_slot_never_falls_back_to_local(self) -> None:
        """The whole point: no local ledger stands in for a tenant's ledger."""
        with _Env(credits="test_providers:build_failing"):
            with self.assertRaises(providers.ProviderError):
                providers.load("credits", local)
        self.assertEqual(providers.describe()["credits"], "local")

    def test_unknown_slot_is_a_programming_error(self) -> None:
        with self.assertRaises(providers.ProviderError):
            providers.variable_for("telemetry")

    def test_variable_names_are_the_documented_ones(self) -> None:
        self.assertEqual(providers.variable_for("credits"), "SPARQUET_STUDIO_CREDITS_PROVIDER")
        self.assertEqual(providers.variable_for("auth"), "SPARQUET_STUDIO_AUTH_PROVIDER")
        self.assertEqual(
            providers.variable_for("workspace"), "SPARQUET_STUDIO_WORKSPACE_PROVIDER"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
