"""Tests for the access rules over datasets, Jobs and Pipelines.

Stdlib plus FastAPI — no Spark, no HTTP client:

    python sparquet-studio/server/test_grants.py

The rules are written in Studio and stored beside the catalog annotations, so
the browser is where they are edited — and nowhere near where they matter. What
this pins is the runner half: that a deny actually closes `/query`, that it
cannot be walked around by running a Job that reads the same table, and that a
resource nobody has written a rule about still behaves exactly as it did before
anyone opened the permissions screen.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from typing import Any, Dict, List

_TMP = tempfile.TemporaryDirectory()
# Point every store at a throwaway directory *before* importing the module: they
# are created at import time, and the developer's own runner is not a fixture.
os.environ["SPARQUET_STUDIO_AUTH_DB"] = os.path.join(_TMP.name, "auth.sqlite3")
os.environ["SPARQUET_STUDIO_CREDITS_DB"] = os.path.join(_TMP.name, "credits.sqlite3")
os.environ["SPARQUET_STUDIO_HISTORY_DB"] = os.path.join(_TMP.name, "history.sqlite3")
os.environ["SPARQUET_STUDIO_WORKSPACE"] = os.path.join(_TMP.name, "workspace")
os.environ.setdefault("SPARQUET_STUDIO_TOKEN", "test-token")

import auth  # noqa: E402
import grants  # noqa: E402
import main  # noqa: E402
from fastapi import HTTPException  # noqa: E402

ANA = grants.Identity(user_id="u1", username="ana", team_id="t-analytics")
BRUNO = grants.Identity(user_id="u2", username="bruno", team_id="t-platform")


def rule(**fields: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "resource": "dataset",
        "resourceId": "/data/orders",
        "principalKind": "team",
        "principalId": "t-analytics",
        "level": "read",
        "effect": "allow",
    }
    base.update(fields)
    return base


def loaded(*rules: Dict[str, Any]) -> List[grants.Grant]:
    return grants.load(list(rules))


class LoadTest(unittest.TestCase):
    """Reading the stored list back."""

    def test_a_malformed_entry_is_dropped_rather_than_guessed_at(self) -> None:
        parsed = loaded(
            rule(),
            rule(level="owner"),
            rule(resource="cluster"),
            rule(principalKind="robot"),
            rule(resourceId=""),
            rule(principalId="   "),
            "not a rule",
        )
        self.assertEqual(len(parsed), 1)

    def test_anything_that_is_not_a_list_reads_as_no_rules(self) -> None:
        for raw in (None, {}, "grants", 7):
            self.assertEqual(grants.load(raw), [])

    def test_an_unknown_effect_is_read_as_allow_not_as_deny(self) -> None:
        # A typo must not silently close a table: a deny is something somebody
        # writes on purpose.
        self.assertEqual(loaded(rule(effect="maybe"))[0].effect, "allow")


class DecideTest(unittest.TestCase):
    """What one identity ends up holding."""

    def test_a_resource_no_rule_names_is_ungoverned(self) -> None:
        governed, level = grants.decide(loaded(rule()), "dataset", "/data/other", ANA)
        self.assertFalse(governed)
        self.assertIsNone(level)
        # …and ungoverned means open, because the action-level policy already ran.
        self.assertTrue(grants.allows(loaded(rule()), "dataset", "/data/other", ANA, "admin"))

    def test_the_first_rule_closes_the_resource_to_everyone_else(self) -> None:
        rules = loaded(rule())
        self.assertTrue(grants.allows(rules, "dataset", "/data/orders", ANA, "read"))
        self.assertFalse(grants.allows(rules, "dataset", "/data/orders", BRUNO, "read"))

    def test_levels_are_cumulative(self) -> None:
        rules = loaded(rule(level="admin"))
        for level in ("read", "write", "admin"):
            self.assertTrue(grants.allows(rules, "dataset", "/data/orders", ANA, level))

    def test_read_does_not_imply_write(self) -> None:
        rules = loaded(rule(level="read"))
        self.assertFalse(grants.allows(rules, "dataset", "/data/orders", ANA, "write"))

    def test_a_deny_beats_an_allow_however_it_was_granted(self) -> None:
        rules = loaded(
            rule(level="admin"),
            rule(principalKind="user", principalId="u1", effect="deny"),
        )
        self.assertFalse(grants.allows(rules, "dataset", "/data/orders", ANA, "read"))

    def test_a_deny_on_write_leaves_reading_intact(self) -> None:
        rules = loaded(
            rule(level="admin"),
            rule(principalKind="user", principalId="u1", level="write", effect="deny"),
        )
        self.assertTrue(grants.allows(rules, "dataset", "/data/orders", ANA, "read"))
        self.assertFalse(grants.allows(rules, "dataset", "/data/orders", ANA, "write"))

    def test_a_wildcard_resource_governs_every_dataset(self) -> None:
        rules = loaded(rule(resourceId="*"))
        self.assertTrue(grants.allows(rules, "dataset", "/data/anything", ANA, "read"))
        self.assertFalse(grants.allows(rules, "dataset", "/data/anything", BRUNO, "read"))

    def test_a_wildcard_principal_reaches_everyone(self) -> None:
        rules = loaded(rule(principalId="*"))
        self.assertTrue(grants.allows(rules, "dataset", "/data/orders", BRUNO, "read"))

    def test_a_rule_on_one_kind_says_nothing_about_another(self) -> None:
        rules = loaded(rule(resource="job", resourceId="j1", effect="deny"))
        self.assertTrue(grants.allows(rules, "dataset", "j1", BRUNO, "read"))
        self.assertFalse(grants.allows(rules, "job", "j1", ANA, "read"))

    def test_a_username_stands_in_for_an_id_on_a_runner_with_no_users(self) -> None:
        rules = loaded(rule(principalKind="user", principalId="ana"))
        alone = grants.Identity(username="ana")
        self.assertTrue(grants.allows(rules, "dataset", "/data/orders", alone, "read"))
        # With ids present, the name is not accepted in place of one — otherwise
        # renaming a user would hand them somebody else's grants.
        self.assertFalse(grants.allows(rules, "dataset", "/data/orders", ANA, "read"))


class AddressTest(unittest.TestCase):
    """The address a rule names has to be the address a run reads."""

    def test_a_trailing_slash_is_the_same_dataset(self) -> None:
        self.assertEqual(main._dataset_id("/data/orders/"), "/data/orders")
        self.assertEqual(main._dataset_id("  /data/orders  "), "/data/orders")

    def test_case_is_kept_because_storage_keeps_it(self) -> None:
        self.assertEqual(main._dataset_id("s3://Bucket/Orders"), "s3://Bucket/Orders")


def principal(**fields: Any) -> auth.Principal:
    base: Dict[str, Any] = {
        "username": "bruno", "user_id": "u2", "roles": ["editor"],
        "team_id": "t-platform",
        "statements": [{"effect": "allow", "actions": ["*"], "resources": ["*"]}],
    }
    base.update(fields)
    return auth.Principal(**base)


class EnforcementTest(unittest.TestCase):
    """The rules as the endpoints apply them, through the stored meta record."""

    def setUp(self) -> None:
        main._workspace.write_meta("grants", [rule()])

    def tearDown(self) -> None:
        main._workspace.delete_meta("grants")

    def test_a_denied_dataset_refuses_with_a_message_naming_it(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            main._authorize_resource(principal(), "dataset", "/data/orders", "read")
        self.assertEqual(raised.exception.status_code, 403)
        self.assertIn("/data/orders", raised.exception.detail)
        self.assertIn("bruno", raised.exception.detail)

    def test_the_granted_team_passes(self) -> None:
        main._authorize_resource(
            principal(username="ana", user_id="u1", team_id="t-analytics"),
            "dataset", "/data/orders", "read",
        )

    def test_a_token_only_runner_is_never_refused(self) -> None:
        # No users exist there, so no rule can name anybody, and a `*` deny would
        # lock the single operator out of their own machine.
        main._authorize_resource(auth.TOKEN_PRINCIPAL, "dataset", "/data/orders", "read")

    def test_a_job_cannot_be_used_to_read_a_denied_table(self) -> None:
        config = {
            "name": "sneak",
            "input": {"format": "parquet", "path": "/data/orders/"},
            "output": {"format": "parquet", "path": "/tmp/mine"},
        }
        with self.assertRaises(HTTPException) as raised:
            main._authorize_datasets(principal(), config)
        self.assertIn("/data/orders", raised.exception.detail)

    def test_writing_to_a_denied_table_is_refused_at_the_write_level(self) -> None:
        main._workspace.write_meta("grants", [rule(principalId="*", level="read")])
        config = {
            "name": "overwrite",
            "input": {"format": "parquet", "path": "/tmp/mine"},
            "output": {"format": "parquet", "path": "/data/orders"},
        }
        # Everyone may read it…
        main._authorize_resource(principal(), "dataset", "/data/orders", "read")
        # …and nobody may have a Job write over it.
        with self.assertRaises(HTTPException) as raised:
            main._authorize_datasets(principal(), config)
        self.assertIn("write", raised.exception.detail)

    def test_a_job_touching_nothing_governed_runs(self) -> None:
        config = {
            "name": "fine",
            "input": {"format": "parquet", "path": "/data/other"},
            "output": {"format": "parquet", "path": "/tmp/mine"},
        }
        main._authorize_datasets(principal(), config)

    def test_a_side_input_of_a_join_is_checked_too(self) -> None:
        config = {
            "name": "join",
            "input": {"format": "parquet", "path": "/data/other"},
            "transformations": [
                {"type": "join", "input": {"format": "parquet", "path": "/data/orders"}}
            ],
            "output": {"format": "parquet", "path": "/tmp/mine"},
        }
        with self.assertRaises(HTTPException):
            main._authorize_datasets(principal(), config)

    def test_no_stored_rules_leaves_every_dataset_open(self) -> None:
        main._workspace.delete_meta("grants")
        main._authorize_resource(principal(), "dataset", "/data/orders", "read")


class MetaGuardTest(unittest.TestCase):
    """Who may rewrite the rules themselves."""

    def test_an_editor_cannot_widen_their_own_access(self) -> None:
        editor = principal(
            statements=[{
                "effect": "allow",
                "actions": ["workspace:*", "run:*"],
                "resources": ["*"],
            }],
        )
        with self.assertRaises(HTTPException) as raised:
            main._guard_meta_key(editor, "grants")
        self.assertEqual(raised.exception.status_code, 403)
        self.assertIn("iam:ManageGrants", raised.exception.detail)
        # Ordinary bookkeeping under the same endpoint is untouched.
        main._guard_meta_key(editor, "storage-version")

    def test_an_administrator_may(self) -> None:
        main._guard_meta_key(principal(), "grants")

    def test_the_action_is_offered_to_the_role_editor(self) -> None:
        self.assertIn("iam:ManageGrants", auth.ACTIONS)
        self.assertIn("dataset", auth.RESOURCE_KINDS)


if __name__ == "__main__":
    unittest.main(verbosity=2)
