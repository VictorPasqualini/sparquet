"""Tests for connection secrets: the store, the references and the masking.

Stdlib plus FastAPI — no Spark, no HTTP client:

    python sparquet-studio/server/test_vault.py

What these pin is the promise the feature is built on. A secret never leaves the
runner as a value: not through the list, not through a check, not through an
error a driver wrote. And the framework never learns that secrets exist — what
reaches it is an ordinary document with ordinary strings in it, which is why
`{secret:...}` had to be a syntax the framework's own `{param}` cannot see.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from typing import Any, Dict

_TMP = tempfile.TemporaryDirectory()
# Point every store at a throwaway directory *before* importing the module: they
# are created at import time, and the developer's own runner is not a fixture.
os.environ["SPARQUET_STUDIO_AUDIT_DB"] = os.path.join(_TMP.name, "audit.sqlite3")
os.environ["SPARQUET_STUDIO_AUTH_DB"] = os.path.join(_TMP.name, "auth.sqlite3")
os.environ["SPARQUET_STUDIO_CREDITS_DB"] = os.path.join(_TMP.name, "credits.sqlite3")
os.environ["SPARQUET_STUDIO_HISTORY_DB"] = os.path.join(_TMP.name, "history.sqlite3")
os.environ["SPARQUET_STUDIO_WORKSPACE"] = os.path.join(_TMP.name, "workspace")
os.environ["SPARQUET_STUDIO_SECRET_KEY"] = "a-test-master-key-long-enough"
os.environ.setdefault("SPARQUET_STUDIO_TOKEN", "test-token")

import auth  # noqa: E402
import main  # noqa: E402
import vault  # noqa: E402
from fastapi import HTTPException  # noqa: E402

try:  # The local provider is the only part that needs it.
    import cryptography  # noqa: F401
    HAS_CRYPTO = True
except Exception:  # pragma: no cover - depends on the environment
    HAS_CRYPTO = False

needs_crypto = unittest.skipUnless(
    HAS_CRYPTO, "cryptography is not installed; the `local` provider is unavailable"
)

MASTER_KEY = "a-test-master-key-long-enough"


def principal(**fields: Any) -> auth.Principal:
    base: Dict[str, Any] = {
        "username": "bruno", "user_id": "u2", "roles": ["editor"],
        "team_id": "t-platform",
        "statements": [{"effect": "allow", "actions": ["*"], "resources": ["*"]}],
    }
    base.update(fields)
    return auth.Principal(**base)


def env_store(**fields: str) -> vault.Store:
    """A store holding one `env` secret, which needs no crypto to resolve."""
    return vault.put(
        vault.Store(), "pg-prod", provider="env", values=dict(fields), actor="bruno",
    )


class NameTest(unittest.TestCase):
    def test_a_name_is_lower_cased_and_trimmed(self) -> None:
        self.assertEqual(vault.normalize("  PG-Prod  "), "pg-prod")

    def test_a_name_with_a_slash_is_refused(self) -> None:
        # The slash separates the name from the field inside a reference, so a
        # name containing one would make `{secret:a/b/c}` ambiguous.
        for bad in ("pg/prod", "", "  ", "-leading", "pg prod"):
            with self.assertRaises(vault.SecretError):
                vault.check_name(bad)


class ReferenceTest(unittest.TestCase):
    """The syntax, and its distance from the two the framework already owns."""

    def test_a_framework_parameter_is_not_a_secret_reference(self) -> None:
        # `{param}` and `{{var}}` belong to the framework, and `\\w+` matches
        # neither `:` nor `/` — which is the whole reason nothing in sparquet
        # had to change.
        document = {"path": "{date}/{{run_id}}/data", "url": "{secret:pg-prod/url}"}
        self.assertEqual(vault.names_in(document), ["pg-prod"])
        self.assertEqual(vault.references(document), [("pg-prod", "url")])

    def test_a_document_without_references_is_left_alone(self) -> None:
        document = {"input": {"format": "parquet", "path": "/data/orders"}}
        rendered, used = vault.render(document, lambda name, field: "never called")
        self.assertEqual(rendered, document)
        self.assertEqual(used, [])

    def test_rendering_never_touches_the_document_it_was_given(self) -> None:
        document = {"input": {"options": {"password": "{secret:pg-prod/password}"}}}
        rendered, used = vault.render(document, lambda name, field: "hunter2")
        self.assertEqual(rendered["input"]["options"]["password"], "hunter2")
        # The original is what history, lineage and the config hash keep.
        self.assertEqual(
            document["input"]["options"]["password"], "{secret:pg-prod/password}"
        )
        self.assertEqual(used, ["hunter2"])

    def test_a_value_with_quotes_and_backslashes_survives_intact(self) -> None:
        # The substitution happens in the serialized JSON, so a password that is
        # itself JSON punctuation would break the document if it were not escaped.
        nasty = "he said \"\\o/\"\tdone"
        document = {"options": {"password": "{secret:pg-prod/password}"}}
        rendered, _ = vault.render(document, lambda name, field: nasty)
        self.assertEqual(rendered["options"]["password"], nasty)

    def test_a_reference_inside_a_longer_string_is_substituted_in_place(self) -> None:
        document = {"url": "jdbc:postgresql://{secret:pg-prod/host}:5432/sales"}
        rendered, _ = vault.render(document, lambda name, field: "db.internal")
        self.assertEqual(rendered["url"], "jdbc:postgresql://db.internal:5432/sales")


class MaskTest(unittest.TestCase):
    """What a driver put in an error, on its way to a person."""

    def test_a_password_quoted_in_a_stack_trace_is_blanked(self) -> None:
        text = "FATAL: password authentication failed for jdbc:...?password=hunter2"
        self.assertNotIn("hunter2", vault.mask(text, ["hunter2"]))

    def test_a_very_short_value_is_left_alone(self) -> None:
        # Masking a three-character value would blank ordinary words out of every
        # message, which makes the log useless and protects nothing worth having.
        self.assertEqual(vault.mask("the port is 543", ["543"]), "the port is 543")

    def test_the_longest_value_is_replaced_first(self) -> None:
        masked = vault.mask("secret-value-long", ["value", "secret-value-long"])
        self.assertEqual(masked, "***")

    def test_scrub_reaches_every_string_in_a_structure(self) -> None:
        payload = {
            "error": "cannot connect with hunter2",
            "logs": [{"message": "url=...hunter2"}],
            "rows_read": 0,
        }
        clean = vault.scrub(payload, ["hunter2"])
        self.assertNotIn("hunter2", str(clean))
        self.assertEqual(clean["rows_read"], 0)


class PutTest(unittest.TestCase):
    """Writing, rotating and removing — as a patch over the fields."""

    def test_rotating_one_field_keeps_the_others(self) -> None:
        store = env_store(url="PG_URL", user="PG_USER", password="PG_PASSWORD")
        rotated = vault.put(
            store, "pg-prod", provider="env", values={"password": "PG_PASSWORD_NEW"},
        )
        secret = rotated.items["pg-prod"]
        self.assertEqual(sorted(secret.fields), ["password", "url", "user"])
        self.assertEqual(secret.fields["password"], "PG_PASSWORD_NEW")
        self.assertEqual(secret.fields["url"], "PG_URL")

    def test_a_field_mapped_to_none_is_removed(self) -> None:
        store = env_store(url="PG_URL", user="PG_USER")
        trimmed = vault.put(store, "pg-prod", provider="env", values={"user": None})
        self.assertEqual(sorted(trimmed.items["pg-prod"].fields), ["url"])

    def test_a_secret_with_no_fields_left_is_refused(self) -> None:
        store = env_store(url="PG_URL")
        with self.assertRaises(vault.SecretError):
            vault.put(store, "pg-prod", provider="env", values={"url": None})

    def test_an_empty_value_is_refused_rather_than_stored(self) -> None:
        with self.assertRaises(vault.SecretError):
            vault.put(vault.Store(), "pg-prod", provider="env", values={"url": ""})

    def test_an_unknown_provider_says_which_ones_exist(self) -> None:
        with self.assertRaises(vault.SecretError) as raised:
            vault.put(
                vault.Store(), "pg-prod", provider="vault-server", values={"a": "B"},
            )
        self.assertIn("env", str(raised.exception))

    def test_removing_a_secret_that_is_not_there_is_an_error_with_its_name(self) -> None:
        with self.assertRaises(vault.SecretError) as raised:
            vault.remove(vault.Store(), "nothing-here")
        self.assertIn("nothing-here", str(raised.exception))


class EnvProviderTest(unittest.TestCase):
    """The provider every cloud secret manager ends up arriving through."""

    def setUp(self) -> None:
        os.environ["PG_PASSWORD_FOR_TEST"] = "hunter2"
        self.addCleanup(os.environ.pop, "PG_PASSWORD_FOR_TEST", None)

    def test_the_value_is_read_from_the_named_variable(self) -> None:
        store = env_store(password="PG_PASSWORD_FOR_TEST")
        self.assertEqual(vault.value_of(store, "pg-prod", "password"), "hunter2")

    def test_a_missing_variable_is_named_in_the_error(self) -> None:
        store = env_store(password="PG_PASSWORD_NOT_SET")
        with self.assertRaises(vault.SecretError) as raised:
            vault.value_of(store, "pg-prod", "password")
        self.assertIn("PG_PASSWORD_NOT_SET", str(raised.exception))

    def test_check_reports_field_by_field_without_returning_a_value(self) -> None:
        store = env_store(password="PG_PASSWORD_FOR_TEST", url="PG_URL_NOT_SET")
        report = vault.check(store, "pg-prod")
        self.assertEqual(report["password"], "ok")
        self.assertNotEqual(report["url"], "ok")
        self.assertNotIn("hunter2", str(report))

    def test_an_unknown_field_is_refused_by_name(self) -> None:
        store = env_store(password="PG_PASSWORD_FOR_TEST")
        with self.assertRaises(vault.SecretError) as raised:
            vault.value_of(store, "pg-prod", "user")
        self.assertIn("user", str(raised.exception))


@needs_crypto
class LocalProviderTest(unittest.TestCase):
    """The encrypted store, which is what a laptop uses."""

    def test_a_sealed_value_comes_back_unchanged(self) -> None:
        store = vault.put(
            vault.Store(), "pg-prod", provider="local", values={"password": "hunter2"},
        )
        self.assertEqual(vault.value_of(store, "pg-prod", "password"), "hunter2")

    def test_the_stored_record_does_not_contain_the_value(self) -> None:
        store = vault.put(
            vault.Store(), "pg-prod", provider="local", values={"password": "hunter2"},
        )
        self.assertNotIn("hunter2", str(store.as_dict()))

    def test_a_ciphertext_from_another_workspace_is_inert(self) -> None:
        # The salt is per-store, so the same master key derives a different
        # encryption key: copying `meta.json` between runners moves nothing.
        first = vault.put(
            vault.Store(), "pg-prod", provider="local", values={"password": "hunter2"},
        )
        stolen = vault.Store(salt=vault.new_salt(), items=dict(first.items))
        with self.assertRaises(vault.SecretError):
            vault.value_of(stolen, "pg-prod", "password")

    def test_a_store_survives_a_round_trip_through_its_stored_shape(self) -> None:
        store = vault.put(
            vault.Store(), "pg-prod", provider="local",
            values={"password": "hunter2"}, tags=["PII"], actor="bruno",
        )
        back = vault.load(store.as_dict())
        self.assertEqual(vault.value_of(back, "pg-prod", "password"), "hunter2")
        self.assertEqual(back.items["pg-prod"].tags, ["pii"])


class MasterKeyTest(unittest.TestCase):
    def test_an_unset_key_refuses_rather_than_falling_back_to_plaintext(self) -> None:
        os.environ.pop(vault.MASTER_KEY_ENV, None)
        self.addCleanup(os.environ.__setitem__, vault.MASTER_KEY_ENV, MASTER_KEY)
        with self.assertRaises(vault.SecretError) as raised:
            vault.master_key()
        self.assertIn(vault.MASTER_KEY_ENV, str(raised.exception))

    def test_a_short_key_is_refused(self) -> None:
        os.environ[vault.MASTER_KEY_ENV] = "short"
        self.addCleanup(os.environ.__setitem__, vault.MASTER_KEY_ENV, MASTER_KEY)
        with self.assertRaises(vault.SecretError):
            vault.master_key()


class LoadTest(unittest.TestCase):
    def test_anything_unreadable_is_no_secrets_at_all(self) -> None:
        # A corrupt record must not stop the runner from booting: every caller
        # already handles "no secret by that name" with a message.
        for raw in (None, [], "secrets", 7, {"items": "nope"}):
            self.assertEqual(vault.load(raw).items, {})


class RedactionTest(unittest.TestCase):
    """What a screen is allowed to know."""

    def test_the_list_carries_field_names_and_no_material(self) -> None:
        store = env_store(password="PG_PASSWORD_FOR_TEST")
        redacted = store.items["pg-prod"].redacted()
        self.assertEqual(redacted["fields"], ["password"])
        # `env` reports which variable it reads — an operational fact, without
        # which a misconfigured runner cannot be diagnosed from the screen.
        self.assertEqual(redacted["binding"], {"password": "PG_PASSWORD_FOR_TEST"})

    @needs_crypto
    def test_a_local_secret_publishes_no_binding_at_all(self) -> None:
        store = vault.put(
            vault.Store(), "pg-prod", provider="local", values={"password": "hunter2"},
        )
        redacted = store.items["pg-prod"].redacted()
        self.assertNotIn("binding", redacted)
        self.assertNotIn("hunter2", str(redacted))


DENY_TEAM = {
    "resource": "secret", "resourceId": "pg-prod",
    "principalKind": "team", "principalId": "t-analytics",
    "level": "read", "effect": "allow",
}


class EndpointTest(unittest.TestCase):
    """The runner half: access, ownership and the sealed meta key."""

    def setUp(self) -> None:
        os.environ["PG_PASSWORD_FOR_TEST"] = "hunter2"
        self.addCleanup(os.environ.pop, "PG_PASSWORD_FOR_TEST", None)
        main._write_secrets(vault.Store())
        main._workspace.write_meta("grants", [])
        main._workspace.write_meta("owners", [])
        self.addCleanup(lambda: main._write_secrets(vault.Store()))
        self.addCleanup(lambda: main._workspace.write_meta("grants", []))
        self.addCleanup(lambda: main._workspace.write_meta("owners", []))

    def write(self, **fields: Any) -> Any:
        body = main.SecretWriteRequest(
            provider="env", values={"password": "PG_PASSWORD_FOR_TEST"}, **fields
        )
        return main.put_secret("pg-prod", body, principal())

    def close_it(self) -> None:
        """Hand the secret to a team this principal is not on."""
        main._workspace.write_meta("owners", [])
        main._workspace.write_meta("grants", [DENY_TEAM])

    def test_creating_a_secret_makes_the_creator_its_owner(self) -> None:
        # An unowned secret is governed by nothing, which would let everyone
        # holding `secrets:Write` repoint somebody else's database.
        out = self.write()
        self.assertTrue(out.owned)
        self.assertEqual(out.fields, ["password"])

    def test_the_workspace_snapshot_leaves_the_secret_store_behind(self) -> None:
        # `GET /workspace` is the read Studio starts with, and `workspace:Read`
        # is held by anyone who may open the library. The store belongs to no
        # browser: not the ciphertext, not the salt, not the env bindings.
        self.write()
        meta = main._workspace.read_meta()
        self.assertIn("secrets", meta)
        self.assertNotIn("secrets", main._public_meta(meta))
        self.assertIn("grants", main._public_meta(meta))

    def test_no_endpoint_ever_returns_a_value(self) -> None:
        self.write()
        listed = main.list_secrets(principal())
        self.assertNotIn("hunter2", str([item.model_dump() for item in listed]))
        checked = main.check_secret("pg-prod", principal())
        self.assertTrue(checked.healthy)
        self.assertNotIn("hunter2", str(checked.model_dump()))

    def test_a_secret_the_caller_cannot_reach_is_not_even_listed(self) -> None:
        # The name of a credential is itself information: a deny on
        # `secret/pg-prod` should not leave the production database on screen.
        self.write()
        self.close_it()
        self.assertEqual(main.list_secrets(principal()), [])

    def test_a_run_that_names_a_denied_secret_is_refused_before_decryption(self) -> None:
        self.write()
        self.close_it()
        document = {"input": {"options": {"password": "{secret:pg-prod/password}"}}}
        with self.assertRaises(HTTPException) as raised:
            main._resolve_secrets(document, principal())
        self.assertEqual(raised.exception.status_code, 403)
        self.assertIn("pg-prod", raised.exception.detail)

    def test_a_granted_run_gets_the_document_and_the_values_to_mask(self) -> None:
        self.write()
        document = {"input": {"options": {"password": "{secret:pg-prod/password}"}}}
        rendered, used = main._resolve_secrets(document, principal())
        self.assertEqual(rendered["input"]["options"]["password"], "hunter2")
        self.assertEqual(used, ["hunter2"])

    def test_a_reference_to_a_secret_that_does_not_exist_is_a_bad_request(self) -> None:
        document = {"input": {"options": {"password": "{secret:ghost/password}"}}}
        with self.assertRaises(HTTPException) as raised:
            main._resolve_secrets(document, principal())
        self.assertEqual(raised.exception.status_code, 400)

    def test_the_secret_store_cannot_be_written_through_the_meta_route(self) -> None:
        # A blanket PUT would skip the encryption, the per-secret access check
        # and the field-level rotation all at once.
        with self.assertRaises(HTTPException) as raised:
            main._guard_meta_key(principal(), "secrets")
        self.assertEqual(raised.exception.status_code, 403)

    def test_deleting_a_secret_that_is_not_there_is_a_not_found(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            main.delete_secret("ghost", principal())
        self.assertEqual(raised.exception.status_code, 404)


class TaggingTest(unittest.TestCase):
    """A secret joins the same tag chain a dataset does."""

    def setUp(self) -> None:
        main._write_secrets(
            vault.put(
                vault.Store(), "pg-prod", provider="env",
                values={"password": "PG_PASSWORD_FOR_TEST"}, tags=["pii"],
            )
        )
        self.addCleanup(lambda: main._write_secrets(vault.Store()))
        self.addCleanup(lambda: main._workspace.write_meta("grants", []))

    def test_a_deny_on_a_tag_closes_the_credential_too(self) -> None:
        main._workspace.write_meta("grants", [
            {"resource": "tag", "resourceId": "pii",
             "principalKind": "user", "principalId": "u2",
             "level": "read", "effect": "deny"},
        ])
        with self.assertRaises(HTTPException) as raised:
            main._authorize_resource(principal(), "secret", "pg-prod", "read")
        self.assertEqual(raised.exception.status_code, 403)


class NewResourceDefaultsTest(unittest.TestCase):
    """What a securable is governed by before anybody writes a rule.

    The reason this needs pinning is that `governed` and `level` are separate
    answers: naming an owner does not only give that person admin, it closes the
    record to everybody the rules do not mention. So a default that fires once
    too often is not a convenience — it is a lockout, and the conditions under
    which it declines to fire are the feature.
    """

    def setUp(self) -> None:
        os.environ.pop("SPARQUET_STUDIO_NEW_RESOURCE_DEFAULT", None)
        self.reset()
        self.addCleanup(self.reset)
        self.addCleanup(
            os.environ.pop, "SPARQUET_STUDIO_NEW_RESOURCE_DEFAULT", None
        )

    def reset(self) -> None:
        main._workspace.write_meta("grants", [])
        main._workspace.write_meta("owners", [])
        main._workspace.write_meta("catalog", {})
        for kind, doc_id in (("job", "j1"), ("workflow", "w1")):
            try:
                main._workspace.delete(kind, doc_id)
            except Exception:
                pass

    def policy(self, value: str) -> None:
        os.environ["SPARQUET_STUDIO_NEW_RESOURCE_DEFAULT"] = value

    def save_job(self, workflow_id: str = "") -> Any:
        record: Dict[str, Any] = {"id": "j1", "name": "Nightly load"}
        if workflow_id:
            record["workflowId"] = workflow_id
        return main.put_workspace_document(
            "job", "j1", main.WorkspaceWriteRequest(record=record), principal()
        )

    def owners(self) -> Any:
        return main._workspace.read_meta().get("owners") or []

    def rules(self) -> Any:
        return main._workspace.read_meta().get("grants") or []

    def test_a_new_job_belongs_to_whoever_saved_it(self) -> None:
        self.save_job()

        self.assertEqual(
            self.owners(),
            [{"resource": "job", "resourceId": "j1",
              "principalKind": "user", "principalId": "u2"}],
        )

    def test_and_their_team_may_edit_it(self) -> None:
        # The other half of `creator+team`: ownership alone would mean a Job is
        # invisible to the people sitting next to its author.
        self.save_job()

        self.assertEqual(
            self.rules(),
            [{"resource": "job", "resourceId": "j1",
              "principalKind": "team", "principalId": "t-platform",
              "level": "write", "effect": "allow"}],
        )

    def test_a_team_may_use_a_credential_without_repointing_it(self) -> None:
        # `read` on a secret is "a run of mine may use it", which is the level a
        # teammate needs; `write` would let them aim `pg-prod` elsewhere.
        main._claim_new_resource(principal(), "secret", "pg-prod")

        self.assertEqual(self.rules()[0]["level"], "read")

    def test_saving_it_again_does_not_hand_it_to_the_next_editor(self) -> None:
        self.save_job()
        main._workspace.write_meta("owners", [
            {"resource": "job", "resourceId": "j1",
             "principalKind": "user", "principalId": "u9"},
        ])

        self.save_job()

        self.assertEqual([row["principalId"] for row in self.owners()], ["u9"])

    def test_a_job_in_a_governed_workflow_inherits_instead(self) -> None:
        # The Workflow already answers "who may touch this", and an owner
        # written here would answer it more narrowly without anybody asking.
        main._workspace.write(
            main.workspace.Document(
                kind="workflow", id="w1", record={"id": "w1", "name": "Sales"},
            )
        )
        main._workspace.write_meta("owners", [
            {"resource": "workflow", "resourceId": "w1",
             "principalKind": "user", "principalId": "u9"},
        ])

        self.save_job(workflow_id="w1")

        self.assertEqual([row["resource"] for row in self.owners()], ["workflow"])

    def test_creator_alone_leaves_the_team_out(self) -> None:
        self.policy("creator")

        self.save_job()

        self.assertEqual(len(self.owners()), 1)
        self.assertEqual(self.rules(), [])

    def test_off_restores_an_ungoverned_record(self) -> None:
        self.policy("off")

        self.save_job()

        self.assertEqual(self.owners(), [])
        self.assertEqual(self.rules(), [])

    def test_a_token_only_caller_claims_nothing(self) -> None:
        # A shared runner token is not a person, so there is nobody to own it.
        main._claim_new_resource(
            principal(user_id=None, token_only=True), "job", "j1"
        )

        self.assertEqual(self.owners(), [])

    def test_annotating_a_dataset_claims_it(self) -> None:
        body = main.WorkspaceMetaRequest(value={"/lake/silver/orders": {"tags": []}})

        main.put_workspace_meta("catalog", body, principal())

        self.assertEqual(
            [row["resourceId"] for row in self.owners()], ["/lake/silver/orders"]
        )

    def test_a_catalog_that_was_already_there_is_not_a_creation(self) -> None:
        # Only the addresses this save added are new. The ones it merely resent
        # belong to whoever they belonged to before.
        main._workspace.write_meta("catalog", {"/lake/bronze/raw": {"tags": []}})
        body = main.WorkspaceMetaRequest(
            value={"/lake/bronze/raw": {"tags": []}, "/lake/silver/orders": {}}
        )

        main.put_workspace_meta("catalog", body, principal())

        self.assertEqual(
            [row["resourceId"] for row in self.owners()], ["/lake/silver/orders"]
        )

    def test_the_first_catalog_a_library_writes_claims_nothing(self) -> None:
        # A browser syncing a catalog it already had is a migration, not a
        # hundred creations, and treating it as one would hand one person every
        # table in the library.
        main._workspace.delete_meta("catalog")
        body = main.WorkspaceMetaRequest(value={"/lake/silver/orders": {"tags": []}})

        main.put_workspace_meta("catalog", body, principal())

        self.assertEqual(self.owners(), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
