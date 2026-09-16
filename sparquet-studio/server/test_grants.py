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
os.environ["SPARQUET_STUDIO_AUDIT_DB"] = os.path.join(_TMP.name, "audit.sqlite3")
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


def owner(**fields: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "resource": "dataset",
        "resourceId": "/data/orders",
        "principalKind": "user",
        "principalId": "u1",
    }
    base.update(fields)
    return base


class AncestorTest(unittest.TestCase):
    """How an address says what contains it."""

    def test_a_path_nests_by_slash(self) -> None:
        self.assertEqual(
            grants.ancestors("/lake/silver/orders"), ["/lake/silver", "/lake"]
        )

    def test_a_qualified_name_nests_by_dot(self) -> None:
        self.assertEqual(
            grants.ancestors("main.silver.orders"), ["main.silver", "main"]
        )

    def test_an_address_with_both_nests_by_its_path(self) -> None:
        # `s3://bucket/main.db/orders` is one table in one bucket, not a column
        # of something called `db`.
        self.assertEqual(
            grants.ancestors("s3://bucket/main.db/orders")[0], "s3://bucket/main.db"
        )

    def test_a_single_segment_has_no_ancestors(self) -> None:
        self.assertEqual(grants.ancestors("orders"), [])
        self.assertEqual(grants.ancestors("*"), [])


class InheritanceTest(unittest.TestCase):
    """A grant on the container reaches what is inside it."""

    def test_a_grant_on_the_parent_folder_reaches_the_table(self) -> None:
        rules = loaded(rule(resourceId="/lake/silver", level="read"))
        self.assertTrue(
            grants.allows(rules, "dataset", "/lake/silver/orders", ANA, "read")
        )

    def test_and_says_where_it_came_from(self) -> None:
        rules = loaded(rule(resourceId="/lake/silver", level="read"))
        decision = grants.evaluate(rules, [], "dataset", "/lake/silver/orders", ANA)
        self.assertEqual(decision.source, "dataset//lake/silver")

    def test_the_best_level_in_the_chain_wins(self) -> None:
        rules = loaded(
            rule(resourceId="/lake", level="read"),
            rule(resourceId="/lake/silver/orders", level="write"),
        )
        self.assertTrue(
            grants.allows(rules, "dataset", "/lake/silver/orders", ANA, "write")
        )

    def test_a_deny_on_the_parent_closes_the_child(self) -> None:
        rules = loaded(
            rule(resourceId="/lake/silver/orders", level="write"),
            rule(resourceId="/lake", level="read", effect="deny"),
        )
        self.assertFalse(
            grants.allows(rules, "dataset", "/lake/silver/orders", ANA, "read")
        )

    def test_a_rule_on_a_sibling_leaves_this_one_ungoverned(self) -> None:
        rules = loaded(rule(resourceId="/lake/bronze/events"))
        governed, level = grants.decide(rules, "dataset", "/warehouse/orders", ANA)
        self.assertFalse(governed)
        self.assertIsNone(level)

    def test_a_job_inherits_from_the_workflow_it_lives_in(self) -> None:
        rules = loaded(rule(resource="workflow", resourceId="w1", level="write"))
        self.assertTrue(
            grants.allows(
                rules, "job", "j1", ANA, "write", parents=[("workflow", "w1")]
            )
        )
        # And the Job of another Workflow is untouched by it.
        self.assertTrue(
            grants.allows(
                rules, "job", "j9", BRUNO, "write", parents=[("workflow", "w2")]
            )
        )


class OwnershipTest(unittest.TestCase):
    """The owner holds everything, and a deny cannot take it away."""

    def test_the_owner_holds_admin_without_any_grant(self) -> None:
        owners = grants.load_owners([owner()])
        self.assertTrue(
            grants.allows([], "dataset", "/data/orders", ANA, "admin", owners=owners)
        )

    def test_a_deny_does_not_reach_the_owner(self) -> None:
        owners = grants.load_owners([owner()])
        rules = loaded(rule(principalId="*", principalKind="user", effect="deny"))
        self.assertTrue(
            grants.allows(rules, "dataset", "/data/orders", ANA, "admin", owners=owners)
        )
        # ...and still reaches everybody else.
        self.assertFalse(
            grants.allows(rules, "dataset", "/data/orders", BRUNO, "read", owners=owners)
        )

    def test_owning_the_container_owns_what_is_inside_it(self) -> None:
        owners = grants.load_owners([owner(resourceId="/lake/silver")])
        decision = grants.evaluate(
            [], owners, "dataset", "/lake/silver/orders", ANA
        )
        self.assertTrue(decision.owned)
        self.assertEqual(decision.source, "dataset//lake/silver")

    def test_a_team_can_own(self) -> None:
        owners = grants.load_owners(
            [owner(principalKind="team", principalId="t-analytics")]
        )
        self.assertTrue(
            grants.allows([], "dataset", "/data/orders", ANA, "admin", owners=owners)
        )
        self.assertFalse(
            grants.allows([], "dataset", "/data/orders", BRUNO, "read", owners=owners)
        )

    def test_an_owner_record_closes_the_resource_to_everyone_else(self) -> None:
        # Naming an owner is itself a decision about the resource, so it governs
        # it: otherwise declaring ownership would leave it wide open.
        owners = grants.load_owners([owner()])
        governed, level = grants.decide(
            [], "dataset", "/data/orders", BRUNO, owners=owners
        )
        self.assertTrue(governed)
        self.assertIsNone(level)

    def test_the_stored_map_shape_reads_back(self) -> None:
        parsed = grants.load_owners({"dataset:/data/orders": owner()})
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].resource_id, "/data/orders")

    def test_a_malformed_owner_is_dropped(self) -> None:
        self.assertEqual(grants.load_owners([{"resource": "nope", "resourceId": "x"}]), [])
        self.assertEqual(grants.load_owners([{"resource": "dataset"}]), [])


class AdministrationTest(unittest.TestCase):
    """Who may change the rules on one resource without running the platform."""

    def test_the_owner_may(self) -> None:
        owners = grants.load_owners([owner()])
        self.assertTrue(grants.may_administer([], owners, "dataset", "/data/orders", ANA))

    def test_an_admin_grant_may(self) -> None:
        rules = loaded(rule(level="admin"))
        self.assertTrue(grants.may_administer(rules, [], "dataset", "/data/orders", ANA))

    def test_a_write_grant_may_not(self) -> None:
        rules = loaded(rule(level="write"))
        self.assertFalse(grants.may_administer(rules, [], "dataset", "/data/orders", ANA))

    def test_nobody_administers_a_resource_no_rule_names(self) -> None:
        self.assertFalse(grants.may_administer([], [], "dataset", "/nothing", ANA))


class OwnerMetaGuardTest(unittest.TestCase):
    """An owner grants on what they own, without `iam:ManageGrants`."""

    EDITOR = [{"effect": "allow", "actions": ["workspace:*", "run:*"], "resources": ["*"]}]

    def setUp(self) -> None:
        main._workspace.write_meta("owners", [
            {"resource": "dataset", "resourceId": "/owned/table",
             "principalKind": "user", "principalId": "u2"},
        ])
        main._workspace.write_meta("grants", [])
        self.addCleanup(lambda: main._workspace.write_meta("owners", []))
        self.addCleanup(lambda: main._workspace.write_meta("grants", []))

    def test_the_owner_may_grant_on_their_own_table(self) -> None:
        main._guard_meta_key(
            principal(statements=self.EDITOR),
            "grants",
            [rule(resourceId="/owned/table", level="read")],
        )

    def test_but_not_on_a_table_somebody_else_owns(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            main._guard_meta_key(
                principal(statements=self.EDITOR),
                "grants",
                [rule(resourceId="/not-yours", level="read")],
            )
        self.assertEqual(raised.exception.status_code, 403)

    def test_and_deleting_the_whole_record_stays_with_an_administrator(self) -> None:
        with self.assertRaises(HTTPException):
            main._guard_meta_key(principal(statements=self.EDITOR), "grants", None)


class EffectiveAccessEndpointTest(unittest.TestCase):
    """`POST /iam/access` - the explanation a screen shows."""

    def setUp(self) -> None:
        main._workspace.write_meta("grants", [
            rule(resourceId="/lake/silver", level="read"),
        ])
        main._workspace.write_meta("owners", [
            {"resource": "dataset", "resourceId": "/lake/gold",
             "principalKind": "team", "principalId": "t-analytics"},
        ])
        self.addCleanup(lambda: main._workspace.write_meta("grants", []))
        self.addCleanup(lambda: main._workspace.write_meta("owners", []))

    def ask(self, who: auth.Principal, *resources: Dict[str, str]) -> List[Any]:
        return main.effective_access(
            main.AccessQueryRequest(resources=list(resources)), who
        )

    def test_it_reports_the_inherited_level_and_its_source(self) -> None:
        ana = principal(username="ana", user_id="u1", team_id="t-analytics")
        [out] = self.ask(ana, {"resource": "dataset", "resourceId": "/lake/silver/orders"})
        self.assertTrue(out.governed)
        self.assertEqual(out.level, "read")
        self.assertEqual(out.source, "dataset//lake/silver")
        self.assertIn("dataset//lake/silver", out.chain)

    def test_it_reports_ownership(self) -> None:
        ana = principal(username="ana", user_id="u1", team_id="t-analytics")
        [out] = self.ask(ana, {"resource": "dataset", "resourceId": "/lake/gold/revenue"})
        self.assertTrue(out.owned)
        self.assertEqual(out.level, "admin")
        self.assertTrue(out.may_administer)

    def test_an_unknown_kind_is_skipped_rather_than_guessed_at(self) -> None:
        self.assertEqual(self.ask(principal(), {"resource": "table", "resourceId": "x"}), [])


class TagGrantTest(unittest.TestCase):
    """A rule written on a tag, reaching every dataset the catalog tags with it.

    The chain a dataset already had is its address; a tag is the one container
    that is not in the address, so it arrives the same way a Workflow does for a
    Job — through `parents`.
    """

    def setUp(self) -> None:
        main._workspace.write_meta("catalog", {
            "/data/orders": {
                "key": "/data/orders",
                "tags": ["PII", "  Finance  "],
                "classification": "confidential",
                "domain": "sales",
            },
            "/data/public_holidays": {"key": "/data/public_holidays", "tags": []},
        })
        self.addCleanup(lambda: main._workspace.write_meta("catalog", {}))
        self.addCleanup(lambda: main._workspace.write_meta("grants", []))

    def test_tags_are_normalized_on_both_sides(self) -> None:
        # Typed with capitals in the catalog, written lowercase in the rule, and
        # they still have to meet.
        self.assertEqual(main._tags_of("/data/orders")[0], ("tag", "pii"))
        self.assertIn(("tag", "finance"), main._tags_of("/data/orders"))
        self.assertEqual(grants.load([rule(resource="tag", resourceId=" PII ")])[0].resource_id, "pii")

    def test_classification_and_domain_are_tags_too(self) -> None:
        scopes = main._tags_of("/data/orders")
        self.assertIn(("tag", "classification:confidential"), scopes)
        self.assertIn(("tag", "domain:sales"), scopes)

    def test_a_tag_grant_reaches_every_dataset_carrying_it(self) -> None:
        rules = loaded(rule(resource="tag", resourceId="pii", level="read"))
        self.assertTrue(grants.allows(
            rules, "dataset", "/data/orders", ANA, "read",
            parents=main._tags_of("/data/orders"),
        ))
        # And no further: a table without the tag is not governed by that rule.
        self.assertFalse(grants.evaluate(
            rules, [], "dataset", "/data/public_holidays", ANA,
            main._tags_of("/data/public_holidays"),
        ).governed)

    def test_a_tag_deny_closes_a_table_a_path_rule_opened(self) -> None:
        rules = loaded(
            rule(resourceId="/data", level="admin"),
            rule(resource="tag", resourceId="pii", effect="deny"),
        )
        decision = grants.evaluate(
            rules, [], "dataset", "/data/orders", ANA, main._tags_of("/data/orders"),
        )
        self.assertTrue(decision.governed)
        self.assertIsNone(decision.level)
        self.assertIn(("tag", "pii"), grants.scope_chain(
            "dataset", "/data/orders", main._tags_of("/data/orders"),
        ))

    def test_a_tag_cannot_be_owned(self) -> None:
        # Ownership is undeniable admin. Handing it out over a label anybody may
        # type onto a table would be handing out admin over tables never seen.
        owners = grants.load_owners([
            {"resource": "tag", "resourceId": "pii",
             "principalKind": "user", "principalId": "ana"},
        ])
        self.assertEqual(owners, [])

    def test_the_runner_refuses_a_query_a_tag_deny_closes(self) -> None:
        main._workspace.write_meta("grants", [
            rule(resourceId="/data", level="read"),
            rule(resource="tag", resourceId="pii", effect="deny"),
        ])
        who = principal(username="ana", user_id="u1", team_id="t-analytics")
        with self.assertRaises(HTTPException) as caught:
            main._authorize_resource(who, "dataset", "/data/orders", "read")
        self.assertEqual(caught.exception.status_code, 403)
        # The sibling without the tag is still readable through the path rule.
        main._authorize_resource(who, "dataset", "/data/public_holidays", "read")


class SimulateEndpointTest(unittest.TestCase):
    """`POST /iam/simulate` - what somebody else would be allowed to do."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.team = main._auth.create_team("analytics", roles=["viewer"])
        cls.carla = main._auth.create_user(
            "carla", "carla-password-123", roles=["operator"], team=cls.team.id,
        )
        main._auth.create_user("dan", "dan-password-123", roles=["viewer"])

    def setUp(self) -> None:
        self.addCleanup(lambda: main._workspace.write_meta("grants", []))
        self.addCleanup(lambda: main._workspace.write_meta("owners", []))
        self.addCleanup(lambda: main._workspace.write_meta("catalog", {}))

    def ask(self, **fields: Any) -> Any:
        return main.simulate_access(main.SimulationRequest(**fields))

    def test_an_unknown_user_is_answered_rather_than_raised(self) -> None:
        out = self.ask(username="nobody", action="run:Execute")
        self.assertFalse(out.found)
        self.assertFalse(out.allowed)
        self.assertIn("nobody", out.reason)

    def test_it_reports_the_roles_the_team_contributes(self) -> None:
        out = self.ask(username="carla")
        self.assertTrue(out.found)
        self.assertIn("operator", out.roles)
        self.assertEqual(out.team_roles, ["viewer"])

    def test_layer_one_refuses_an_action_no_role_allows(self) -> None:
        out = self.ask(username="dan", action="run:Execute")
        self.assertFalse(out.allowed)
        self.assertFalse(out.policy.allowed)
        self.assertIn("run:Execute", out.reason)

    def test_layer_two_refuses_what_layer_one_allows(self) -> None:
        main._workspace.write_meta("grants", [
            {"resource": "dataset", "resourceId": "/data/orders",
             "principalKind": "user", "principalId": "someone-else",
             "level": "read", "effect": "allow"},
        ])
        out = self.ask(
            username="carla", action="catalog:Query",
            resource="dataset", resource_id="/data/orders",
        )
        self.assertTrue(out.policy.allowed)
        self.assertTrue(out.access.governed)
        self.assertIsNone(out.access.level)
        self.assertFalse(out.allowed)
        self.assertIn("read access", out.reason)

    def test_an_ungoverned_resource_leaves_the_answer_to_layer_one(self) -> None:
        out = self.ask(
            username="carla", action="catalog:Query",
            resource="dataset", resource_id="/data/orders",
        )
        self.assertTrue(out.allowed)
        self.assertFalse(out.access.governed)

    def test_running_a_job_is_asked_of_layer_two_as_write(self) -> None:
        # What `/run/job` itself demands. Asking `read` here would report a
        # person as able to run a Job the runner would refuse them.
        out = self.ask(
            username="carla", action="run:Execute", resource="job", resource_id="j1",
        )
        self.assertEqual(out.level_asked, "write")

    def test_a_tag_grant_shows_up_as_the_source(self) -> None:
        main._workspace.write_meta("catalog", {
            "/data/orders": {"key": "/data/orders", "tags": ["pii"]},
        })
        main._workspace.write_meta("grants", [
            {"resource": "tag", "resourceId": "pii",
             "principalKind": "user", "principalId": self.carla.id,
             "level": "write", "effect": "allow"},
        ])
        out = self.ask(
            username="carla", resource="dataset", resource_id="/data/orders",
        )
        self.assertEqual(out.access.level, "write")
        self.assertEqual(out.access.source, "tag/pii")
        self.assertIn("tag/pii", out.access.chain)

    def test_an_unknown_action_is_a_bad_request_not_a_silent_no(self) -> None:
        with self.assertRaises(HTTPException) as caught:
            self.ask(username="carla", action="run:Everything")
        self.assertEqual(caught.exception.status_code, 400)


class SavedQueryGrantTest(unittest.TestCase):
    """A saved query is a securable of its own.

    Two things have to be true at once, and they pull in opposite directions.
    A rule on the file has to close the file — otherwise `query` in the picker
    is decoration. And it must never stand in for the tables: a query somebody
    is allowed to open still reads tables they may be denied, so the sources are
    checked on their own every single time.
    """

    def setUp(self) -> None:
        main._workspace.write_meta(
            "grants",
            [
                rule(
                    resource="query",
                    resourceId="q-revenue",
                    principalKind="team",
                    principalId="t-analytics",
                    level="read",
                )
            ],
        )

    def tearDown(self) -> None:
        main._workspace.delete_meta("grants")

    def test_a_governed_query_is_closed_to_everybody_the_rules_miss(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            main._authorize_resource(principal(), "query", "q-revenue", "read")
        self.assertEqual(raised.exception.status_code, 403)
        self.assertIn("q-revenue", raised.exception.detail)

    def test_the_granted_team_opens_it(self) -> None:
        main._authorize_resource(
            principal(username="ana", user_id="u1", team_id="t-analytics"),
            "query", "q-revenue", "read",
        )

    def test_read_on_the_file_is_not_write_on_it(self) -> None:
        # Cumulative levels run one way only: being allowed to open somebody's
        # query does not make it yours to rewrite.
        with self.assertRaises(HTTPException):
            main._authorize_resource(
                principal(username="ana", user_id="u1", team_id="t-analytics"),
                "query", "q-revenue", "write",
            )

    def test_the_document_guard_only_speaks_for_queries(self) -> None:
        # A Job is governed where it runs. Widening this guard to it would be a
        # separate decision with separate consequences, so it does nothing here.
        main._authorize_document(principal(), "job", "q-revenue", "write")
        with self.assertRaises(HTTPException):
            main._authorize_document(principal(), "query", "q-revenue", "read")

    def test_an_unsaved_buffer_is_governed_only_by_its_tables(self) -> None:
        # No id, nothing to check: a draft has no file for a rule to name.
        main._authorize_document(principal(), "query", "", "read")

    def test_a_query_nobody_wrote_a_rule_about_stays_open(self) -> None:
        main._authorize_resource(principal(), "query", "q-scratch", "read")

    def test_a_query_is_ownable_and_a_dataset_rule_does_not_reach_it(self) -> None:
        self.assertIn("query", grants.OWNABLE_KINDS)
        self.assertIn("query", grants.RESOURCE_KINDS)
        # Same id, different kind: the namespaces do not bleed into each other.
        main._authorize_resource(principal(), "dataset", "q-revenue", "read")


class ColumnAddressTest(unittest.TestCase):
    """How a column is spelled as a resource, and read back."""

    def test_the_address_joins_the_table_and_the_column(self) -> None:
        self.assertEqual(
            grants.column_resource("main.silver.orders", "CPF"),
            "main.silver.orders#cpf",
        )

    def test_a_half_missing_address_is_no_address(self) -> None:
        self.assertEqual(grants.column_resource("", "cpf"), "")
        self.assertEqual(grants.column_resource("main.orders", "  "), "")

    def test_it_reads_back_into_its_two_halves(self) -> None:
        self.assertEqual(
            grants.parse_column_resource("s3://bucket/orders#cpf"),
            ("s3://bucket/orders", "cpf"),
        )

    def test_an_id_that_is_not_a_column_is_refused(self) -> None:
        # A plain table, a dangling separator, and a name with nothing before it
        # are all "this is not a column", not "this is a column called empty".
        self.assertIsNone(grants.parse_column_resource("main.orders"))
        self.assertIsNone(grants.parse_column_resource("main.orders#"))
        self.assertIsNone(grants.parse_column_resource("#cpf"))

    def test_the_chain_reaches_the_table_but_not_a_folder_called_hash(self) -> None:
        chain = grants.scope_chain("column", "/lake/silver/orders#cpf")
        self.assertEqual(chain[0], ("column", "/lake/silver/orders#cpf"))
        self.assertIn(("column", grants.ANY), chain)
        self.assertIn(("dataset", "/lake/silver/orders"), chain)
        self.assertIn(("dataset", "/lake/silver"), chain)
        # The column id itself is never walked as a path: its "ancestors" are
        # the table's, and inventing `column//lake/silver` would let a rule
        # meant for a folder land on a column.
        self.assertEqual(
            [ident for kind, ident in chain if kind == "column"],
            ["/lake/silver/orders#cpf", grants.ANY],
        )

    def test_a_column_is_not_ownable(self) -> None:
        self.assertIn("column", grants.RESOURCE_KINDS)
        self.assertNotIn("column", grants.OWNABLE_KINDS)


class ColumnGrantTest(unittest.TestCase):
    """One column of one table, governed on its own.

    The point of the kind is the sentence "the whole table except the document
    number". That needs two things the dataset kind alone cannot give: a rule
    that names a column, and a deny on it that survives an allow on the table
    above.
    """

    def setUp(self) -> None:
        main._workspace.write_meta(
            "catalog",
            {
                "/data/orders": {
                    "tags": ["finance"],
                    "classification": "internal",
                    "columns": {
                        "cpf": {
                            "column": "cpf",
                            "description": "",
                            "classification": "restricted",
                            "tags": ["pii"],
                        }
                    },
                }
            },
        )
        main._workspace.write_meta(
            "grants",
            [
                rule(level="write"),
                rule(
                    resource="column",
                    resourceId="/data/orders#cpf",
                    level="read",
                    effect="deny",
                ),
            ],
        )

    def tearDown(self) -> None:
        main._workspace.delete_meta("grants")
        main._workspace.delete_meta("catalog")

    def _ana(self) -> Any:
        return principal(username="ana", user_id="u1", team_id="t-analytics")

    def test_the_table_stays_open_to_the_granted_team(self) -> None:
        main._authorize_resource(self._ana(), "dataset", "/data/orders", "write")

    def test_the_denied_column_is_closed_to_that_same_team(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            main._authorize_resource(
                self._ana(), "column", "/data/orders#cpf", "read"
            )
        self.assertEqual(raised.exception.status_code, 403)
        self.assertIn("cpf", raised.exception.detail)

    def test_a_column_nobody_named_inherits_the_table(self) -> None:
        main._authorize_resource(self._ana(), "column", "/data/orders#total", "write")

    def test_a_column_of_another_table_is_untouched(self) -> None:
        # Same column name, different table: the address is the whole identity.
        main._authorize_resource(self._ana(), "column", "/data/people#cpf", "read")

    def test_the_column_carries_its_own_tags_into_the_chain(self) -> None:
        parents = main._parents_of("column", "/data/orders#cpf")
        self.assertIn(("tag", "pii"), parents)
        self.assertIn(("tag", "classification:restricted"), parents)
        # And the table's, because a rule on the table's vocabulary still
        # reaches everything inside it.
        self.assertIn(("tag", "finance"), parents)
        self.assertIn(("tag", "classification:internal"), parents)

    def test_a_rule_on_the_column_tag_closes_every_column_that_wears_it(self) -> None:
        main._workspace.write_meta(
            "grants",
            [
                rule(level="write"),
                rule(
                    resource="tag",
                    resourceId="pii",
                    level="read",
                    effect="deny",
                ),
            ],
        )
        with self.assertRaises(HTTPException):
            main._authorize_resource(
                self._ana(), "column", "/data/orders#cpf", "read"
            )
        # The table it belongs to is not classified `pii`, so it stays open.
        main._authorize_resource(self._ana(), "dataset", "/data/orders", "write")

    def test_an_id_that_is_not_a_column_has_no_parents(self) -> None:
        self.assertEqual(main._parents_of("column", "/data/orders"), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
