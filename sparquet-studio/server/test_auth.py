"""Tests for identity and permissions (`auth.py`).

Stdlib only — no pytest, no FastAPI, no Spark — so it runs the same way the rest
of the repo's tests do:

    python sparquet-studio/server/test_auth.py

What is pinned here is what a mistake would cost: a policy that fails open, a
password or a session token readable in the database file, a role change that
leaves nobody able to administer the runner.
"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import auth

PASSWORD = "correct horse battery"


class PolicyTest(unittest.TestCase):
    """The evaluator, with no database in the way."""

    ALLOW_WORKSPACE = [
        {"effect": "allow", "actions": ["workspace:*"], "resources": ["*"]}
    ]

    def test_nothing_is_allowed_by_default(self) -> None:
        # An empty policy is not a permissive one.
        self.assertFalse(auth.authorize([], "workspace:Read"))

    def test_a_wildcard_covers_the_verbs_of_one_service_only(self) -> None:
        self.assertTrue(auth.authorize(self.ALLOW_WORKSPACE, "workspace:Write"))
        self.assertFalse(auth.authorize(self.ALLOW_WORKSPACE, "run:Execute"))

    def test_deny_beats_allow_whatever_the_order(self) -> None:
        # The property that makes revoking access possible: one deny settles it,
        # so nobody has to audit every other role a person holds.
        deny_delete = {"effect": "deny", "actions": ["workspace:Delete"], "resources": ["*"]}
        self.assertFalse(auth.authorize([*self.ALLOW_WORKSPACE, deny_delete], "workspace:Delete"))
        self.assertFalse(auth.authorize([deny_delete, *self.ALLOW_WORKSPACE], "workspace:Delete"))
        # And only that action.
        self.assertTrue(auth.authorize([*self.ALLOW_WORKSPACE, deny_delete], "workspace:Write"))

    def test_a_statement_can_be_scoped_to_one_record(self) -> None:
        scoped = [{"effect": "allow", "actions": ["workspace:Write"], "resources": ["job/j1"]}]
        self.assertTrue(auth.authorize(scoped, "workspace:Write", "job/j1"))
        self.assertFalse(auth.authorize(scoped, "workspace:Write", "job/j2"))

    def test_a_resource_wildcard_does_not_cross_the_kind(self) -> None:
        scoped = [{"effect": "allow", "actions": ["workspace:Read"], "resources": ["job/*"]}]
        self.assertTrue(auth.authorize(scoped, "workspace:Read", "job/anything"))
        self.assertFalse(auth.authorize(scoped, "workspace:Read", "workflow/w1"))

    def test_the_builtin_roles_say_what_they_promise(self) -> None:
        viewer = auth.BUILTIN_ROLES["viewer"].statements
        self.assertTrue(auth.authorize(viewer, "workspace:Read"))
        self.assertFalse(auth.authorize(viewer, "workspace:Write"))
        self.assertFalse(auth.authorize(viewer, "run:Execute"))

        editor = auth.BUILTIN_ROLES["editor"].statements
        self.assertTrue(auth.authorize(editor, "workspace:Delete"))
        self.assertTrue(auth.authorize(editor, "run:Execute"))
        # An editor builds pipelines; it does not decide who else can.
        self.assertFalse(auth.authorize(editor, "iam:ManageUsers"))

        operator = auth.BUILTIN_ROLES["operator"].statements
        self.assertTrue(auth.authorize(operator, "run:Execute"))
        self.assertFalse(auth.authorize(operator, "workspace:Write"))

    def test_garbage_in_a_policy_grants_nothing(self) -> None:
        self.assertFalse(auth.authorize(["not a statement"], "workspace:Read"))
        self.assertFalse(auth.authorize([{"actions": "workspace:Read"}], "workspace:Read"))


class PasswordTest(unittest.TestCase):
    def test_a_password_is_never_stored_as_itself(self) -> None:
        stored = auth.hash_password(PASSWORD)
        self.assertNotIn(PASSWORD, stored)
        self.assertTrue(stored.startswith(("scrypt$", "pbkdf2$")))

    def test_the_same_password_hashes_differently_every_time(self) -> None:
        # Salted: two people with the same password must not look the same.
        self.assertNotEqual(auth.hash_password(PASSWORD), auth.hash_password(PASSWORD))

    def test_verification_accepts_the_password_and_nothing_else(self) -> None:
        stored = auth.hash_password(PASSWORD)
        self.assertTrue(auth.verify_password(PASSWORD, stored))
        self.assertFalse(auth.verify_password(PASSWORD + " ", stored))
        self.assertFalse(auth.verify_password("", stored))

    def test_a_password_too_short_to_defend_the_runner_is_refused(self) -> None:
        with self.assertRaises(auth.AuthError):
            auth.hash_password("short")

    def test_an_unreadable_hash_is_not_a_way_in(self) -> None:
        for stored in ("", "plaintext", "scrypt$nonsense", "md5$x$y"):
            self.assertFalse(auth.verify_password(PASSWORD, stored))


class StoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name) / "auth.sqlite3"
        self.store = auth.AuthStore(self.path)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _admin(self, username: str = "root") -> auth.User:
        return self.store.create_user(username, PASSWORD, roles=["admin"])

    # ---- users

    def test_an_empty_store_is_the_token_only_runner(self) -> None:
        self.assertFalse(self.store.has_users())
        self._admin()
        self.assertTrue(self.store.has_users())

    def test_a_user_without_a_role_can_only_look(self) -> None:
        user = self.store.create_user("ana", PASSWORD)
        self.assertEqual(user.roles, [auth.DEFAULT_ROLE])

    def test_two_people_cannot_share_a_username(self) -> None:
        self._admin("ana")
        # Case does not make it a different person.
        with self.assertRaises(auth.AuthError):
            self.store.create_user("ANA", PASSWORD)

    def test_a_role_that_does_not_exist_is_refused(self) -> None:
        with self.assertRaises(auth.AuthError):
            self.store.create_user("ana", PASSWORD, roles=["superuser"])

    # ---- login

    def test_login_returns_a_session_and_the_roles_behind_it(self) -> None:
        self._admin("ana")
        session = self.store.login("ana", PASSWORD)
        self.assertIsNotNone(session)
        principal = self.store.resolve_session(session.token)
        self.assertEqual(principal.username, "ana")
        self.assertTrue(principal.allows("iam:ManageUsers"))
        self.assertFalse(principal.token_only)

    def test_the_username_is_not_case_sensitive(self) -> None:
        self._admin("ana")
        self.assertIsNotNone(self.store.login("ANA", PASSWORD))

    def test_a_wrong_password_and_an_unknown_user_fail_the_same_way(self) -> None:
        self._admin("ana")
        self.assertIsNone(self.store.login("ana", "wrong password"))
        self.assertIsNone(self.store.login("nobody", PASSWORD))

    def test_a_disabled_account_cannot_log_in(self) -> None:
        self._admin("root")
        ana = self.store.create_user("ana", PASSWORD, roles=["editor"])
        self.store.set_disabled(ana.id, True)
        self.assertIsNone(self.store.login("ana", PASSWORD))

    def test_verifying_a_password_does_not_open_a_session(self) -> None:
        # `set_password` checks the current one; it must not mint a session doing it.
        ana = self.store.create_user("ana", PASSWORD, roles=["editor"])
        self.assertTrue(self.store.verify_credentials("ana", PASSWORD))
        self.assertFalse(self.store.verify_credentials("ana", "wrong password"))
        with closing(sqlite3.connect(self.path)) as conn:
            sessions = conn.execute(
                "SELECT COUNT(*) FROM session WHERE user_id = ?", (ana.id,)
            ).fetchone()[0]
        self.assertEqual(sessions, 0)

    # ---- sessions

    def test_a_session_token_is_not_readable_in_the_database(self) -> None:
        # A copy of this file must not be a set of live logins.
        self._admin("ana")
        session = self.store.login("ana", PASSWORD)
        with closing(sqlite3.connect(self.path)) as conn:
            stored = conn.execute("SELECT token_hash FROM session").fetchone()[0]
        self.assertNotEqual(stored, session.token)
        self.assertNotIn(session.token, stored)

    def test_logging_out_ends_the_session(self) -> None:
        self._admin("ana")
        session = self.store.login("ana", PASSWORD)
        self.store.logout(session.token)
        self.assertIsNone(self.store.resolve_session(session.token))

    def test_an_expired_session_stops_working(self) -> None:
        self._admin("ana")
        session = self.store.login("ana", PASSWORD)
        past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
        with closing(sqlite3.connect(self.path)) as conn:
            conn.execute("UPDATE session SET expires_at = ?", (past,))
            conn.commit()
        self.assertIsNone(self.store.resolve_session(session.token))

    def test_disabling_an_account_cuts_the_session_it_already_had(self) -> None:
        # Otherwise revoking access takes effect only at the next login.
        self._admin("root")
        ana = self.store.create_user("ana", PASSWORD, roles=["editor"])
        session = self.store.login("ana", PASSWORD)
        self.store.set_disabled(ana.id, True)
        self.assertIsNone(self.store.resolve_session(session.token))

    def test_changing_a_password_ends_every_session_it_opened(self) -> None:
        # A password is changed because it leaked or because someone is being
        # locked out; both mean the old sessions have to go.
        self._admin("root")
        ana = self.store.create_user("ana", PASSWORD, roles=["editor"])
        session = self.store.login("ana", PASSWORD)
        self.store.set_password(ana.id, "another good password")
        self.assertIsNone(self.store.resolve_session(session.token))
        self.assertIsNotNone(self.store.login("ana", "another good password"))

    def test_an_unknown_token_resolves_to_nobody(self) -> None:
        self._admin("ana")
        self.assertIsNone(self.store.resolve_session("made up"))
        self.assertIsNone(self.store.resolve_session(""))

    # ---- the way back in

    def test_the_last_administrator_cannot_be_demoted_disabled_or_deleted(self) -> None:
        root = self._admin("root")
        self.store.create_user("ana", PASSWORD, roles=["editor"])
        for action in (
            lambda: self.store.set_roles(root.id, ["viewer"]),
            lambda: self.store.set_disabled(root.id, True),
            lambda: self.store.delete_user(root.id),
        ):
            with self.assertRaises(auth.AuthError):
                action()
        self.assertIn("admin", self.store.get_user(root.id).roles)

    def test_a_second_administrator_makes_the_first_removable(self) -> None:
        root = self._admin("root")
        self._admin("ana")
        self.store.delete_user(root.id)
        self.assertIsNone(self.store.get_user(root.id))

    def test_roles_can_be_changed_when_someone_else_still_administers(self) -> None:
        self._admin("root")
        ana = self.store.create_user("ana", PASSWORD, roles=["viewer"])
        self.store.set_roles(ana.id, ["editor", "operator"])
        principal = self.store.resolve_session(self.store.login("ana", PASSWORD).token)
        self.assertEqual(principal.roles, ["editor", "operator"])
        self.assertTrue(principal.allows("workspace:Write"))

    def test_deleting_a_user_takes_their_sessions_with_them(self) -> None:
        self._admin("root")
        ana = self.store.create_user("ana", PASSWORD, roles=["editor"])
        session = self.store.login("ana", PASSWORD)
        self.store.delete_user(ana.id)
        self.assertIsNone(self.store.resolve_session(session.token))

    # ---- password recovery

    def test_a_recovery_code_sets_a_new_password(self) -> None:
        ana = self.store.create_user("ana", PASSWORD, roles=["editor"])
        code, _ = self.store.issue_recovery(ana.id, issued_by="root")
        self.store.redeem_recovery(code, "outra-senha-boa")
        self.assertIsNone(self.store.login("ana", PASSWORD))
        self.assertIsNotNone(self.store.login("ana", "outra-senha-boa"))

    def test_a_recovery_code_works_once(self) -> None:
        ana = self.store.create_user("ana", PASSWORD, roles=["editor"])
        code, _ = self.store.issue_recovery(ana.id)
        self.store.redeem_recovery(code, "outra-senha-boa")
        with self.assertRaises(auth.AuthError):
            self.store.redeem_recovery(code, "terceira-senha")

    def test_issuing_a_code_kills_the_previous_one(self) -> None:
        """Two live codes mean two chances for a leaked one to still work."""
        ana = self.store.create_user("ana", PASSWORD, roles=["editor"])
        first, _ = self.store.issue_recovery(ana.id)
        second, _ = self.store.issue_recovery(ana.id)
        with self.assertRaises(auth.AuthError):
            self.store.redeem_recovery(first, "outra-senha-boa")
        self.store.redeem_recovery(second, "outra-senha-boa")

    def test_an_expired_code_is_refused(self) -> None:
        ana = self.store.create_user("ana", PASSWORD, roles=["editor"])
        code, _ = self.store.issue_recovery(ana.id)
        conn = sqlite3.connect(self.path)
        try:
            conn.execute("UPDATE recovery SET expires_at = '2000-01-01T00:00:00Z'")
            conn.commit()
        finally:
            conn.close()
        with self.assertRaises(auth.AuthError):
            self.store.redeem_recovery(code, "outra-senha-boa")
        self.assertIsNotNone(self.store.login("ana", PASSWORD))

    def test_an_unknown_code_is_refused(self) -> None:
        self.store.create_user("ana", PASSWORD, roles=["editor"])
        with self.assertRaises(auth.AuthError):
            self.store.redeem_recovery("nao-existe", "outra-senha-boa")

    def test_a_disabled_account_cannot_be_recovered_into(self) -> None:
        """Recovery repairs a forgotten password, not a revoked account."""
        self._admin()
        ana = self.store.create_user("ana", PASSWORD, roles=["editor"])
        code, _ = self.store.issue_recovery(ana.id)
        self.store.set_disabled(ana.id, True)
        with self.assertRaises(auth.AuthError):
            self.store.redeem_recovery(code, "outra-senha-boa")

    def test_the_code_is_not_stored_as_itself(self) -> None:
        ana = self.store.create_user("ana", PASSWORD, roles=["editor"])
        code, _ = self.store.issue_recovery(ana.id)
        raw = self.path.read_bytes()
        self.assertNotIn(code.encode("utf-8"), raw)

    def test_a_recovery_ends_the_sessions_the_old_password_opened(self) -> None:
        ana = self.store.create_user("ana", PASSWORD, roles=["editor"])
        session = self.store.login("ana", PASSWORD)
        code, _ = self.store.issue_recovery(ana.id)
        self.store.redeem_recovery(code, "outra-senha-boa")
        self.assertIsNone(self.store.resolve_session(session.token))

    def test_a_short_password_does_not_burn_the_code(self) -> None:
        """The refusal is about the new password, so the person keeps their one try."""
        ana = self.store.create_user("ana", PASSWORD, roles=["editor"])
        code, _ = self.store.issue_recovery(ana.id)
        with self.assertRaises(auth.AuthError):
            self.store.redeem_recovery(code, "curta")
        self.store.redeem_recovery(code, "outra-senha-boa")
        self.assertIsNotNone(self.store.login("ana", "outra-senha-boa"))

    def test_a_code_cannot_be_issued_for_someone_who_does_not_exist(self) -> None:
        with self.assertRaises(auth.AuthError):
            self.store.issue_recovery("nao-existe")

    def test_a_user_is_findable_by_name_for_the_operator_commands(self) -> None:
        ana = self.store.create_user("ana", PASSWORD, roles=["editor"])
        self.assertEqual(self.store.find_user("ANA").id, ana.id)
        self.assertIsNone(self.store.find_user("bruno"))

    # ---- roles

    def test_the_builtin_roles_are_there_on_a_fresh_database(self) -> None:
        names = {role.name for role in self.store.list_roles()}
        self.assertEqual(names, set(auth.BUILTIN_ROLES))
        self.assertTrue(all(not role.custom for role in self.store.list_roles()))

    def test_reopening_the_store_keeps_everyone(self) -> None:
        self._admin("ana")
        reopened = auth.AuthStore(self.path)
        self.assertTrue(reopened.has_users())
        self.assertIsNotNone(reopened.login("ana", PASSWORD))


class CustomRoleTest(unittest.TestCase):
    """Roles written through the interface, next to the ones the module ships."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name) / "auth.sqlite3"
        self.store = auth.AuthStore(self.path)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    READ_ONLY = [{"effect": "allow", "actions": ["workspace:Read"], "resources": ["*"]}]

    def test_a_custom_role_grants_what_its_statements_say(self) -> None:
        self.store.create_role("auditor", "Looks, never touches", self.READ_ONLY)
        user = self.store.create_user("ana", PASSWORD, roles=["auditor"])
        session = self.store.login("ana", PASSWORD)
        principal = self.store.resolve_session(session.token)
        self.assertTrue(principal.allows("workspace:Read"))
        self.assertFalse(principal.allows("workspace:Write"))
        self.assertEqual(user.roles, ["auditor"])

    def test_a_builtin_name_cannot_be_taken_over(self) -> None:
        # The shipped roles are rewritten on every start, so an edit here would
        # silently disappear on the next restart.
        with self.assertRaises(auth.AuthError):
            self.store.create_role("admin", "mine now", self.READ_ONLY)

    def test_a_builtin_role_can_be_neither_edited_nor_removed(self) -> None:
        with self.assertRaises(auth.AuthError):
            self.store.update_role("viewer", statements=self.READ_ONLY)
        with self.assertRaises(auth.AuthError):
            self.store.delete_role("viewer")

    def test_two_roles_cannot_share_a_name(self) -> None:
        self.store.create_role("auditor", "", self.READ_ONLY)
        with self.assertRaises(auth.AuthError):
            self.store.create_role("auditor", "", self.READ_ONLY)

    def test_editing_a_role_changes_what_its_holders_may_do(self) -> None:
        self.store.create_role("auditor", "", self.READ_ONLY)
        self.store.create_user("ana", PASSWORD, roles=["auditor"])
        session = self.store.login("ana", PASSWORD)
        self.store.update_role(
            "auditor",
            statements=[{"effect": "allow", "actions": ["workspace:*"], "resources": ["*"]}],
        )
        # The policy is read per request, so a session already open sees the change.
        self.assertTrue(self.store.resolve_session(session.token).allows("workspace:Write"))

    def test_a_role_still_held_cannot_be_deleted(self) -> None:
        self.store.create_role("auditor", "", self.READ_ONLY)
        self.store.create_user("root", PASSWORD, roles=["admin"])
        self.store.create_user("ana", PASSWORD, roles=["auditor"])
        with self.assertRaises(auth.AuthError):
            self.store.delete_role("auditor")
        self.store.set_roles(self.store.find_user("ana").id, ["viewer"])
        self.store.delete_role("auditor")
        self.assertNotIn("auditor", [role.name for role in self.store.list_roles()])

    def test_a_role_held_by_a_team_cannot_be_deleted_either(self) -> None:
        self.store.create_role("auditor", "", self.READ_ONLY)
        self.store.create_team("data", roles=["auditor"])
        with self.assertRaises(auth.AuthError):
            self.store.delete_role("auditor")

    def test_a_statement_that_makes_no_sense_is_refused_at_the_door(self) -> None:
        with self.assertRaises(auth.AuthError):
            self.store.create_role("broken", "", [{"effect": "maybe", "actions": ["*"]}])

    def test_a_custom_role_survives_a_restart_and_is_not_rewritten(self) -> None:
        self.store.create_role("auditor", "Looks", self.READ_ONLY)
        reopened = auth.AuthStore(self.path)
        role = next(r for r in reopened.list_roles() if r.name == "auditor")
        self.assertTrue(role.custom)
        self.assertEqual(role.statements, self.READ_ONLY)


class TeamTest(unittest.TestCase):
    """Teams: who is charged for a run, and a second place roles come from."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name) / "auth.sqlite3"
        self.store = auth.AuthStore(self.path)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    RUNNER = [{"effect": "allow", "actions": ["run:Execute"], "resources": ["*"]}]

    def test_everybody_lands_in_the_default_team(self) -> None:
        user = self.store.create_user("ana", PASSWORD)
        self.assertIsNotNone(user.team_id)
        self.assertEqual((user.team_name or "").lower(), auth.DEFAULT_TEAM)

    def test_a_team_can_be_created_with_its_members_counted(self) -> None:
        team = self.store.create_team("data")
        self.store.create_user("ana", PASSWORD, team=team.id)
        self.store.create_user("bo", PASSWORD, team=team.id)
        self.assertEqual(self.store.get_team(team.id).members, 2)

    def test_a_team_can_be_named_instead_of_addressed_by_id(self) -> None:
        self.store.create_team("data")
        user = self.store.create_user("ana", PASSWORD, team="data")
        self.assertEqual(self.store.get_team(user.team_id).name, "data")

    def test_two_teams_cannot_share_a_name(self) -> None:
        self.store.create_team("data")
        with self.assertRaises(auth.AuthError):
            self.store.create_team("DATA")

    def test_a_team_role_is_added_to_the_personal_ones(self) -> None:
        self.store.create_role("runner", "", self.RUNNER)
        team = self.store.create_team("data", roles=["runner"])
        self.store.create_user("ana", PASSWORD, team=team.id)  # viewer, personally
        principal = self.store.resolve_session(self.store.login("ana", PASSWORD).token)
        self.assertEqual(principal.roles, [auth.DEFAULT_ROLE])
        self.assertEqual(principal.team_roles, ["runner"])
        self.assertTrue(principal.allows("run:Execute"))
        self.assertTrue(principal.allows("workspace:Read"))

    def test_a_deny_in_the_personal_role_still_wins_over_the_team_grant(self) -> None:
        # A team is a way of granting, never a way of overriding a refusal.
        self.store.create_role("runner", "", self.RUNNER)
        self.store.create_role(
            "no-runs", "", [{"effect": "deny", "actions": ["run:Execute"], "resources": ["*"]}]
        )
        team = self.store.create_team("data", roles=["runner"])
        self.store.create_user("ana", PASSWORD, roles=["no-runs"], team=team.id)
        principal = self.store.resolve_session(self.store.login("ana", PASSWORD).token)
        self.assertFalse(principal.allows("run:Execute"))

    def test_moving_somebody_changes_who_pays_from_now_on(self) -> None:
        data = self.store.create_team("data")
        platform = self.store.create_team("platform")
        user = self.store.create_user("ana", PASSWORD, team=data.id)
        moved = self.store.set_user_team(user.id, platform.id)
        self.assertEqual(moved.team_id, platform.id)
        self.assertEqual(self.store.get_team(data.id).members, 0)

    def test_deleting_a_team_moves_its_members_to_the_default_one(self) -> None:
        team = self.store.create_team("data")
        user = self.store.create_user("ana", PASSWORD, team=team.id)
        self.store.delete_team(team.id)
        self.assertEqual((self.store.get_user(user.id).team_name or "").lower(), auth.DEFAULT_TEAM)

    def test_the_default_team_cannot_be_removed(self) -> None:
        user = self.store.create_user("ana", PASSWORD)
        with self.assertRaises(auth.AuthError):
            self.store.delete_team(user.team_id)

    def test_a_team_role_can_be_changed_after_the_fact(self) -> None:
        self.store.create_role("runner", "", self.RUNNER)
        team = self.store.create_team("data")
        self.store.create_user("ana", PASSWORD, team=team.id)
        session = self.store.login("ana", PASSWORD)
        self.assertFalse(self.store.resolve_session(session.token).allows("run:Execute"))
        self.store.update_team(team.id, roles=["runner"])
        self.assertTrue(self.store.resolve_session(session.token).allows("run:Execute"))

    def test_an_unknown_team_is_refused_rather_than_silently_defaulted(self) -> None:
        with self.assertRaises(auth.AuthError):
            self.store.create_user("ana", PASSWORD, team="nowhere")


if __name__ == "__main__":
    unittest.main()
