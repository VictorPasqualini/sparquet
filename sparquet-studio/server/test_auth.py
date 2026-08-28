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


if __name__ == "__main__":
    unittest.main()
