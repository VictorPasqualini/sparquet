"""Tests for execution credits (`credits.py`).

Stdlib only — no pytest, no FastAPI — so it runs like the rest of the repo's tests:

    python sparquet-studio/server/test_credits.py

Two things are being protected here, and they are the two ways this feature could
quietly hurt somebody. The first is **who gets charged**: a local run must cost
nothing, a run pointed at a cluster must cost a coin, and the decision must come
from the configuration rather than from anything the caller can claim about
itself. The second is **the upgrade path**: with enforcement off the ledger has
to record everything and block nothing, and turning enforcement on later must
start from the balance that was granted, not from the consumption that was merely
observed.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

import credits


class TargetTests(unittest.TestCase):
    """What counts as leaving this machine."""

    def test_default_configuration_is_local_and_free(self):
        target = credits.target_of({"name": "vendas"})
        self.assertTrue(target.local)
        self.assertEqual(target.cost, 0)

    def test_explicit_local_master_is_free(self):
        for master in ("local", "local[*]", "local[4]"):
            with self.subTest(master=master):
                target = credits.target_of({"spark": {"master": master}})
                self.assertTrue(target.local)
                self.assertEqual(target.cost, 0)

    def test_cluster_master_costs_a_coin(self):
        for master in ("yarn", "spark://cluster:7077", "k8s://https://api:6443"):
            with self.subTest(master=master):
                target = credits.target_of({"spark": {"master": master}})
                self.assertFalse(target.local)
                self.assertEqual(target.cost, 1)
                self.assertEqual(target.label, master)

    def test_spark_connect_costs_a_coin_even_with_a_local_master(self):
        """`spark.remote` wins: the master is ignored and the work is elsewhere."""
        target = credits.target_of(
            {"spark": {"master": "local[*]", "configs": {"spark.remote": "sc://host:15002"}}}
        )
        self.assertFalse(target.local)
        self.assertEqual(target.cost, 1)

    def test_master_inside_configs_is_honoured(self):
        """`configs` is passed straight to the builder, so a master hidden there is
        just as real as the top-level one."""
        target = credits.target_of({"spark": {"configs": {"spark.master": "yarn"}}})
        self.assertFalse(target.local)

    def test_runner_inside_a_managed_cluster_charges_regardless_of_master(self):
        original = credits._runner_environment
        credits._runner_environment = lambda: "databricks"
        try:
            target = credits.target_of({"spark": {"master": "local[*]"}})
        finally:
            credits._runner_environment = original
        self.assertFalse(target.local)
        self.assertEqual(target.label, "databricks")

    def test_junk_configuration_does_not_crash_or_charge(self):
        self.assertTrue(credits.target_of({}).local)
        self.assertTrue(credits.target_of({"spark": "nonsense"}).local)


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.store = credits.CreditStore(Path(self._tmp.name) / "credits.sqlite3")
        self._enforce(False)

    def tearDown(self):
        os.environ.pop("SPARQUET_STUDIO_CREDITS", None)
        os.environ.pop("SPARQUET_STUDIO_CREDITS_INITIAL", None)
        self._tmp.cleanup()

    @staticmethod
    def _enforce(on: bool) -> None:
        os.environ["SPARQUET_STUDIO_CREDITS"] = "on" if on else "off"

    @staticmethod
    def _remote(cost: int = 1) -> credits.Target:
        return credits.Target(local=False, label="spark://cluster:7077", cost=cost)

    @staticmethod
    def _local() -> credits.Target:
        return credits.Target(local=True, label="local[*]", cost=0)


class MeteringTests(StoreTestCase):
    """Enforcement off: record everything, block nothing."""

    def test_a_remote_run_is_recorded_without_touching_the_balance(self):
        charge = self.store.charge("u1", self._remote(), username="ana")
        self.assertEqual(charge.amount, 1)
        self.assertFalse(charge.applied)
        account = self.store.account("u1")
        self.assertEqual(account.balance, 0)
        self.assertEqual(account.spent, 1)

    def test_a_zero_balance_does_not_stop_anything(self):
        for _ in range(5):
            self.store.charge("u1", self._remote(), username="ana")
        self.assertEqual(self.store.account("u1").spent, 5)

    def test_a_local_run_writes_no_ledger_row(self):
        charge = self.store.charge("u1", self._local(), username="ana")
        self.assertEqual(charge.amount, 0)
        self.assertFalse(charge.charged)
        self.assertEqual(self.store.ledger("u1"), [])
        self.assertEqual(self.store.account("u1").spent, 0)

    def test_turning_enforcement_on_starts_from_what_was_granted(self):
        """The whole reason `balance` and `spent` are different columns."""
        self.store.charge("u1", self._remote(), username="ana")
        self.store.charge("u1", self._remote(), username="ana")
        self.store.grant("u1", 3)
        self._enforce(True)
        self.store.charge("u1", self._remote(), username="ana")
        self.assertEqual(self.store.account("u1").balance, 2)


class EnforcementTests(StoreTestCase):
    def setUp(self):
        super().setUp()
        self._enforce(True)

    def test_a_charge_takes_from_the_balance(self):
        self.store.grant("u1", 2, username="ana")
        charge = self.store.charge("u1", self._remote(), username="ana")
        self.assertTrue(charge.applied)
        self.assertEqual(charge.balance_after, 1)
        self.assertEqual(self.store.account("u1").balance, 1)

    def test_an_empty_account_is_refused(self):
        with self.assertRaises(credits.InsufficientCredits) as caught:
            self.store.charge("u1", self._remote(), username="ana")
        self.assertEqual(caught.exception.needed, 1)
        self.assertEqual(caught.exception.balance, 0)

    def test_a_refused_charge_leaves_no_trace(self):
        """A run that never happened must not appear to have been paid for."""
        with self.assertRaises(credits.InsufficientCredits):
            self.store.charge("u1", self._remote(), username="ana")
        self.assertEqual(self.store.ledger("u1"), [])
        self.assertEqual(self.store.account("u1").spent, 0)

    def test_a_local_run_is_free_even_with_no_credits(self):
        self.store.charge("u1", self._local(), username="ana")
        self.assertEqual(self.store.account("u1").balance, 0)

    def test_credits_run_out_exactly_when_the_balance_does(self):
        self.store.grant("u1", 2, username="ana")
        self.store.charge("u1", self._remote(), username="ana")
        self.store.charge("u1", self._remote(), username="ana")
        with self.assertRaises(credits.InsufficientCredits):
            self.store.charge("u1", self._remote(), username="ana")

    def test_a_job_costing_more_than_one_is_refused_as_a_whole(self):
        """No partial payment: two credits left cannot buy a three-credit Job."""
        self.store.grant("u1", 2, username="ana")
        with self.assertRaises(credits.InsufficientCredits):
            self.store.charge("u1", self._remote(cost=3), username="ana")
        self.assertEqual(self.store.account("u1").balance, 2)

    def test_accounts_do_not_share_a_balance(self):
        self.store.grant("u1", 1, username="ana")
        self.store.charge("u1", self._remote(), username="ana")
        with self.assertRaises(credits.InsufficientCredits):
            self.store.charge("u2", self._remote(), username="bruno")


class AccountTests(StoreTestCase):
    def test_an_account_appears_on_first_use(self):
        self.store.charge("u1", self._remote(), username="ana")
        self.assertEqual([item.id for item in self.store.list_accounts()], ["u1"])

    def test_the_opening_balance_is_configurable(self):
        os.environ["SPARQUET_STUDIO_CREDITS_INITIAL"] = "10"
        self.assertEqual(self.store.account("u1", "ana").balance, 10)

    def test_a_rename_keeps_the_same_account(self):
        """The id pays, the username is a label."""
        self.store.grant("u1", 5, username="ana")
        account = self.store.account("u1", "ana.souza")
        self.assertEqual(account.balance, 5)
        self.assertEqual(account.username, "ana.souza")
        self.assertEqual(len(self.store.list_accounts()), 1)

    def test_credits_can_be_taken_back(self):
        self.store.grant("u1", 5, username="ana")
        self.assertEqual(self.store.grant("u1", -2).balance, 3)

    def test_a_grant_cannot_push_an_account_below_zero(self):
        self.store.grant("u1", 1, username="ana")
        with self.assertRaises(credits.CreditError):
            self.store.grant("u1", -5)
        self.assertEqual(self.store.account("u1").balance, 1)

    def test_a_grant_of_zero_is_refused(self):
        with self.assertRaises(credits.CreditError):
            self.store.grant("u1", 0, username="ana")


class LedgerTests(StoreTestCase):
    def test_a_charge_records_what_it_paid_for(self):
        self.store.charge(
            "u1", self._remote(), username="ana", job_run_id="jr1",
            pipeline_run_id="pr1", job_name="vendas",
        )
        entry = self.store.ledger("u1")[0]
        self.assertEqual(entry.reason, credits.REASON_RUN)
        self.assertEqual(entry.amount, -1)
        self.assertEqual(entry.job_run_id, "jr1")
        self.assertEqual(entry.pipeline_run_id, "pr1")
        self.assertEqual(entry.job_name, "vendas")
        self.assertEqual(entry.target, "spark://cluster:7077")

    def test_the_ledger_is_newest_first_and_scoped_to_one_account(self):
        self.store.grant("u1", 1, username="ana")
        self.store.charge("u1", self._remote(), username="ana")
        self.store.grant("u2", 1, username="bruno")
        reasons = [entry.reason for entry in self.store.ledger("u1")]
        self.assertEqual(reasons, [credits.REASON_RUN, credits.REASON_GRANT])
        self.assertEqual([entry.account_id for entry in self.store.ledger()], ["u2", "u1", "u1"])

    def test_an_unapplied_charge_says_so(self):
        """Metering rows have to be distinguishable from rows that cost somebody
        a credit, or a bill built from this table would be wrong."""
        self.store.charge("u1", self._remote(), username="ana")
        entry = self.store.ledger("u1")[0]
        self.assertFalse(entry.applied)
        self.assertEqual(entry.balance_after, 0)


class PrincipalTests(unittest.TestCase):
    class _Principal:
        def __init__(self, username, user_id=None):
            self.username = username
            self.user_id = user_id

    def test_a_user_pays_from_their_own_account(self):
        account_id, label = credits.account_for(self._Principal("ana", "u1"))
        self.assertEqual((account_id, label), ("u1", "ana"))

    def test_token_only_mode_has_one_shared_account(self):
        account_id, _ = credits.account_for(self._Principal("local"))
        self.assertEqual(account_id, credits.TOKEN_ACCOUNT)


if __name__ == "__main__":
    unittest.main(verbosity=2)
