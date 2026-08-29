"""Tests for execution credits (`credits.py`).

Stdlib only — no pytest, no FastAPI — so it runs like the rest of the repo's tests:

    python sparquet-studio/server/test_credits.py

Three things are being protected here, and each one is a way this feature could
quietly take money from somebody who did not owe it. The first is **what gets
charged**: a successful write to a cluster, never a run that failed before
writing and never work that stayed on the operator's own machine — and the
decision must come from the configuration that is about to run rather than from
anything the caller can claim about itself. The second is **the free monthly
allowance**: it has to refill on its own when the month turns, never accumulate,
and always be spent before a balance somebody paid for. The third is **the
upgrade path**: with enforcement off the ledger records everything and blocks
nothing, and turning enforcement on later has to start from the balance that was
granted rather than from consumption that was merely observed.
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
        self.assertEqual(target.unit_cost, 0)

    def test_explicit_local_master_is_free(self):
        for master in ("local", "local[*]", "local[4]"):
            with self.subTest(master=master):
                target = credits.target_of({"spark": {"master": master}})
                self.assertTrue(target.local)
                self.assertEqual(target.unit_cost, 0)

    def test_cluster_master_costs_a_credit_per_write(self):
        for master in ("yarn", "spark://cluster:7077", "k8s://https://api:6443"):
            with self.subTest(master=master):
                target = credits.target_of({"spark": {"master": master}})
                self.assertFalse(target.local)
                self.assertEqual(target.unit_cost, 1)
                self.assertEqual(target.label, master)

    def test_spark_connect_costs_even_with_a_local_master(self):
        """`spark.remote` wins: the master is ignored and the work is elsewhere."""
        target = credits.target_of(
            {"spark": {"master": "local[*]", "configs": {"spark.remote": "sc://host:15002"}}}
        )
        self.assertFalse(target.local)
        self.assertEqual(target.unit_cost, 1)

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

    def test_the_price_of_a_write_is_configurable(self):
        os.environ["SPARQUET_STUDIO_CREDITS_PER_WRITE"] = "5"
        try:
            self.assertEqual(credits.target_of({"spark": {"master": "yarn"}}).unit_cost, 5)
        finally:
            os.environ.pop("SPARQUET_STUDIO_CREDITS_PER_WRITE", None)

    def test_junk_configuration_does_not_crash_or_charge(self):
        self.assertTrue(credits.target_of({}).local)
        self.assertTrue(credits.target_of({"spark": "nonsense"}).local)


class StoreTestCase(unittest.TestCase):
    """The free allowance is switched off unless a test is about it.

    Otherwise every assertion about a balance would first have to burn forty free
    credits, and the test would be about the allowance instead of about the thing
    it is checking.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.store = credits.CreditStore(Path(self._tmp.name) / "credits.sqlite3")
        self._enforce(False)
        self._free(0)

    def tearDown(self):
        for name in (
            "SPARQUET_STUDIO_CREDITS", "SPARQUET_STUDIO_CREDITS_INITIAL",
            "SPARQUET_STUDIO_CREDITS_FREE_MONTHLY", "SPARQUET_STUDIO_CREDITS_PER_WRITE",
        ):
            os.environ.pop(name, None)
        self._tmp.cleanup()

    @staticmethod
    def _enforce(on: bool) -> None:
        os.environ["SPARQUET_STUDIO_CREDITS"] = "on" if on else "off"

    @staticmethod
    def _free(amount: int) -> None:
        os.environ["SPARQUET_STUDIO_CREDITS_FREE_MONTHLY"] = str(amount)

    @staticmethod
    def _remote(unit_cost: int = 1) -> credits.Target:
        return credits.Target(local=False, label="spark://cluster:7077", unit_cost=unit_cost)

    @staticmethod
    def _local() -> credits.Target:
        return credits.Target(local=True, label="local[*]", unit_cost=0)


class MeteringTests(StoreTestCase):
    """Enforcement off: record everything, block nothing."""

    def test_a_remote_write_is_recorded_without_touching_the_balance(self):
        charge = self.store.charge("t1", self._remote(), 1, username="data")
        self.assertEqual(charge.amount, 1)
        self.assertFalse(charge.applied)
        account = self.store.account("t1")
        self.assertEqual(account.balance, 0)
        self.assertEqual(account.spent, 1)

    def test_a_zero_balance_does_not_stop_anything(self):
        for _ in range(5):
            self.store.charge("t1", self._remote(), 1, username="data")
        self.assertEqual(self.store.account("t1").spent, 5)
        self.store.precheck("t1", self._remote())

    def test_metering_does_not_burn_the_free_allowance(self):
        """A month that was never charged must not look spent."""
        self._free(40)
        self.store.charge("t1", self._remote(), 3, username="data")
        self.assertEqual(self.store.account("t1").free_used, 0)

    def test_a_local_run_writes_no_ledger_row(self):
        charge = self.store.charge("t1", self._local(), 4, username="data")
        self.assertEqual(charge.amount, 0)
        self.assertFalse(charge.charged)
        self.assertEqual(self.store.ledger("t1"), [])
        self.assertEqual(self.store.account("t1").spent, 0)

    def test_a_run_that_wrote_nothing_costs_nothing(self):
        """The whole of "errors do not spend": a failure has fewer writes, and a
        failure before the first write has none."""
        charge = self.store.charge("t1", self._remote(), 0, username="data")
        self.assertEqual(charge.amount, 0)
        self.assertEqual(self.store.ledger("t1"), [])

    def test_turning_enforcement_on_starts_from_what_was_granted(self):
        """The whole reason `balance` and `spent` are different columns."""
        self.store.charge("t1", self._remote(), 2, username="data")
        self.store.grant("t1", 3)
        self._enforce(True)
        self.store.charge("t1", self._remote(), 1, username="data")
        self.assertEqual(self.store.account("t1").balance, 2)


class FreeAllowanceTests(StoreTestCase):
    def setUp(self):
        super().setUp()
        self._enforce(True)
        self._free(40)

    def test_forty_writes_a_month_are_free_by_default(self):
        os.environ.pop("SPARQUET_STUDIO_CREDITS_FREE_MONTHLY", None)
        self.assertEqual(self.store.account("t1", "data").free_monthly, 40)

    def test_the_allowance_pays_before_any_granted_balance(self):
        """A team must never burn credits it paid for while free ones sit unused."""
        self._free(3)
        self.store.grant("t1", 10, username="data")
        charge = self.store.charge("t1", self._remote(), 2, username="data")
        self.assertEqual(charge.free_amount, 2)
        self.assertEqual(self.store.account("t1").balance, 10)
        self.assertEqual(self.store.account("t1").free_used, 2)

    def test_a_charge_spills_from_the_allowance_into_the_balance(self):
        self._free(2)
        self.store.grant("t1", 10, username="data")
        charge = self.store.charge("t1", self._remote(), 5, username="data")
        self.assertEqual(charge.free_amount, 2)
        self.assertEqual(charge.balance_after, 7)
        self.assertEqual(self.store.account("t1").free_remaining, 0)

    def test_an_exhausted_allowance_refuses_the_next_run(self):
        self._free(2)
        self.store.charge("t1", self._remote(), 2, username="data")
        with self.assertRaises(credits.InsufficientCredits):
            self.store.precheck("t1", self._remote())

    def test_the_allowance_refills_when_the_month_turns_and_does_not_accumulate(self):
        self._free(2)
        original = credits.current_period
        credits.current_period = lambda moment=None: "2020-01"
        try:
            self.store.charge("t1", self._remote(), 2, username="data")
            self.assertEqual(self.store.account("t1").free_remaining, 0)
            credits.current_period = lambda moment=None: "2020-02"
            account = self.store.account("t1")
        finally:
            credits.current_period = original
        self.assertEqual(account.period, "2020-02")
        self.assertEqual(account.free_used, 0)
        # Two, not four: an unspent month is gone, not banked.
        self.assertEqual(account.free_remaining, 2)

    def test_available_is_the_allowance_plus_the_balance(self):
        self._free(4)
        self.store.grant("t1", 6, username="data")
        self.assertEqual(self.store.account("t1").available, 10)


class EnforcementTests(StoreTestCase):
    def setUp(self):
        super().setUp()
        self._enforce(True)

    def test_a_charge_takes_from_the_balance(self):
        self.store.grant("t1", 2, username="data")
        charge = self.store.charge("t1", self._remote(), 1, username="data")
        self.assertTrue(charge.applied)
        self.assertEqual(charge.balance_after, 1)
        self.assertEqual(self.store.account("t1").balance, 1)

    def test_an_empty_account_is_refused_before_spark_starts(self):
        with self.assertRaises(credits.InsufficientCredits) as caught:
            self.store.precheck("t1", self._remote(), username="data")
        self.assertEqual(caught.exception.needed, 1)
        self.assertEqual(caught.exception.available, 0)

    def test_a_refused_admission_leaves_no_trace(self):
        """A run that never happened must not appear to have been paid for."""
        with self.assertRaises(credits.InsufficientCredits):
            self.store.precheck("t1", self._remote(), username="data")
        self.assertEqual(self.store.ledger("t1"), [])
        self.assertEqual(self.store.account("t1").spent, 0)

    def test_a_local_run_is_admitted_with_no_credits(self):
        self.store.precheck("t1", self._local(), username="data")
        self.store.charge("t1", self._local(), 3, username="data")
        self.assertEqual(self.store.account("t1").balance, 0)

    def test_writing_more_than_the_account_can_pay_records_a_shortfall(self):
        """The cluster time is already spent; a ledger that pretended otherwise
        would be a worse record than none. The next admission is what refuses."""
        self.store.grant("t1", 2, username="data")
        charge = self.store.charge("t1", self._remote(), 5, username="data")
        self.assertEqual(charge.amount, 5)
        self.assertEqual(charge.shortfall, 3)
        self.assertEqual(self.store.account("t1").balance, 0)
        with self.assertRaises(credits.InsufficientCredits):
            self.store.precheck("t1", self._remote())

    def test_credits_run_out_exactly_when_the_balance_does(self):
        self.store.grant("t1", 2, username="data")
        self.store.charge("t1", self._remote(), 2, username="data")
        with self.assertRaises(credits.InsufficientCredits):
            self.store.precheck("t1", self._remote())

    def test_a_dearer_write_costs_more(self):
        self.store.grant("t1", 10, username="data")
        self.store.charge("t1", self._remote(unit_cost=3), 2, username="data")
        self.assertEqual(self.store.account("t1").balance, 4)

    def test_teams_do_not_share_a_balance(self):
        self.store.grant("t1", 1, username="data")
        self.store.charge("t1", self._remote(), 1, username="data")
        with self.assertRaises(credits.InsufficientCredits):
            self.store.precheck("t2", self._remote(), username="plataforma")


class DeclaredWritesTests(unittest.TestCase):
    """How big a hold a configuration asks for."""

    def test_one_per_output(self):
        pipeline = {"outputs": [{"format": "parquet"}, {"format": "delta"}]}
        self.assertEqual(credits.declared_writes(pipeline), 2)

    def test_a_configuration_without_outputs_still_declares_one(self):
        """Reserving nothing would let an empty account start a cluster."""
        self.assertEqual(credits.declared_writes({"name": "vendas"}), 1)
        self.assertEqual(credits.declared_writes({"outputs": []}), 1)

    def test_a_malformed_configuration_does_not_raise(self):
        self.assertEqual(credits.declared_writes({"outputs": "parquet"}), 1)
        self.assertEqual(credits.declared_writes(None), 1)


class ReservationTests(StoreTestCase):
    """What a run holds while it is in flight, and what comes back."""

    def setUp(self):
        super().setUp()
        self._enforce(True)
        self._free(0)

    def _charges(self, account_id):
        """The ledger without the grants that set the tests up."""
        return [item for item in self.store.ledger(account_id) if item.reason != "grant"]

    def test_a_hold_leaves_available_without_leaving_the_balance(self):
        self.store.grant("t1", 10, username="data")
        self.store.reserve("t1", self._remote(), 3, username="data")
        account = self.store.account("t1")
        self.assertEqual(account.held, 3)
        self.assertEqual(account.balance, 10)
        self.assertEqual(account.available, 7)

    def test_a_hold_is_not_on_the_ledger(self):
        """Only a settlement is a movement; a promise is not."""
        self.store.grant("t1", 10, username="data")
        self.store.reserve("t1", self._remote(), 3, username="data")
        self.assertEqual(self._charges("t1"), [])
        self.assertEqual(self.store.account("t1").spent, 0)

    def test_a_run_that_cannot_cover_its_declaration_is_refused(self):
        self.store.grant("t1", 2, username="data")
        with self.assertRaises(credits.InsufficientCredits) as caught:
            self.store.reserve("t1", self._remote(), 5, username="data")
        self.assertEqual(caught.exception.needed, 5)
        self.assertEqual(caught.exception.available, 2)
        self.assertEqual(self.store.account("t1").held, 0)

    def test_two_runs_cannot_hold_the_same_credits(self):
        """The whole point of holding rather than checking twice."""
        self.store.grant("t1", 4, username="data")
        self.store.reserve("t1", self._remote(), 3, username="data")
        with self.assertRaises(credits.InsufficientCredits):
            self.store.reserve("t1", self._remote(), 2, username="data")

    def test_a_local_run_holds_nothing(self):
        reservation = self.store.reserve("t1", self._local(), 5, username="data")
        self.assertFalse(reservation.held)
        self.assertEqual(self.store.account("t1").held, 0)

    def test_nothing_is_held_when_enforcement_is_off(self):
        self._enforce(False)
        reservation = self.store.reserve("t1", self._remote(), 5, username="data")
        self.assertEqual(reservation.amount, 0)
        self.assertEqual(self.store.account("t1").held, 0)

    def test_settling_charges_what_ran_and_gives_the_rest_back(self):
        self.store.grant("t1", 10, username="data")
        reservation = self.store.reserve("t1", self._remote(), 5, username="data")
        charge = self.store.settle(reservation, self._remote(), 2, username="data")
        account = self.store.account("t1")
        self.assertEqual(charge.amount, 2)
        self.assertEqual(account.held, 0)
        self.assertEqual(account.balance, 8)
        self.assertEqual(account.available, 8)

    def test_a_run_that_wrote_more_than_it_declared_spends_its_own_hold(self):
        """Releasing before charging is what keeps this from a false shortfall."""
        self.store.grant("t1", 6, username="data")
        reservation = self.store.reserve("t1", self._remote(), 5, username="data")
        charge = self.store.settle(reservation, self._remote(), 6, username="data")
        self.assertEqual(charge.amount, 6)
        self.assertEqual(charge.shortfall, 0)
        self.assertEqual(self.store.account("t1").balance, 0)

    def test_a_run_that_failed_before_writing_gives_everything_back(self):
        self.store.grant("t1", 10, username="data")
        reservation = self.store.reserve("t1", self._remote(), 4, username="data")
        self.assertEqual(self.store.release(reservation), 4)
        account = self.store.account("t1")
        self.assertEqual(account.held, 0)
        self.assertEqual(account.balance, 10)
        self.assertEqual(self._charges("t1"), [])

    def test_releasing_twice_gives_back_once(self):
        """The failure path and `settle` both release; a double refund would be
        worse than either."""
        self.store.grant("t1", 10, username="data")
        reservation = self.store.reserve("t1", self._remote(), 4, username="data")
        self.store.release(reservation)
        self.assertEqual(self.store.release(reservation), 0)
        self.assertEqual(self.store.account("t1").held, 0)

    def test_settling_after_a_release_still_charges_the_writes(self):
        self.store.grant("t1", 10, username="data")
        reservation = self.store.reserve("t1", self._remote(), 4, username="data")
        self.store.release(reservation)
        charge = self.store.settle(reservation, self._remote(), 2, username="data")
        self.assertEqual(charge.amount, 2)
        self.assertEqual(self.store.account("t1").balance, 8)
        self.assertEqual(self.store.account("t1").held, 0)

    def test_settling_without_a_reservation_is_a_plain_charge(self):
        """A local run holds nothing, and still has to be metered."""
        self.store.grant("t1", 10, username="data")
        charge = self.store.settle(
            None, self._remote(), 2, account_id="t1", username="data"
        )
        self.assertEqual(charge.amount, 2)
        self.assertEqual(self.store.account("t1").balance, 8)

    def test_a_restart_gives_back_what_the_crash_was_holding(self):
        """A hold belongs to a run in flight; if this process is starting, there
        are none."""
        self.store.grant("t1", 10, username="data")
        self.store.reserve("t1", self._remote(), 4, username="data")
        self.store.reserve("t1", self._remote(), 2, username="data")
        self.assertEqual(self.store.release_stale(), 2)
        account = self.store.account("t1")
        self.assertEqual(account.held, 0)
        self.assertEqual(account.available, 10)
        self.assertEqual(self.store.release_stale(), 0)

    def test_the_allowance_can_be_held_like_a_balance(self):
        self._free(5)
        reservation = self.store.reserve("t1", self._remote(), 4, username="data")
        self.assertEqual(self.store.account("t1").available, 1)
        self.store.settle(reservation, self._remote(), 4, username="data")
        account = self.store.account("t1")
        self.assertEqual(account.free_used, 4)
        self.assertEqual(account.available, 1)


class AccountTests(StoreTestCase):
    def test_an_account_appears_on_first_use(self):
        self.store.charge("t1", self._remote(), 1, username="data")
        self.assertEqual([item.id for item in self.store.list_accounts()], ["t1"])

    def test_the_opening_balance_is_configurable(self):
        os.environ["SPARQUET_STUDIO_CREDITS_INITIAL"] = "10"
        self.assertEqual(self.store.account("t1", "data").balance, 10)

    def test_a_rename_keeps_the_same_account(self):
        """The id pays, the team name is a label."""
        self.store.grant("t1", 5, username="data")
        account = self.store.account("t1", "data-engineering")
        self.assertEqual(account.balance, 5)
        self.assertEqual(account.username, "data-engineering")
        self.assertEqual(len(self.store.list_accounts()), 1)

    def test_credits_can_be_taken_back(self):
        self.store.grant("t1", 5, username="data")
        self.assertEqual(self.store.grant("t1", -2).balance, 3)

    def test_a_grant_cannot_push_an_account_below_zero(self):
        self.store.grant("t1", 1, username="data")
        with self.assertRaises(credits.CreditError):
            self.store.grant("t1", -5)
        self.assertEqual(self.store.account("t1").balance, 1)

    def test_a_grant_of_zero_is_refused(self):
        with self.assertRaises(credits.CreditError):
            self.store.grant("t1", 0, username="data")


class LedgerTests(StoreTestCase):
    def test_a_charge_records_what_it_paid_for(self):
        self.store.charge(
            "t1", self._remote(), 2, username="data", job_run_id="jr1",
            pipeline_run_id="pr1", job_name="vendas",
        )
        entry = self.store.ledger("t1")[0]
        self.assertEqual(entry.reason, credits.REASON_RUN)
        self.assertEqual(entry.amount, -2)
        self.assertEqual(entry.writes, 2)
        self.assertEqual(entry.job_run_id, "jr1")
        self.assertEqual(entry.pipeline_run_id, "pr1")
        self.assertEqual(entry.job_name, "vendas")
        self.assertEqual(entry.target, "spark://cluster:7077")
        self.assertEqual(entry.period, credits.current_period())

    def test_the_ledger_is_newest_first_and_scoped_to_one_account(self):
        self.store.grant("t1", 1, username="data")
        self.store.charge("t1", self._remote(), 1, username="data")
        self.store.grant("t2", 1, username="plataforma")
        reasons = [entry.reason for entry in self.store.ledger("t1")]
        self.assertEqual(reasons, [credits.REASON_RUN, credits.REASON_GRANT])
        self.assertEqual([entry.account_id for entry in self.store.ledger()], ["t2", "t1", "t1"])

    def test_an_unapplied_charge_says_so(self):
        """Metering rows have to be distinguishable from rows that cost somebody
        a credit, or a bill built from this table would be wrong."""
        self.store.charge("t1", self._remote(), 1, username="data")
        entry = self.store.ledger("t1")[0]
        self.assertFalse(entry.applied)
        self.assertEqual(entry.balance_after, 0)

    def test_charges_can_be_found_by_job_run(self):
        """This is what puts a price on the execution-history screen."""
        self.store.charge("t1", self._remote(), 2, username="data", job_run_id="jr1")
        self.store.charge("t1", self._remote(), 1, username="data", job_run_id="jr2")
        found = self.store.entries_for_job_runs(["jr1", "jr2", "missing"])
        self.assertEqual(sorted(found), ["jr1", "jr2"])
        self.assertEqual(found["jr1"].writes, 2)
        self.assertEqual(self.store.entries_for_job_runs([]), {})

    def test_usage_separates_what_was_waived_from_what_was_charged(self):
        """"You used 40 of your 40 free" and "you owe 40" are not the same
        sentence, and the difference is this column."""
        self._enforce(True)
        self._free(3)
        self.store.grant("t1", 10, username="data")
        self.store.charge("t1", self._remote(), 5, username="data")
        usage = self.store.usage("t1")
        self.assertEqual(usage["writes"], 5)
        self.assertEqual(usage["charged"], 5)
        self.assertEqual(usage["waived"], 3)


class TagTests(StoreTestCase):
    """Billing by tag: labels frozen on the entry, and rows that overlap.

    Tags are the one dimension that does not partition the ledger — a run wearing
    two of them is counted under both — so what is protected here is that the
    overlap is deliberate and that the month's total never inherits it.
    """

    def setUp(self):
        super().setUp()
        self._enforce(True)
        self._free(0)
        self.store.grant("t1", 100, username="data")

    def _run(self, writes, **kwargs):
        return self.store.charge("t1", self._remote(), writes, **kwargs)

    def _by_key(self, rows):
        return {row["key"]: row["charged"] for row in rows}

    def test_groups_by_tag(self):
        self._run(2, job_name="vendas", tags=["finance", "nightly"])
        self._run(3, job_name="estoque", tags=["finance"])
        rows = self.store.breakdown(group_by="tag", account_id="t1")
        self.assertEqual(self._by_key(rows), {"finance": 5, "nightly": 2})

    def test_a_run_counts_in_full_under_each_of_its_tags(self):
        """The rows overlap on purpose: "what does finance cost me" does not care
        that the run was also nightly."""
        self._run(4, tags=["finance", "nightly"])
        rows = self.store.breakdown(group_by="tag", account_id="t1")
        self.assertEqual(self._by_key(rows), {"finance": 4, "nightly": 4})
        self.assertEqual(sum(row["charged"] for row in rows), 8)
        self.assertEqual(self.store.totals(account_id="t1")["charged"], 4)

    def test_untagged_spending_is_its_own_row(self):
        self._run(2, tags=["finance"])
        self._run(3)
        rows = self.store.breakdown(group_by="tag", account_id="t1")
        self.assertEqual(self._by_key(rows), {"finance": 2, None: 3})

    def test_a_month_with_no_tag_at_all_is_one_row(self):
        self._run(3)
        rows = self.store.breakdown(group_by="tag", account_id="t1")
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["key"])

    def test_an_empty_month_has_no_untagged_row(self):
        self.assertEqual(self.store.breakdown(group_by="tag", account_id="t1"), [])

    def test_the_tags_are_frozen_on_the_entry(self):
        """Retagging a Job says what it costs from now on. A closed month does not
        change because somebody renamed a cost centre."""
        self._run(2, tags=["finance"])
        entry = self.store.ledger(account_id="t1", limit=1)[0]
        self.assertEqual(entry.tags, ["finance"])

    def test_tags_are_deduplicated_case_insensitively(self):
        """`Prod` and `prod` as two rows would split a bill for a reason nobody
        could guess from the screen."""
        self._run(2, tags=["Prod", "prod", " prod "])
        rows = self.store.breakdown(group_by="tag", account_id="t1")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["key"], "Prod")

    def test_junk_never_reaches_the_ledger(self):
        self._run(2, tags=["  ", "", None, 7, "boa"])
        rows = self.store.breakdown(group_by="tag", account_id="t1")
        self.assertEqual(self._by_key(rows), {"boa": 2})

    def test_the_number_of_tags_is_bounded(self):
        self._run(2, tags=[f"tag-{index}" for index in range(credits.MAX_TAGS + 10)])
        rows = self.store.breakdown(group_by="tag", account_id="t1")
        self.assertEqual(len(rows), credits.MAX_TAGS)

    def test_another_account_is_not_in_the_slice(self):
        self.store.grant("t2", 100, username="plataforma")
        self._run(2, tags=["finance"])
        self.store.charge("t2", self._remote(), 5, tags=["finance"])
        rows = self.store.breakdown(group_by="tag", account_id="t1")
        self.assertEqual(self._by_key(rows), {"finance": 2})
        self.assertEqual(
            self._by_key(self.store.breakdown(group_by="tag")), {"finance": 7}
        )

    def test_a_settled_run_carries_its_tags(self):
        hold = self.store.reserve("t1", self._remote(), 2, username="data")
        self.store.settle(
            hold, self._remote(), 2, account_id="t1", tags=["finance"]
        )
        rows = self.store.breakdown(group_by="tag", account_id="t1")
        self.assertEqual(self._by_key(rows), {"finance": 2})

    def test_grants_are_not_tagged_spending(self):
        self.store.grant("t1", 50, note="top-up")
        self.assertEqual(self.store.breakdown(group_by="tag", account_id="t1"), [])


class TotalsAndTimelineTests(StoreTestCase):
    """The month's real total, and the series that says whether it is normal."""

    def setUp(self):
        super().setUp()
        self._enforce(True)
        self._free(0)
        self.store.grant("t1", 100, username="data")

    def test_totals_count_each_run_once(self):
        self.store.charge("t1", self._remote(), 4, tags=["a", "b", "c"])
        totals = self.store.totals(account_id="t1")
        self.assertEqual(totals["charged"], 4)
        self.assertEqual(totals["runs"], 1)
        self.assertEqual(totals["writes"], 4)

    def test_totals_of_the_whole_runner(self):
        self.store.grant("t2", 100)
        self.store.charge("t1", self._remote(), 2)
        self.store.charge("t2", self._remote(), 3)
        self.assertEqual(self.store.totals()["charged"], 5)
        self.assertEqual(self.store.totals(account_id="t1")["charged"], 2)

    def test_the_timeline_ends_in_the_current_month(self):
        self.store.charge("t1", self._remote(), 2)
        periods = self.store.usage_timeline(months=6, account_id="t1")
        self.assertEqual(len(periods), 6)
        self.assertEqual(periods[-1]["period"], credits.current_period())
        self.assertEqual(periods[-1]["charged"], 2)

    def test_a_quiet_month_is_a_zero_not_a_gap(self):
        """A missing month would make the line lie about the shape of the
        spending. It reads as zero, which is what it was."""
        periods = self.store.usage_timeline(months=3, account_id="t1")
        self.assertEqual([row["charged"] for row in periods], [0, 0, 0])
        self.assertEqual(len({row["period"] for row in periods}), 3)

    def test_the_series_is_oldest_first(self):
        periods = self.store.usage_timeline(months=4)
        self.assertEqual([row["period"] for row in periods], sorted(
            row["period"] for row in periods
        ))

    def test_the_length_is_bounded(self):
        self.assertEqual(len(self.store.usage_timeline(months=999)), 36)
        self.assertEqual(len(self.store.usage_timeline(months=0)), 1)


class PrincipalTests(unittest.TestCase):
    class _Principal:
        def __init__(self, username, user_id=None, team_id=None, team_name=None,
                     token_only=False):
            self.username = username
            self.user_id = user_id
            self.team_id = team_id
            self.team_name = team_name
            self.token_only = token_only

    def test_the_team_pays_for_what_its_members_run(self):
        account_id, label = credits.account_for(
            self._Principal("ana", "u1", team_id="t1", team_name="data")
        )
        self.assertEqual((account_id, label), ("t1", "data"))

    def test_a_user_without_a_team_still_pays(self):
        """Should not happen — `auth` puts everyone in a team — but refusing to
        run would be a worse answer than charging the person."""
        account_id, label = credits.account_for(self._Principal("ana", "u1"))
        self.assertEqual((account_id, label), ("u1", "ana"))

    def test_token_only_mode_has_one_shared_account(self):
        account_id, _ = credits.account_for(
            self._Principal("local", team_id="t1", token_only=True)
        )
        self.assertEqual(account_id, credits.TOKEN_ACCOUNT)
        self.assertEqual(credits.account_for(None)[0], credits.TOKEN_ACCOUNT)

    def test_the_actor_is_the_person_not_the_payer(self):
        """The team pays; the bill still has to say who spent it."""
        principal = self._Principal("ana", "u1", team_id="t1", team_name="data")
        self.assertEqual(credits.account_for(principal)[0], "t1")
        self.assertEqual(credits.actor_for(principal), "ana")

    def test_a_shared_token_names_nobody(self):
        self.assertIsNone(credits.actor_for(None))
        self.assertIsNone(
            credits.actor_for(self._Principal("local", "u1", token_only=True))
        )


class BreakdownTests(StoreTestCase):
    """Reading one month back by team, by person, by workflow and by job."""

    def setUp(self):
        super().setUp()
        self._enforce(True)
        self._free(0)
        self.store.grant("t1", 100, username="data")
        self.store.grant("t2", 100, username="plataforma")

    def _run(self, account, writes, **kwargs):
        return self.store.charge(account, self._remote(), writes, **kwargs)

    def _by_key(self, rows):
        return {row["key"]: row["charged"] for row in rows}

    def test_groups_by_workflow(self):
        self._run("t1", 2, workflow_id="w1", job_name="vendas", actor="ana")
        self._run("t1", 3, workflow_id="w2", job_name="estoque", actor="bruno")
        self._run("t1", 1, workflow_id="w1", job_name="vendas", actor="bruno")
        rows = self.store.breakdown(group_by="workflow", account_id="t1")
        self.assertEqual(self._by_key(rows), {"w1": 3, "w2": 3})
        self.assertEqual([row["runs"] for row in rows if row["key"] == "w1"], [2])

    def test_groups_by_user_and_by_job(self):
        self._run("t1", 2, workflow_id="w1", job_name="vendas", actor="ana")
        self._run("t1", 5, workflow_id="w1", job_name="estoque", actor="bruno")
        self.assertEqual(
            self._by_key(self.store.breakdown(group_by="user", account_id="t1")),
            {"ana": 2, "bruno": 5},
        )
        self.assertEqual(
            self._by_key(self.store.breakdown(group_by="job", account_id="t1")),
            {"vendas": 2, "estoque": 5},
        )

    def test_grouping_by_team_spans_the_accounts(self):
        self._run("t1", 2, workflow_id="w1", actor="ana")
        self._run("t2", 4, workflow_id="w1", actor="ana")
        rows = self.store.breakdown(group_by="team")
        self.assertEqual(self._by_key(rows), {"t1": 2, "t2": 4})
        self.assertEqual(
            {row["key"]: row["label"] for row in rows},
            {"t1": "data", "t2": "plataforma"},
        )

    def test_an_account_only_sees_itself_when_scoped(self):
        self._run("t1", 2, workflow_id="w1")
        self._run("t2", 4, workflow_id="w1")
        rows = self.store.breakdown(group_by="workflow", account_id="t1")
        self.assertEqual(self._by_key(rows), {"w1": 2})

    def test_spending_with_no_workflow_is_reported_not_dropped(self):
        """A run from a script belongs to no workflow. A total that omitted it
        would be wrong; a row that says "unattributed" is merely honest."""
        self._run("t1", 2, workflow_id="w1")
        self._run("t1", 3)
        rows = self.store.breakdown(group_by="workflow", account_id="t1")
        self.assertEqual(self._by_key(rows), {"w1": 2, None: 3})
        self.assertEqual(sum(row["charged"] for row in rows), 5)

    def test_grants_are_not_spending(self):
        self._run("t1", 2, workflow_id="w1")
        self.store.grant("t1", 50, note="top-up")
        rows = self.store.breakdown(group_by="workflow", account_id="t1")
        self.assertEqual(self._by_key(rows), {"w1": 2})

    def test_another_month_is_another_bill(self):
        self._run("t1", 2, workflow_id="w1")
        rows = self.store.breakdown(group_by="workflow", period="1999-01")
        self.assertEqual(rows, [])

    def test_what_the_allowance_covered_is_reported_apart(self):
        """`charged` is the whole cost and `waived` is the part the free
        allowance absorbed — the same convention `usage()` already uses, so a
        month that cost nothing out of pocket reads charged 4, waived 4."""
        self._free(10)
        self._run("t1", 4, workflow_id="w1")
        rows = self.store.breakdown(group_by="workflow", account_id="t1")
        self.assertEqual(rows[0]["charged"], 4)
        self.assertEqual(rows[0]["waived"], 4)
        self.assertEqual(rows[0]["writes"], 4)
        self.assertEqual(self.store.account("t1").balance, 100)

    def test_an_unknown_grouping_is_refused(self):
        """The dimension goes straight into the SQL, so nothing but the four
        known columns may ever reach it."""
        with self.assertRaises(credits.CreditError):
            self.store.breakdown(group_by="account_id; DROP TABLE ledger")

    def test_a_settled_run_carries_its_attribution(self):
        hold = self.store.reserve("t1", self._remote(), 2, username="data")
        self.store.settle(
            hold, self._remote(), 2, account_id="t1",
            workflow_id="w1", actor="ana", job_name="vendas",
        )
        rows = self.store.breakdown(group_by="user", account_id="t1")
        self.assertEqual(self._by_key(rows), {"ana": 2})


if __name__ == "__main__":
    unittest.main(verbosity=2)
