"""What a schedule means, pinned against a fixed clock.

Two things are worth being sure of here and neither needs a thread. The first is
that an expression is read the way every other cron reads it — the day-of-month
and weekday union, Sunday being both 0 and 7, a step over a range — because an
expression that is *almost* understood runs a Job at a time nobody chose. The
second is the catch-up rule: a runner that was off must come back and run at most
one occurrence, and only if it is recent, or switching a laptop on in the morning
replays the night.
"""

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import scheduling  # noqa: E402


def at(text: str) -> datetime:
    """`2026-09-13 06:00` as an instant in UTC."""
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def schedule(cron: str, **over) -> scheduling.Schedule:
    record = {"id": over.pop("id", "j1"), "name": over.pop("name", "Daily sales")}
    record["schedule"] = {"cron": cron, "timezone": over.pop("timezone", "UTC"), **over}
    return scheduling.from_record(scheduling.JOB, record)


class ParseCronTest(unittest.TestCase):
    def test_every_minute(self):
        parsed = scheduling.parse_cron("* * * * *")
        self.assertEqual(len(parsed.minutes), 60)
        self.assertEqual(len(parsed.hours), 24)
        self.assertFalse(parsed.day_union)

    def test_lists_ranges_and_steps(self):
        parsed = scheduling.parse_cron("0,30 9-17/4 * * *")
        self.assertEqual(sorted(parsed.minutes), [0, 30])
        self.assertEqual(sorted(parsed.hours), [9, 13, 17])

    def test_step_over_a_star(self):
        parsed = scheduling.parse_cron("*/15 * * * *")
        self.assertEqual(sorted(parsed.minutes), [0, 15, 30, 45])

    def test_a_bare_number_with_a_step_runs_to_the_top_of_the_field(self):
        # `5/10` is "from 5, every 10" in every cron that accepts it.
        parsed = scheduling.parse_cron("5/10 * * * *")
        self.assertEqual(sorted(parsed.minutes), [5, 15, 25, 35, 45, 55])

    def test_names_for_months_and_weekdays(self):
        parsed = scheduling.parse_cron("0 6 * jan mon-fri")
        self.assertEqual(sorted(parsed.months), [1])
        self.assertEqual(sorted(parsed.weekdays), [1, 2, 3, 4, 5])

    def test_sunday_is_both_zero_and_seven(self):
        self.assertEqual(
            scheduling.parse_cron("0 0 * * 7").weekdays,
            scheduling.parse_cron("0 0 * * 0").weekdays,
        )

    def test_six_fields_are_refused_rather_than_guessed_at(self):
        # Reading `0 0 6 * * *` as a five-field expression would run a daily Job
        # every minute.
        with self.assertRaises(scheduling.CronError):
            scheduling.parse_cron("0 0 6 * * *")

    def test_out_of_range_and_nonsense(self):
        for expression in ("99 * * * *", "* 25 * * *", "0 0 * * funday", "0 0 * * */0"):
            with self.assertRaises(scheduling.CronError, msg=expression):
                scheduling.parse_cron(expression)

    def test_a_range_that_ends_before_it_starts(self):
        with self.assertRaises(scheduling.CronError):
            scheduling.parse_cron("0 17-9 * * *")


class DayFieldsTest(unittest.TestCase):
    def test_both_day_fields_restricted_are_unioned(self):
        # The one thing about cron that surprises everybody exactly once.
        parsed = scheduling.parse_cron("0 0 1 * mon")
        self.assertTrue(parsed.day_union)
        # 2026-09-01 is a Tuesday: matched by the day of the month alone.
        self.assertTrue(parsed.matches(at("2026-09-01 00:00")))
        # 2026-09-07 is a Monday, not the first: matched by the weekday alone.
        self.assertTrue(parsed.matches(at("2026-09-07 00:00")))
        self.assertFalse(parsed.matches(at("2026-09-02 00:00")))

    def test_one_day_field_restricted_is_an_ordinary_match(self):
        parsed = scheduling.parse_cron("0 0 * * mon")
        self.assertFalse(parsed.day_union)
        self.assertTrue(parsed.matches(at("2026-09-07 00:00")))
        self.assertFalse(parsed.matches(at("2026-09-08 00:00")))


class NextFireTest(unittest.TestCase):
    def test_strictly_after_the_moment_given(self):
        daily = schedule("0 6 * * *")
        # Asked at exactly six, the answer is tomorrow — otherwise a sweep that
        # runs twice in the same minute would fire the same occurrence twice.
        self.assertEqual(
            scheduling.next_fire(daily, at("2026-09-13 06:00")),
            at("2026-09-14 06:00"),
        )

    def test_later_the_same_day(self):
        self.assertEqual(
            scheduling.next_fire(schedule("0 6,18 * * *"), at("2026-09-13 07:00")),
            at("2026-09-13 18:00"),
        )

    def test_skips_to_the_next_matching_weekday(self):
        # 2026-09-13 is a Sunday.
        self.assertEqual(
            scheduling.next_fire(schedule("0 6 * * mon-fri"), at("2026-09-13 07:00")),
            at("2026-09-14 06:00"),
        )

    def test_a_sparse_expression_still_resolves(self):
        self.assertEqual(
            scheduling.next_fire(schedule("0 0 29 2 *"), at("2026-09-13 00:00")),
            at("2028-02-29 00:00"),
        )

    def test_an_unparseable_schedule_fires_at_no_time_at_all(self):
        broken = schedule("not a cron")
        self.assertIsNotNone(broken.error)
        self.assertFalse(broken.runnable)
        self.assertIsNone(scheduling.next_fire(broken, at("2026-09-13 00:00")))

    def test_the_expression_is_read_in_its_own_timezone(self):
        # Six in São Paulo is nine in UTC: the whole reason the field exists.
        local = schedule("0 6 * * *", timezone="America/Sao_Paulo")
        self.assertEqual(
            scheduling.next_fire(local, at("2026-09-13 00:00")),
            at("2026-09-13 09:00"),
        )

    def test_an_unknown_timezone_falls_back_instead_of_never_firing(self):
        # A Job that stops running because the tz database is missing is a
        # silence nobody notices until the data is two days old.
        odd = schedule("*/5 * * * *", timezone="Mars/Olympus")
        self.assertIsNone(odd.error)
        self.assertIsNotNone(scheduling.next_fire(odd, at("2026-09-13 00:00")))


class DueAtTest(unittest.TestCase):
    def test_nothing_is_due_before_the_time_comes(self):
        self.assertIsNone(
            scheduling.due_at(
                schedule("0 6 * * *"),
                anchor=at("2026-09-13 00:00"),
                now=at("2026-09-13 05:59"),
                grace_seconds=900,
            )
        )

    def test_the_occurrence_that_has_just_passed_is_due(self):
        self.assertEqual(
            scheduling.due_at(
                schedule("0 6 * * *"),
                anchor=at("2026-09-13 05:59"),
                now=at("2026-09-13 06:00"),
                grace_seconds=900,
            ),
            at("2026-09-13 06:00"),
        )

    def test_an_occurrence_already_fired_is_not_due_again(self):
        self.assertIsNone(
            scheduling.due_at(
                schedule("0 6 * * *"),
                anchor=at("2026-09-13 06:00"),
                now=at("2026-09-13 06:00"),
                grace_seconds=900,
            )
        )

    def test_a_night_of_missed_runs_collapses_into_one(self):
        # The runner was off from six in the evening to eight in the morning and
        # an hourly Job missed fourteen occurrences. It comes back and runs once.
        due = scheduling.due_at(
            schedule("0 * * * *"),
            anchor=at("2026-09-12 18:00"),
            now=at("2026-09-13 08:05"),
            grace_seconds=900,
        )
        self.assertEqual(due, at("2026-09-13 08:00"))

    def test_an_occurrence_older_than_the_grace_window_is_dropped(self):
        # Nobody wants yesterday's six o'clock run starting at half past nine.
        self.assertIsNone(
            scheduling.due_at(
                schedule("0 6 * * *"),
                anchor=at("2026-09-12 00:00"),
                now=at("2026-09-13 09:30"),
                grace_seconds=900,
            )
        )

    def test_a_short_restart_does_not_lose_the_occurrence(self):
        # The runner restarted at 06:00:30 with no anchor at all. The six o'clock
        # run is thirty seconds old, well inside the window, so it still runs.
        self.assertEqual(
            scheduling.due_at(
                schedule("0 6 * * *"),
                anchor=None,
                now=at("2026-09-13 06:00") + timedelta(seconds=30),
                grace_seconds=900,
            ),
            at("2026-09-13 06:00"),
        )

    def test_a_paused_schedule_is_never_due(self):
        paused = schedule("* * * * *", enabled=False)
        self.assertFalse(paused.runnable)
        self.assertIsNone(
            scheduling.due_at(
                paused, anchor=None, now=at("2026-09-13 06:00"), grace_seconds=900
            )
        )


class FromRecordTest(unittest.TestCase):
    def test_a_record_with_no_schedule_block(self):
        self.assertIsNone(scheduling.from_record("job", {"id": "j1", "name": "x"}))

    def test_a_schedule_with_no_expression_is_not_a_schedule(self):
        self.assertIsNone(
            scheduling.from_record("job", {"id": "j1", "schedule": {"cron": "  "}})
        )

    def test_the_workspace_id_wins_over_the_record_one(self):
        # The file is the record's address; a stale `id` inside it must not point
        # a schedule at a different Job.
        found = scheduling.from_record(
            "job", {"id": "old", "schedule": {"cron": "0 6 * * *"}}, "j9"
        )
        self.assertEqual(found.id, "j9")

    def test_defaults(self):
        found = scheduling.from_record("job", {"id": "j1", "schedule": {"cron": "0 6 * * *"}})
        self.assertTrue(found.enabled)
        self.assertEqual(found.timezone, scheduling.LOCAL)
        self.assertEqual(found.run_as, "")
        self.assertIn("local time", found.describe())

    def test_run_as_in_either_spelling(self):
        # The Studio writes `runAs`; a file edited by hand may say `run_as`.
        for key in ("runAs", "run_as"):
            found = scheduling.from_record(
                "job", {"id": "j1", "schedule": {"cron": "0 6 * * *", key: "ana"}}
            )
            self.assertEqual(found.run_as, "ana")

    def test_an_unknown_kind_is_not_schedulable(self):
        self.assertIsNone(
            scheduling.from_record("query", {"id": "q1", "schedule": {"cron": "0 6 * * *"}})
        )

    def test_read_schedules_skips_what_has_none(self):
        found = scheduling.read_schedules([
            ("job", "j1", {"name": "One", "schedule": {"cron": "0 6 * * *"}}),
            ("job", "j2", {"name": "Two"}),
            ("pipeline", "p1", {"name": "Nightly", "schedule": {"cron": "@daily"}}),
        ])
        self.assertEqual([item.id for item in found], ["j1", "p1"])
        # The macro form is not supported, and says so instead of being silent.
        self.assertIsNotNone(found[1].error)
        self.assertIn("invalid schedule", found[1].describe())


class OrderStagesTest(unittest.TestCase):
    def test_links_decide_the_order(self):
        stages = [{"id": "b"}, {"id": "a"}, {"id": "c"}]
        links = [{"source": "a", "target": "b"}, {"source": "b", "target": "c"}]
        ordered, cyclic = scheduling.order_stages(stages, links)
        self.assertEqual([stage["id"] for stage in ordered], ["a", "b", "c"])
        self.assertEqual(cyclic, [])

    def test_unlinked_stages_are_sorted_by_name(self):
        # The same tie-break the Studio uses, so the canvas and the schedule
        # cannot disagree about what runs first.
        stages = [{"id": "s2"}, {"id": "s1"}]
        ordered, _ = scheduling.order_stages(
            stages, [], {"s1": "Zebra", "s2": "Alpha"}
        )
        self.assertEqual([stage["id"] for stage in ordered], ["s2", "s1"])

    def test_a_cycle_is_reported_and_run_last(self):
        stages = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
        links = [{"source": "b", "target": "c"}, {"source": "c", "target": "b"}]
        ordered, cyclic = scheduling.order_stages(stages, links)
        self.assertEqual([stage["id"] for stage in ordered], ["a", "b", "c"])
        self.assertEqual(cyclic, ["b", "c"])

    def test_a_link_to_a_stage_that_is_gone_is_ignored(self):
        ordered, cyclic = scheduling.order_stages(
            [{"id": "a"}], [{"source": "a", "target": "deleted"}]
        )
        self.assertEqual([stage["id"] for stage in ordered], ["a"])
        self.assertEqual(cyclic, [])


class EnvironmentTest(unittest.TestCase):
    def test_on_unless_turned_off(self):
        self.assertTrue(scheduling.scheduling_enabled({}))
        for value in ("off", "0", "false", "no", "OFF"):
            self.assertFalse(scheduling.scheduling_enabled({"SPARQUET_STUDIO_SCHEDULER": value}))

    def test_interval_is_floored(self):
        self.assertEqual(scheduling.interval_seconds({}), scheduling.DEFAULT_INTERVAL_SECONDS)
        self.assertEqual(
            scheduling.interval_seconds({"SPARQUET_STUDIO_SCHEDULER_INTERVAL": "1"}), 5
        )
        self.assertEqual(
            scheduling.interval_seconds({"SPARQUET_STUDIO_SCHEDULER_INTERVAL": "nope"}),
            scheduling.DEFAULT_INTERVAL_SECONDS,
        )

    def test_grace_can_be_turned_off_but_not_made_negative(self):
        self.assertEqual(scheduling.grace_seconds({}), scheduling.DEFAULT_GRACE_SECONDS)
        self.assertEqual(scheduling.grace_seconds({"SPARQUET_STUDIO_SCHEDULER_GRACE": "0"}), 0)
        self.assertEqual(scheduling.grace_seconds({"SPARQUET_STUDIO_SCHEDULER_GRACE": "-5"}), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
