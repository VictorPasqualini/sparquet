"""Parsing of the scoped/annotated quarantine keys — pure, no SparkSession.

`validations.outputs.{valid,invalid}` gained two keys that only make sense where a row
was actually rejected:

- `rules`    — the rule CODES that feed this destination (absent = every row-level rule);
- `annotate` — the `array<string>` column, on `invalid` only, holding the codes of the
               rules that rejected each row.

Both are config, so both are decided here — before any Spark call. What is tested is
mostly the REFUSALS: a key that reaches the JSON and then quietly does nothing is worse
than a config error, because the pipeline keeps promising traceability it never delivers.

    PYTHONPATH=. python tests/validation/test_quarantine_config.py
"""
from __future__ import annotations

import unittest

from sparquet.core.config import PipelineConfig


def config(validations: dict, output: dict | None = None, outputs: list | None = None):
    data = {
        "name": "quarantine",
        "input": {"format": "csv", "path": "/data/in"},
        "validations": validations,
    }
    if outputs is not None:
        data["outputs"] = outputs
    else:
        data["output"] = output or {"format": "delta", "path": "silver.main"}
    return PipelineConfig.from_dict(data)


RULES = [
    {"type": "not_null", "columns": ["email"]},
    {"type": "range", "column": "age", "min": 1, "max": 99, "code": "AGE_RANGE"},
    {"type": "row_count", "min": 1},
]


class TestQuarantineKeys(unittest.TestCase):
    def test_rules_and_annotate_are_parsed_on_invalid(self):
        cfg = config({
            "rules": RULES,
            "outputs": {
                "invalid": {
                    "format": "delta",
                    "path": "silver.quarantine",
                    "rules": ["AGE_RANGE", "not_null(email)"],
                    "annotate": "dq_codes",
                }
            },
        })
        invalid = cfg.validations.outputs["invalid"]
        self.assertEqual(invalid.rules, ["AGE_RANGE", "not_null(email)"])
        self.assertEqual(invalid.annotate, "dq_codes")

    def test_absent_keys_stay_none(self):
        """No scoping and no annotation is the historical behaviour, untouched."""
        cfg = config({
            "rules": RULES,
            "outputs": {"invalid": {"format": "delta", "path": "silver.quarantine"}},
        })
        invalid = cfg.validations.outputs["invalid"]
        self.assertIsNone(invalid.rules)
        self.assertIsNone(invalid.annotate)

    def test_valid_may_be_scoped_too(self):
        cfg = config({
            "rules": RULES,
            "outputs": {
                "valid": {"format": "delta", "path": "silver.ok", "rules": ["AGE_RANGE"]}
            },
        })
        self.assertEqual(cfg.validations.outputs["valid"].rules, ["AGE_RANGE"])

    def test_codes_are_trimmed(self):
        cfg = config({
            "rules": RULES,
            "outputs": {
                "invalid": {
                    "format": "delta",
                    "path": "silver.quarantine",
                    "rules": [" AGE_RANGE "],
                    "annotate": " dq_codes ",
                }
            },
        })
        invalid = cfg.validations.outputs["invalid"]
        self.assertEqual(invalid.rules, ["AGE_RANGE"])
        self.assertEqual(invalid.annotate, "dq_codes")


class TestAnnotateIsInvalidOnly(unittest.TestCase):
    """`annotate` outside the `invalid` quarantine is refused, with the reason."""

    def assertRefused(self, build, *, mentions: str):
        with self.assertRaises(ValueError) as caught:
            build()
        message = str(caught.exception)
        self.assertIn("annotate", message)
        self.assertIn(mentions, message)
        # The message has to say WHY, not just that the key is wrong.
        self.assertIn("invalid", message)

    def test_refused_on_the_valid_quarantine(self):
        self.assertRefused(
            lambda: config({
                "rules": RULES,
                "outputs": {
                    "valid": {"format": "delta", "path": "silver.ok", "annotate": "dq_codes"}
                },
            }),
            mentions="nenhuma regra",
        )

    def test_refused_on_the_report(self):
        self.assertRefused(
            lambda: config({
                "rules": RULES,
                "report": {"format": "csv", "path": "/dq/report", "annotate": "dq_codes"},
            }),
            mentions="uma linha por REGRA",
        )

    def test_refused_on_the_single_main_output(self):
        self.assertRefused(
            lambda: config(
                {"rules": RULES},
                output={"format": "delta", "path": "silver.main", "annotate": "dq_codes"},
            ),
            mentions="df COMPLETO",
        )

    def test_refused_on_a_main_output_of_a_list(self):
        self.assertRefused(
            lambda: config(
                {"rules": RULES},
                outputs=[
                    {"format": "delta", "path": "silver.a"},
                    {"format": "delta", "path": "silver.b", "annotate": "dq_codes"},
                ],
            ),
            mentions="outputs[1]",
        )

    def test_rules_is_refused_on_a_main_output(self):
        with self.assertRaises(ValueError) as caught:
            config(
                {"rules": RULES},
                output={"format": "delta", "path": "silver.main", "rules": ["AGE_RANGE"]},
            )
        self.assertIn("rules", str(caught.exception))


class TestMalformedValues(unittest.TestCase):
    def quarantine(self, **keys):
        return config({
            "rules": RULES,
            "outputs": {"invalid": {"format": "delta", "path": "silver.q", **keys}},
        })

    def test_rules_must_be_a_list(self):
        # A bare string would be iterated character by character.
        with self.assertRaises(ValueError) as caught:
            self.quarantine(rules="AGE_RANGE")
        self.assertIn("LISTA", str(caught.exception))

    def test_rules_rejects_an_empty_code(self):
        with self.assertRaises(ValueError):
            self.quarantine(rules=["AGE_RANGE", "  "])

    def test_annotate_must_be_a_non_empty_name(self):
        for value in ("", "   ", 7, True):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    self.quarantine(annotate=value)


if __name__ == "__main__":
    unittest.main(verbosity=2)
