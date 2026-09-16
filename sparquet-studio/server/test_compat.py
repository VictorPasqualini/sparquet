"""The framework version range, and that the three places it is written agree."""
from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import compat  # noqa: E402


class ParseTest(unittest.TestCase):
    def test_reads_a_version(self):
        self.assertEqual(compat.parse("0.12.1"), (0, 12, 1))
        self.assertEqual(compat.parse("1.0"), (1, 0, 0))
        self.assertEqual(compat.parse("v0.12.0"), (0, 12, 0))

    def test_ignores_what_comes_after_the_numbers(self):
        self.assertEqual(compat.parse("0.12.0rc1"), (0, 12, 0))
        self.assertEqual(compat.parse("0.12.0+local"), (0, 12, 0))

    def test_gives_back_nothing_for_a_non_version(self):
        self.assertIsNone(compat.parse(None))
        self.assertIsNone(compat.parse(""))
        self.assertIsNone(compat.parse("unknown"))


class CheckTest(unittest.TestCase):
    def test_a_version_inside_the_range_is_supported_and_silent(self):
        result = compat.check("0.12.4")
        self.assertTrue(result.supported)
        self.assertIsNone(result.message)

    def test_too_old_says_so_and_gives_the_command(self):
        result = compat.check("0.11.9")
        self.assertFalse(result.supported)
        self.assertIn("0.11.9", result.message)
        self.assertIn("pip install", result.message)

    def test_too_new_says_nothing_has_proven_it(self):
        result = compat.check("0.13.0")
        self.assertFalse(result.supported)
        self.assertIn("0.13.0", result.message)

    def test_no_framework_is_not_an_incompatibility(self):
        # Absent is reported by `spark_available` and by the import error.
        # Calling it incompatible sends somebody after the wrong fix.
        self.assertTrue(compat.check(None).supported)
        self.assertIsNone(compat.check(None).message)


class DeclarationTest(unittest.TestCase):
    """`REQUIREMENT` is what an install reads and the tuples are what the runtime
    compares. A drift between them would be invisible until somebody installed a
    version the runner then refused."""

    def test_the_requirement_string_matches_the_tuples(self):
        found = re.fullmatch(
            r"sparquet>=(\d+)\.(\d+),<(\d+)\.(\d+)", compat.REQUIREMENT
        )
        self.assertIsNotNone(found, compat.REQUIREMENT)
        low = (int(found.group(1)), int(found.group(2)))
        high = (int(found.group(3)), int(found.group(4)))
        self.assertEqual(low, compat.MINIMUM)
        self.assertEqual(high, compat.BELOW)

    def test_requirements_txt_pins_the_same_range(self):
        text = (Path(__file__).resolve().parent / "requirements.txt").read_text(
            encoding="utf-8"
        )
        pinned = [
            line.strip()
            for line in text.splitlines()
            if line.strip().startswith("sparquet")
        ]
        self.assertEqual(pinned, [compat.REQUIREMENT])


if __name__ == "__main__":
    unittest.main(verbosity=2)
