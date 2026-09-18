"""What the wheel is supposed to contain, checked against what is on disk.

Two things here can rot silently, and both are the kind of rot nobody notices
until somebody installs the package:

* **The package list.** `pyproject.toml` names the subpackages explicitly, so
  that no other project in this repository — `sparquet-studio/`, `docs/`,
  `tests/` — can end up in the distribution by accident. The cost of being
  explicit is that a subpackage added later is missing from the wheel while every
  test in the checkout keeps passing, because a checkout imports from the source
  tree.

* **The examples.** They ship as `sparquet.examples` because they are fixtures,
  not only documentation: the Studio's compiler is pinned to them, and the Studio
  is a separate repository that only sees what the installed package carries.

Pure: reads two files and walks a directory. Runs with
`PYTHONPATH=. python tests/test_packaging.py`.
"""
from __future__ import annotations

import json
import unittest
from pathlib import Path

import sparquet

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"


def load_pyproject() -> dict:
    try:
        import tomllib  # Python 3.11+
    except ModuleNotFoundError:  # pragma: no cover - only on 3.9/3.10
        import tomli as tomllib  # type: ignore[no-redef]
    return tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))


def packages_on_disk() -> set:
    """Every importable subpackage of `sparquet`, as dotted names."""
    package_root = ROOT / "sparquet"
    found = {"sparquet"}
    for init in package_root.rglob("__init__.py"):
        relative = init.parent.relative_to(package_root)
        if relative.parts:
            found.add("sparquet." + ".".join(relative.parts))
    return found


class TestDeclaredPackages(unittest.TestCase):
    def test_every_subpackage_is_declared(self):
        declared = set(load_pyproject()["tool"]["setuptools"]["packages"])
        missing = packages_on_disk() - declared
        self.assertEqual(
            missing,
            set(),
            "these subpackages exist but are not in pyproject.toml's "
            f"[tool.setuptools] packages, so they would not ship: {sorted(missing)}",
        )

    def test_no_declared_package_is_gone(self):
        config = load_pyproject()["tool"]["setuptools"]
        declared = set(config["packages"])
        # `sparquet.examples` lives at `examples/` through the package-dir
        # mapping, so it is not on disk under `sparquet/`.
        mapped = set(config.get("package-dir", {}))
        stale = declared - packages_on_disk() - mapped
        self.assertEqual(
            stale, set(), f"declared in pyproject.toml but not on disk: {sorted(stale)}"
        )


class TestPackagedExamples(unittest.TestCase):
    def test_examples_are_mapped_into_the_package(self):
        config = load_pyproject()["tool"]["setuptools"]
        self.assertEqual(
            config.get("package-dir", {}).get("sparquet.examples"),
            "examples",
            "the examples must be mapped into the wheel: they are the fixtures "
            "sparquet-studio's compiler tests run against, and a separate "
            "repository can only reach what the installed package carries.",
        )
        self.assertIn(
            "*.json",
            config.get("package-data", {}).get("sparquet.examples", []),
            "mapping the directory is not enough — the .json files themselves "
            "have to be declared as package data or setuptools leaves them out.",
        )

    def test_examples_path_finds_the_configs(self):
        path = sparquet.examples_path()
        self.assertTrue(path.is_dir(), f"{path} is not a directory")
        found = sorted(item.name for item in path.glob("*.json"))
        self.assertGreater(len(found), 0, f"no .json example under {path}")
        for name in found:
            with self.subTest(example=name):
                json.loads((path / name).read_text(encoding="utf-8"))

    def test_the_examples_directory_is_a_package(self):
        marker = ROOT / "examples" / "__init__.py"
        self.assertTrue(
            marker.is_file(),
            f"{marker} is what lets setuptools treat examples/ as a package. "
            "Without it the configs stop shipping and the Studio's round-trip "
            "tests lose their fixtures.",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
