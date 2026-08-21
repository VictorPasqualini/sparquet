"""Every shipped example must be a valid pipeline config.

These files are documentation *and* fixtures: the Studio round-trips them, the
README points at them, and users copy them. Parsing them here catches a broken
example before anyone runs it, and — because the list is discovered rather than
written down — a new example is covered the moment it is added.

Pure: `PipelineConfig.from_dict` only builds dataclasses, so no SparkSession and
no Java are needed. Runs with `PYTHONPATH=. python tests/test_examples.py`.
"""
from __future__ import annotations

import json
import unittest
from pathlib import Path

from sparquet.core.config import PipelineConfig

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"


def discover() -> list[Path]:
    return sorted(EXAMPLES_DIR.glob("*.json"))


class TestExamplesExist(unittest.TestCase):
    def test_directory_is_populated(self):
        self.assertTrue(
            EXAMPLES_DIR.is_dir(),
            f"{EXAMPLES_DIR} is missing. The examples are fixtures for the test "
            "suite (here and in sparquet-studio) — restore them with "
            "`git restore examples`.",
        )
        self.assertGreater(
            len(discover()), 0, f"No .json example found in {EXAMPLES_DIR}."
        )


class TestExamplesParse(unittest.TestCase):
    def test_every_example_parses(self):
        for path in discover():
            with self.subTest(example=path.name):
                data = json.loads(path.read_text(encoding="utf-8"))
                config = PipelineConfig.from_dict(data)
                # A config without a name or a destination would fail at runtime,
                # long after the user copied the file.
                self.assertTrue(config.name, "example has no 'name'")
                self.assertGreater(
                    len(config.outputs), 0, "example declares no output"
                )

    def test_declared_validations_are_known(self):
        """Um `type` de validação inexistente só falharia ao rodar o pipeline.

        Foi essa a lacuna que deixou o wrapper `check` sobreviver num exemplo depois
        de removido do registry: o teste cobria transformações e não validações.
        """
        from sparquet_cola import Cola

        known = set(Cola().available)
        for path in discover():
            with self.subTest(example=path.name):
                config = PipelineConfig.from_dict(
                    json.loads(path.read_text(encoding="utf-8"))
                )
                for rule in config.validations.rules:
                    self.assertIn(
                        rule.type, known, f"validação desconhecida em {path.name}"
                    )

    def test_quarantine_scopes_name_codes_that_exist(self):
        """Um codigo escrito errado em `outputs.*.rules` escopa a quarentena a NADA.

        E silencioso por natureza: a regra roda, o dataset e escrito, e sai vazio —
        indistinguivel de "nenhuma linha violou". O pipeline avisa em tempo de
        execucao (`_check_quarantine_scope`), o que so ajuda quem ja rodou; aqui os
        exemplos, que sao o que as pessoas copiam, sao conferidos antes disso.
        """
        from sparquet_cola import Cola

        cola = Cola()
        for path in discover():
            with self.subTest(example=path.name):
                data = json.loads(path.read_text(encoding="utf-8"))
                validations = data.get("validations") or {}
                outputs = validations.get("outputs") or {}
                scoped = [
                    code
                    for dataset in outputs.values()
                    if isinstance(dataset, dict)
                    for code in (dataset.get("rules") or [])
                ]
                if not scoped:
                    continue
                produced = set(cola.codes(validations.get("rules", [])))
                for code in scoped:
                    self.assertIn(
                        code,
                        produced,
                        f"{path.name}: nenhuma regra produz o codigo {code!r} — a "
                        f"quarentena escopada por ele sairia vazia. Produzidos: "
                        f"{sorted(produced)}",
                    )

    def test_declared_transformations_are_known(self):
        """A typo in an example would teach the wrong keyword to whoever copies it."""
        from sparquet.transform.engine import TransformationEngine

        known = set(TransformationEngine()._registry)
        for path in discover():
            with self.subTest(example=path.name):
                config = PipelineConfig.from_dict(
                    json.loads(path.read_text(encoding="utf-8"))
                )
                for step in config.transformations:
                    self.assertIn(step.type, known, f"unknown transformation in {path.name}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
