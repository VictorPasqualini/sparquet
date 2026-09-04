"""Estratégia de particionamento: `repartition` e os transforms do Iceberg.

Cobre só o que não precisa de Spark — a validação de parâmetros do `repartition`
(que roda antes de tocar o DataFrame) e o parse de `partition_by` do Iceberg.

    python tests/transform/test_partitioning.py
"""
from __future__ import annotations

import unittest

from sparquet.core.config import TransformationConfig
from sparquet.io.iceberg import parse_partition_spec
from sparquet.transform.builtin import RepartitionTransformation


def repartition(**params):
    return RepartitionTransformation(TransformationConfig(type="repartition", params=params))


class SpyDF:
    """Registra qual método de reparticionamento foi chamado, e com o quê."""

    def __init__(self) -> None:
        self.calls: list = []

    def _record(self, name):
        def call(*args):
            self.calls.append((name, args))
            return self
        return call

    def __getattr__(self, name):
        if name in ("repartition", "repartitionByRange", "coalesce"):
            return self._record(name)
        raise AttributeError(name)


class TestRepartitionParams(unittest.TestCase):
    def test_neither_num_nor_columns_raises(self):
        with self.assertRaises(ValueError) as ctx:
            repartition().apply(SpyDF())
        self.assertIn("num_partitions", str(ctx.exception))

    def test_zero_partitions_raises(self):
        with self.assertRaises(ValueError):
            repartition(num_partitions=0).apply(SpyDF())

    def test_non_integer_partitions_raises(self):
        with self.assertRaises(ValueError):
            repartition(num_partitions="oito").apply(SpyDF())

    def test_coalesce_with_columns_raises(self):
        with self.assertRaises(ValueError) as ctx:
            repartition(num_partitions=1, columns=["dt"], coalesce=True).apply(SpyDF())
        self.assertIn("coalesce", str(ctx.exception))

    def test_coalesce_without_num_raises(self):
        with self.assertRaises(ValueError):
            repartition(coalesce=True, columns=[]).apply(SpyDF())

    def test_coalesce_and_range_are_exclusive(self):
        with self.assertRaises(ValueError):
            repartition(num_partitions=1, coalesce=True, range=True).apply(SpyDF())

    def test_range_without_columns_raises(self):
        with self.assertRaises(ValueError) as ctx:
            repartition(num_partitions=8, range=True).apply(SpyDF())
        self.assertIn("range", str(ctx.exception))

    def test_num_only_calls_repartition(self):
        df = SpyDF()
        repartition(num_partitions=200).apply(df)
        self.assertEqual(df.calls, [("repartition", (200,))])

    def test_coalesce_calls_coalesce(self):
        df = SpyDF()
        repartition(num_partitions=1, coalesce=True).apply(df)
        self.assertEqual(df.calls, [("coalesce", (1,))])


class TestIcebergPartitionSpec(unittest.TestCase):
    def test_plain_names_are_identity(self):
        specs, has_transform = parse_partition_spec(["regiao", "uf"])
        self.assertFalse(has_transform)
        self.assertEqual(specs, [("identity", "regiao"), ("identity", "uf")])

    def test_bucket_and_time_transforms(self):
        specs, has_transform = parse_partition_spec(["bucket(16, id)", "days(ts)", "regiao"])
        self.assertTrue(has_transform)
        self.assertEqual(
            specs, [("bucket", 16, "id"), ("days", "ts"), ("identity", "regiao")]
        )

    def test_empty_partition_by_has_no_transform(self):
        self.assertEqual(parse_partition_spec([]), ([], False))

    def test_bucket_arity_and_count_are_checked(self):
        for bad in ("bucket(id)", "bucket(0, id)", "bucket(16, a, b)", "bucket(n, id)"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                parse_partition_spec([bad])

    def test_time_transform_takes_one_column(self):
        with self.assertRaises(ValueError):
            parse_partition_spec(["days(a, b)"])

    def test_unknown_transform_names_the_supported_set(self):
        with self.assertRaises(ValueError) as ctx:
            parse_partition_spec(["truncate(4, cep)"])
        self.assertIn("bucket", str(ctx.exception))


if __name__ == "__main__":
    unittest.main(verbosity=2)
