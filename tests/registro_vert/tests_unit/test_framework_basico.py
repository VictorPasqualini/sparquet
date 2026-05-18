"""Sanity tests para validar que as features de framework usadas pelas confs
do registro_vert estão funcionando corretamente:

- skip_if_null em transformação
- with_columns (plural)
- group_by func=expr
- join with: self
- ${param} substitution
- OutputConfig.transformations
- ViewWriter com checkpoint
"""
from __future__ import annotations

import pytest
from pyspark.sql import Row


def test_skip_if_null_skipa_filter(fw, spark):
    """Quando param está None, o filter com skip_if_null é skipado."""
    df = spark.createDataFrame([Row(id=1), Row(id=2), Row(id=3)])
    df.createOrReplaceTempView("test_input")

    conf = {
        "name":  "test_skip",
        "input": {"format": "view", "path": "test_input"},
        "transformations": [
            {"type": "filter",
             "skip_if_null": "param_lista",
             "condition": "array_contains(param_lista, id)"}
        ],
        "output": {"format": "view", "path": "test_skip_out"},
    }

    # Param ausente → filter skipado → todas as linhas passam
    r = fw.run_from_dict(conf, columns={"param_lista": None})
    assert r.success
    assert spark.table("test_skip_out").count() == 3


def test_skip_if_null_aplica_filter_quando_param_presente(fw, spark):
    """Quando param tem valor, o filter é aplicado normalmente."""
    df = spark.createDataFrame([Row(id=1), Row(id=2), Row(id=3)])
    df.createOrReplaceTempView("test_input2")

    conf = {
        "name":  "test_skip2",
        "input": {"format": "view", "path": "test_input2"},
        "transformations": [
            {"type": "filter",
             "skip_if_null": "param_lista",
             "condition": "array_contains(param_lista, id)"}
        ],
        "output": {"format": "view", "path": "test_skip_out2"},
    }

    r = fw.run_from_dict(conf, columns={"param_lista": [1, 3]})
    assert r.success
    rows = sorted([r["id"] for r in spark.table("test_skip_out2").collect()])
    assert rows == [1, 3]


def test_with_columns_cria_multiplas_colunas(fw, spark):
    df = spark.createDataFrame([Row(x=10), Row(x=20)])
    df.createOrReplaceTempView("test_wc_in")

    conf = {
        "name":  "test_wc",
        "input": {"format": "view", "path": "test_wc_in"},
        "transformations": [
            {"type": "with_columns",
             "columns": [
                 {"name": "y", "expression": "x * 2"},
                 {"name": "z", "expression": "y + 1"},   # usa coluna criada anteriormente
             ]}
        ],
        "output": {"format": "view", "path": "test_wc_out"},
    }
    r = fw.run_from_dict(conf)
    assert r.success
    rows = sorted([(r["x"], r["y"], r["z"]) for r in spark.table("test_wc_out").collect()])
    assert rows == [(10, 20, 21), (20, 40, 41)]


def test_group_by_func_expr(fw, spark):
    df = spark.createDataFrame([
        Row(g="A", a="x", b="1"),
        Row(g="A", a="x", b="2"),
        Row(g="A", a="y", b="1"),
        Row(g="B", a="x", b="1"),
    ])
    df.createOrReplaceTempView("test_gb_in")

    conf = {
        "name":  "test_gb",
        "input": {"format": "view", "path": "test_gb_in"},
        "transformations": [
            {"type": "group_by",
             "by": ["g"],
             "agg": [
                 {"func": "expr",
                  "expression": "count(distinct struct(a, b)) > 1",
                  "alias": "multi"}
             ]}
        ],
        "output": {"format": "view", "path": "test_gb_out"},
    }
    r = fw.run_from_dict(conf)
    assert r.success
    rows = {r["g"]: r["multi"] for r in spark.table("test_gb_out").collect()}
    assert rows == {"A": True, "B": False}


def test_join_with_self(fw, spark):
    df = spark.createDataFrame([
        Row(id=1, cat="X"),
        Row(id=2, cat="Y"),
        Row(id=1, cat="X"),
    ])
    df.createOrReplaceTempView("test_self_in")

    conf = {
        "name":  "test_self",
        "input": {"format": "view", "path": "test_self_in"},
        "transformations": [
            {"type": "join",
             "with":  "self",
             "with_transformations": [
                 {"type": "group_by", "by": ["id"],
                  "agg": [{"func": "count", "alias": "n"}]}
             ],
             "on":  ["id"],
             "how": "left"}
        ],
        "output": {"format": "view", "path": "test_self_out"},
    }
    r = fw.run_from_dict(conf)
    assert r.success
    # id=1 aparece 2x → n=2; id=2 aparece 1x → n=1
    rows = sorted([(r["id"], r["n"]) for r in spark.table("test_self_out").select("id", "n").distinct().collect()])
    assert rows == [(1, 2), (2, 1)]


def test_param_substitution_em_path(fw, spark):
    """${param_name} em strings do JSON é substituído pelo valor de columns."""
    df = spark.createDataFrame([Row(x=1)])
    df.createOrReplaceTempView("test_sub_src")

    conf = {
        "name":  "test_sub",
        "input": {"format": "view", "path": "${param_input_view}"},
        "transformations": [],
        "output": {"format": "view", "path": "${param_output_view}"},
    }
    r = fw.run_from_dict(conf, columns={
        "param_input_view":  "test_sub_src",
        "param_output_view": "test_sub_out",
    })
    assert r.success, r.error
    assert spark.table("test_sub_out").count() == 1


def test_output_transformations(fw, spark):
    """OutputConfig.transformations aplica transforms específicas por output."""
    df = spark.createDataFrame([Row(a=1, b=2), Row(a=3, b=4)])
    df.createOrReplaceTempView("test_out_in")

    conf = {
        "name":  "test_out_transf",
        "input": {"format": "view", "path": "test_out_in"},
        "outputs": [
            {"format": "view", "path": "test_out_v1",
             "transformations": [{"type": "with_column", "name": "c", "expression": "a + b"}]},
            {"format": "view", "path": "test_out_v2",
             "transformations": [{"type": "with_column", "name": "c", "expression": "a * b"}]},
        ],
    }
    r = fw.run_from_dict(conf)
    assert r.success

    v1 = sorted([(r["a"], r["b"], r["c"]) for r in spark.table("test_out_v1").collect()])
    v2 = sorted([(r["a"], r["b"], r["c"]) for r in spark.table("test_out_v2").collect()])
    assert v1 == [(1, 2, 3), (3, 4, 7)]
    assert v2 == [(1, 2, 2), (3, 4, 12)]


def test_view_writer_checkpoint(fw, spark):
    """Quando 'checkpoint': true, o ViewWriter faz localCheckpoint."""
    spark.sparkContext.setCheckpointDir("/tmp/spark_fw_test_ckpt")
    df = spark.createDataFrame([Row(x=1), Row(x=2)])
    df.createOrReplaceTempView("test_ckpt_in")

    conf = {
        "name":  "test_ckpt",
        "input": {"format": "view", "path": "test_ckpt_in"},
        "transformations": [],
        "output": {"format": "view", "path": "test_ckpt_out",
                   "options": {"checkpoint": "true"}},
    }
    r = fw.run_from_dict(conf)
    assert r.success
    assert spark.table("test_ckpt_out").count() == 2
