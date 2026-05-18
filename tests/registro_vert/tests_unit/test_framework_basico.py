"""Sanity tests para validar features do framework usadas pelas confs:

- skip_if (expressão SQL avaliada com params substituídos)
- with_columns (plural)
- group_by func=expr
- ${param} substitution (SQL escape + !raw)
- OutputConfig.transformations
- select com expressões {name, expression}
- checkpoint/cache como transformação
"""
from __future__ import annotations

import pytest
from pyspark.sql import Row


def test_skip_if_skipa_quando_expr_falsa(fw, spark):
    """Quando ${param} é NULL, a expressão fica 'NULL IS NULL' (true) — skipa."""
    df = spark.createDataFrame([Row(id=1), Row(id=2), Row(id=3)])
    df.createOrReplaceTempView("test_input")

    conf = {
        "name":  "test_skip",
        "input": {"format": "view", "path": "test_input"},
        "transformations": [
            {"type": "filter",
             "skip_if":   "${param_lista} IS NULL",
             "condition": "array_contains(${param_lista}, id)"}
        ],
        "output": {"format": "view", "path": "test_skip_out"},
    }

    # Param None → ${param_lista} vira NULL → skip_if true → filter skipado
    r = fw.run_from_dict(conf, params={"param_lista": None})
    assert r.success, r.error
    assert spark.table("test_skip_out").count() == 3


def test_skip_if_aplica_quando_expr_verdadeira(fw, spark):
    """Quando param tem valor, ${param} vira array(...) — skip_if false → aplica."""
    df = spark.createDataFrame([Row(id=1), Row(id=2), Row(id=3)])
    df.createOrReplaceTempView("test_input2")

    conf = {
        "name":  "test_skip2",
        "input": {"format": "view", "path": "test_input2"},
        "transformations": [
            {"type": "filter",
             "skip_if":   "${param_lista} IS NULL",
             "condition": "array_contains(${param_lista}, id)"}
        ],
        "output": {"format": "view", "path": "test_skip_out2"},
    }

    r = fw.run_from_dict(conf, params={"param_lista": [1, 3]})
    assert r.success, r.error
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
                 {"name": "z", "expression": "y + 1"},
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


def test_param_substitution_sql_string(fw, spark):
    """${param} para string vira 'value' com aspas SQL escapadas."""
    df = spark.createDataFrame([Row(tipo="A"), Row(tipo="B"), Row(tipo="C")])
    df.createOrReplaceTempView("test_sub_in")

    conf = {
        "name":  "test_sub",
        "input": {"format": "view", "path": "test_sub_in"},
        "transformations": [
            {"type": "filter", "condition": "tipo = ${param_tipo}"}
        ],
        "output": {"format": "view", "path": "test_sub_out"},
    }
    r = fw.run_from_dict(conf, params={"param_tipo": "B"})
    assert r.success, r.error
    rows = [r["tipo"] for r in spark.table("test_sub_out").collect()]
    assert rows == ["B"]


def test_param_substitution_raw(fw, spark):
    """${param!raw} retorna valor sem escape — para uso em paths."""
    df = spark.createDataFrame([Row(x=1)])
    df.createOrReplaceTempView("test_raw_src")

    conf = {
        "name":  "test_raw",
        "input": {"format": "view", "path": "${param_input!raw}"},
        "transformations": [],
        "output": {"format": "view", "path": "${param_output!raw}"},
    }
    r = fw.run_from_dict(conf, params={
        "param_input":  "test_raw_src",
        "param_output": "test_raw_out",
    })
    assert r.success, r.error
    assert spark.table("test_raw_out").count() == 1


def test_param_substitution_lista_array_sql(fw, spark):
    """Listas viram array(...) para uso com array_contains."""
    df = spark.createDataFrame([Row(id=1), Row(id=2), Row(id=3)])
    df.createOrReplaceTempView("test_list_in")

    conf = {
        "name":  "test_list",
        "input": {"format": "view", "path": "test_list_in"},
        "transformations": [
            {"type": "filter",
             "condition": "array_contains(${param_ids}, id)"}
        ],
        "output": {"format": "view", "path": "test_list_out"},
    }
    r = fw.run_from_dict(conf, params={"param_ids": [1, 3]})
    assert r.success, r.error
    rows = sorted([r["id"] for r in spark.table("test_list_out").collect()])
    assert rows == [1, 3]


def test_output_transformations(fw, spark):
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


def test_select_com_expressoes_sintaxe_string(fw, spark):
    """select aceita strings SQL com 'expr as alias' direto (sintaxe preferida)."""
    df = spark.createDataFrame([Row(a=10, b=2), Row(a=20, b=3)])
    df.createOrReplaceTempView("test_sel_in")

    conf = {
        "name":  "test_sel",
        "input": {"format": "view", "path": "test_sel_in"},
        "transformations": [
            {"type": "select",
             "columns": [
                 "a",
                 "a * b as produto",
                 "a * 2 as dobro_a",
             ]}
        ],
        "output": {"format": "view", "path": "test_sel_out"},
    }
    r = fw.run_from_dict(conf)
    assert r.success, r.error
    rows = sorted([(r["a"], r["produto"], r["dobro_a"]) for r in spark.table("test_sel_out").collect()])
    assert rows == [(10, 20, 20), (20, 60, 40)]
    # 'b' não está nas colunas resultantes (foi dropada)
    assert "b" not in spark.table("test_sel_out").columns


def test_select_com_expressoes_dict_backward_compat(fw, spark):
    """select ainda aceita {name, expression} para retrocompat."""
    df = spark.createDataFrame([Row(a=10, b=2)])
    df.createOrReplaceTempView("test_sel_dict_in")

    conf = {
        "name":  "test_sel_dict",
        "input": {"format": "view", "path": "test_sel_dict_in"},
        "transformations": [
            {"type": "select",
             "columns": [
                 "a",
                 {"name": "produto", "expression": "a * b"},
             ]}
        ],
        "output": {"format": "view", "path": "test_sel_dict_out"},
    }
    r = fw.run_from_dict(conf)
    assert r.success, r.error
    row = spark.table("test_sel_dict_out").first()
    assert row["a"] == 10
    assert row["produto"] == 20


def test_checkpoint_transformation(fw, spark):
    """checkpoint quebra a lineage no meio das transformations."""
    df = spark.createDataFrame([Row(x=1), Row(x=2), Row(x=3)])
    df.createOrReplaceTempView("test_ckpt_in")

    conf = {
        "name":  "test_ckpt",
        "input": {"format": "view", "path": "test_ckpt_in"},
        "transformations": [
            {"type": "with_column", "name": "y", "expression": "x * 10"},
            {"type": "checkpoint"},
            {"type": "with_column", "name": "z", "expression": "y + 1"},
        ],
        "output": {"format": "view", "path": "test_ckpt_out"},
    }
    r = fw.run_from_dict(conf)
    assert r.success, r.error
    rows = sorted([(r["x"], r["y"], r["z"]) for r in spark.table("test_ckpt_out").collect()])
    assert rows == [(1, 10, 11), (2, 20, 21), (3, 30, 31)]


def test_cache_transformation(fw, spark):
    """cache materializa em memória sem quebrar lineage."""
    df = spark.createDataFrame([Row(x=1), Row(x=2)])
    df.createOrReplaceTempView("test_cache_in")

    conf = {
        "name":  "test_cache",
        "input": {"format": "view", "path": "test_cache_in"},
        "transformations": [
            {"type": "with_column", "name": "y", "expression": "x * 100"},
            {"type": "cache"},
        ],
        "output": {"format": "view", "path": "test_cache_out"},
    }
    r = fw.run_from_dict(conf)
    assert r.success
    rows = sorted([(r["x"], r["y"]) for r in spark.table("test_cache_out").collect()])
    assert rows == [(1, 100), (2, 200)]
