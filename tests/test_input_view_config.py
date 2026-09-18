"""`input_view` como chave do JSON, e a precedência entre ela e o argumento Python.

Sem Spark: tudo aqui é parse de configuração e a decisão de qual das duas fontes
vence. A execução em si — registrar e cachear a temp view — já é exercitada pelos
testes de pipeline que rodam com Java.

    python tests/test_input_view_config.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sparquet.core.config import PipelineConfig  # noqa: E402


BASE = {
    "name": "pedidos",
    "input": {"format": "parquet", "path": "/in"},
    "output": {"format": "parquet", "path": "/out"},
}


def conf(**extra):
    return PipelineConfig.from_dict({**BASE, **extra})


def test_absent_is_none():
    assert conf().input_view is None


def test_string_becomes_session_scope():
    assert conf(input_view="orders").input_view == {"name": "orders", "type": "session"}


def test_dict_keeps_scope():
    parsed = conf(input_view={"name": "orders", "type": "global"}).input_view
    assert parsed == {"name": "orders", "type": "global"}


def test_dotted_name_is_refused():
    try:
        conf(input_view="vendas.orders")
    except ValueError as error:
        assert "temp view" in str(error)
    else:
        raise AssertionError("um nome qualificado por ponto deveria ser recusado")


def test_unknown_scope_is_refused():
    try:
        conf(input_view={"name": "orders", "type": "cluster"})
    except ValueError as error:
        assert "session" in str(error)
    else:
        raise AssertionError("um escopo desconhecido deveria ser recusado")


def test_empty_name_is_refused():
    for value in ("", "   ", {"name": ""}):
        try:
            conf(input_view=value)
        except ValueError:
            continue
        raise AssertionError(f"{value!r} deveria ser recusado")


def test_pipeline_reads_the_config_when_the_argument_is_absent():
    from sparquet.core.pipeline import Pipeline

    pipeline = Pipeline(conf(input_view={"name": "orders", "type": "global"}))
    assert pipeline._input_view == "orders"
    assert pipeline._input_view_scope == "global"


def test_argument_wins_over_the_json():
    from sparquet.core.pipeline import Pipeline

    pipeline = Pipeline(conf(input_view="do_json"), input_view="do_argumento")
    assert pipeline._input_view == "do_argumento"
    assert pipeline._input_view_scope == "session"


def test_no_view_anywhere_stays_off():
    from sparquet.core.pipeline import Pipeline

    pipeline = Pipeline(conf())
    assert pipeline._input_view is None


if __name__ == "__main__":
    failures = 0
    for name, test in sorted(globals().items()):
        if not name.startswith("test_") or not callable(test):
            continue
        try:
            test()
            print(f"  ok  {name}")
        except Exception as error:  # noqa: BLE001
            failures += 1
            print(f"FAIL  {name}: {error}")
    print("falhas:", failures)
    sys.exit(1 if failures else 0)
