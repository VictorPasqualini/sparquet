"""Ação `pushdown` do debug e a guarda de tamanho do `collect`.

Não precisa de Spark: o parse do plano é função pura e o `collect` é exercitado
com um DataFrame de mentira que registra as chamadas.

    python tests/transform/test_pushdown_debug.py
"""
from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout

from sparquet.core.config import TransformationConfig
from sparquet.transform.builtin import (
    CollectTransformation,
    DebugTransformation,
    parse_pushdown,
)

# Plano real, capturado de um `spark.read.parquet(...).filter("grupo = 3")` no
# Spark 4.1.1 (a linha do FileScan está na íntegra, como o Spark imprime).
PLANO_PARQUET = """AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[sum(id#2L)])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=12]
      +- HashAggregate(keys=[], functions=[partial_sum(id#2L)])
         +- Project [id#2L]
            +- Filter (isnotnull(grupo#3L) AND (grupo#3L = 3))
               +- FileScan parquet [id#2L,grupo#3L] Batched: true, DataFilters: [isnotnull(grupo#3L), (grupo#3L = 3)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/dados], PartitionFilters: [], PushedFilters: [IsNotNull(grupo), EqualTo(grupo,3)], ReadSchema: struct<id:bigint,grupo:bigint>
"""

# JDBC v1: o `*` antes do predicado é o Spark dizendo que ele foi inteiramente
# traduzido para o WHERE — sem `*`, sobra avaliação do lado do Spark.
PLANO_JDBC = """*(1) Scan JDBCRelation(vendas) [numPartitions=8] [id#0,valor#1] PushedFilters: [*IsNotNull(id), *GreaterThan(id,10)], ReadSchema: struct<id:int,valor:decimal(10,2)>
"""

PLANO_PARTICAO = """+- FileScan parquet [valor#5,dia#6] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/fato], PartitionFilters: [isnotnull(dia#6), (dia#6 = 2026-09-04)], PushedFilters: [], ReadSchema: struct<valor:double>
"""

PLANO_SEM_NADA = """+- FileScan parquet [a#1,b#2,c#3] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/tudo], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:int,b:int,c:struct<x:int,y:int>>
"""

PLANO_MEMORIA = """*(1) Project [id#0L]
+- *(1) Scan ExistingRDD[id#0L]
"""

# DataSource v2 do arquivo (`spark.sql.sources.useV1SourceList` sem `parquet`) com
# `spark.sql.parquet.aggregatePushdown=true`: a etiqueta é `PushedAggregation`, e o
# `ReadSchema` não é o último campo da linha.
PLANO_V2_AGREGADO = """+- BatchScan parquet file:/tmp/dados[max(id)#22L, count(*)#23L] ParquetScan DataFilters: [], Format: parquet, Location: InMemoryFileIndex(1 paths)[file:/tmp/dados], PartitionFilters: [], PushedAggregation: [MAX(id), COUNT(*)], PushedFilters: [], PushedGroupBy: [], PushedVariantExtractions: [], ReadSchema: struct<max(id):bigint,count(*):bigint> RuntimeFilters: []
"""


class TestParsePushdown(unittest.TestCase):
    def test_file_scan_traz_filtros_e_projecao(self):
        (scan,) = parse_pushdown(PLANO_PARQUET)
        self.assertEqual(scan.kind, "FileScan")
        self.assertEqual(scan.source, "parquet")
        self.assertEqual(scan.pushed_filters, ["IsNotNull(grupo)", "EqualTo(grupo,3)"])
        self.assertEqual(scan.partition_filters, [])
        self.assertEqual(scan.columns_read, 2)
        self.assertEqual(scan.location, "file:/tmp/dados")
        self.assertFalse(scan.pushes_nothing)

    def test_predicado_com_virgula_nao_e_dividido(self):
        (scan,) = parse_pushdown(PLANO_PARQUET)
        self.assertIn("EqualTo(grupo,3)", scan.pushed_filters)

    def test_jdbc_mantem_o_asterisco_de_totalmente_empurrado(self):
        (scan,) = parse_pushdown(PLANO_JDBC)
        self.assertEqual(scan.kind, "Scan")
        self.assertEqual(scan.source, "JDBCRelation(vendas)")
        self.assertEqual(scan.pushed_filters, ["*IsNotNull(id)", "*GreaterThan(id,10)"])
        self.assertEqual(scan.columns_read, 2)

    def test_particao_podada_conta_como_pushdown(self):
        (scan,) = parse_pushdown(PLANO_PARTICAO)
        self.assertEqual(len(scan.partition_filters), 2)
        self.assertEqual(scan.pushed_filters, [])
        self.assertFalse(scan.pushes_nothing)

    def test_struct_aninhado_conta_uma_coluna_de_topo(self):
        (scan,) = parse_pushdown(PLANO_SEM_NADA)
        self.assertEqual(scan.columns_read, 3)
        self.assertTrue(scan.pushes_nothing)

    def test_agregacao_empurrada_no_v2_do_arquivo(self):
        (scan,) = parse_pushdown(PLANO_V2_AGREGADO)
        self.assertEqual(scan.kind, "BatchScan")
        self.assertEqual(scan.pushed_aggregates, ["MAX(id)", "COUNT(*)"])
        self.assertEqual(scan.pushed_group_by, [])
        self.assertFalse(scan.pushes_nothing)

    def test_read_schema_no_meio_da_linha_ainda_e_contado(self):
        (scan,) = parse_pushdown(PLANO_V2_AGREGADO)
        self.assertEqual(scan.columns_read, 2)

    def test_dados_em_memoria_nao_sao_scan_de_fonte(self):
        self.assertEqual(parse_pushdown(PLANO_MEMORIA), [])

    def test_plano_sem_leitura_devolve_lista_vazia(self):
        self.assertEqual(parse_pushdown("LocalTableScan [id#0]"), [])


class PlanoFake:
    """DataFrame só o bastante para `_plan_text` chegar num plano conhecido."""

    def __init__(self, plan: str) -> None:
        self.plan = plan

    # _jdf.queryExecution().executedPlan().toString()
    @property
    def _jdf(self):
        return self

    def queryExecution(self):
        return self

    def executedPlan(self):
        return self

    def toString(self):
        return self.plan


def relatorio(plan: str) -> str:
    debug = DebugTransformation(
        TransformationConfig(type="debug", params={"actions": ["pushdown"]})
    )
    saida = io.StringIO()
    with redirect_stdout(saida):
        debug.apply(PlanoFake(plan))
    return saida.getvalue()


class TestAcaoPushdown(unittest.TestCase):
    def test_lista_o_que_desceu_ate_a_fonte(self):
        texto = relatorio(PLANO_PARQUET)
        self.assertIn("scan 1: FileScan parquet", texto)
        self.assertIn("file:/tmp/dados", texto)
        self.assertIn("PushedFilters: IsNotNull(grupo), EqualTo(grupo,3)", texto)
        self.assertIn("colunas lidas: 2", texto)

    def test_avisa_quando_o_scan_le_tudo(self):
        self.assertIn("nada empurrado", relatorio(PLANO_SEM_NADA))

    def test_nao_avisa_quando_ha_pushdown(self):
        self.assertNotIn("nada empurrado", relatorio(PLANO_PARQUET))

    def test_conta_os_filtros_que_ficaram_acima_do_scan(self):
        self.assertIn("1 nó(s) `Filter` acima", relatorio(PLANO_PARQUET))

    def test_df_de_memoria_explica_a_ausencia_de_scan(self):
        self.assertIn("nenhum nó de leitura", relatorio(PLANO_MEMORIA))

    def test_debug_devolve_o_df_original(self):
        debug = DebugTransformation(
            TransformationConfig(type="debug", params={"actions": ["pushdown"]})
        )
        df = PlanoFake(PLANO_PARQUET)
        with redirect_stdout(io.StringIO()):
            self.assertIs(debug.apply(df), df)


class ColunaFake:
    """Registra `limit` e devolve `quantidade` valores distintos."""

    def __init__(self, quantidade: int) -> None:
        self.quantidade = quantidade
        self.limite = None

    def select(self, _column):
        return self

    def distinct(self):
        return self

    def limit(self, n):
        self.limite = n
        return self

    def collect(self):
        alvo = self.quantidade if self.limite is None else min(self.quantidade, self.limite)
        return [(i,) for i in range(alvo)]


def coletar(df, **params):
    params.setdefault("column", "id")
    params.setdefault("as", "ids")
    transform = CollectTransformation(TransformationConfig(type="collect", params=params))
    transform.apply(df)
    return transform.runtime


class TestGuardaDoCollect(unittest.TestCase):
    def test_lista_pequena_passa(self):
        self.assertEqual(coletar(ColunaFake(3))["ids"], [0, 1, 2])

    def test_teto_e_aplicado_na_consulta_nao_no_driver(self):
        df = ColunaFake(3)
        coletar(df, max_values=10)
        self.assertEqual(df.limite, 11)

    def test_acima_do_teto_falha_apontando_a_alternativa(self):
        with self.assertRaises(ValueError) as ctx:
            coletar(ColunaFake(50), max_values=10)
        mensagem = str(ctx.exception)
        self.assertIn("mais de 10 valores distintos", mensagem)
        self.assertIn("join", mensagem)
        self.assertIn("max_values", mensagem)

    def test_exatamente_no_teto_passa(self):
        self.assertEqual(len(coletar(ColunaFake(10), max_values=10)["ids"]), 10)

    def test_teto_zero_desliga_a_guarda(self):
        df = ColunaFake(5)
        runtime = coletar(df, max_values=0)
        self.assertIsNone(df.limite)
        self.assertEqual(len(runtime["ids"]), 5)

    def test_teto_invalido_falha_antes_de_coletar(self):
        for ruim in (-1, "10", 1.5, True):
            with self.subTest(ruim=ruim), self.assertRaises(ValueError) as ctx:
                coletar(ColunaFake(1), max_values=ruim)
            self.assertIn("max_values", str(ctx.exception))


if __name__ == "__main__":
    unittest.main(verbosity=2)
