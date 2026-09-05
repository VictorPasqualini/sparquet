"""O que cada transformacao faz com os DADOS — nao o que ela aceita de config.

O `test_examples.py` confere que o `type` existe e que o JSON monta; a validacao
de parametro de `repartition`/`collect` esta em `test_partitioning.py` e
`test_pushdown_debug.py`, com dubles, sem Spark. Faltava o meio: rodar cada
transformacao sobre um DataFrame de verdade e olhar o que sai. Uma expressao de
agregacao trocada, um alias perdido, uma coluna que some — nada disso aparece na
montagem, so no resultado.

Uma classe por transformacao, na ordem do registry do engine. O que cada uma
trava, quando nao e obvio pelo nome do teste:

  select        expressao com alias vira coluna com aquele nome, na ordem pedida.
  with_column   as tres formas (`column`, `name` compat, `columns` mapa) e a
                ordem do mapa — a segunda coluna enxerga a primeira.
  struct        dot-path aninhando de verdade, prefixo comum mesclado, e o
                conflito folha-vs-mapa recusado.
  collect       o {{var}} do passo seguinte: lista vira literal de `IN (...)`,
                lista vazia vira `NULL` (nao casa nada), string ganha aspas e
                aspas dentro da string sao escapadas.
  group_by      pivot nas duas formas — com e sem `values` explicito.
  join          `broadcast` como dica no plano fisico; a sessao sobe com
                autoBroadcastJoinThreshold=-1 justamente para que broadcast so
                aconteca se alguem pediu.
  repartition   a promessa e de custo, nao de dado: as linhas continuam as
                mesmas e a mesma chave cai na MESMA particao.
  debug         inspeciona uma copia; o df que segue no pipeline e o original.
  skip_if_false os tres casos do engine (None, "", expressao) mais o valor
                literal que nao e booleano.

Os DataFrames nascem de `spark.sql(... VALUES ...)`: `createDataFrame` sobe um
worker Python e, em master local com `PYSPARK_PYTHON` desalinhado, o arquivo
inteiro morreria por um motivo que nada tem a ver com transformacao. Mesmo
motivo do fixture em `tests/test_formats_roundtrip_spark.py`.

Sem jar nem servico externo. Sem pyspark ou sem Java a classe e **pulada**.

    PYTHONPATH=. python tests/transform/test_builtin_behavior_spark.py
"""
from __future__ import annotations

import contextlib
import io
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

try:  # pyspark pode nao estar instalado no ambiente de testes puros
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
except Exception:  # pragma: no cover - ambiente sem pyspark
    SparkSession = None  # type: ignore[assignment]
    F = None  # type: ignore[assignment]

from sparquet.core.config import TransformationConfig  # noqa: E402
from sparquet.transform.base import PipelineStop  # noqa: E402
from sparquet.transform.builtin import _plan_text  # noqa: E402
from sparquet.transform.engine import TransformationEngine  # noqa: E402

# Sem isto o worker sobe com outro Python e morre com "Python worker exited
# unexpectedly" na primeira etapa que cria worker. Vale so em master local.
for _var in ("PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON"):
    os.environ.setdefault(_var, sys.executable)


class BaseTransformacao(unittest.TestCase):
    """Sessao, fixtures e o atalho que roda a transformacao pelo engine."""

    spark = None
    tmp = None

    @classmethod
    def setUpClass(cls) -> None:
        if SparkSession is None:
            raise unittest.SkipTest("pyspark nao instalado")
        try:
            cls.spark = (
                SparkSession.builder
                .master("local[2]")
                .appName("sparquet-transform-behavior-tests")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "4")
                # Sem isto o Spark faz broadcast sozinho em qualquer fixture
                # pequena, e o teste da dica `broadcast` nao provaria nada.
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .getOrCreate()
            )
            cls.spark.sql("SELECT 1").count()
        except Exception as exc:  # pragma: no cover - ambiente sem Java/Spark
            cls.spark = None
            raise unittest.SkipTest(f"Spark/Java indisponivel: {exc}")
        cls.tmp = tempfile.mkdtemp(prefix="sparquet-transform-")

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.spark is not None:
            cls.spark.stop()
        if cls.tmp:
            shutil.rmtree(cls.tmp, ignore_errors=True)

    # ---- infra

    def relacao(self, valores: str, colunas: str):
        return self.spark.sql(f"SELECT * FROM VALUES {valores} AS t({colunas})")

    def gravar(self, nome: str, valores: str, colunas: str) -> str:
        caminho = str(Path(self.tmp) / nome)
        self.relacao(valores, colunas).write.mode("overwrite").parquet(caminho)
        return caminho

    def base(self):
        """Tres linhas, dois grupos — o suficiente para agrupar e deduplicar."""
        return self.relacao(
            "(1, 'alpha', CAST(10.0 AS DOUBLE), 'x'), "
            "(2, 'beta',  CAST(20.0 AS DOUBLE), 'y'), "
            "(3, 'gamma', CAST(30.0 AS DOUBLE), 'x')",
            "id, nome, valor, grupo",
        )

    def aplicar(self, df, *configs, runtime=None):
        """Roda pelo engine (e nao pela classe direto) porque e o engine que
        resolve {{var}} e decide `skip_if_false` — o caminho real."""
        self.engine = TransformationEngine(runtime=runtime)
        cfgs = [TransformationConfig.from_dict(c) for c in configs]
        return self.engine.apply(df, cfgs)

    def ids(self, df) -> list:
        return [row["id"] for row in df.orderBy("id").collect()]


class TestFilter(BaseTransformacao):
    def test_mantem_so_as_linhas_da_condicao(self) -> None:
        out = self.aplicar(self.base(), {"type": "filter", "condition": "valor > 15"})
        self.assertEqual(self.ids(out), [2, 3])

    def test_condicao_e_sql_completo_nao_so_igualdade(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "filter", "condition": "grupo = 'x' AND nome LIKE 'a%'"},
        )
        self.assertEqual(self.ids(out), [1])


class TestSelect(BaseTransformacao):
    def test_projeta_na_ordem_pedida(self) -> None:
        out = self.aplicar(self.base(), {"type": "select", "columns": ["grupo", "id"]})
        self.assertEqual(out.columns, ["grupo", "id"])

    def test_expressao_com_alias_vira_coluna_com_aquele_nome(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "select", "columns": [
                "nome",
                "valor * 2 AS dobro",
                "CAST(id AS STRING) AS id_str",
            ]},
        )
        self.assertEqual(out.columns, ["nome", "dobro", "id_str"])
        linha = out.orderBy("id_str").first()
        self.assertEqual(
            (linha["nome"], linha["dobro"], linha["id_str"]), ("alpha", 20.0, "1")
        )

    def test_expressao_sem_alias_mantem_o_texto_como_nome(self) -> None:
        out = self.aplicar(self.base(), {"type": "select", "columns": ["upper(nome)"]})
        self.assertEqual(out.columns, ["upper(nome)"])
        self.assertEqual(out.orderBy("upper(nome)").first()[0], "ALPHA")


class TestDrop(BaseTransformacao):
    def test_remove_as_colunas_e_preserva_o_resto(self) -> None:
        out = self.aplicar(self.base(), {"type": "drop", "columns": ["valor", "grupo"]})
        self.assertEqual(out.columns, ["id", "nome"])
        self.assertEqual(out.count(), 3)


class TestRename(BaseTransformacao):
    def test_renomeia_pelo_mapa_sem_mexer_no_valor(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "rename", "mappings": {"nome": "titulo", "valor": "montante"}},
        )
        self.assertEqual(out.columns, ["id", "titulo", "montante", "grupo"])
        linha = out.orderBy("id").first()
        self.assertEqual((linha["titulo"], linha["montante"]), ("alpha", 10.0))


class TestCast(BaseTransformacao):
    def test_troca_o_tipo_e_conserva_a_posicao_da_coluna(self) -> None:
        out = self.aplicar(
            self.base(), {"type": "cast", "columns": {"id": "string", "valor": "int"}}
        )
        tipos = dict(out.dtypes)
        self.assertEqual(tipos["id"], "string")
        self.assertEqual(tipos["valor"], "int")
        self.assertEqual(out.columns, ["id", "nome", "valor", "grupo"])
        linha = out.orderBy("id").first()
        self.assertEqual((linha["id"], linha["valor"]), ("1", 10))


class TestWithColumn(BaseTransformacao):
    def test_forma_column(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "with_column", "column": "dobro", "expression": "valor * 2"},
        )
        self.assertEqual(out.orderBy("id").first()["dobro"], 20.0)

    def test_forma_name_continua_valendo(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "with_column", "name": "dobro", "expression": "valor * 2"},
        )
        self.assertEqual(out.orderBy("id").first()["dobro"], 20.0)

    def test_mapa_cria_na_ordem_de_escrita_entao_uma_enxerga_a_anterior(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "with_column", "columns": {
                "dobro": "valor * 2",
                "dobro_mais_um": "dobro + 1",
            }},
        )
        self.assertEqual(out.columns[-2:], ["dobro", "dobro_mais_um"])
        linha = out.orderBy("id").first()
        self.assertEqual((linha["dobro"], linha["dobro_mais_um"]), (20.0, 21.0))

    def test_nome_existente_substitui_a_coluna_sem_duplicar(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "with_column", "column": "valor", "expression": "valor * 0"},
        )
        self.assertEqual(out.columns.count("valor"), 1)
        self.assertEqual({r["valor"] for r in out.collect()}, {0.0})

    def test_sem_nome_o_erro_diz_quais_chaves_servem(self) -> None:
        with self.assertRaises(ValueError) as capturado:
            self.aplicar(self.base(), {"type": "with_column", "expression": "valor * 2"})
        mensagem = str(capturado.exception)
        self.assertIn("column", mensagem)
        self.assertIn("columns", mensagem)


class TestStruct(BaseTransformacao):
    def test_dot_path_aninha_de_verdade(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "struct", "column": "payload", "fields": {
                "id_externo": "id",
                "data.nc.nome": "nome",
                "data.nc.valor": "valor",
            }},
        )
        campos_topo = [c.name for c in out.schema["payload"].dataType.fields]
        self.assertEqual(campos_topo, ["id_externo", "data"])
        linha = out.orderBy("id").select(
            "payload.id_externo", "payload.data.nc.nome", "payload.data.nc.valor"
        ).first()
        self.assertEqual(tuple(linha), (1, "alpha", 10.0))

    def test_prefixo_comum_e_mesclado_num_no_so(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "struct", "column": "payload", "fields": {
                "data.nc.nome": "nome",
                "data.total": "valor",
            }},
        )
        data = out.schema["payload"].dataType["data"].dataType
        self.assertEqual([c.name for c in data.fields], ["nc", "total"])

    def test_mapa_aninhado_e_dot_path_convivem(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "struct", "column": "payload", "fields": {
                "cabecalho": {"id": "id"},
                "corpo.nome": "nome",
            }},
        )
        linha = out.orderBy("id").select(
            "payload.cabecalho.id", "payload.corpo.nome"
        ).first()
        self.assertEqual(tuple(linha), (1, "alpha"))

    def test_folha_usada_depois_como_mapa_e_recusada(self) -> None:
        with self.assertRaises(ValueError) as capturado:
            self.aplicar(
                self.base(),
                {"type": "struct", "column": "payload", "fields": {
                    "data": "id",
                    "data.nc": "nome",
                }},
            )
        self.assertIn("conflito", str(capturado.exception))

    def test_sem_nome_de_coluna_o_erro_e_explicito(self) -> None:
        with self.assertRaises(ValueError) as capturado:
            self.aplicar(self.base(), {"type": "struct", "fields": {"a": "id"}})
        self.assertIn("column", str(capturado.exception))


class TestDeduplicacao(BaseTransformacao):
    def repetidas(self):
        return self.relacao("(1, 'a'), (1, 'b'), (2, 'c'), (2, 'c')", "id, txt")

    def test_drop_duplicates_com_columns_deduplica_so_por_elas(self) -> None:
        out = self.aplicar(
            self.repetidas(), {"type": "drop_duplicates", "columns": ["id"]}
        )
        self.assertEqual(out.count(), 2)

    def test_drop_duplicates_sem_columns_olha_a_linha_inteira(self) -> None:
        out = self.aplicar(self.repetidas(), {"type": "drop_duplicates"})
        self.assertEqual(out.count(), 3)

    def test_distinct_olha_a_linha_inteira(self) -> None:
        out = self.aplicar(self.repetidas(), {"type": "distinct"})
        self.assertEqual(out.count(), 3)


class TestSort(BaseTransformacao):
    def test_ascendente_e_o_default(self) -> None:
        out = self.aplicar(self.base(), {"type": "sort", "columns": ["valor"]})
        self.assertEqual([r["id"] for r in out.collect()], [1, 2, 3])

    def test_ascending_false_inverte_todas(self) -> None:
        out = self.aplicar(
            self.base(), {"type": "sort", "columns": ["valor"], "ascending": False}
        )
        self.assertEqual([r["id"] for r in out.collect()], [3, 2, 1])

    def test_ascending_como_lista_vale_coluna_a_coluna(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "sort", "columns": ["grupo", "valor"], "ascending": [True, False]},
        )
        self.assertEqual([r["id"] for r in out.collect()], [3, 1, 2])


class TestFillNa(BaseTransformacao):
    def com_nulos(self):
        return self.relacao(
            "(1, 'a', CAST(NULL AS DOUBLE)), "
            "(2, CAST(NULL AS STRING), CAST(2.0 AS DOUBLE))",
            "id, nome, valor",
        )

    def test_constante_com_columns_so_toca_as_listadas(self) -> None:
        out = self.aplicar(
            self.com_nulos(), {"type": "fill_na", "value": 0.0, "columns": ["valor"]}
        )
        linhas = {r["id"]: r for r in out.collect()}
        self.assertEqual(linhas[1]["valor"], 0.0)
        self.assertIsNone(linhas[2]["nome"])  # fora do subset, continua nulo

    def test_mapa_preenche_cada_coluna_com_o_seu_valor(self) -> None:
        out = self.aplicar(
            self.com_nulos(),
            {"type": "fill_na", "value": {"nome": "?", "valor": -1.0}},
        )
        linhas = {r["id"]: r for r in out.collect()}
        self.assertEqual(linhas[1]["valor"], -1.0)
        self.assertEqual(linhas[2]["nome"], "?")


class TestGroupBy(BaseTransformacao):
    def test_agrega_com_o_alias_da_expressao(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "group_by", "by": ["grupo"],
             "agg": ["sum(valor) as total", "count(*) as n"]},
        )
        self.assertEqual(out.columns, ["grupo", "total", "n"])
        linhas = {r["grupo"]: r for r in out.collect()}
        self.assertEqual((linhas["x"]["total"], linhas["x"]["n"]), (40.0, 2))
        self.assertEqual((linhas["y"]["total"], linhas["y"]["n"]), (20.0, 1))

    def meses(self):
        return self.relacao(
            "(1, 'jan', CAST(10.0 AS DOUBLE)), "
            "(1, 'fev', CAST(20.0 AS DOUBLE)), "
            "(2, 'jan', CAST(5.0 AS DOUBLE))",
            "id, mes, valor",
        )

    def test_pivot_como_string_gera_uma_coluna_por_valor(self) -> None:
        out = self.aplicar(
            self.meses(),
            {"type": "group_by", "by": ["id"], "pivot": "mes",
             "agg": ["sum(valor) as total"]},
        )
        self.assertEqual(set(out.columns), {"id", "jan", "fev"})
        linhas = {r["id"]: r for r in out.collect()}
        self.assertEqual((linhas[1]["jan"], linhas[1]["fev"]), (10.0, 20.0))
        self.assertIsNone(linhas[2]["fev"])  # sem linha no mes → nulo

    def test_pivot_com_values_explicitos_limita_as_colunas(self) -> None:
        out = self.aplicar(
            self.meses(),
            {"type": "group_by", "by": ["id"],
             "pivot": {"column": "mes", "values": ["jan"]},
             "agg": ["sum(valor) as total"]},
        )
        self.assertEqual(out.columns, ["id", "jan"])
        linhas = {r["id"]: r for r in out.collect()}
        self.assertEqual(linhas[1]["jan"], 10.0)


class TestSql(BaseTransformacao):
    def test_o_df_entra_como_a_view_default(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "sql", "query": "SELECT id, nome FROM _df WHERE grupo = 'x'"},
        )
        self.assertEqual(out.columns, ["id", "nome"])
        self.assertEqual(self.ids(out), [1, 3])

    def test_view_name_troca_o_nome_da_view(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "sql", "view_name": "entrada",
             "query": "SELECT count(*) AS n FROM entrada"},
        )
        self.assertEqual(out.first()["n"], 3)


class TestCollectERuntime(BaseTransformacao):
    def test_valores_coletados_ficam_no_runtime(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "filter", "condition": "grupo = 'x'"},
            {"type": "collect", "column": "id", "as": "ids"},
        )
        self.assertEqual(sorted(self.engine.runtime["ids"]), [1, 3])
        self.assertEqual(self.ids(out), [1, 3])  # collect nao altera o df

    def test_a_variavel_coletada_vale_no_passo_seguinte(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "collect", "column": "grupo", "as": "grupos"},
            {"type": "sql",
             "query": "SELECT * FROM _df WHERE grupo IN ({{grupos}}) AND id > 1"},
        )
        self.assertEqual(self.ids(out), [2, 3])

    def test_lista_vira_literal_de_in(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "filter", "condition": "id IN ({{ids}})"},
            runtime={"ids": [1, 3]},
        )
        self.assertEqual(self.ids(out), [1, 3])

    def test_lista_vazia_vira_null_e_nao_casa_nada(self) -> None:
        """`IN (NULL)` e o ponto: sem candidatos, o passo seguinte nao pode
        silenciosamente processar a tabela inteira."""
        out = self.aplicar(
            self.base(),
            {"type": "filter", "condition": "id IN ({{ids}})"},
            runtime={"ids": []},
        )
        self.assertEqual(out.count(), 0)

    def test_string_ganha_aspas_e_a_aspas_de_dentro_e_escapada(self) -> None:
        df = self.relacao("(1, 'd''agua'), (2, 'outro')", "id, nome")
        out = self.aplicar(
            df,
            {"type": "filter", "condition": "nome = {{alvo}}"},
            runtime={"alvo": "d'agua"},
        )
        self.assertEqual(self.ids(out), [1])

    def test_placeholder_sem_variavel_fica_literal(self) -> None:
        """Um engine aninhado resolve depois; substituir por vazio aqui geraria
        SQL invalido em vez de esperar a variavel."""
        engine = TransformationEngine(runtime={"outra": [1]})
        cfg = TransformationConfig.from_dict(
            {"type": "filter", "condition": "id IN ({{ids}})"}
        )
        resolvido = engine._resolve_runtime(cfg)
        self.assertEqual(resolvido.params["condition"], "id IN ({{ids}})")


class TestStopIfEmpty(BaseTransformacao):
    def test_df_vazio_encerra_com_pipeline_stop_e_a_mensagem_dada(self) -> None:
        vazio = self.base().filter("id > 99")
        with self.assertRaises(PipelineStop) as capturado:
            self.aplicar(
                vazio, {"type": "stop_if_empty", "message": "Sem cessoes a processar"}
            )
        self.assertIn("Sem cessoes a processar", str(capturado.exception))

    def test_com_linhas_o_df_segue_intacto(self) -> None:
        out = self.aplicar(self.base(), {"type": "stop_if_empty"})
        self.assertEqual(self.ids(out), [1, 2, 3])


class TestCheckpoint(BaseTransformacao):
    def test_local_checkpoint_materializa_sem_mudar_os_dados(self) -> None:
        out = self.aplicar(self.base(), {"type": "checkpoint"})
        self.assertEqual(out.columns, ["id", "nome", "valor", "grupo"])
        self.assertEqual(self.ids(out), [1, 2, 3])

    def test_method_invalido_e_ignorado_e_o_df_segue(self) -> None:
        out = self.aplicar(self.base(), {"type": "checkpoint", "method": "naoexiste"})
        self.assertEqual(self.ids(out), [1, 2, 3])


class TestRepartition(BaseTransformacao):
    def particoes_por_grupo(self, df) -> dict:
        pares = (
            df.select("grupo", F.spark_partition_id().alias("p")).distinct().collect()
        )
        mapa: dict = {}
        for row in pares:
            mapa.setdefault(row["grupo"], set()).add(row["p"])
        return mapa

    def test_a_mesma_chave_cai_na_mesma_particao(self) -> None:
        out = self.aplicar(
            self.base(),
            {"type": "repartition", "num_partitions": 4, "columns": ["grupo"]},
        )
        for grupo, particoes in self.particoes_por_grupo(out).items():
            self.assertEqual(len(particoes), 1, f"grupo {grupo} espalhado: {particoes}")
        self.assertEqual(self.ids(out), [1, 2, 3])

    def test_coalesce_reduz_para_uma_particao_sem_perder_linha(self) -> None:
        espalhado = self.aplicar(
            self.base(), {"type": "repartition", "num_partitions": 4}
        )
        out = self.aplicar(
            espalhado, {"type": "repartition", "num_partitions": 1, "coalesce": True}
        )
        particoes = out.select(F.spark_partition_id()).distinct().count()
        self.assertEqual(particoes, 1)
        self.assertEqual(self.ids(out), [1, 2, 3])


class TestJoinBroadcast(BaseTransformacao):
    """A dica de broadcast — o resto do join esta em `test_join_spark.py`."""

    def juntar(self, **params):
        caminho = self.gravar(
            "dim_broadcast.parquet", "(1, 'um'), (2, 'dois')", "id, rotulo"
        )
        return self.aplicar(
            self.base(),
            {"type": "join",
             "input": {"format": "parquet", "path": caminho},
             "on": "id", **params},
        )

    def test_sem_a_dica_o_plano_nao_tem_broadcast(self) -> None:
        plano = _plan_text(self.juntar())
        self.assertNotIn("BroadcastHashJoin", plano)

    def test_broadcast_true_espalha_o_lado_direito(self) -> None:
        juntado = self.juntar(broadcast=True)
        plano = _plan_text(juntado)
        self.assertIn("BroadcastHashJoin", plano)
        self.assertIn("BuildRight", plano)
        self.assertEqual(self.ids(juntado), [1, 2])

    def test_broadcast_left_espalha_o_principal(self) -> None:
        plano = _plan_text(self.juntar(broadcast="left"))
        self.assertIn("BroadcastHashJoin", plano)
        self.assertIn("BuildLeft", plano)

    def test_broadcast_invalido_diz_o_que_aceita(self) -> None:
        with self.assertRaises(ValueError) as capturado:
            self.juntar(broadcast="talvez")
        self.assertIn("broadcast", str(capturado.exception))

    def test_how_invalido_lista_as_opcoes(self) -> None:
        with self.assertRaises(ValueError) as capturado:
            self.juntar(how="lateral")
        mensagem = str(capturado.exception)
        self.assertIn("lateral", mensagem)
        self.assertIn("leftanti", mensagem)


class TestUnion(BaseTransformacao):
    def test_acrescenta_as_linhas_da_segunda_fonte(self) -> None:
        caminho = self.gravar(
            "union_igual.parquet",
            "(4, 'delta', CAST(40.0 AS DOUBLE), 'z')",
            "id, nome, valor, grupo",
        )
        out = self.aplicar(
            self.base(),
            {"type": "union", "input": {"format": "parquet", "path": caminho}},
        )
        self.assertEqual(self.ids(out), [1, 2, 3, 4])

    def test_allow_missing_columns_preenche_o_que_falta_com_nulo(self) -> None:
        caminho = self.gravar("union_parcial.parquet", "(4, 'delta')", "id, nome")
        out = self.aplicar(
            self.base(),
            {"type": "union",
             "input": {"format": "parquet", "path": caminho},
             "allow_missing_columns": True},
        )
        self.assertEqual(self.ids(out), [1, 2, 3, 4])
        nova = out.filter("id = 4").first()
        self.assertIsNone(nova["valor"])
        self.assertIsNone(nova["grupo"])

    def test_sem_input_o_erro_diz_qual_chave_falta(self) -> None:
        with self.assertRaises(ValueError) as capturado:
            self.aplicar(self.base(), {"type": "union"})
        self.assertIn("input", str(capturado.exception))


class TestDebug(BaseTransformacao):
    def test_inspeciona_uma_copia_e_devolve_o_df_original(self) -> None:
        saida = io.StringIO()
        with contextlib.redirect_stdout(saida):
            out = self.aplicar(
                self.base(),
                {"type": "debug", "label": "so o grupo x", "actions": ["count"],
                 "transformations": [{"type": "filter", "condition": "grupo = 'x'"}]},
            )
        texto = saida.getvalue()
        self.assertIn("so o grupo x", texto)
        self.assertIn("count: 2", texto)            # a copia foi filtrada
        self.assertEqual(self.ids(out), [1, 2, 3])  # o pipeline nao

    def test_acao_desconhecida_avisa_sem_derrubar_o_pipeline(self) -> None:
        saida = io.StringIO()
        with contextlib.redirect_stdout(saida):
            out = self.aplicar(self.base(), {"type": "debug", "actions": ["nao_existe"]})
        self.assertIn("nao_existe", saida.getvalue())
        self.assertEqual(self.ids(out), [1, 2, 3])


class TestSkipIfFalse(BaseTransformacao):
    """O que decide se a etapa roda. O filtro abaixo zera o df quando roda —
    entao a contagem diz, sem ambiguidade, se foi pulada."""

    def contar_com(self, skip):
        cfg = {"type": "filter", "condition": "1 = 0"}
        if skip is not ...:
            cfg["skip_if_false"] = skip
        return self.aplicar(self.base(), cfg).count()

    def test_ausente_a_etapa_roda(self) -> None:
        self.assertEqual(self.contar_com(...), 0)

    def test_string_vazia_pula(self) -> None:
        self.assertEqual(self.contar_com(""), 3)

    def test_expressao_falsa_pula(self) -> None:
        self.assertEqual(self.contar_com("'EMISSAO' in ('REGISTRO')"), 3)

    def test_expressao_verdadeira_roda(self) -> None:
        self.assertEqual(self.contar_com("'EMISSAO' in ('EMISSAO', 'REGISTRO')"), 0)

    def test_valor_que_nao_e_booleano_roda(self) -> None:
        """Compat: o param foi substituido por um valor qualquer ('CERC'), o que
        significa 'presente' — nao 'falso'."""
        self.assertEqual(self.contar_com("CERC"), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
