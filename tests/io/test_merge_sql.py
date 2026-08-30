"""A montagem do comando MERGE INTO, sem Spark.

`sparquet/io/merge.py` decide duas coisas que so aparecem no SQL emitido: qual e
a condicao do `ON` e quais clausulas `WHEN ...` saem, em que ordem. As duas
mudam o dado gravado e nenhuma delas e visivel no `PipelineResult` — um MERGE
com a clausula errada grava silenciosamente o resultado errado.

Ha uma unica forma de escrever um merge: `on` com a condicao inteira e `actions`
com as clausulas, as duas obrigatorias. O texto passa cru, na ordem dada. O que o
framework faz e recusar o que o Spark recusaria, com um erro que diz o que
arrumar em vez de um erro de analise do SQL — e recusar as opcoes da forma
declarativa antiga (`merge_keys`, `merge_condition`, `delete_when`,
`delete_not_matched_by_source`), que aceitas em silencio fariam o merge rodar
sem a chave e sem a exclusao pedidas.

    python tests/io/test_merge_sql.py
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sparquet.io.merge import (  # noqa: E402
    action_clauses,
    check_merge_options,
    merge_sql,
    on_condition,
    validate_merge_options,
)

UPSERT = ["WHEN MATCHED THEN UPDATE SET *", "WHEN NOT MATCHED THEN INSERT *"]


class OnConditionTest(unittest.TestCase):
    def test_o_on_passa_cru(self) -> None:
        opts = {"on": "  S.id = T.id AND S.dt >= T.dt  "}
        self.assertEqual(on_condition(opts, "W"), "S.id = T.id AND S.dt >= T.dt")

    def test_sem_on_o_erro_diz_o_que_escrever(self) -> None:
        with self.assertRaises(ValueError) as capturado:
            on_condition({}, "DeltaWriter")
        mensagem = str(capturado.exception)
        self.assertIn("DeltaWriter", mensagem)
        self.assertIn("'on'", mensagem)
        self.assertIn("T.id = S.id", mensagem)

    def test_on_vazio_e_recusado_em_vez_de_virar_um_on_em_branco(self) -> None:
        for valor in ("", "   ", 7, ["T.id = S.id"]):
            with self.subTest(valor=valor):
                with self.assertRaises(ValueError):
                    on_condition({"on": valor}, "W")


class OpcoesRemovidasTest(unittest.TestCase):
    """A forma declarativa antiga nao e ignorada: e recusada dizendo o que usar."""

    def test_cada_opcao_removida_diz_o_substituto(self) -> None:
        casos = {
            "merge_keys": "'on'",
            "merge_condition": "'on'",
            "delete_when": "WHEN MATCHED AND <cond> THEN DELETE",
            "delete_not_matched_by_source": "WHEN NOT MATCHED BY SOURCE THEN DELETE",
        }
        for chave, esperado in casos.items():
            with self.subTest(chave=chave):
                with self.assertRaises(ValueError) as capturado:
                    check_merge_options({chave: True}, "DeltaWriter")
                mensagem = str(capturado.exception)
                self.assertIn(f"'{chave}' nao existe mais", mensagem)
                self.assertIn(esperado, mensagem)

    def test_o_merge_inteiro_recusa_antes_de_montar_o_sql(self) -> None:
        with self.assertRaises(ValueError) as capturado:
            merge_sql("cat.db.t", "_view", {"merge_keys": ["id"]}, "DeltaWriter")
        self.assertIn("nao existe mais", str(capturado.exception))

    def test_validate_cobre_legado_on_e_actions(self) -> None:
        """O Iceberg valida antes do atalho da primeira carga; isto e o que ele
        chama, e precisa pegar os tres erros."""
        with self.assertRaises(ValueError):
            validate_merge_options({"merge_keys": ["id"]}, "IcebergWriter")
        with self.assertRaises(ValueError):
            validate_merge_options({"actions": UPSERT}, "IcebergWriter")
        with self.assertRaises(ValueError):
            validate_merge_options({"on": "T.id = S.id"}, "IcebergWriter")
        validate_merge_options(
            {"on": "T.id = S.id", "actions": UPSERT}, "IcebergWriter"
        )


class ActionClausesTest(unittest.TestCase):
    def test_as_clausulas_saem_na_ordem_dada(self) -> None:
        escritas = [
            "WHEN MATCHED AND S.op = 'D' THEN DELETE",
            "WHEN MATCHED THEN UPDATE SET T.a = S.a",
            "WHEN NOT MATCHED THEN INSERT (a) VALUES (S.a)",
        ]
        self.assertEqual(action_clauses({"actions": escritas}, "W"), escritas)

    def test_espaco_e_quebra_de_linha_sao_normalizados(self) -> None:
        escritas = ["WHEN MATCHED\n  THEN UPDATE SET\n    T.a = S.a"]
        acoes = action_clauses({"actions": escritas}, "W")
        self.assertEqual(acoes, ["WHEN MATCHED THEN UPDATE SET T.a = S.a"])

    def test_sem_actions_o_erro_diz_o_que_escrever(self) -> None:
        with self.assertRaises(ValueError) as capturado:
            action_clauses({"on": "T.id = S.id"}, "IcebergWriter")
        mensagem = str(capturado.exception)
        self.assertIn("IcebergWriter", mensagem)
        self.assertIn("'actions'", mensagem)
        self.assertIn("WHEN MATCHED THEN UPDATE SET *", mensagem)

    def test_actions_precisa_ser_lista_nao_vazia_de_texto(self) -> None:
        for valor in ([], "WHEN MATCHED THEN UPDATE SET *", [""], [None], [3]):
            with self.subTest(valor=valor):
                with self.assertRaises(ValueError):
                    action_clauses({"actions": valor}, "W")

    def test_clausula_que_nao_comeca_com_when_e_recusada(self) -> None:
        with self.assertRaises(ValueError) as capturado:
            action_clauses({"actions": ["UPDATE SET T.a = S.a"]}, "W")
        self.assertIn("WHEN MATCHED", str(capturado.exception))

    def test_incondicional_no_meio_do_grupo_e_recusada(self) -> None:
        """O erro facil: mover o DELETE para o fim, onde ele nunca e alcancado."""
        with self.assertRaises(ValueError) as capturado:
            action_clauses(
                {
                    "actions": [
                        "WHEN MATCHED THEN UPDATE SET T.a = S.a",
                        "WHEN MATCHED AND S.op = 'D' THEN DELETE",
                    ]
                },
                "W",
            )
        mensagem = str(capturado.exception)
        self.assertIn("inalcancavel", mensagem)
        self.assertIn("item 1", mensagem)

    def test_grupos_diferentes_nao_se_atrapalham(self) -> None:
        """Uma incondicional so bloqueia o proprio grupo."""
        escritas = [
            "WHEN MATCHED THEN UPDATE SET *",
            "WHEN NOT MATCHED THEN INSERT *",
            "WHEN NOT MATCHED BY SOURCE THEN DELETE",
        ]
        self.assertEqual(action_clauses({"actions": escritas}, "W"), escritas)

    def test_not_matched_by_source_nao_e_confundido_com_not_matched(self) -> None:
        escritas = [
            "WHEN NOT MATCHED BY SOURCE THEN DELETE",
            "WHEN NOT MATCHED THEN INSERT *",
        ]
        self.assertEqual(action_clauses({"actions": escritas}, "W"), escritas)


class MergeSqlTest(unittest.TestCase):
    def test_o_comando_tem_alvo_origem_on_e_clausulas_nessa_ordem(self) -> None:
        sql = " ".join(
            merge_sql(
                "cat.db.destino",
                "_view",
                {
                    "on": "T.id = S.id",
                    "actions": [
                        "WHEN MATCHED AND S.op = 'D' THEN DELETE",
                        *UPSERT,
                        "WHEN NOT MATCHED BY SOURCE THEN DELETE",
                    ],
                },
                "W",
            ).split()
        )
        self.assertEqual(
            sql,
            "MERGE INTO cat.db.destino AS T USING _view AS S ON T.id = S.id "
            "WHEN MATCHED AND S.op = 'D' THEN DELETE "
            "WHEN MATCHED THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT * "
            "WHEN NOT MATCHED BY SOURCE THEN DELETE",
        )

    def test_o_alvo_por_caminho_entra_como_veio(self) -> None:
        sql = " ".join(
            merge_sql(
                "delta.`/lake/t`",
                "_view",
                {
                    "on": "S.id = T.id",
                    "actions": ["WHEN MATCHED THEN UPDATE SET T.a = S.a"],
                },
                "W",
            ).split()
        )
        self.assertEqual(
            sql,
            "MERGE INTO delta.`/lake/t` AS T USING _view AS S ON S.id = T.id "
            "WHEN MATCHED THEN UPDATE SET T.a = S.a",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
