"""A montagem do comando MERGE INTO, sem Spark.

`sparquet/io/merge.py` decide duas coisas que so aparecem no SQL emitido: qual e
a condicao do `ON` e quais clausulas `WHEN ...` saem, em que ordem. As duas
mudam o dado gravado e nenhuma delas e visivel no `PipelineResult` — um MERGE
com a clausula errada grava silenciosamente o resultado errado.

Ha duas formas de escrever um merge no JSON, e este arquivo trava as duas:

  declarativa   `merge_keys` (+ `merge_condition`, `delete_when`,
                `delete_not_matched_by_source`): o framework monta tudo, e a
                ordem importa — o DELETE por marca da origem tem de sair ANTES
                do UPDATE, porque em MERGE INTO a primeira clausula que casa e a
                que vale.
  explicita     `on` e `actions`: o texto passa cru, na ordem dada. O que o
                framework faz aqui e recusar o que o Spark recusaria, com um erro
                que diz o que arrumar em vez de um erro de analise do SQL.

    python tests/io/test_merge_sql.py
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sparquet.io.merge import (  # noqa: E402
    action_clauses,
    delete_clauses,
    merge_sql,
    on_condition,
)

#: As clausulas que um writer passaria como default; o conteudo nao importa
#: aqui, so a posicao delas no resultado.
DEFAULTS = ("WHEN MATCHED THEN UPDATE SET *", "WHEN NOT MATCHED THEN INSERT *")


class OnConditionTest(unittest.TestCase):
    def test_merge_keys_viram_a_igualdade_de_cada_chave(self) -> None:
        self.assertEqual(
            on_condition({"merge_keys": ["id", "loja"]}, "W"),
            "T.id = S.id AND T.loja = S.loja",
        )

    def test_merge_condition_entra_como_um_and_a_parte(self) -> None:
        """Parentetizado: sem isso um `merge_condition` com `OR` dentro mudaria o
        alcance do AND e casaria linha que nao devia."""
        self.assertEqual(
            on_condition({"merge_keys": ["id"], "merge_condition": "T.a = 1 OR T.b = 2"}, "W"),
            "(T.id = S.id) AND (T.a = 1 OR T.b = 2)",
        )

    def test_on_escrito_a_mao_vence_e_passa_cru(self) -> None:
        opts = {"on": "S.id = T.id AND S.dt >= T.dt", "merge_keys": ["outra"]}
        self.assertEqual(on_condition(opts, "W"), "S.id = T.id AND S.dt >= T.dt")

    def test_sem_on_e_sem_merge_keys_o_erro_cita_os_dois_caminhos(self) -> None:
        with self.assertRaises(ValueError) as capturado:
            on_condition({}, "DeltaWriter")
        mensagem = str(capturado.exception)
        self.assertIn("merge_keys", mensagem)
        self.assertIn("on", mensagem)
        self.assertIn("DeltaWriter", mensagem)

    def test_on_vazio_e_recusado_em_vez_de_virar_um_on_em_branco(self) -> None:
        for valor in ("", "   ", 7, ["T.id = S.id"]):
            with self.subTest(valor=valor):
                with self.assertRaises(ValueError):
                    on_condition({"on": valor}, "W")


class DeleteClausesTest(unittest.TestCase):
    def test_sem_nada_as_duas_saem_vazias(self) -> None:
        self.assertEqual(delete_clauses({}), ("", ""))

    def test_delete_when_vira_uma_matched_condicional(self) -> None:
        matched, _ = delete_clauses({"delete_when": "S.op = 'D'"})
        self.assertEqual(matched, "WHEN MATCHED AND (S.op = 'D') THEN DELETE")

    def test_not_matched_by_source_aceita_flag_e_condicao(self) -> None:
        _, por_origem = delete_clauses({"delete_not_matched_by_source": True})
        self.assertEqual(por_origem, "WHEN NOT MATCHED BY SOURCE THEN DELETE")

        _, com_cond = delete_clauses({"delete_not_matched_by_source": "T.origem = 'erp'"})
        self.assertEqual(
            com_cond, "WHEN NOT MATCHED BY SOURCE AND (T.origem = 'erp') THEN DELETE"
        )

    def test_texto_de_flag_e_lido_como_flag_nao_como_condicao(self) -> None:
        """`"true"` chega assim quando veio de `{param}`, que interpola texto."""
        _, ligado = delete_clauses({"delete_not_matched_by_source": "true"})
        self.assertEqual(ligado, "WHEN NOT MATCHED BY SOURCE THEN DELETE")
        _, desligado = delete_clauses({"delete_not_matched_by_source": "false"})
        self.assertEqual(desligado, "")

    def test_valor_sem_sentido_levanta(self) -> None:
        with self.assertRaises(ValueError):
            delete_clauses({"delete_not_matched_by_source": 3})


class ActionClausesTest(unittest.TestCase):
    def test_sem_actions_o_delete_por_marca_vem_antes_do_update(self) -> None:
        """A ordem e o comportamento: com o DELETE depois do UPDATE incondicional
        a linha marcada seria atualizada e nunca sairia da tabela."""
        acoes = action_clauses(
            {"merge_keys": ["id"], "delete_when": "S.op = 'D'"}, DEFAULTS, "W"
        )
        self.assertEqual(acoes[0], "WHEN MATCHED AND (S.op = 'D') THEN DELETE")
        self.assertEqual(acoes[1:], list(DEFAULTS))

    def test_sem_actions_o_delete_por_ausencia_vem_por_ultimo(self) -> None:
        acoes = action_clauses(
            {"merge_keys": ["id"], "delete_not_matched_by_source": True}, DEFAULTS, "W"
        )
        self.assertEqual(acoes[-1], "WHEN NOT MATCHED BY SOURCE THEN DELETE")

    def test_actions_substitui_os_defaults_na_ordem_dada(self) -> None:
        escritas = [
            "WHEN MATCHED AND S.op = 'D' THEN DELETE",
            "WHEN MATCHED THEN UPDATE SET T.nome = S.nome",
            "WHEN NOT MATCHED THEN INSERT (id, nome) VALUES (S.id, S.nome)",
        ]
        self.assertEqual(action_clauses({"actions": escritas}, DEFAULTS, "W"), escritas)

    def test_actions_ignora_os_deletes_declarativos(self) -> None:
        """As duas formas nao se misturam: quem escreveu `actions` escreveu a
        lista inteira, e um DELETE injetado por fora mudaria o comando debaixo
        dele."""
        acoes = action_clauses(
            {"actions": ["WHEN MATCHED THEN DELETE"], "delete_when": "S.op = 'D'"},
            DEFAULTS,
            "W",
        )
        self.assertEqual(acoes, ["WHEN MATCHED THEN DELETE"])

    def test_espaco_e_quebra_de_linha_sao_normalizados(self) -> None:
        """Uma clausula escrita em varias linhas no JSON vira uma linha so — o
        comando montado continua legivel quando aparece num log de erro."""
        acoes = action_clauses(
            {"actions": ["WHEN MATCHED\n  THEN UPDATE SET\n    T.a = S.a"]},
            DEFAULTS,
            "W",
        )
        self.assertEqual(acoes, ["WHEN MATCHED THEN UPDATE SET T.a = S.a"])

    def test_clausula_que_nao_comeca_com_when_e_recusada(self) -> None:
        with self.assertRaises(ValueError) as capturado:
            action_clauses({"actions": ["UPDATE SET T.a = S.a"]}, DEFAULTS, "W")
        self.assertIn("WHEN", str(capturado.exception))

    def test_incondicional_no_meio_do_grupo_e_recusada(self) -> None:
        """O erro facil: mover o DELETE para o fim da lista, onde le melhor. A
        clausula sem AND sempre casa primeiro e torna a seguinte inalcancavel —
        o Spark recusa, e aqui a recusa vem com o motivo."""
        with self.assertRaises(ValueError) as capturado:
            action_clauses(
                {
                    "actions": [
                        "WHEN MATCHED THEN UPDATE SET T.a = S.a",
                        "WHEN MATCHED AND S.op = 'D' THEN DELETE",
                    ]
                },
                DEFAULTS,
                "W",
            )
        self.assertIn("inalcancavel", str(capturado.exception))

    def test_grupos_diferentes_nao_se_atrapalham(self) -> None:
        """`WHEN MATCHED` incondicional nao bloqueia um `WHEN NOT MATCHED`: sao
        ramos diferentes do MERGE, nao alternativas do mesmo."""
        escritas = [
            "WHEN MATCHED THEN UPDATE SET T.a = S.a",
            "WHEN NOT MATCHED THEN INSERT *",
            "WHEN NOT MATCHED BY SOURCE THEN DELETE",
        ]
        self.assertEqual(action_clauses({"actions": escritas}, DEFAULTS, "W"), escritas)

    def test_not_matched_by_source_nao_e_confundido_com_not_matched(self) -> None:
        """Os dois comecam igual; se o prefixo mais curto casasse primeiro, uma
        clausula `BY SOURCE` bloquearia o INSERT sem relacao com ela."""
        escritas = [
            "WHEN NOT MATCHED BY SOURCE THEN DELETE",
            "WHEN NOT MATCHED THEN INSERT *",
        ]
        self.assertEqual(action_clauses({"actions": escritas}, DEFAULTS, "W"), escritas)

    def test_actions_precisa_ser_lista_nao_vazia_de_texto(self) -> None:
        for valor in ([], "WHEN MATCHED THEN DELETE", [{"when": "matched"}], [""]):
            with self.subTest(valor=valor):
                with self.assertRaises(ValueError):
                    action_clauses({"actions": valor}, DEFAULTS, "W")


class MergeSqlTest(unittest.TestCase):
    def test_o_comando_tem_alvo_origem_on_e_clausulas_nessa_ordem(self) -> None:
        sql = " ".join(
            merge_sql(
                "cat.db.destino",
                "_view",
                {"merge_keys": ["id"], "delete_when": "S.op = 'D'"},
                DEFAULTS,
                "W",
            ).split()
        )
        self.assertEqual(
            sql,
            "MERGE INTO cat.db.destino AS T USING _view AS S ON T.id = S.id "
            "WHEN MATCHED AND (S.op = 'D') THEN DELETE "
            "WHEN MATCHED THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *",
        )

    def test_a_forma_explicita_monta_o_mesmo_esqueleto(self) -> None:
        sql = " ".join(
            merge_sql(
                "delta.`/lake/t`",
                "_view",
                {
                    "on": "S.id = T.id",
                    "actions": ["WHEN MATCHED THEN UPDATE SET T.a = S.a"],
                },
                DEFAULTS,
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
