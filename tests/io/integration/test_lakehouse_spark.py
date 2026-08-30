"""Integração dos formatos de lakehouse: `delta` e `iceberg`.

Os dois trazem o que parquet não tem — transação, versão, `MERGE INTO` — e é
exatamente isso que um teste de montagem não alcança: o `merge` dos dois monta
SQL na mão, e SQL só se prova executando.

O que cada teste trava:

  delta     ida e volta por caminho físico; `append` soma sem apagar;
            `merge` atualiza a linha que existe e insere a que não existe;
            time travel devolve o estado anterior por `versionAsOf`.
  iceberg   ida e volta por tabela de catálogo (`local.db.x`, catálogo hadoop,
            sem metastore); `merge` com `merge_keys`; `partition_by` na criação.

A referência muda de forma entre os dois de propósito: Delta aceita caminho e
nome de tabela (`is_table_name` decide, em `sparquet/io/base.py`), Iceberg só faz
sentido com tabela de catálogo — e o writer a cria quando não existe. Testar cada um pelo caminho que o usuário realmente usa vale mais do
que uniformizar.

    SPARQUET_IT=1 python tests/io/integration/test_lakehouse_spark.py
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness  # noqa: E402


@harness.requires_integration
class DeltaTest(unittest.TestCase):
    def test_ida_e_volta_por_caminho_fisico(self) -> None:
        directory = (harness.WORK / "delta").as_posix()

        written, read_back = harness.round_trip(
            "delta",
            {"format": "delta", "path": directory, "mode": "overwrite"},
            {"format": "delta", "path": directory},
        )

        self.assertTrue(written.success, msg=written.error)
        self.assertTrue(read_back.success, msg=read_back.error)
        self.assertEqual(read_back.rows_read, len(harness.SEED_ROWS))

        por_id = {linha["id"]: linha for linha in harness.rows_back("delta")}
        self.assertEqual(por_id["2"]["nome"], harness.SEED_ROWS[1][1])
        self.assertEqual(por_id["3"]["nome"], "")

    def test_append_soma_sem_apagar_o_que_estava(self) -> None:
        directory = (harness.WORK / "delta-append").as_posix()
        escrita = {"format": "delta", "path": directory, "mode": "overwrite"}

        harness.run(
            {"name": "it-delta-append-1", "input": harness.seed_input(), "output": escrita}
        )
        harness.run(
            {
                "name": "it-delta-append-2",
                "input": harness.seed_input(),
                "output": {**escrita, "mode": "append"},
            }
        )
        lido = harness.run(
            {
                "name": "it-delta-append-leitura",
                "input": {"format": "delta", "path": directory},
                "output": {"format": "view", "path": "it_delta_append"},
            }
        )

        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS) * 2)

    def test_merge_atualiza_o_que_existe_e_insere_o_que_falta(self) -> None:
        """O caminho que o teste de montagem não cobre: o SQL é montado na mão."""
        directory = (harness.WORK / "delta-merge").as_posix()

        harness.run(
            {
                "name": "it-delta-merge-base",
                "input": harness.seed_input(),
                "output": {"format": "delta", "path": directory, "mode": "overwrite"},
            }
        )
        # id=1 já existe e vira "atualizado"; id=9 não existe e entra novo.
        origem = harness.work_dir("delta-merge-origem") / "novo.csv"
        origem.write_text(
            "id,nome,valor\n1,atualizado,9.9\n9,novo,0.5\n", encoding="utf-8"
        )
        resultado = harness.run(
            {
                "name": "it-delta-merge",
                "input": {
                    "format": "csv",
                    "path": origem.as_posix(),
                    "options": {"header": "true", "inferSchema": "true"},
                },
                "output": {
                    "format": "delta",
                    "path": directory,
                    "mode": "merge",
                    "options": {"merge_keys": ["id"]},
                },
            }
        )
        self.assertTrue(resultado.success, msg=resultado.error)

        harness.run(
            {
                "name": "it-delta-merge-leitura",
                "input": {"format": "delta", "path": directory},
                "output": {
                    "format": "csv",
                    "path": (harness.WORK / "back-delta-merge").as_posix(),
                    "mode": "overwrite",
                    "options": {"header": "true"},
                },
            }
        )
        por_id = {linha["id"]: linha for linha in harness.rows_back("delta-merge")}
        self.assertEqual(set(por_id), {"1", "2", "3", "9"})
        self.assertEqual(por_id["1"]["nome"], "atualizado")
        self.assertEqual(por_id["9"]["nome"], "novo")
        # Quem o merge não tocou continua como estava.
        self.assertEqual(por_id["2"]["nome"], harness.SEED_ROWS[1][1])

    def test_merge_apaga_a_linha_que_a_origem_marcou_como_excluida(self) -> None:
        """`delete_when`: o CDC de exclusao. A origem TRAZ a linha apagada, com uma
        marca; sem esta clausula ela seria atualizada e nunca sairia da tabela."""
        directory = (harness.WORK / "delta-merge-delete").as_posix()

        harness.run(
            {
                "name": "it-delta-del-base",
                "input": harness.seed_input(),
                "output": {"format": "delta", "path": directory, "mode": "overwrite"},
            }
        )
        # A origem tem uma coluna a mais, `op`, que decide o destino da linha. O
        # UPDATE do Delta so mexe nas colunas listadas, entao a marca pode vir
        # junto sem sujar a tabela.
        origem = harness.work_dir("delta-del-origem") / "cdc.csv"
        origem.write_text(
            "id,nome,valor,op\n1,ignorado,0.0,D\n2,atualizado,4.2,U\n",
            encoding="utf-8",
        )
        resultado = harness.run(
            {
                "name": "it-delta-del",
                "input": {
                    "format": "csv",
                    "path": origem.as_posix(),
                    "options": {"header": "true", "inferSchema": "true"},
                },
                "output": {
                    "format": "delta",
                    "path": directory,
                    "mode": "merge",
                    "options": {"merge_keys": ["id"], "delete_when": "S.op = 'D'"},
                },
            }
        )
        self.assertTrue(resultado.success, msg=resultado.error)

        por_id = self._ler("delta-del", directory)
        # id=1 saiu, id=2 foi atualizado, id=3 nao foi tocado.
        self.assertEqual(set(por_id), {"2", "3"})
        self.assertEqual(por_id["2"]["nome"], "atualizado")

    def test_merge_escrito_a_mao_roda_as_clausulas_na_ordem_dada(self) -> None:
        """A forma explicita: `on` e `actions` passam crus para o SQL. O que so
        aparece aqui e que o comando montado e aceito pelo Delta — o teste de
        montagem trava o texto, nao o dialeto — e que um UPDATE parcial mexe so
        nas colunas listadas, deixando o resto da linha como estava."""
        directory = (harness.WORK / "delta-merge-actions").as_posix()

        harness.run(
            {
                "name": "it-delta-actions-base",
                "input": harness.seed_input(),
                "output": {"format": "delta", "path": directory, "mode": "overwrite"},
            }
        )
        origem = harness.work_dir("delta-actions-origem") / "cdc.csv"
        origem.write_text(
            "id,nome,valor,op\n1,ignorado,9.9,D\n2,renomeado,9.9,U\n9,novo,7.5,I\n",
            encoding="utf-8",
        )
        resultado = harness.run(
            {
                "name": "it-delta-actions",
                "input": {
                    "format": "csv",
                    "path": origem.as_posix(),
                    "options": {"header": "true", "inferSchema": "true"},
                },
                "output": {
                    "format": "delta",
                    "path": directory,
                    "mode": "merge",
                    "options": {
                        "on": "S.id = T.id",
                        "actions": [
                            "WHEN MATCHED AND S.op = 'D' THEN DELETE",
                            "WHEN MATCHED THEN UPDATE SET T.nome = S.nome",
                            "WHEN NOT MATCHED THEN INSERT (id, nome, valor) "
                            "VALUES (S.id, S.nome, S.valor)",
                        ],
                    },
                },
            }
        )
        self.assertTrue(resultado.success, msg=resultado.error)

        por_id = self._ler("delta-actions", directory)
        # id=1 saiu pelo DELETE, id=9 entrou pelo INSERT, id=3 nao foi tocado.
        self.assertEqual(set(por_id), {"2", "3", "9"})
        # O UPDATE listou so `nome`: o valor original de id=2 continua la.
        self.assertEqual(por_id["2"]["nome"], "renomeado")
        self.assertEqual(por_id["2"]["valor"], "-0.25")
        self.assertEqual(por_id["9"]["nome"], "novo")

    def test_merge_apaga_o_que_a_origem_nao_trouxe(self) -> None:
        """`delete_not_matched_by_source`: sincroniza o destino com um snapshot
        COMPLETO da origem. Contra uma carga incremental isto apagaria tudo o que
        aquela carga nao repetiu — por isso fica desligado por default."""
        directory = (harness.WORK / "delta-merge-sync").as_posix()

        harness.run(
            {
                "name": "it-delta-sync-base",
                "input": harness.seed_input(),
                "output": {"format": "delta", "path": directory, "mode": "overwrite"},
            }
        )
        # O snapshot tem 1 e 3; o 2, que sumiu da origem, deve sair do destino.
        origem = harness.work_dir("delta-sync-origem") / "snapshot.csv"
        origem.write_text(
            "id,nome,valor\n1,alpha,1.5\n3,chegou,7.0\n", encoding="utf-8"
        )
        resultado = harness.run(
            {
                "name": "it-delta-sync",
                "input": {
                    "format": "csv",
                    "path": origem.as_posix(),
                    "options": {"header": "true", "inferSchema": "true"},
                },
                "output": {
                    "format": "delta",
                    "path": directory,
                    "mode": "merge",
                    "options": {
                        "merge_keys": ["id"],
                        "delete_not_matched_by_source": True,
                    },
                },
            }
        )
        self.assertTrue(resultado.success, msg=resultado.error)

        por_id = self._ler("delta-sync", directory)
        self.assertEqual(set(por_id), {"1", "3"})
        self.assertEqual(por_id["3"]["nome"], "chegou")

    def _ler(self, rotulo: str, directory: str) -> dict:
        """Le a tabela de volta como CSV e devolve as linhas por id."""
        harness.run(
            {
                "name": "it-" + rotulo + "-leitura",
                "input": {"format": "delta", "path": directory},
                "output": {
                    "format": "csv",
                    "path": (harness.WORK / ("back-" + rotulo)).as_posix(),
                    "mode": "overwrite",
                    "options": {"header": "true"},
                },
            }
        )
        return {linha["id"]: linha for linha in harness.rows_back(rotulo)}

    def test_merge_sem_merge_keys_falha_dizendo_o_que_falta(self) -> None:
        directory = (harness.WORK / "delta-merge-sem-chave").as_posix()

        harness.run(
            {
                "name": "it-delta-merge-sem-chave-base",
                "input": harness.seed_input(),
                "output": {"format": "delta", "path": directory, "mode": "overwrite"},
            }
        )
        resultado = harness.run(
            {
                "name": "it-delta-merge-sem-chave",
                "input": harness.seed_input(),
                "output": {"format": "delta", "path": directory, "mode": "merge"},
            }
        )

        self.assertFalse(resultado.success)
        self.assertIn("merge_keys", resultado.error or "")

    def test_time_travel_devolve_a_versao_anterior(self) -> None:
        """A promessa do formato: a versão 0 continua legível depois do overwrite."""
        directory = (harness.WORK / "delta-tempo").as_posix()
        escrita = {"format": "delta", "path": directory, "mode": "overwrite"}

        harness.run(
            {"name": "it-delta-tempo-v0", "input": harness.seed_input(), "output": escrita}
        )
        menor = harness.work_dir("delta-tempo-origem") / "uma.csv"
        menor.write_text("id,nome,valor\n1,so uma,1.0\n", encoding="utf-8")
        harness.run(
            {
                "name": "it-delta-tempo-v1",
                "input": {
                    "format": "csv",
                    "path": menor.as_posix(),
                    "options": {"header": "true", "inferSchema": "true"},
                },
                "output": {**escrita, "options": {"overwriteSchema": "true"}},
            }
        )

        agora = harness.run(
            {
                "name": "it-delta-tempo-agora",
                "input": {"format": "delta", "path": directory},
                "output": {"format": "view", "path": "it_delta_agora"},
            }
        )
        antes = harness.run(
            {
                "name": "it-delta-tempo-antes",
                "input": {
                    "format": "delta",
                    "path": directory,
                    "options": {"versionAsOf": "0"},
                },
                "output": {"format": "view", "path": "it_delta_antes"},
            }
        )

        self.assertEqual(agora.rows_read, 1)
        self.assertEqual(antes.rows_read, len(harness.SEED_ROWS))

    def test_particionar_e_substituir_uma_particao_com_replace_where(self) -> None:
        """`replaceWhere` é o overwrite cirúrgico do Delta: troca só a partição
        que a condição descreve e deixa as outras onde estão. Sem ele, corrigir
        um dia de dado obriga a reescrever a tabela inteira."""
        directory = (harness.WORK / "delta-particionado").as_posix()

        gravado = harness.run(
            {
                "name": "it-delta-particionado",
                "input": harness.seed_input(),
                "output": {
                    "format": "delta",
                    "path": directory,
                    "mode": "overwrite",
                    "partition_by": ["id"],
                },
            }
        )
        self.assertTrue(gravado.success, msg=gravado.error)

        origem = harness.work_dir("delta-replace-origem") / "so-o-um.csv"
        origem.write_text("id,nome,valor\n1,corrigido,7.7\n", encoding="utf-8")
        substituido = harness.run(
            {
                "name": "it-delta-replace-where",
                "input": {
                    "format": "csv",
                    "path": origem.as_posix(),
                    "options": {"header": "true", "inferSchema": "true"},
                },
                "output": {
                    "format": "delta",
                    "path": directory,
                    "mode": "overwrite",
                    "partition_by": ["id"],
                    "options": {"replaceWhere": "id = 1"},
                },
            }
        )
        self.assertTrue(substituido.success, msg=substituido.error)

        harness.run(
            {
                "name": "it-delta-replace-leitura",
                "input": {"format": "delta", "path": directory},
                "output": {
                    "format": "csv",
                    "path": (harness.WORK / "back-delta-replace").as_posix(),
                    "mode": "overwrite",
                    "options": {"header": "true"},
                },
            }
        )
        por_id = {linha["id"]: linha for linha in harness.rows_back("delta-replace")}
        # A partição 1 foi trocada; as outras duas continuam como a semente as deixou.
        self.assertEqual(set(por_id), {"1", "2", "3"})
        self.assertEqual(por_id["1"]["nome"], "corrigido")
        self.assertEqual(por_id["2"]["nome"], harness.SEED_ROWS[1][1])


@harness.requires_integration
class IcebergTest(unittest.TestCase):
    """Catálogo `local`, tipo hadoop — um diretório, sem metastore nem serviço."""

    def test_ida_e_volta_por_tabela_de_catalogo(self) -> None:
        tabela = "local.db.round_trip"

        written, read_back = harness.round_trip(
            "iceberg",
            {"format": "iceberg", "path": tabela, "mode": "overwrite"},
            {"format": "iceberg", "path": tabela},
        )

        self.assertTrue(written.success, msg=written.error)
        self.assertTrue(read_back.success, msg=read_back.error)
        self.assertEqual(read_back.rows_read, len(harness.SEED_ROWS))

        por_id = {linha["id"]: linha for linha in harness.rows_back("iceberg")}
        self.assertEqual(por_id["2"]["nome"], harness.SEED_ROWS[1][1])
        self.assertEqual(por_id["3"]["valor"], "")

    def test_append_soma_sem_apagar_o_que_estava(self) -> None:
        tabela = "local.db.acumula"
        escrita = {"format": "iceberg", "path": tabela, "mode": "overwrite"}

        harness.run(
            {"name": "it-iceberg-append-1", "input": harness.seed_input(), "output": escrita}
        )
        harness.run(
            {
                "name": "it-iceberg-append-2",
                "input": harness.seed_input(),
                "output": {**escrita, "mode": "append"},
            }
        )
        lido = harness.run(
            {
                "name": "it-iceberg-append-leitura",
                "input": {"format": "iceberg", "path": tabela},
                "output": {"format": "view", "path": "it_iceberg_append"},
            }
        )

        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS) * 2)

    def test_merge_atualiza_e_insere(self) -> None:
        tabela = "local.db.merge_alvo"

        harness.run(
            {
                "name": "it-iceberg-merge-base",
                "input": harness.seed_input(),
                "output": {"format": "iceberg", "path": tabela, "mode": "overwrite"},
            }
        )
        origem = harness.work_dir("iceberg-merge-origem") / "novo.csv"
        origem.write_text(
            "id,nome,valor\n1,atualizado,9.9\n9,novo,0.5\n", encoding="utf-8"
        )
        resultado = harness.run(
            {
                "name": "it-iceberg-merge",
                "input": {
                    "format": "csv",
                    "path": origem.as_posix(),
                    "options": {"header": "true", "inferSchema": "true"},
                },
                "output": {
                    "format": "iceberg",
                    "path": tabela,
                    "mode": "merge",
                    "options": {"merge_keys": ["id"]},
                },
            }
        )
        self.assertTrue(resultado.success, msg=resultado.error)

        harness.run(
            {
                "name": "it-iceberg-merge-leitura",
                "input": {"format": "iceberg", "path": tabela},
                "output": {
                    "format": "csv",
                    "path": (harness.WORK / "back-iceberg-merge").as_posix(),
                    "mode": "overwrite",
                    "options": {"header": "true"},
                },
            }
        )
        por_id = {linha["id"]: linha for linha in harness.rows_back("iceberg-merge")}
        self.assertEqual(set(por_id), {"1", "2", "3", "9"})
        self.assertEqual(por_id["1"]["nome"], "atualizado")
        self.assertEqual(por_id["9"]["nome"], "novo")

    def test_merge_apaga_a_linha_que_a_origem_marcou_como_excluida(self) -> None:
        """Mesmo CDC do Delta. A diferenca esta em como cada um trata a coluna de
        controle: o Delta lista as colunas do destino no UPDATE, o Iceberg usa
        `UPDATE SET *`. Este teste trava que a marca `op`, que o destino nao tem,
        atravessa o `UPDATE SET *` sem quebrar e sem virar coluna da tabela."""
        tabela = "local.db.merge_delete"

        harness.run(
            {
                "name": "it-iceberg-del-base",
                "input": harness.seed_input(),
                "output": {"format": "iceberg", "path": tabela, "mode": "overwrite"},
            }
        )
        origem = harness.work_dir("iceberg-del-origem") / "cdc.csv"
        origem.write_text(
            "id,nome,valor,op\n1,ignorado,0.0,D\n2,atualizado,4.2,U\n",
            encoding="utf-8",
        )
        resultado = harness.run(
            {
                "name": "it-iceberg-del",
                "input": {
                    "format": "csv",
                    "path": origem.as_posix(),
                    "options": {"header": "true", "inferSchema": "true"},
                },
                "output": {
                    "format": "iceberg",
                    "path": tabela,
                    "mode": "merge",
                    "options": {"merge_keys": ["id"], "delete_when": "S.op = 'D'"},
                },
            }
        )
        self.assertTrue(resultado.success, msg=resultado.error)

        por_id = self._ler("iceberg-del", tabela)
        self.assertEqual(set(por_id), {"2", "3"})
        self.assertEqual(por_id["2"]["nome"], "atualizado")

    def test_merge_apaga_o_que_a_origem_nao_trouxe(self) -> None:
        tabela = "local.db.merge_sync"

        harness.run(
            {
                "name": "it-iceberg-sync-base",
                "input": harness.seed_input(),
                "output": {"format": "iceberg", "path": tabela, "mode": "overwrite"},
            }
        )
        origem = harness.work_dir("iceberg-sync-origem") / "snapshot.csv"
        origem.write_text(
            "id,nome,valor\n1,alpha,1.5\n3,chegou,7.0\n", encoding="utf-8"
        )
        resultado = harness.run(
            {
                "name": "it-iceberg-sync",
                "input": {
                    "format": "csv",
                    "path": origem.as_posix(),
                    "options": {"header": "true", "inferSchema": "true"},
                },
                "output": {
                    "format": "iceberg",
                    "path": tabela,
                    "mode": "merge",
                    "options": {
                        "merge_keys": ["id"],
                        "delete_not_matched_by_source": True,
                    },
                },
            }
        )
        self.assertTrue(resultado.success, msg=resultado.error)

        por_id = self._ler("iceberg-sync", tabela)
        self.assertEqual(set(por_id), {"1", "3"})
        self.assertEqual(por_id["3"]["nome"], "chegou")

    def _ler(self, rotulo: str, tabela: str) -> dict:
        harness.run(
            {
                "name": "it-" + rotulo + "-leitura",
                "input": {"format": "iceberg", "path": tabela},
                "output": {
                    "format": "csv",
                    "path": (harness.WORK / ("back-" + rotulo)).as_posix(),
                    "mode": "overwrite",
                    "options": {"header": "true"},
                },
            }
        )
        return {linha["id"]: linha for linha in harness.rows_back(rotulo)}

    def test_particao_na_criacao_nao_muda_o_que_volta(self) -> None:
        tabela = "local.db.particionada"

        harness.run(
            {
                "name": "it-iceberg-particionada",
                "input": harness.seed_input(),
                "output": {
                    "format": "iceberg",
                    "path": tabela,
                    "mode": "overwrite",
                    "partition_by": ["id"],
                },
            }
        )
        lido = harness.run(
            {
                "name": "it-iceberg-particionada-leitura",
                "input": {"format": "iceberg", "path": tabela},
                "output": {"format": "view", "path": "it_iceberg_particionada"},
            }
        )

        self.assertTrue(lido.success, msg=lido.error)
        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS))


if __name__ == "__main__":
    unittest.main(verbosity=2)
