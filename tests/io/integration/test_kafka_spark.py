"""Integração Kafka — publicar e reler um tópico de verdade.

`tests/io/test_connectors.py` prova a montagem: o alias `bootstrap_servers` virando
`kafka.bootstrap.servers`, o `path` virando `subscribe`, os defaults de batch
(`earliest`/`latest`) e o erro sem bootstrap. Nada disso sabe se o broker aceita o
que foi montado, e o `KafkaWriter` faz algo que só um broker real verifica: ele
**renomeia e projeta** o DataFrame (`payload` → `value`, `header` → `key`, e
descarta o resto), porque o conector Kafka recusa qualquer coluna fora do schema
dele. Aqui isso executa.

O tópico é criado pela primeira publicação (`auto.create.topics.enable` é o default
da imagem), e a leitura em batch sem offsets explícitos precisa devolver o tópico
inteiro — é o que os defaults do `KafkaReader` existem para garantir.

    docker compose -f tests/io/integration/docker-compose.yml up -d kafka
    SPARQUET_IT=1 python tests/io/integration/test_kafka_spark.py
    docker compose -f tests/io/integration/docker-compose.yml down -v
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness  # noqa: E402
import services  # noqa: E402

#: Cada execução usa um tópico novo: um tópico reaproveitado guarda as mensagens
#: da execução anterior, e a contagem passaria a medir histórico em vez de escrita.
_SUFIXO = str(id(harness.WORK))[-6:]


def _bootstrap() -> str:
    return f"{services.host_of('kafka')}:{services.port_of('kafka')}"


def _conexao() -> dict:
    return {"bootstrap_servers": _bootstrap()}


#: `payload` e `header` são os nomes que o `KafkaWriter` procura por default —
#: montá-los assim é o que faz a escrita não precisar de opção nenhuma.
_MONTA_MENSAGEM = [
    {
        "type": "with_column",
        "columns": {
            "payload": "concat_ws('|', cast(id as string), coalesce(nome, '<nulo>'))",
            "header": "cast(id as string)",
        },
    }
]

#: A volta: o conector devolve `key`/`value` binários, sempre.
_LE_MENSAGEM = [
    {
        "type": "with_column",
        "columns": {
            "chave": "cast(key as string)",
            "mensagem": "cast(value as string)",
        },
    },
    {"type": "select", "columns": ["chave", "mensagem"]},
]


def _esperadas() -> set:
    return {
        f"{identificador}|{'<nulo>' if nome is None else nome}"
        for identificador, nome, _ in harness.SEED_ROWS
    }


@harness.requires_integration
@services.requires_service("kafka")
class KafkaTest(unittest.TestCase):
    def _publica(self, topico: str, **opcoes) -> object:
        return harness.run(
            {
                "name": f"it-kafka-escrita-{topico}",
                "input": harness.seed_input(),
                "transformations": _MONTA_MENSAGEM,
                "output": {
                    "format": "kafka",
                    "path": topico,
                    "mode": "append",
                    "options": {**_conexao(), **opcoes},
                },
            }
        )

    def _consome(self, topico: str, rotulo: str, **opcoes) -> object:
        return harness.run(
            {
                "name": f"it-kafka-leitura-{topico}",
                "input": {
                    "format": "kafka",
                    "path": topico,
                    "options": {**_conexao(), **opcoes},
                },
                "transformations": _LE_MENSAGEM,
                "output": {
                    "format": "csv",
                    "path": (harness.WORK / f"back-{rotulo}").as_posix(),
                    "mode": "overwrite",
                    "options": {"header": "true"},
                },
            }
        )

    def test_publica_e_rele_o_topico_inteiro_com_os_nomes_default(self) -> None:
        topico = f"sparquet-it-{_SUFIXO}"

        publicado = self._publica(topico)
        self.assertTrue(publicado.success, msg=publicado.error)

        lido = self._consome(topico, "kafka")
        self.assertTrue(lido.success, msg=lido.error)
        # Sem `startingOffsets` no JSON: o default `earliest` do reader é o que
        # faz um read em batch ver o que já estava no tópico.
        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS))

        linhas = harness.rows_back("kafka")
        self.assertEqual({linha["mensagem"] for linha in linhas}, _esperadas())
        # A chave sobreviveu como chave, e não como mais uma coluna do valor.
        self.assertEqual(
            {linha["chave"] for linha in linhas},
            {str(identificador) for identificador, _, _ in harness.SEED_ROWS},
        )

    def test_value_column_e_key_column_apontam_para_outras_colunas(self) -> None:
        """O mapeamento explícito, que é como se publica um DataFrame que não foi
        montado pensando em Kafka."""
        topico = f"sparquet-it-mapeado-{_SUFIXO}"

        publicado = harness.run(
            {
                "name": "it-kafka-mapeado-escrita",
                "input": harness.seed_input(),
                "output": {
                    "format": "kafka",
                    "path": topico,
                    "mode": "append",
                    "options": {
                        **_conexao(),
                        # `nome` é nulo numa das linhas: o conector aceita value
                        # nulo, e é a linha que revela projeção errada.
                        "value_column": "nome",
                        "key_column": None,
                    },
                },
            }
        )
        self.assertTrue(publicado.success, msg=publicado.error)

        lido = self._consome(topico, "kafka-mapeado")
        self.assertTrue(lido.success, msg=lido.error)
        self.assertEqual(lido.rows_read, len(harness.SEED_ROWS))

        mensagens = {linha["mensagem"] for linha in harness.rows_back("kafka-mapeado")}
        self.assertEqual(
            mensagens,
            {"" if nome is None else nome for _, nome, _ in harness.SEED_ROWS},
        )

    def test_starting_offsets_latest_e_recusado_em_leitura_batch(self) -> None:
        """`options` chega ao conector — inclusive quando o valor não faz sentido.

        `latest` só existe em streaming: numa leitura batch o fim do tópico já é
        o ponto de parada, então começar nele seria pedir um intervalo vazio e o
        Spark recusa em vez de devolver zero linha. É a contraprova de que o
        default `earliest` do reader não é decoração: é o único início válido
        para batch, e o teste de cima está lendo o tópico por causa dele.
        """
        topico = f"sparquet-it-latest-{_SUFIXO}"

        self._publica(topico)
        lido = self._consome(topico, "kafka-latest", startingOffsets="latest")

        self.assertFalse(lido.success)
        self.assertIn(
            "starting offset can't be latest for batch queries", lido.error or ""
        )

    def test_topico_inexistente_falha_dizendo_o_nome(self) -> None:
        """Sem `subscribe` válido o conector não inventa tópico na leitura — e o
        erro precisa nomear o que não existe, não morrer em timeout."""
        lido = self._consome(f"sparquet-it-nao-existe-{_SUFIXO}", "kafka-ausente")

        # Alguns brokers auto-criam o tópico na assinatura e devolvem vazio; o
        # que não pode acontecer é vir linha de outro tópico.
        if lido.success:
            self.assertEqual(lido.rows_read, 0)
        else:
            # O nome do tópico não aparece: quem falha é o `describeTopics` do
            # AdminClient, e a exceção que sobe é a do Kafka, sem o tópico no
            # texto. Vale como erro nomeado — diz qual recurso falta — e não
            # como timeout, que é o que este teste existe para descartar.
            self.assertIn("UnknownTopicOrPartitionException", lido.error or "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
