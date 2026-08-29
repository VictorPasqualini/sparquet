"""Linhas gravadas lidas do próprio Spark, sem uma action a mais.

O problema
----------
Spark é lazy: um DataFrame só descreve o plano, e nada acontece até uma *action*.
Para dizer quantas linhas um destino recebeu, o caminho óbvio é `df.count()` antes
de escrever — só que isso é uma action inteira, e executa a cadeia
`read → transformations → projeção` **uma vez a mais** por destino. Com M destinos
a cadeia roda M+1 vezes em vez de M. Medido em 6M linhas (CSV → Parquet, `local[4]`),
o `count()` de um destino custou +57% sobre o tempo da escrita.

A saída
-------
O job da escrita **já conta** as linhas: o Spark instrumenta o comando de gravação
com `BasicWriteJobStatsTracker`, que publica `number of output rows` junto com
`number of written files`, `written output`, `number of dynamic part` e os dois
tempos de commit. O valor chega ao `SQLAppStatusStore` (o mesmo que alimenta a aba
SQL da UI) pelo barramento de eventos. Ler de lá é **exato** e não dispara job
nenhum: é o número que a própria escrita apurou.

Como o número é achado
----------------------
`number of output rows` aparece em vários nós do plano (a leitura da fonte tem o
seu, o `Range` tem o seu). O que distingue o da escrita é a companhia: os nomes em
`_WRITE_ANCHORS` só existem no rastreador de escrita, e as métricas de um mesmo
rastreador recebem ids de acumulador em bloco. Então o contador certo é o
`number of output rows` cujo id cai dentro da faixa de ids das âncoras.

Sem âncora — escrita JDBC, um formato que usa outro protocolo de commit, Spark
Connect, uma versão que mova essas APIs — nada é adivinhado: `measure()` devolve
`None` e quem chamou volta ao `count()`. Errar o número seria pior que pagar a
action, porque `rows_written` alimenta relatório, histórico e cobrança.
"""
from __future__ import annotations

import re
from typing import Any, Callable, List, Optional

#: Nomes que só o rastreador de escrita publica. Bastaria um, mas qualquer um
#: deles serve de âncora e formatos diferentes registram subconjuntos diferentes.
_WRITE_ANCHORS = frozenset(
    {
        "number of written files",
        "written output",
        "number of dynamic part",
        "task commit time",
        "job commit time",
    }
)

#: O contador em si. O nome é estável desde o Spark 2.x.
_ROWS_METRIC = "number of output rows"

#: As métricas chegam ao store pelo barramento de eventos, que é assíncrono — a
#: escrita pode retornar antes de o valor estar lá. Esperar o barramento drenar
#: custa milissegundos; o teto existe só para nunca travar o pipeline.
_BUS_TIMEOUT_MS = 15_000

#: O valor vem formatado para leitura humana (`"1,000"`), então a leitura tira
#: tudo que não for dígito em vez de assumir a formatação de um locale.
_NOT_DIGITS = re.compile(r"[^0-9]")


class WriteMetrics:
    """Lê quantas linhas a última escrita gravou, ou admite que não sabe.

    Instanciar é barato e não falha: se a sessão não expõe as APIs de que isto
    depende (Spark Connect, por exemplo), o objeto simplesmente nasce indisponível
    e todo `measure()` devolve `None`.
    """

    def __init__(self, spark: Any) -> None:
        self._store: Any = None
        self._bus: Any = None
        try:
            self._store = spark._jsparkSession.sharedState().statusStore()
            self._bus = spark.sparkContext._jsc.sc().listenerBus()
        except Exception:  # pragma: no cover - depende da distribuição do Spark
            self._store = None
            self._bus = None

    @property
    def available(self) -> bool:
        """Se dá para tentar. `measure()` ainda pode devolver `None` com isto True:
        o formato pode não publicar métrica de escrita."""
        return self._store is not None

    def measure(self, write: Callable[[], None]) -> Optional[int]:
        """Executa `write()` e devolve as linhas que ele gravou, ou `None`.

        `None` significa *não sei*, nunca *zero*: quem chamou precisa contar por
        conta própria. A escrita acontece exatamente uma vez em qualquer caminho,
        inclusive quando a leitura da métrica falha depois dela.
        """
        if not self.available:
            write()
            return None

        baseline = self._last_execution_id()
        write()
        if baseline is None:
            return None
        try:
            self._drain_events()
            return self._rows_after(baseline)
        except Exception:  # pragma: no cover - defensivo
            return None

    # ------------------------------------------------------------------ interno

    def _last_execution_id(self) -> Optional[int]:
        """Maior id de execução SQL já registrado, para saber o que é novo.

        A lista vem ordenada por id crescente, e o store descarta as mais antigas
        (`spark.sql.ui.retainedExecutions`) — por isso a marca é o id do fim da
        lista, e não a contagem, que não é monotônica.
        """
        try:
            executions = self._store.executionsList()
            size = executions.size()
            if size == 0:
                return -1
            return int(executions.apply(size - 1).executionId())
        except Exception:  # pragma: no cover - defensivo
            return None

    def _drain_events(self) -> None:
        if self._bus is None:
            return
        try:
            self._bus.waitUntilEmpty(_BUS_TIMEOUT_MS)
        except Exception:
            # Barramento cheio ou assinatura diferente: segue e tenta ler mesmo
            # assim. Se o valor ainda não chegou, `_rows_after` devolve None.
            pass

    def _rows_after(self, baseline: int) -> Optional[int]:
        """Percorre as execuções novas, da mais recente para a mais antiga, e
        devolve o contador da primeira que for uma escrita.

        Da mais recente para trás porque um único `write()` pode disparar mais de
        uma execução (um formato transacional lê o log antes de gravar), e a que
        interessa é a que gravou por último.
        """
        executions = self._store.executionsList()
        for index in range(executions.size() - 1, -1, -1):
            execution = executions.apply(index)
            if int(execution.executionId()) <= baseline:
                break
            rows = self._write_rows_of(execution)
            if rows is not None:
                return rows
        return None

    def _write_rows_of(self, execution: Any) -> Optional[int]:
        """O `number of output rows` do rastreador de escrita desta execução."""
        anchors: List[int] = []
        candidates: List[int] = []
        metrics = execution.metrics()
        iterator = metrics.iterator()
        while iterator.hasNext():
            metric = iterator.next()
            name = str(metric.name())
            accumulator = int(metric.accumulatorId())
            if name in _WRITE_ANCHORS:
                anchors.append(accumulator)
            elif name == _ROWS_METRIC:
                candidates.append(accumulator)

        if not anchors or not candidates:
            return None

        # As métricas de um rastreador são registradas em bloco, então o contador
        # da escrita é o que cai entre os ids das âncoras. Um `number of output
        # rows` de um scan fica fora da faixa.
        low, high = min(anchors), max(anchors)
        inside = [acc for acc in candidates if low < acc < high]
        if len(inside) != 1:
            return None

        values = self._store.executionMetrics(execution.executionId())
        accumulator = inside[0]
        if not values.contains(accumulator):
            return None
        return _as_int(str(values.apply(accumulator)))


def _as_int(formatted: str) -> Optional[int]:
    digits = _NOT_DIGITS.sub("", formatted)
    return int(digits) if digits else None
