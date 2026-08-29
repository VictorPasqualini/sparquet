"""De onde sai `rows_written` — e o que acontece quando a métrica não existe.

O número de linhas gravadas era um `count()` por destino: uma action inteira, que
reexecuta `read → transformations → projeção` só para contar. O job da escrita já
conta enquanto grava, e `WriteMetrics` lê esse contador do `SQLAppStatusStore`.

Dois blocos aqui, por motivos diferentes:

- `TestMetricSelection` não precisa de Spark. Ele trava a *escolha* do contador
  entre as métricas do plano, que é a parte que pode errar em silêncio:
  `number of output rows` aparece em vários nós, e pegar o do scan em vez do da
  escrita daria um número plausível e errado. Os objetos JVM são dublês.
- `TestAgainstSpark` roda o Spark de verdade e compara o número lido com o
  `count()` do que foi gravado. É o teste que garante que a leitura corresponde à
  realidade, incluindo o caso em que o filtro faz scan e escrita divergirem.

    python tests/test_write_metrics.py
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest

try:  # pyspark pode nao estar instalado no ambiente de testes puros
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - ambiente sem pyspark
    SparkSession = None  # type: ignore[assignment]

from sparquet.core.write_metrics import WriteMetrics, _as_int


# --------------------------------------------------------------- dublês da JVM
#
# py4j devolve coleções Scala, não listas Python: `size()`/`apply(i)` numa Seq e
# `contains(k)`/`apply(k)` num Map. Os dublês imitam essa forma, porque é ela que
# o código sob teste usa.


class FakeSeq:
    def __init__(self, items):
        self._items = list(items)

    def size(self):
        return len(self._items)

    def apply(self, index):
        return self._items[index]

    def iterator(self):
        return FakeIterator(self._items)


class FakeIterator:
    def __init__(self, items):
        self._items = list(items)
        self._index = 0

    def hasNext(self):
        return self._index < len(self._items)

    def next(self):
        item = self._items[self._index]
        self._index += 1
        return item


class FakeMap:
    def __init__(self, values):
        self._values = dict(values)

    def contains(self, key):
        return key in self._values

    def apply(self, key):
        return self._values[key]


class FakeMetric:
    def __init__(self, accumulator_id, name):
        self._id = accumulator_id
        self._name = name

    def accumulatorId(self):
        return self._id

    def name(self):
        return self._name


class FakeExecution:
    def __init__(self, execution_id, metrics):
        self._id = execution_id
        self._metrics = metrics

    def executionId(self):
        return self._id

    def metrics(self):
        return FakeSeq(self._metrics)


class FakeStore:
    def __init__(self, executions, values):
        self.executions = list(executions)
        self._values = values

    def executionsList(self):
        return FakeSeq(self.executions)

    def executionMetrics(self, execution_id):  # noqa: ARG002
        return FakeMap(self._values)


class FakeBus:
    def __init__(self):
        self.drained = 0

    def waitUntilEmpty(self, timeout_ms):  # noqa: ARG002
        self.drained += 1


def write_execution(execution_id=1, rows_accumulator=3):
    """Uma execução como o Spark registra a gravação de um arquivo.

    As métricas do rastreador de escrita saem em bloco e nessa ordem — arquivos,
    bytes, LINHAS, partições, e os dois commits — então o contador da escrita fica
    entre os ids das âncoras. O `number of output rows` de id 8 é o do scan da
    fonte: mesmo nome, outro nó, valor diferente.
    """
    return FakeExecution(
        execution_id,
        [
            FakeMetric(1, "number of written files"),
            FakeMetric(2, "written output"),
            FakeMetric(rows_accumulator, "number of output rows"),
            FakeMetric(4, "number of dynamic part"),
            FakeMetric(5, "task commit time"),
            FakeMetric(6, "job commit time"),
            FakeMetric(7, "duration"),
            FakeMetric(8, "number of output rows"),
        ],
    )


def reader(store, bus=None):
    """Um `WriteMetrics` sobre dublês, sem passar pelo construtor (que quer uma
    SparkSession de verdade)."""
    metrics = WriteMetrics.__new__(WriteMetrics)
    metrics._store = store
    metrics._bus = bus or FakeBus()
    return metrics


# ------------------------------------------------------------------- sem Spark


class TestMetricSelection(unittest.TestCase):
    def store(self, executions, values):
        return FakeStore(executions, values)

    def test_reads_the_counter_the_write_itself_published(self):
        metrics = reader(self.store([write_execution()], {3: "1,000", 8: "5,000"}))
        store = metrics._store

        rows = metrics.measure(lambda: store.executions.append(write_execution(2)))

        self.assertEqual(rows, 1000)

    def test_never_takes_the_scan_counter_by_mistake(self):
        """O nó de leitura tem o MESMO nome de métrica. Com um filtro no meio, o
        scan lê 5000 e a escrita grava 1000: pegar o de fora da faixa das âncoras
        daria um número plausível e errado."""
        store = self.store([], {3: "1,000", 8: "5,000"})
        metrics = reader(store)

        rows = metrics.measure(lambda: store.executions.append(write_execution(1)))

        self.assertEqual(rows, 1000)
        self.assertNotEqual(rows, 5000)

    def test_the_write_runs_exactly_once(self):
        store = self.store([], {3: "7"})
        calls = []

        reader(store).measure(lambda: (calls.append(1), store.executions.append(write_execution())))

        self.assertEqual(len(calls), 1)

    def test_waits_for_the_event_bus_before_reading(self):
        """As métricas chegam ao store de forma assíncrona; ler sem esperar
        devolveria `None` de vez em quando, que é o pior tipo de falha."""
        store = self.store([], {3: "7"})
        bus = FakeBus()

        reader(store, bus).measure(lambda: store.executions.append(write_execution()))

        self.assertEqual(bus.drained, 1)

    def test_an_execution_that_is_not_a_write_is_not_used(self):
        """Sem as âncoras do rastreador de escrita não dá para saber qual contador
        é qual — então não se chuta."""
        scan_only = FakeExecution(1, [FakeMetric(7, "duration"), FakeMetric(8, "number of output rows")])
        store = self.store([], {8: "5,000"})

        rows = reader(store).measure(lambda: store.executions.append(scan_only))

        self.assertIsNone(rows)

    def test_two_writes_in_one_call_take_the_last(self):
        """Um formato transacional pode registrar mais de uma execução por
        `write()`; a que interessa é a última que gravou."""
        store = self.store([], {3: "10", 13: "20"})
        second = FakeExecution(
            2,
            [
                FakeMetric(11, "number of written files"),
                FakeMetric(13, "number of output rows"),
                FakeMetric(16, "job commit time"),
            ],
        )

        rows = reader(store).measure(
            lambda: store.executions.extend([write_execution(1), second])
        )

        self.assertEqual(rows, 20)

    def test_an_execution_that_was_already_there_is_ignored(self):
        """A marca é o maior id ANTES da escrita. Uma execução anterior — a
        contagem da leitura, por exemplo — não pode ser lida como esta escrita."""
        store = self.store([write_execution(1)], {3: "1,000"})

        rows = reader(store).measure(lambda: None)

        self.assertIsNone(rows)

    def test_a_metric_with_no_value_yet_is_not_guessed_as_zero(self):
        store = self.store([], {})

        rows = reader(store).measure(lambda: store.executions.append(write_execution()))

        self.assertIsNone(rows)

    def test_a_session_without_the_apis_is_simply_unavailable(self):
        """Spark Connect e distribuições que movam essas classes não podem
        derrubar o pipeline — só devolvem `None` e o chamador conta."""
        metrics = WriteMetrics(object())
        calls = []

        self.assertFalse(metrics.available)
        self.assertIsNone(metrics.measure(lambda: calls.append(1)))
        self.assertEqual(len(calls), 1)

    def test_a_failure_reading_the_store_still_leaves_the_write_done(self):
        class Broken(FakeStore):
            def executionsList(self):
                raise RuntimeError("store gone")

        calls = []
        rows = reader(Broken([], {})).measure(lambda: calls.append(1))

        self.assertIsNone(rows)
        self.assertEqual(len(calls), 1)

    def test_the_formatted_value_is_read_whatever_the_grouping(self):
        self.assertEqual(_as_int("1,000"), 1000)
        self.assertEqual(_as_int("1.000.000"), 1000000)
        self.assertEqual(_as_int("42"), 42)
        self.assertIsNone(_as_int(""))
        self.assertIsNone(_as_int("n/a"))


# ------------------------------------------------------------------ com Spark


class TestAgainstSpark(unittest.TestCase):
    """O número lido tem de ser o número gravado. Sem Java, a classe é pulada."""

    spark = None
    tmp = None

    @classmethod
    def setUpClass(cls) -> None:
        if SparkSession is None:
            raise unittest.SkipTest("pyspark nao instalado")
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        try:
            cls.spark = (
                SparkSession.builder
                .master("local[2]")
                .appName("sparquet-write-metrics-tests")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate()
            )
            # `range` roda inteiro na JVM. Uma sonda que crie um worker Python
            # falharia por um motivo que não tem a ver com o que se testa aqui.
            cls.spark.range(1).count()
        except Exception as exc:  # pragma: no cover - ambiente sem Java/Spark
            cls.spark = None
            raise unittest.SkipTest(f"Spark/Java indisponivel: {exc}")
        cls.tmp = tempfile.mkdtemp(prefix="sparquet-write-metrics-")

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.spark is not None:
            cls.spark.stop()
        if cls.tmp:
            shutil.rmtree(cls.tmp, ignore_errors=True)

    def path(self, name: str) -> str:
        target = os.path.join(self.tmp, name)
        shutil.rmtree(target, ignore_errors=True)
        return target

    def test_the_number_read_is_the_number_written(self):
        metrics = WriteMetrics(self.spark)
        self.assertTrue(metrics.available, "esta versão do Spark deveria expor o status store")
        df = self.spark.range(0, 5000).toDF("id")
        target = self.path("plain")

        rows = metrics.measure(lambda: df.write.mode("overwrite").parquet(target))

        self.assertEqual(rows, 5000)
        self.assertEqual(rows, self.spark.read.parquet(target).count())

    def test_a_filter_makes_scan_and_write_diverge_and_the_write_wins(self):
        """O caso que expõe a escolha errada: o scan lê 5000 linhas e a escrita
        grava 500. Um leitor que pegasse a métrica do scan devolveria 5000."""
        metrics = WriteMetrics(self.spark)
        df = self.spark.range(0, 5000).toDF("id").filter("id < 500")
        target = self.path("filtered")

        rows = metrics.measure(lambda: df.write.mode("overwrite").parquet(target))

        self.assertEqual(rows, 500)

    def test_an_empty_result_is_zero_and_not_unknown(self):
        metrics = WriteMetrics(self.spark)
        df = self.spark.range(0, 100).toDF("id").filter("id < 0")
        target = self.path("empty")

        rows = metrics.measure(lambda: df.write.mode("overwrite").parquet(target))

        self.assertEqual(rows, 0)

    def test_each_write_is_measured_on_its_own(self):
        """Vários destinos numa execução: cada leitura tem de ver a SUA escrita, e
        não a anterior."""
        metrics = WriteMetrics(self.spark)
        first = self.spark.range(0, 300).toDF("id")
        second = self.spark.range(0, 700).toDF("id")

        self.assertEqual(metrics.measure(lambda: first.write.mode("overwrite").parquet(self.path("a"))), 300)
        self.assertEqual(metrics.measure(lambda: second.write.mode("overwrite").parquet(self.path("b"))), 700)

    def test_csv_and_json_publish_the_metric_too(self):
        metrics = WriteMetrics(self.spark)
        df = self.spark.range(0, 250).toDF("id")

        self.assertEqual(metrics.measure(lambda: df.write.mode("overwrite").csv(self.path("csv"))), 250)
        self.assertEqual(metrics.measure(lambda: df.write.mode("overwrite").json(self.path("json"))), 250)


class TestPipelineUsesIt(unittest.TestCase):
    """A ponta que importa para quem usa o framework: `PipelineResult` traz o mesmo
    número de sempre, e diz de onde ele veio."""

    spark = None
    tmp = None

    @classmethod
    def setUpClass(cls) -> None:
        if SparkSession is None:
            raise unittest.SkipTest("pyspark nao instalado")
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        try:
            cls.spark = (
                SparkSession.builder
                .master("local[2]")
                .appName("sparquet-write-metrics-pipeline")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate()
            )
            # `range` roda inteiro na JVM. Uma sonda que crie um worker Python
            # falharia por um motivo que não tem a ver com o que se testa aqui.
            cls.spark.range(1).count()
        except Exception as exc:  # pragma: no cover - ambiente sem Java/Spark
            cls.spark = None
            raise unittest.SkipTest(f"Spark/Java indisponivel: {exc}")
        cls.tmp = tempfile.mkdtemp(prefix="sparquet-write-metrics-pipeline-")

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.spark is not None:
            cls.spark.stop()
        if cls.tmp:
            shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_rows_written_matches_what_is_on_disk_for_every_destination(self):
        from sparquet.core.pipeline import Pipeline

        source = os.path.join(self.tmp, "source")
        self.spark.range(0, 2000).toDF("id").write.mode("overwrite").parquet(source)
        first = os.path.join(self.tmp, "dest-all")
        second = os.path.join(self.tmp, "dest-half")

        result = Pipeline.from_dict({
            "name": "write-metrics",
            "input": {"format": "parquet", "path": source},
            "outputs": [
                {"format": "parquet", "path": first, "mode": "overwrite"},
                {
                    "format": "parquet", "path": second, "mode": "overwrite",
                    "transformations": [{"type": "filter", "condition": "id < 800"}],
                },
            ],
        }).run()

        self.assertTrue(result.success, result.error)
        self.assertEqual(result.rows_read, 2000)
        self.assertEqual([m.rows_written for m in result.output_metrics], [2000, 800])
        self.assertEqual(result.rows_written, 2800)
        self.assertEqual(self.spark.read.parquet(first).count(), 2000)
        self.assertEqual(self.spark.read.parquet(second).count(), 800)
        # Parquet publica a métrica, então nenhum destino precisou de count().
        self.assertEqual({m.rows_from for m in result.output_metrics}, {"write_metrics"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
