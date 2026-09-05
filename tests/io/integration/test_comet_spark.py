# -*- coding: utf-8 -*-
"""DataFusion Comet: o pipeline roda igual, e os operadores viram nativos?

Comet é um plugin que troca operadores do plano físico do Spark por
implementações nativas (Rust/Arrow) e cai de volta para o Spark no que não
suporta. Não é um formato nem um conector: é uma config de sessão. Então o que
este arquivo mede não é "o framework suporta Comet" — é o que um usuário precisa
saber antes de ligar:

  1. **O resultado não muda.** O mesmo pipeline, com e sem o plugin, devolve as
     mesmas linhas. Se mudasse, Comet estaria fora de questão.
  2. **A aceleração acontece de verdade.** O plano da execução com Comet tem nós
     `Comet*`; o da execução sem Comet não tem nenhum. Isso importa porque a
     degradação do plugin é *silenciosa*: sem lib nativa ele se desabilita e a
     consulta roda igual, só sem ganho nenhum.

Por que subprocesso: o plugin é carregado na construção do `SparkContext`, antes
de `spark.jars` ser resolvido, então o jar tem de estar no classpath do driver
**antes de a JVM subir** (`--driver-class-path`). Isso não se faz numa sessão já
criada — e as duas execuções precisam de JVMs separadas de qualquer forma, porque
plugin é por JVM.

Requisitos, e o teste pula dizendo qual falta:

  * Linux — o jar embarca `libcomet.so` para linux/{amd64,aarch64} e nada mais.
  * o jar, apontado por `SPARQUET_IT_COMET_JAR`:

        base=https://repo1.maven.org/maven2/org/apache/datafusion
        curl -sLO $base/comet-spark-spark4.1_2.13/1.0.0/comet-spark-spark4.1_2.13-1.0.0.jar

    O artefato é por linha do Spark (`spark3.4` … `spark4.1`) — a coordenada que
    combina com o pyspark instalado está em `docs/PIPELINE_SCHEMA.md`, seção
    "DataFusion Comet".

Executar (o `-v` mostra o que cada execução decidiu):

    SPARQUET_IT=1 SPARQUET_IT_COMET_JAR=/caminho/comet.jar \\
        python tests/io/integration/test_comet_spark.py -v
"""
from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
import unittest
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

import harness  # noqa: E402

#: Raiz do repo — o subprocesso importa `sparquet` de lá, não do site-packages.
_RAIZ = Path(__file__).resolve().parents[3]

#: Configs que ligam o plugin. São as do usuário, copiadas de
#: `docs/PIPELINE_SCHEMA.md` — este teste não tem config privilegiada.
_CONFIGS_COMET = {
    "spark.plugins": "org.apache.spark.CometPlugin",
    "spark.shuffle.manager": (
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager"
    ),
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "1g",
    # Cada operador que caiu de volta para o Spark é registrado com o motivo —
    # é o que torna o fallback diagnosticável em vez de silencioso.
    "spark.comet.explain.fallback.enabled": "true",
}

#: Marcador da linha que o filho imprime; o resto do stdout é log do Spark.
_MARCA = "RESULTADO_COMET "


def _jar() -> Optional[str]:
    caminho = os.environ.get("SPARQUET_IT_COMET_JAR", "").strip()
    if caminho and Path(caminho).is_file():
        return caminho
    return None


def _razao_do_skip() -> Optional[str]:
    """O que falta para medir Comet — `None` quando nada falta."""
    if platform.system() != "Linux":
        return (
            f"Comet só tem binário nativo para Linux (aqui: {platform.system()}). "
            "Em outro sistema o plugin carrega, se desabilita sozinho e a "
            "consulta roda sem nenhum operador nativo — não há o que medir."
        )
    if _jar() is None:
        return (
            "jar do Comet ausente — baixe o artefato da linha do seu Spark "
            "(ver docstring) e aponte SPARQUET_IT_COMET_JAR para ele"
        )
    return None


_SEM_COMET = _razao_do_skip()


# --------------------------------------------------------------- subprocesso


def _pipeline(destino: Path) -> dict:
    """Um pipeline de verdade, no formato em que Comet tem o que acelerar:
    Parquet na entrada, filtro e agregação no meio."""
    return {
        "name": "comet-agregado",
        "input": {
            "format": "parquet",
            "path": destino.as_posix(),
        },
        "transformations": [
            {"type": "filter", "condition": "valor is not null"},
            {
                "type": "group_by",
                "by": ["nome"],
                "agg": ["sum(valor) as total", "count(1) as linhas"],
            },
        ],
        # `cache: false` porque o ViewWriter, por default, faz `cache()` +
        # `count()`: o plano da view viria embrulhado em `InMemoryTableScan` e a
        # asserção olharia o cache em vez do plano que executou.
        "output": {
            "format": "view",
            "path": "comet_saida",
            "options": {"cache": "false"},
        },
    }


def _executa_filho(com_comet: bool) -> None:
    """Roda o pipeline nesta JVM e imprime uma linha JSON com plano e linhas.

    Chamado como `python test_comet_spark.py --child comet|plain`. A JVM já subiu
    com o jar no classpath do driver (o pai põe `PYSPARK_SUBMIT_ARGS`), então
    aqui só falta a config que liga o plugin.
    """
    # Nenhum jar de formato: este teste lê Parquet, que é nativo do Spark, e um
    # `spark.jars.packages` a mais só somaria download e classpath.
    harness.USE_BASE_PACKAGES = False

    sys.path.insert(0, str(_RAIZ))
    from sparquet import Sparquet

    trabalho = harness.work_dir("comet")
    semente = harness.seed_csv()
    parquet = trabalho / "entrada.parquet"

    bloco = harness.spark_block("comet-" + ("on" if com_comet else "off"))
    if com_comet:
        bloco["configs"].update(_CONFIGS_COMET)

    app = Sparquet()
    saida: dict = {"comet": com_comet}
    try:
        # CSV -> Parquet primeiro: a entrada colunar é o que Comet acelera.
        preparo = app.run_from_dict(
            {
                "name": "comet-preparo",
                "spark": bloco,
                "input": {
                    "format": "csv",
                    "path": semente.as_posix(),
                    "options": {"header": "true", "inferSchema": "true"},
                },
                "output": {
                    "format": "parquet",
                    "path": parquet.as_posix(),
                    "mode": "overwrite",
                },
            }
        )
        saida["preparo_ok"] = preparo.success
        saida["preparo_erro"] = (preparo.error or "")[:400]

        spec = _pipeline(parquet)
        spec["spark"] = bloco
        resultado = app.run_from_dict(spec)
        saida["ok"] = resultado.success
        saida["erro"] = (resultado.error or "")[:400]

        if resultado.success:
            spark = app.spark
            df = spark.table("comet_saida")
            plano = df._jdf.queryExecution().executedPlan().toString()
            saida["plano"] = plano
            saida["nos_comet"] = sorted(
                {
                    palavra.strip("(),*+- ")
                    for palavra in plano.split()
                    if palavra.startswith("Comet")
                }
            )
            saida["linhas"] = sorted(
                [
                    [linha["nome"], linha["total"], linha["linhas"]]
                    for linha in df.collect()
                ],
                key=lambda linha: str(linha[0]),
            )
    except Exception as erro:  # o pai precisa do motivo, não de um exit code
        saida["excecao"] = f"{type(erro).__name__}: {erro}"[:600]

    print(_MARCA + json.dumps(saida, default=str))


def _roda(com_comet: bool, jar: str, timeout: int = 900) -> dict:
    ambiente = dict(os.environ)
    # `--driver-class-path` é o que resolve o `ClassNotFoundException:
    # org.apache.spark.CometPlugin`: o plugin é instanciado antes de o Spark
    # resolver `spark.jars`, então o jar tem de estar na linha de comando da JVM.
    ambiente["PYSPARK_SUBMIT_ARGS"] = (
        f"--jars {jar} --driver-class-path {jar} pyspark-shell"
    )
    ambiente["PYTHONPATH"] = os.pathsep.join(
        [str(_RAIZ), ambiente.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    processo = subprocess.run(
        [sys.executable, "-u", str(Path(__file__).resolve()), "--child",
         "comet" if com_comet else "plain"],
        cwd=str(_RAIZ),
        env=ambiente,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    for linha in processo.stdout.splitlines():
        if linha.startswith(_MARCA):
            return json.loads(linha[len(_MARCA):])
    cauda = (processo.stdout[-1500:] + processo.stderr[-1500:]).strip()
    raise AssertionError(
        f"o subprocesso ({'comet' if com_comet else 'plain'}) não imprimiu "
        f"resultado (exit {processo.returncode}):\n{cauda}"
    )


# ---------------------------------------------------------------------- teste


@harness.requires_integration
@unittest.skipIf(_SEM_COMET is not None, _SEM_COMET or "")
class CometTest(unittest.TestCase):
    """Duas execuções do mesmo pipeline, uma com o plugin e uma sem."""

    @classmethod
    def setUpClass(cls) -> None:
        jar = _jar()
        assert jar is not None  # o decorador já garantiu
        cls.com = _roda(True, jar)
        cls.sem = _roda(False, jar)

    def test_o_pipeline_roda_com_o_plugin_ligado(self) -> None:
        """Primeiro requisito: ligar Comet não quebra o pipeline."""
        self.assertNotIn("excecao", self.com, msg=self.com.get("excecao"))
        self.assertTrue(self.com.get("preparo_ok"), msg=self.com.get("preparo_erro"))
        self.assertTrue(self.com.get("ok"), msg=self.com.get("erro"))

    def test_o_resultado_e_o_mesmo_com_e_sem_comet(self) -> None:
        """O que Comet promete é velocidade, não semântica. Se as linhas
        divergirem, o plugin está fora de questão para o framework."""
        self.assertTrue(self.sem.get("ok"), msg=self.sem.get("erro"))
        self.assertEqual(self.com.get("linhas"), self.sem.get("linhas"))
        self.assertEqual(len(self.com.get("linhas") or []), 2)

    def test_o_plano_com_comet_tem_operador_nativo(self) -> None:
        """A parte que não se pode presumir: sem lib nativa o plugin se
        desabilita em silêncio e o pipeline passa igual, sem ganho nenhum. Só o
        plano diz a diferença."""
        nos = self.com.get("nos_comet") or []
        self.assertTrue(
            nos,
            msg=(
                "nenhum operador Comet no plano — o plugin carregou e caiu de "
                f"volta para o Spark. Plano:\n{self.com.get('plano')}"
            ),
        )
        self.assertFalse(
            self.sem.get("nos_comet"),
            msg="a execução SEM as configs do Comet não deveria ter nó Comet",
        )


if __name__ == "__main__":
    if sys.argv[1:2] == ["--child"]:
        _executa_filho(sys.argv[2] == "comet")
    else:
        unittest.main(verbosity=2)
