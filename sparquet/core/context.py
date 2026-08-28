from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

from sparquet.core.config import SparkConfig
from sparquet.utils.logger import defer_warning


def _detect_environment() -> str:
    """Detecta o ambiente de execução atual.

    Returns:
        "databricks" | "emr" | "dataproc" | "synapse" | "local"
    """
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return "databricks"
    if os.environ.get("EMR_CLUSTER_ID") or os.path.exists("/mnt/var/lib/info/job-flow.json"):
        return "emr"
    if os.environ.get("DATAPROC_IMAGE_VERSION") or os.environ.get("DATAPROC_CLUSTER_NAME"):
        return "dataproc"
    if os.environ.get("SYNAPSE_WORKSPACE_NAME") or os.environ.get("AZURE_DATABRICKS_ORG_ID"):
        return "synapse"
    return "local"


def _pin_local_worker_python(env: str, master: str) -> None:
    """Aponta os workers Python do Spark para o MESMO interpretador do driver.

    Sem isso, o Spark lança o worker com o `python` que estiver no PATH. Se for um
    build diferente do driver — cenário comum quando o job roda a partir de um venv
    invocado por caminho absoluto, com o PATH ainda apontando para o Python do
    sistema — o worker morre com um `Python worker exited unexpectedly (crashed)`,
    que não diz nada sobre a causa real.

    O sintoma engana: leitura/escrita puramente JVM (CSV → Parquet) não cria worker
    nenhum, então o pipeline roda normalmente até a primeira etapa que precisa de um
    — tipicamente o `validations.report`, montado no driver com `createDataFrame`.

    **Só se aplica a master local**, onde driver e executores são a mesma máquina e
    o caminho do interpretador é garantidamente válido. Num cluster (yarn, k8s,
    EMR…) o executor roda em outro host e apontá-lo para um caminho do driver
    quebraria o job — lá a variável é responsabilidade da plataforma.

    Usa `setdefault`: uma escolha explícita do usuário nunca é sobrescrita.
    """
    if env != "local" or not str(master).startswith("local"):
        return
    for var in ("PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON"):
        os.environ.setdefault(var, sys.executable)


def _declared_spark_version(home: Path) -> str | None:
    """Versão declarada dentro de um `SPARK_HOME`, nos dois layouts que existem:
    instalação via pip (`version.py` na raiz do pacote) e distribuição do Spark
    (`python/pyspark/version.py`). `None` quando não há como saber."""
    for relative in ("version.py", "python/pyspark/version.py"):
        try:
            text = (home / relative).read_text(encoding="utf-8")
        except OSError:
            continue
        match = re.search(r"""__version__[^=]*=\s*["']([^"']+)["']""", text)
        if match:
            return match.group(1)
    return None


def _align_local_spark_home(env: str, master: str) -> None:
    """Faz o `SPARK_HOME` do processo concordar com o pyspark que foi importado.

    Um `SPARK_HOME` herdado do ambiente (perfil do shell, instalação antiga, outro
    venv) vence a descoberta automática do pyspark: o driver importa o pacote do
    venv atual, mas a JVM e o `pyspark.zip` que vai para o PYTHONPATH do worker
    saem do `SPARK_HOME`. Versões diferentes nas duas pontas e o worker sobe com um
    pyspark incompatível e morre **sem traceback nenhum** — o mesmo
    `Python worker exited unexpectedly (crashed)` de `_pin_local_worker_python`, com
    a mesma pegadinha de só aparecer na primeira etapa que cria worker (tipicamente
    o `validations.report`).

    Só corrige quando dá para provar a divergência: `SPARK_HOME` aponta para outro
    diretório E a versão declarada lá é diferente da importada. Um `SPARK_HOME` sem
    versão legível (distribuição montada à mão) fica intacto — pode ser deliberado.

    **Só se aplica a master local**: num cluster o `SPARK_HOME` é do ambiente que
    submeteu o job, e reescrevê-lo com um caminho do driver quebraria a execução.
    """
    if env != "local" or not str(master).startswith("local"):
        return
    home = os.environ.get("SPARK_HOME")
    if not home:
        return  # sem a variável, o pyspark resolve o caminho dele mesmo

    ours = Path(pyspark.__file__).resolve().parent
    try:
        if Path(home).resolve() == ours:
            return
    except OSError:
        return

    theirs = _declared_spark_version(Path(home))
    if theirs is None or theirs == pyspark.__version__:
        return

    os.environ["SPARK_HOME"] = str(ours)
    defer_warning(
        "SPARK_HOME divergia do pyspark importado e foi realinhado",
        spark_home_descartado=home,
        versao_descartada=theirs,
        spark_home=str(ours),
        versao=pyspark.__version__,
    )


class SparkContextManager:
    """Gerencia um singleton de SparkSession para toda a vida do pipeline.

    Em Databricks reutiliza a sessão ativa do runtime — nunca cria uma nova.
    Em outros ambientes (EMR, Dataproc, Synapse, local) cria/reutiliza via builder.
    """

    _session: SparkSession | None = None

    @classmethod
    def get_or_create(cls, config: SparkConfig) -> SparkSession:
        if cls._session is not None:
            return cls._session

        env = _detect_environment()

        if env == "databricks":
            # No Databricks a sessão já existe; criar uma nova causaria erro.
            cls._session = SparkSession.getActiveSession()
            if cls._session is None:
                # Fallback improvável, mas seguro
                cls._session = SparkSession.builder.getOrCreate()
        else:
            builder = SparkSession.builder.appName(config.app_name)

            # Master só faz sentido fora de clusters gerenciados
            if env == "local":
                builder = builder.master(config.master)

            for key, value in config.configs.items():
                builder = builder.config(key, value)

            _pin_local_worker_python(env, config.master)
            _align_local_spark_home(env, config.master)

            cls._session = builder.getOrCreate()
            cls._session.sparkContext.setLogLevel("WARN")

        return cls._session

    @classmethod
    def stop(cls) -> None:
        env = _detect_environment()
        if cls._session is not None and env != "databricks":
            cls._session.stop()
        cls._session = None

    @classmethod
    def current_environment(cls) -> str:
        """Retorna o ambiente detectado (útil para logs e diagnóstico)."""
        return _detect_environment()
