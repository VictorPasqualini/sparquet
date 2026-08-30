"""Infraestrutura comum dos testes de integração de IO.

Os testes em `tests/io/test_connectors.py` são de *montagem*: provam que o
conector chama `format(...)` certo e monta as opções certas, com um Spark falso.
Isso pega erro de digitação, não pega conector que não existe mais, opção que o
jar renomeou ou escrita que grava num layout que o próprio reader não relê. Os
testes deste diretório executam de verdade — jar real, dado real, ida e volta.

Cada conector cai em um de três grupos:

  **sem jar extra**  `xml` e `binary` rodam com o que vem no pyspark.
  **um jar**         `avro`, `delta`, `iceberg` e `hudi` precisam de um pacote
                     Maven; nada mais.
  **um serviço**     Kafka, Mongo, Cassandra, Elasticsearch/OpenSearch e os
                     bancos JDBC de verdade precisam de um servidor de pé (ver
                     `services.py` e o `docker-compose.yml` ao lado). O caminho
                     JDBC em si é exercitado sem serviço nenhum, com H2 dentro
                     da própria JVM — `test_jdbc_spark.py`.
  **só nuvem**       BigQuery, Snowflake, Redshift, DynamoDB. Não são
                     reproduzíveis aqui e continuam cobertos por montagem.

Como rodar:

    SPARQUET_IT=1 python tests/io/integration/test_files_spark.py
    SPARQUET_IT=1 python tests/io/integration/test_lakehouse_spark.py
    SPARQUET_IT=1 python tests/io/integration/test_jdbc_spark.py

Sem `SPARQUET_IT=1` os testes são **pulados**: a primeira execução baixa dezenas
de MB de jar do Maven Central, e uma suíte de unidade não pode depender de rede.
Depois do primeiro download o ivy tem cache local e as execuções seguintes são
offline — por isso o gate também abre sozinho quando o cache já tem os jars.

Por que uma sessão só, com TODOS os pacotes: `spark.jars.packages` só vale na
criação da sessão, e a `SparkSession` é singleton do processo. Se cada arquivo
subisse a sua, o segundo a rodar herdaria a do primeiro — sem o jar dele, e com
uma falha que não parece o que é. A união é montada aqui e vale para qualquer
combinação de arquivos, rodando junto ou separado.
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Dict, Iterable, Optional

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:  # rodar o arquivo direto, sem PYTHONPATH
    sys.path.insert(0, str(_ROOT))

# Mesma razão de `sparquet/core/context.py`: em master local o worker Python
# precisa ser o mesmo do driver.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

#: Coordenada Maven de cada conector que precisa de jar. Sobrescreva com
#: `SPARQUET_IT_<CONECTOR>_PACKAGE` para testar outra versão sem editar o teste —
#: é o que se faz quando o Spark do ambiente é outro.
_PACKAGES: Dict[str, str] = {
    "avro": "org.apache.spark:spark-avro_2.13:4.1.1",
    # delta-spark 4.3.x é a linha compilada contra o Spark 4.1 (4.4 já é 4.2).
    "delta": "io.delta:delta-spark_2.13:4.3.1",
    "iceberg": "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0",
    # H2 roda dentro da própria JVM: é o único banco que exercita o caminho
    # JDBC de verdade sem subir serviço nenhum.
    "h2": "com.h2database:h2:2.3.232",
}


def package_for(connector: str) -> str:
    return os.environ.get(
        f"SPARQUET_IT_{connector.upper()}_PACKAGE", _PACKAGES.get(connector, "")
    )


def _packages() -> str:
    """Os pacotes que a sessão carrega.

    Os de `_PACKAGES` entram sempre — são baratos e valem para qualquer máquina.
    Os de serviço entram **só quando o serviço está de pé**: cada coordenada é
    uma resolução no Maven na criação da sessão, e quem não subiu container
    nenhum não deve pagar por driver de banco que não vai usar.
    """
    from services import packages_for_reachable

    coordenadas = [
        coordinate
        for connector in _PACKAGES
        if (coordinate := package_for(connector))
    ]
    coordenadas.extend(packages_for_reachable())
    return ",".join(coordenadas)


def _ivy_caches() -> list:
    """Onde o Spark guarda o jar baixado.

    O diretório mudou de versão: o Spark 4 usa `~/.ivy2.5.2`, o 3.x usava
    `~/.ivy2`. Os dois são olhados porque o gate só precisa saber se *algum*
    cache já tem o jar — o que decide é a execução ser offline ou não.
    """
    override = os.environ.get("SPARQUET_IT_IVY")
    if override:
        return [Path(override) / "jars"]
    return [Path.home() / ".ivy2.5.2" / "jars", Path.home() / ".ivy2" / "jars"]


def _jars_cached() -> bool:
    """Todos os pacotes já baixados? Então a execução é offline e pode rodar."""
    names = set()
    for cache in _ivy_caches():
        if cache.is_dir():
            names |= {path.name for path in cache.glob("*.jar")}
    if not names:
        return False
    for connector in _PACKAGES:
        coordinate = package_for(connector)
        if not coordinate:
            continue
        group, artifact, version = coordinate.split(":")
        if f"{group}_{artifact}-{version}.jar" not in names:
            return False
    return True


def integration_enabled() -> bool:
    """`SPARQUET_IT=1`, ou os jars já em cache — nunca baixar por acidente."""
    if os.environ.get("SPARQUET_IT", "").strip() in {"1", "true", "yes"}:
        return True
    return _jars_cached()


def java_available() -> bool:
    return bool(shutil.which("java") or os.environ.get("JAVA_HOME"))


def pyspark_available() -> bool:
    try:
        import pyspark  # noqa: F401
    except Exception:
        return False
    return True


#: Motivo único do skip, para a mensagem dizer o que fazer.
def skip_reason() -> Optional[str]:
    if not pyspark_available():
        return "pyspark não está instalado"
    if not java_available():
        return "nenhuma JVM encontrada (java/JAVA_HOME)"
    if not integration_enabled():
        return (
            "testes de integração desligados: rode com SPARQUET_IT=1 para baixar "
            "os jars do Maven Central na primeira vez"
        )
    return None


requires_integration = unittest.skipIf(skip_reason() is not None, skip_reason() or "")


# --------------------------------------------------------------- diretórios

#: Um diretório por execução, apagado no fim. O warehouse do Iceberg precisa
#: existir *antes* da sessão (vai numa config do catálogo), então nasce aqui.
_WORK = tempfile.TemporaryDirectory(prefix="sparquet-it-")
WORK = Path(_WORK.name)
WAREHOUSE = WORK / "warehouse"
WAREHOUSE.mkdir(parents=True, exist_ok=True)


def work_dir(name: str) -> Path:
    path = WORK / name
    path.mkdir(parents=True, exist_ok=True)
    return path


# ------------------------------------------------------------ bloco `spark`

_DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
_ICEBERG_EXTENSION = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def spark_block(app_name: str = "sparquet-integration") -> Dict[str, object]:
    """O bloco `spark` do JSON — é assim que o usuário liga um conector.

    Nada aqui é privilégio de teste: `spark.jars.packages`, as extensões e o
    catálogo saem do mesmo lugar num pipeline de produção, o que faz este teste
    valer também como exemplo executável de configuração.
    """
    configs = {
        "spark.sql.extensions": f"{_DELTA_EXTENSION},{_ICEBERG_EXTENSION}",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # Catálogo Hadoop: um diretório, sem metastore nem serviço.
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": WAREHOUSE.as_posix(),
        # O teste é sobre IO, não sobre paralelismo: 1 partição deixa a saída
        # previsível (um arquivo por escrita) e o tempo baixo.
        "spark.sql.shuffle.partitions": "1",
        "spark.sql.warehouse.dir": (WORK / "spark-warehouse").as_posix(),
        "spark.ui.enabled": "false",
    }
    packages = _packages()
    if packages:
        configs["spark.jars.packages"] = packages
    return {"app_name": app_name, "master": "local[2]", "configs": configs}


# ------------------------------------------------------------------ semente

#: Uma linha comum, uma cheia do que quebra texto delimitado, e uma quase toda
#: nula — o nulo é o que mais se perde numa ida e volta.
SEED_ROWS = (
    (1, "alpha", 1.5),
    (2, 'com "aspas", virgula e acento: cessao', -0.25),
    (3, None, None),
)

SEED_HEADER = "id,nome,valor\n"


def seed_csv(directory: Optional[Path] = None) -> Path:
    """CSV de origem, escrito como texto — sem `createDataFrame`, que subiria um
    worker Python e faria o arquivo morrer por um motivo alheio a conector."""
    directory = directory or work_dir("seed")
    path = directory / "seed.csv"
    lines = [SEED_HEADER]
    for identifier, nome, valor in SEED_ROWS:
        campo = "" if nome is None else '"' + nome.replace('"', '""') + '"'
        lines.append(f"{identifier},{campo},{'' if valor is None else valor}\n")
    path.write_text("".join(lines), encoding="utf-8")
    return path


def seed_input(path: Optional[Path] = None) -> Dict[str, object]:
    csv_path = path or seed_csv()
    return {
        "format": "csv",
        "path": csv_path.as_posix(),
        "options": {"header": "true", "inferSchema": "true"},
    }


# ---------------------------------------------------------------- execução


def run(spec: Dict[str, object]):
    """Executa um pipeline pelo framework, como a lib faria.

    O bloco `spark` vai **no JSON**, que é o caminho do usuário: `Sparquet()` sem
    argumento nenhum, e a sessão nasce na primeira execução já com
    `spark.jars.packages` do JSON. Isto também é o teste dessa garantia — se a
    sessão voltar a nascer no construtor, os jars não entram e todo este
    diretório falha.
    """
    from sparquet import Sparquet

    payload = dict(spec)
    payload.setdefault("spark", spark_block())
    return Sparquet().run_from_dict(payload)


def round_trip(
    connector: str,
    write_output: Dict[str, object],
    read_input: Dict[str, object],
    transformations: Optional[Iterable[Dict[str, object]]] = None,
):
    """Grava a semente no conector e lê de volta — o par que interessa.

    Devolve `(escrita, leitura)`; a leitura grava um CSV em `back-<conector>`
    para o teste conferir valor por valor, e não só a contagem.
    """
    written = run(
        {
            "name": f"it-{connector}-escrita",
            "input": seed_input(),
            "transformations": list(transformations or []),
            "output": write_output,
        }
    )
    read_back = run(
        {
            "name": f"it-{connector}-leitura",
            "input": read_input,
            "output": {
                "format": "csv",
                "path": (WORK / f"back-{connector}").as_posix(),
                "mode": "overwrite",
                "options": {"header": "true"},
            },
        }
    )
    return written, read_back


def rows_back(connector: str) -> list:
    """As linhas que a leitura devolveu, lidas do CSV como o usuário leria."""
    import csv

    directory = WORK / f"back-{connector}"
    rows: list = []
    for part in sorted(directory.glob("*.csv")):
        with part.open(encoding="utf-8", newline="") as handle:
            rows.extend(csv.DictReader(handle))
    return rows
