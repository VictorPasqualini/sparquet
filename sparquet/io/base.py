from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from sparquet.core.config import InputConfig, OutputConfig


class BaseReader(ABC):
    def __init__(self, spark: SparkSession, config: InputConfig) -> None:
        self.spark = spark
        self.config = config

    @abstractmethod
    def read(self) -> DataFrame:
        ...


class BaseWriter(ABC):
    def __init__(self, spark: SparkSession, config: OutputConfig) -> None:
        self.spark = spark
        self.config = config

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        ...


def is_table_name(path: str) -> bool:
    """'catalog.schema.tabela' / 'schema.tabela' -> True; qualquer path/URI -> False.

    Regra independente de scheme: um path físico ou URI sempre tem '/' (e URIs têm
    ':'), enquanto um identificador de catálogo não tem nenhum dos dois. Assim todos
    os schemes de object storage funcionam sem precisar enumerá-los — s3://, s3a://,
    s3n://, gs://, abfss://, abfs://, wasbs://, wasb://, adl://, hdfs://, dbfs:/,
    oss://, file:… — e caminhos relativos/Windows (./out.delta, C:/data/x) também são
    tratados como path.

    Ambiguidade residual: um nome pontuado sem barra nem ':' (ex.: 'out.delta') ainda
    é lido como tabela — prefixe com './' ou '/' para forçar path.

    Mora aqui, e não no módulo de um formato, porque a distinção é a mesma para
    todo formato que aceita as duas referências: Delta e Iceberg hoje, e qualquer
    catálogo que venha depois.
    """
    if "/" in path or ":" in path:
        return False
    return "." in path
