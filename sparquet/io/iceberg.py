from __future__ import annotations

import re
from typing import Callable, Dict, List, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from sparquet.io.base import BaseReader, BaseWriter, is_table_name
from sparquet.io.merge import (
    MERGE_OPTIONS as _MERGE_OPTIONS,
    validate_merge_options,
    merge_sql,
)

# Transforms de particionamento oculto do Iceberg, aceitos como entrada de
# `partition_by`. São os que o Spark expõe como função de transform (Spark >= 3.1):
# `bucket` recebe o número de buckets + a coluna; os de tempo recebem só a coluna.
# `truncate` do Iceberg NÃO está aqui porque o Spark não tem função equivalente —
# declare-o no DDL da tabela.
_TIME_TRANSFORMS: Dict[str, Callable[[str], Column]] = {
    "years": F.years,
    "months": F.months,
    "days": F.days,
    "hours": F.hours,
}
_TRANSFORM_CALL = re.compile(r"^([A-Za-z_]\w*)\s*\((.*)\)$", re.DOTALL)

# Entrada normalizada de `partition_by`: ("identity", "regiao") | ("days", "ts")
# | ("bucket", 16, "id"). Separar o parse da construção da Column deixa a
# validação testável sem SparkSession.
PartitionSpec = Tuple


def parse_partition_spec(entries: List[str]) -> Tuple[List[PartitionSpec], bool]:
    """Valida `partition_by` e diz se há transform (particionamento oculto).

    `has_transform` é False quando todas as entradas são nomes de coluna — nesse
    caso o chamador mantém o `partitionBy` de sempre, que não exige catálogo.
    Levanta ValueError para transform desconhecido ou com aridade errada.
    """
    specs: List[PartitionSpec] = []
    has_transform = False

    for entry in entries:
        raw = str(entry).strip()
        match = _TRANSFORM_CALL.match(raw)
        if not match:
            specs.append(("identity", raw))
            continue

        name = match.group(1).lower()
        args = [part.strip() for part in match.group(2).split(",") if part.strip()]
        has_transform = True

        if name == "bucket":
            if len(args) != 2 or not args[0].isdigit() or int(args[0]) < 1:
                raise ValueError(
                    f"IcebergWriter: partition_by {raw!r} inválido — a forma é "
                    f"bucket(<n>, <coluna>) com n >= 1, ex: bucket(16, id)."
                )
            specs.append(("bucket", int(args[0]), args[1]))
        elif name in _TIME_TRANSFORMS:
            if len(args) != 1:
                raise ValueError(
                    f"IcebergWriter: partition_by {raw!r} inválido — {name} recebe uma "
                    f"coluna só, ex: {name}(data_evento)."
                )
            specs.append((name, args[0]))
        else:
            supported = ", ".join(["bucket", *_TIME_TRANSFORMS])
            raise ValueError(
                f"IcebergWriter: transform de particionamento {name!r} não suportado. "
                f"Disponíveis: {supported}. Para os demais (truncate, etc.) declare o "
                f"particionamento no DDL da tabela e deixe partition_by vazio."
            )

    return specs, has_transform


def _partition_columns(specs: List[PartitionSpec]) -> List[Column]:
    """Constrói as Columns de `partitionedBy` — exige SparkSession ativa."""
    columns: List[Column] = []
    for spec in specs:
        kind = spec[0]
        if kind == "identity":
            columns.append(F.col(spec[1]))
        elif kind == "bucket":
            columns.append(F.bucket(spec[1], spec[2]))
        else:
            columns.append(_TIME_TRANSFORMS[kind](spec[1]))
    return columns


class IcebergReader(BaseReader):
    """Lê tabelas Iceberg.

    Aceita as duas formas de referência em 'path':
      - Tabela de catálogo: "catalogo.schema.tabela" (forma normal)
      - Caminho físico da tabela: "/warehouse/db/tabela", "s3://bucket/db/tabela"

    Opções de time travel via 'options': 'snapshot-id', 'as-of-timestamp'.
    """

    def read(self) -> DataFrame:
        reader = self.spark.read.format("iceberg")
        for key, value in self.config.options.items():
            reader = reader.option(key, value)
        return reader.load(self.config.path)


class IcebergWriter(BaseWriter):
    """Escreve em tabelas Iceberg.

    Modos via 'mode': overwrite, append e merge (upsert).

    O merge requer 'on' (a condição inteira, sobre T = destino e S = origem) e
    'actions' (a lista de cláusulas "WHEN ..." escritas à mão, emitidas na ordem
    dada). O upsert de sempre são duas: "WHEN MATCHED THEN UPDATE SET *" e
    "WHEN NOT MATCHED THEN INSERT *" — o `*` do Iceberg casa as colunas por nome
    e tolera uma coluna a mais na origem. Ver `sparquet/io/merge.py`.

    Quando 'path' é um identificador de catálogo ("catalogo.schema.tabela"), a
    escrita usa `saveAsTable`, que **cria a tabela se ela ainda não existir** —
    incluindo o particionamento de 'partition_by'. Isso importa porque o caminho
    alternativo, `save`, exige tabela pré-existente: no Spark 4 apontar um output
    para uma tabela nova falhava com `[TABLE_OR_VIEW_NOT_FOUND]`, e o primeiro
    carregamento de qualquer pipeline só funcionava com um DDL feito à mão fora
    do framework. Para caminho físico (tem '/' ou ':') a escrita continua em
    `save`, que é a única forma que aceita path.

    `partition_by` aceita, além de nomes de coluna, os transforms de
    particionamento oculto do Iceberg: `bucket(16, id)`, `years(ts)`,
    `months(ts)`, `days(ts)`, `hours(ts)`. Com transform a escrita passa pelo
    `writeTo` (DataFrameWriterV2), porque o `partitionBy` do writer V1 só aceita
    nome de coluna — e por isso o destino tem de ser tabela de catálogo. A
    diferença em relação a materializar o bucket numa coluna é o pruning: um
    filtro por `id` poda os buckets do Iceberg sozinho, enquanto uma coluna
    `bucket` gravada à mão só é podada por um filtro sobre `bucket`.

    A spec de partição pertence à TABELA, não à escrita: ela é aplicada na
    criação (`create`/`createOrReplace`) e um append numa tabela existente usa a
    spec que a tabela já tem. Mudar `partition_by` de um pipeline em modo append
    não reparticiona o que já está gravado.
    """

    def write(self, df: DataFrame) -> None:
        if self.config.mode == "merge":
            self._merge(df)
            return

        specs, has_transform = parse_partition_spec(self.config.partition_by)
        if has_transform:
            self._write_partitioned_v2(df, specs)
            return

        writer = df.write.format("iceberg").mode(self.config.mode)
        for key, value in self.config.options.items():
            if key in _MERGE_OPTIONS:
                continue
            writer = writer.option(key, value)
        if self.config.partition_by:
            writer = writer.partitionBy(*self.config.partition_by)

        if is_table_name(self.config.path):
            writer.saveAsTable(self.config.path)
        else:
            writer.save(self.config.path)

    def _write_partitioned_v2(self, df: DataFrame, specs: list) -> None:
        """Escreve declarando transforms de particionamento (DataFrameWriterV2).

        `partitionBy` do writer V1 só aceita nome de coluna, então bucket/days/…
        exigem `writeTo`, que existe apenas sobre tabela de catálogo. A spec de
        partição é propriedade da TABELA no Iceberg, não da escrita: ela é
        aplicada na criação (create/createOrReplace) e um append numa tabela
        existente usa a spec que a tabela já tem — mudar o partition_by de um
        pipeline em modo append não reparticiona nada.
        """
        target = self.config.path
        if not is_table_name(target):
            raise ValueError(
                f"IcebergWriter: transforms de particionamento "
                f"({', '.join(['bucket', *_TIME_TRANSFORMS])}) só existem em tabela de "
                f"catálogo — 'path' precisa ser 'catalogo.schema.tabela', não um caminho "
                f"físico ({target!r}). Para um destino em path, materialize o bucket "
                f"antes (with_column com pmod(hash(col), N)) e use esse nome de coluna "
                f"em partition_by."
            )

        writer = df.writeTo(target)
        for key, value in self.config.options.items():
            if key in _MERGE_OPTIONS:
                continue
            writer = writer.option(key, value)

        if self.config.mode == "append" and self.spark.catalog.tableExists(target):
            writer.append()
            return

        writer = writer.using("iceberg").partitionedBy(*_partition_columns(specs))
        if self.config.mode == "append":
            writer.create()
        else:
            writer.createOrReplace()

    def _merge(self, df: DataFrame) -> None:
        options = self.config.options
        target = self.config.path
        # Antes do atalho da primeira carga: um merge mal escrito tem de falhar
        # já na execução em que a tabela ainda não existe, não só na seguinte.
        validate_merge_options(options, "IcebergWriter")
        # MERGE INTO só existe sobre tabela do catálogo, e só depois que ela
        # existe. Na primeira carga não há o que atualizar: gravar tudo cria a
        # tabela e deixa o mesmo estado que um merge contra tabela vazia deixaria.
        if is_table_name(target) and not self.spark.catalog.tableExists(target):
            self._first_load(df, target)
            return

        view = "_spark_fw_merge_src"
        df.createOrReplaceTempView(view)

        self.spark.sql(merge_sql(target, view, options, "IcebergWriter"))

    def _first_load(self, df: DataFrame, target: str) -> None:
        """Primeira carga de um merge: cria a tabela com o particionamento declarado.

        A spec de partição do Iceberg se fixa na criação da tabela, e esta é a
        única execução em que o merge cria a tabela — deixar `partition_by` de
        fora aqui produziria uma tabela sem partição que nenhuma execução
        seguinte reparticiona.
        """
        specs, has_transform = parse_partition_spec(self.config.partition_by)
        if has_transform:
            df.writeTo(target).using("iceberg").partitionedBy(
                *_partition_columns(specs)
            ).create()
            return

        writer = df.write.format("iceberg").mode("append")
        if self.config.partition_by:
            writer = writer.partitionBy(*self.config.partition_by)
        writer.saveAsTable(target)
