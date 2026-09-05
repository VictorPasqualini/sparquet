from __future__ import annotations

import re
from typing import List, NamedTuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sparquet.core.config import InputConfig
from sparquet.transform.base import BaseTransformation, PipelineStop
from sparquet.utils.logger import defer_warning, logger


class FilterTransformation(BaseTransformation):
    """Keeps rows that satisfy a SQL-style boolean expression."""

    def apply(self, df: DataFrame) -> DataFrame:
        return df.filter(self.config.params["condition"])


class SelectTransformation(BaseTransformation):
    """Projects a subset of columns or SQL expressions.

    Cada item pode ser um nome de coluna simples ou uma expressão SQL completa
    com alias, ex: "to_json(payload) AS value", "CAST(id AS STRING) AS id_str".
    """

    def apply(self, df: DataFrame) -> DataFrame:
        return df.select(*[F.expr(c) for c in self.config.params["columns"]])


class DropTransformation(BaseTransformation):
    """Removes columns from the DataFrame."""

    def apply(self, df: DataFrame) -> DataFrame:
        return df.drop(*self.config.params["columns"])


class RenameTransformation(BaseTransformation):
    """Renames columns using an old→new mapping."""

    def apply(self, df: DataFrame) -> DataFrame:
        result = df
        for old, new in self.config.params["mappings"].items():
            result = result.withColumnRenamed(old, new)
        return result


class CastTransformation(BaseTransformation):
    """Casts columns to new data types."""

    def apply(self, df: DataFrame) -> DataFrame:
        result = df
        for col_name, dtype in self.config.params["columns"].items():
            result = result.withColumn(col_name, F.col(col_name).cast(dtype))
        return result


class WithColumnTransformation(BaseTransformation):
    """Adiciona ou substitui colunas computadas a partir de expressões SQL.

    Três formas (uma coluna ou várias):
      { "type": "with_column", "column": "col", "expression": "SQL expr" }
      { "type": "with_column", "name": "col", "expression": "SQL expr" }   // compat
      { "type": "with_column", "columns": { "c1": "expr1", "c2": "expr2" } }

    Na forma múltipla (`columns`) as colunas são criadas na ordem do mapa, então
    uma coluna pode referenciar outra definida antes no mesmo bloco.
    """

    def apply(self, df: DataFrame) -> DataFrame:
        params = self.config.params

        columns = params.get("columns")
        if columns is not None:
            for name, expression in columns.items():
                df = df.withColumn(name, F.expr(expression))
            return df

        name = params.get("column", params.get("name"))
        if name is None:
            raise ValueError(
                "with_column requer 'column' (ou 'name'), "
                "ou 'columns' para múltiplas colunas."
            )
        return df.withColumn(name, F.expr(params["expression"]))


class StructTransformation(BaseTransformation):
    """Monta uma coluna struct (aninhada) a partir de um mapa campo → expressão.

    Bem mais legível que named_struct(...) para payloads aninhados. Cada chave é o
    nome do campo; o valor é uma expressão SQL (string) ou outro mapa (struct
    aninhado). Para evitar aninhamento profundo de JSON, a chave pode usar
    **dot-path** (`"data.nc.issuerName"`), que auto-aninha — deixando o payload
    como uma tabela plana campo→expressão (ótimo para ler/diff/revisar). As duas
    formas podem ser misturadas; a ordem dos campos segue a ordem de escrita.

    JSON params:
      column (ou name) – nome da coluna struct de saída
      fields           – mapa campo → expressão SQL | mapa aninhado, com chaves
                         simples ou em dot-path

    Exemplo (dot-path, plano):
      { "type": "struct", "column": "payload",
        "fields": {
          "id_externo": "id_vert",
          "data.nc.issueTypeCode": "codigo_tipo_emissao",
          "data.nc.issuerName": "nome_sacado",
          "data.nc.paymentMethod.indexCode": "lpad(codigo_indexador, 4, '0')"
        } }
      // equivale a named_struct('id_externo', id_vert, 'data', named_struct('nc', ...))
    """

    def apply(self, df: DataFrame) -> DataFrame:
        params = self.config.params
        name = params.get("column", params.get("name"))
        if name is None:
            raise ValueError("struct requer 'column' (ou 'name').")
        return df.withColumn(name, self._build(params["fields"]))

    @classmethod
    def _build(cls, fields: dict):
        fields = cls._expand_dotpaths(fields)
        cols = []
        for field_name, value in fields.items():
            if isinstance(value, dict):
                col = cls._build(value).alias(field_name)
            else:
                col = F.expr(value).alias(field_name)
            cols.append(col)
        return F.struct(*cols)

    @classmethod
    def _expand_dotpaths(cls, fields: dict) -> dict:
        """Expande chaves em dot-path ("a.b.c") para mapas aninhados, preservando
        a ordem de escrita e mesclando prefixos comuns. Chaves sem ponto ficam
        como estão. Conflito (mesmo caminho usado como folha e como mapa) → erro.
        """
        root: dict = {}
        for key, value in fields.items():
            parts = key.split(".")
            node = root
            for part in parts[:-1]:
                nxt = node.setdefault(part, {})
                if not isinstance(nxt, dict):
                    raise ValueError(
                        f"struct: conflito no caminho '{key}': '{part}' já é um valor folha"
                    )
                node = nxt
            leaf = parts[-1]
            if leaf in node and isinstance(node[leaf], dict) and isinstance(value, dict):
                node[leaf] = {**node[leaf], **value}
            elif leaf in node:
                raise ValueError(f"struct: campo conflitante/duplicado '{key}'")
            else:
                node[leaf] = value
        return root


class DropDuplicatesTransformation(BaseTransformation):
    """Removes duplicate rows, optionally scoped to specific columns."""

    def apply(self, df: DataFrame) -> DataFrame:
        columns: list[str] | None = self.config.params.get("columns")
        return df.dropDuplicates(columns) if columns else df.dropDuplicates()


class DistinctTransformation(BaseTransformation):
    """Removes duplicate rows, using all columns."""

    def apply(self, df: DataFrame) -> DataFrame:
        return df.distinct()


class RepartitionTransformation(BaseTransformation):
    """Redistribui as partições do DataFrame — muda o custo, nunca os dados.

    É a peça entre `partition_by` (quais diretórios existem no destino) e o
    número de arquivos gravados (quantas tasks escrevem em cada diretório). Sem
    ela, TODA task que contém linhas de um diretório abre um arquivo próprio
    ali — a origem do problema de small files.

    JSON params:
      num_partitions – número de partições alvo (int)
      columns        – expressões de particionamento: nome de coluna ou SQL
                       (`pmod(hash(id), 64)`). Linhas com o mesmo valor caem
                       na MESMA partição.
      coalesce       – true usa df.coalesce(n): apenas REDUZ, sem shuffle
                       (default: false)
      range          – true usa df.repartitionByRange(...): divide por FAIXA de
                       valor em vez de hash (default: false)

    Pelo menos um de num_partitions/columns é obrigatório.

    Combinação canônica contra small files: repartition pelas MESMAS expressões
    do `partition_by` do destino. Cada valor de chave cai numa única task, então
    sai exatamente UM arquivo por diretório — para qualquer num_partitions, que
    aí só controla o paralelismo da escrita.

    Exemplos:
      { "type": "repartition", "num_partitions": 200 }
      { "type": "repartition", "columns": ["dt"] }
      { "type": "repartition", "num_partitions": 64, "columns": ["pmod(hash(id), 64)"] }
      { "type": "repartition", "num_partitions": 1, "coalesce": true }
      { "type": "repartition", "num_partitions": 8, "columns": ["data_evento"], "range": true }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        params = self.config.params
        raw_num = params.get("num_partitions")
        columns = params.get("columns") or []
        use_coalesce = bool(params.get("coalesce", False))
        by_range = bool(params.get("range", False))

        if isinstance(columns, str):
            columns = [columns]

        if raw_num is None and not columns:
            raise ValueError(
                "repartition: informe 'num_partitions', 'columns', ou os dois — "
                "sem nenhum dos dois não há redistribuição a fazer."
            )

        num: int | None = None
        if raw_num is not None:
            try:
                num = int(raw_num)
            except (TypeError, ValueError):
                raise ValueError(
                    f"repartition: 'num_partitions' precisa ser um inteiro "
                    f"(recebido: {raw_num!r})."
                ) from None
            if num < 1:
                raise ValueError(
                    f"repartition: 'num_partitions' precisa ser >= 1 (recebido: {num})."
                )

        if use_coalesce:
            if by_range:
                raise ValueError(
                    "repartition: 'coalesce' e 'range' são exclusivos — coalesce funde "
                    "partições vizinhas sem shuffle, repartitionByRange redistribui por "
                    "faixa de valor com shuffle."
                )
            if columns:
                raise ValueError(
                    "repartition: 'coalesce' não aceita 'columns' — coalesce funde "
                    "partições vizinhas sem shuffle, então não há chave para agrupar. "
                    "Remova 'columns', ou remova 'coalesce' para redistribuir por chave."
                )
            if num is None:
                raise ValueError("repartition: 'coalesce' exige 'num_partitions'.")
            # coalesce só reduz: pedir mais partições do que existem é no-op silencioso.
            return df.coalesce(num)

        exprs = [F.expr(str(column)) for column in columns]

        if by_range:
            if not exprs:
                raise ValueError(
                    "repartition: 'range' exige 'columns' — repartitionByRange divide "
                    "por faixa de valor, e sem coluna não há faixa."
                )
            if num is None:
                return df.repartitionByRange(*exprs)
            return df.repartitionByRange(num, *exprs)

        if exprs:
            if num is None:
                return df.repartition(*exprs)
            return df.repartition(num, *exprs)
        return df.repartition(num)


class CheckpointTransformation(BaseTransformation):
    """Materializa o DataFrame e trunca seu plano lógico (checkpoint).

    Equivale aos checkpoint()/localCheckpoint() usados em jobs Spark longos para
    quebrar a linhagem após joins pesados — mantém o planner rápido e evita
    recomputar estágios anteriores a cada ação. Não altera os dados.

    JSON params:
      method – qual método Spark chamar (default: "localCheckpoint")
                 "localCheckpoint" → df.localCheckpoint() — grava no disco local
                                     dos executors; rápido, mas perdido se um
                                     executor morre (recomputa). É o usado no job.
                 "checkpoint"      → df.checkpoint() — grava em storage confiável;
                                     requer spark.sparkContext.setCheckpointDir.
      eager  – materializa imediatamente (default: true)

    Se `method` vier com valor inválido, a transformação é ignorada (o df segue
    intacto para as próximas etapas) e um warning é emitido no fim do pipeline.

    Exemplo:
      { "type": "checkpoint" }                              // localCheckpoint(eager=True)
      { "type": "checkpoint", "method": "checkpoint" }      // checkpoint confiável
      { "type": "checkpoint", "eager": false }
    """

    _METHODS = {"localcheckpoint", "checkpoint"}

    def apply(self, df: DataFrame) -> DataFrame:
        eager = self.config.params.get("eager", True)
        method = self.config.params.get("method", "localCheckpoint")
        normalized = method.lower() if isinstance(method, str) else None

        if normalized not in self._METHODS:
            defer_warning(
                "checkpoint ignorado: method inválido",
                method=method,
                opcoes=["localCheckpoint", "checkpoint"],
            )
            return df

        if normalized == "checkpoint":
            return df.checkpoint(eager=eager)
        return df.localCheckpoint(eager=eager)


class StopIfEmptyTransformation(BaseTransformation):
    """Encerra o pipeline graciosamente se o DataFrame estiver vazio.

    Evita iniciar processamento pesado (joins, payloads) e abrir escritas vazias
    quando não há dados a processar. Levanta PipelineStop, capturada pelo
    Pipeline, que retorna um resultado com success=True, skipped=True e
    rows_written=0 — sem rodar as transformações seguintes nem as saídas.

    Dispara uma action (isEmpty) — posicione logo após o filtro que define o
    conjunto a processar, antes de checkpoint/joins.

    JSON params:
      message – texto opcional logado no encerramento

    Exemplo:
      { "type": "stop_if_empty", "message": "Sem cessoes a processar" }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        if df.isEmpty():
            raise PipelineStop(
                self.config.params.get("message", "DataFrame vazio — nada a processar")
            )
        return df


class CollectTransformation(BaseTransformation):
    """Coleta os valores distintos de uma coluna para uma variável de runtime.

    Os valores ficam disponíveis nas transformações seguintes (inclusive dentro de
    with_transformations) via placeholder {{nome}}, resolvido em tempo de execução
    e formatado como lista SQL — pronto para `IN (...)`.

    Use para empurrar um filtro literal (predicate pushdown / data skipping) nas
    leituras de tabelas grandes carregadas depois. Equivale ao .collect()+.isin()
    de jobs Spark: traz a lista para o driver uma vez e reusa nos reads seguintes.

    JSON params:
      column     – coluna cujos valores distintos serão coletados
      as         – nome da variável de runtime (referenciada como {{as}})
      max_values – teto de valores distintos (default 10000; 0 desliga o teto)

    Não altera o DataFrame, mas dispara uma action Spark (collect no driver) —
    use após um `checkpoint` para evitar recomputar a linhagem inteira.

    Guarda de tamanho: a lista coletada vira literal dentro de `IN (...)`, e a
    partir de alguns milhares de valores o remédio passa a ser o problema — o
    plano cresce, o Catalyst gasta tempo analisando o predicado e o pushdown
    degrada. Acima de `max_values` a transformação **falha** dizendo o que usar
    no lugar (um `join` semi/inner com a lista como DataFrame, que o Spark
    resolve como broadcast join sem trazer nada para o driver). O teto é
    aplicado na própria consulta (`limit(max_values + 1)`), então uma coluna com
    milhões de valores distintos não chega a ser materializada no driver.

    Exemplo:
      { "type": "collect", "column": "id_cessao", "as": "cessoes_pendentes" }
      // depois, no read de uma tabela grande:
      // { "type": "filter", "condition": "id_cessao IN ({{cessoes_pendentes}})" }
      { "type": "collect", "column": "id", "as": "ids", "max_values": 50000 }
    """

    #: Acima disto a lista deixa de ser um bom empurrão de filtro (ver docstring).
    _MAX_VALUES = 10_000
    #: Aviso antes do teto: ainda funciona, já merece revisão.
    _WARN_VALUES = 1_000

    def apply(self, df: DataFrame) -> DataFrame:
        column = self.config.params["column"]
        var_name = self.config.params["as"]
        max_values = self.config.params.get("max_values", self._MAX_VALUES)

        if not isinstance(max_values, int) or isinstance(max_values, bool) or max_values < 0:
            raise ValueError(
                f"collect: 'max_values' deve ser inteiro >= 0 (recebido: {max_values!r}). "
                f"Use 0 para desligar o teto."
            )

        distintos = df.select(column).distinct()
        # Teto aplicado na consulta: com N+1 sabemos que passou do limite sem
        # trazer o resto. Sem teto (0), coleta tudo.
        limitado = distintos.limit(max_values + 1) if max_values else distintos
        values = [row[0] for row in limitado.collect()]

        if max_values and len(values) > max_values:
            raise ValueError(
                f"collect: a coluna '{column}' tem mais de {max_values} valores distintos, "
                f"e a lista é usada como literal em IN (...). Nesse tamanho o filtro literal "
                f"deixa de ajudar: o plano cresce e o pushdown degrada. Troque por um join "
                f"(semi/inner) contra a lista como DataFrame — o Spark resolve como broadcast "
                f"join sem passar pelo driver — ou aumente o teto com "
                f'"max_values": <n> (0 desliga) se a lista realmente for pequena o bastante '
                f"para o banco de destino."
            )

        if len(values) > self._WARN_VALUES:
            defer_warning(
                "Lista coletada é grande para usar como literal em IN (...)",
                coluna=column,
                variavel=var_name,
                quantidade=len(values),
                limiar=self._WARN_VALUES,
                alternativa="join semi/inner contra a lista como DataFrame",
            )

        self.runtime[var_name] = values

        logger.info(
            "Valores coletados para runtime",
            coluna=column,
            variavel=var_name,
            quantidade=len(values),
        )
        return df


class SqlTransformation(BaseTransformation):
    """Runs an arbitrary SQL query against the DataFrame exposed as a temp view."""

    def apply(self, df: DataFrame) -> DataFrame:
        view = self.config.params.get("view_name", "_df")
        df.createOrReplaceTempView(view)
        return df.sparkSession.sql(self.config.params["query"])


class FillNaTransformation(BaseTransformation):
    """Fills null values with a constant or per-column mapping."""

    def apply(self, df: DataFrame) -> DataFrame:
        value = self.config.params["value"]
        columns: list[str] | None = self.config.params.get("columns")
        return df.fillna(value, subset=columns)


class SortTransformation(BaseTransformation):
    """Orders the DataFrame by one or more columns."""

    def apply(self, df: DataFrame) -> DataFrame:
        columns: list[str] = self.config.params["columns"]
        ascending = self.config.params.get("ascending", True)

        if isinstance(ascending, list):
            order_cols = [
                F.col(c).asc() if asc else F.col(c).desc()
                for c, asc in zip(columns, ascending)
            ]
        else:
            order_cols = [
                F.col(c).asc() if ascending else F.col(c).desc()
                for c in columns
            ]
        return df.orderBy(*order_cols)


class GroupByTransformation(BaseTransformation):
    """Groups the DataFrame and applies SQL aggregation expressions.

    JSON params:
      by    – lista de colunas para agrupar
      agg   – lista de expressões SQL de agregação completas (strings). Cada item
              é passado direto para F.expr(), então qualquer função/sintaxe SQL do
              Spark é válida, incluindo alias e expressões compostas.
      pivot – (opcional) pivota os grupos por uma coluna. Aceita:
                "coluna"                                   → pivot simples
                { "column": "coluna", "values": [...] }    → pivot com valores
                  explícitos (mais eficiente, evita um scan extra do Spark)

    Example:
      { "type": "group_by",
        "by": ["id", "categoria"],
        "agg": [
          "sum(valor) as total",
          "min(status) as status",
          "count(*) as n",
          "first(tipo_contrato) as tipo_contrato",
          "count(distinct struct(tipo_ativo, registradora)) > 1 as multi_ativos"
        ] }

      { "type": "group_by",
        "by": ["id"],
        "pivot": { "column": "mes", "values": ["jan", "fev", "mar"] },
        "agg": ["sum(valor) as total"] }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        by: list[str] = self.config.params["by"]
        agg_specs: list[str] = self.config.params["agg"]
        agg_exprs = [F.expr(spec) for spec in agg_specs]

        grouped = df.groupBy(*by)

        pivot = self.config.params.get("pivot")
        if pivot is not None:
            if isinstance(pivot, dict):
                values = pivot.get("values")
                grouped = (
                    grouped.pivot(pivot["column"], values)
                    if values
                    else grouped.pivot(pivot["column"])
                )
            else:
                grouped = grouped.pivot(pivot)

        return grouped.agg(*agg_exprs)


_VALID_JOIN_TYPES = {
    "inner", "cross",
    "outer", "full", "fullouter", "full_outer",
    "left", "leftouter", "left_outer",
    "right", "rightouter", "right_outer",
    "semi", "leftsemi", "left_semi",
    "anti", "leftanti", "left_anti",
}

#: Joins que devolvem só as colunas do lado esquerdo — não há o que desambiguar.
_SO_LADO_ESQUERDO = {
    "semi", "leftsemi", "left_semi",
    "anti", "leftanti", "left_anti",
}


def _fonte(params: dict, transformacao: str) -> dict:
    """A configuração da segunda fonte, em `input`.

    `input` é o mesmo nome do bloco de entrada do pipeline — a fonte de um join é
    uma entrada como qualquer outra, e chamá-la de outra coisa era uma palavra a
    mais para aprender. `with` era esse nome e não é mais aceito: em vez de ser
    ignorada em silêncio (o que faria o join rodar sem a segunda fonte), a chave
    antiga é recusada dizendo qual usar.
    """
    if "with" in params and "input" not in params:
        raise ValueError(
            f"{transformacao}: 'with' nao existe mais; renomeie a chave para "
            "'input'."
        )
    source = params.get("input")
    if source is None:
        raise ValueError(
            f"{transformacao}: falta 'input' com a segunda fonte (formato + path)."
        )
    return source


def _nome_livre(base: str, usados: set) -> str:
    """`nome` → `nome_r`, e `nome_r2`, `nome_r3`… se o anterior já existir."""
    candidato = f"{base}_r"
    n = 1
    while candidato in usados:
        n += 1
        candidato = f"{base}_r{n}"
    return candidato


class JoinTransformation(BaseTransformation):
    """Joins the main DataFrame with a second source.

    Supports all Spark join types: inner, cross, left, right, full/outer,
    semi/leftsemi, anti/leftanti and their underscore variants.

    The left DataFrame is aliased as 'l' and the right as 'r', so SQL
    expressions in 'on' can use l.campo and r.campo to disambiguate columns.

    Columns present on both sides are renamed on the right with a '_r' suffix
    ('nome' and 'nome_r'), so the result never carries two columns with the same
    name. See `_desambiguar`.

    JSON params:
      input                – source config (format + path + options)
      on                   – column name, list of column names, or SQL expression
                             (SQL expressions containing spaces use l./r. aliases)
      how                  – join type (default: inner)
      broadcast            – dica de broadcast (map-side) join: espalha o lado pequeno
                             em todos os executors e evita o shuffle do lado grande.
                             true / "right" → broadcast do lado direito (o `input`, ex:
                             dimensão/lookup pequeno); "left" → broadcast do principal;
                             false / ausente → sem dica (o Spark decide por tamanho).
      with_transformations – list of transformations applied to the right-side
                             DataFrame before the join (all builtin types supported)

    Examples:
      { "type": "join",
        "input": { "format": "parquet", "path": "/ref/products" },
        "on": "product_id",
        "how": "leftanti" }

      { "type": "join",
        "input": { "format": "delta", "path": "ref.dim_produto" },
        "on": "produto_id",
        "how": "left",
        "broadcast": true }

      { "type": "join",
        "input": { "format": "delta", "path": "catalog.schema.contratos" },
        "with_transformations": [
          { "type": "filter", "condition": "status = 1" },
          { "type": "select", "columns": ["id", "nome"] }
        ],
        "on": "id",
        "how": "inner" }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        from sparquet.io.factory import ReaderFactory

        params = self.config.params
        source_cfg = InputConfig.from_dict(_fonte(params, "join"))
        other = ReaderFactory.create(df.sparkSession, source_cfg).read()

        raw_transforms = params.get("with_transformations", [])
        if raw_transforms:
            from sparquet.core.config import TransformationConfig
            from sparquet.transform.engine import TransformationEngine
            # Compartilha o runtime para que {{var}} coletadas no escopo externo
            # valham nos with_transformations aninhados.
            engine = TransformationEngine(runtime=self.runtime)
            cfgs = [TransformationConfig.from_dict(t) for t in raw_transforms]
            other = engine.apply(other, cfgs)

        on = params["on"]
        how: str = params.get("how", "inner").lower()

        if how not in _VALID_JOIN_TYPES:
            raise ValueError(
                f"Tipo de join '{how}' invalido. "
                f"Opcoes: {sorted(_VALID_JOIN_TYPES)}"
            )

        left = df.alias("l")
        right = other.alias("r")

        # Broadcast (map-side) join: espalha o lado pequeno em todos os executors,
        # evitando o shuffle do lado grande. O alias é preservado dentro da dica.
        side = self._broadcast_side()
        if side == "right":
            right = F.broadcast(right)
        elif side == "left":
            left = F.broadcast(left)

        # String with spaces → SQL expression (supports l.campo / r.campo)
        if isinstance(on, str) and " " in on:
            joined = left.join(right, F.expr(on), how)
            chaves: list[str] = []
        else:
            joined = left.join(right, on, how)
            # Com `on` por nome o Spark já funde a chave numa coluna só: ela não
            # é ambígua e não deve ser renomeada.
            chaves = [on] if isinstance(on, str) else list(on)

        return self._desambiguar(joined, df, other, how, chaves)

    @staticmethod
    def _desambiguar(
        joined: DataFrame,
        left: DataFrame,
        right: DataFrame,
        how: str,
        chaves: list[str],
    ) -> DataFrame:
        """Garante nomes únicos no resultado, renomeando o lado direito.

        Um join entre fontes que compartilham nomes de coluna — o caso normal num
        self join — devolvia duas colunas `nome`, e qualquer transformação
        seguinte que citasse `nome` falhava com `AMBIGUOUS_REFERENCE`. Aqui as
        repetidas do lado direito viram `nome_r` (`_r2`, `_r3`… se já existir),
        na mesma linha do alias `r` que o join usa.

        A projeção é montada DEPOIS do join, sobre os aliases `l`/`r`, para não
        interferir na condição do `on`: um `on` escrito à mão cita `r.nome`, e
        renomear antes de juntar tiraria essa referência do lugar.

        Quando não há nome repetido o DataFrame volta como estava — o caso comum
        não paga nada, nem em plano nem em comportamento.
        """
        if how in _SO_LADO_ESQUERDO:
            return joined

        repetidas = [c for c in right.columns if c in left.columns and c not in chaves]
        if not repetidas:
            return joined

        usados = set(left.columns) | set(chaves)
        projecao = [F.col(f"`{k}`") for k in chaves]
        for c in left.columns:
            if c not in chaves:
                projecao.append(F.col(f"l.`{c}`").alias(c))
        for c in right.columns:
            if c in chaves:
                continue
            nome = c if c not in repetidas else _nome_livre(c, usados)
            usados.add(nome)
            projecao.append(F.col(f"r.`{c}`").alias(nome))
        return joined.select(*projecao)

    def _broadcast_side(self) -> str:
        """Normaliza o param 'broadcast' → "left" | "right" | "" (sem dica)."""
        value = self.config.params.get("broadcast", False)
        if value is True:
            return "right"
        if value is False or value is None:
            return ""
        if isinstance(value, str):
            side = value.strip().lower()
            if side in ("true", "yes", "right"):
                return "right"
            if side == "left":
                return "left"
            if side in ("", "false", "no", "none"):
                return ""
        raise ValueError(
            f"join: 'broadcast' invalido '{value}'. Use true/false ou 'left'/'right'."
        )



class ScanPushdown(NamedTuple):
    """O que um nó de leitura do plano físico diz ter empurrado para a fonte."""

    #: `FileScan`, `BatchScan`, `Scan` (JDBC/v2) ou `RowDataSourceScan`.
    kind: str
    #: Formato ou relação: `parquet`, `iceberg`, `JDBCRelation(vendas)`.
    source: str
    #: Predicados resolvidos por diretório de partição — não abrem arquivo nenhum.
    partition_filters: List[str]
    #: Predicados entregues à fonte (row group do Parquet, WHERE do banco).
    pushed_filters: List[str]
    #: Agregações empurradas. O v2 de arquivo imprime `PushedAggregation`, o
    #: JDBC imprime `PushedAggregates` — as duas etiquetas caem aqui.
    pushed_aggregates: List[str]
    #: Chaves de group by que a fonte assumiu (`PushedGroupBy`).
    pushed_group_by: List[str]
    #: Filtros dinâmicos, resolvidos em runtime (DPP e bloom filter de join).
    runtime_filters: List[str]
    #: Colunas de primeiro nível que o scan devolve (projeção efetiva).
    columns_read: int
    #: Caminho ou tabela, quando o plano informa.
    location: str

    @property
    def pushes_nothing(self) -> bool:
        return not (self.partition_filters or self.pushed_filters or self.pushed_aggregates)


#: Início de um nó de leitura, já sem os glifos da árvore (`+-`, `:-`, `*(1)`).
_SCAN_START = re.compile(r"^(FileScan|BatchScan|RowDataSourceScan|Scan)\b\s*(\S*)")
#: `PushedFilters: [IsNotNull(id), EqualTo(id,3)]` e parentes.
_LISTA = r"{campo}:\s*\[([^\]]*)\]"
_LOCATION = re.compile(r"Location:\s*\w+\((?:[^)]*)\)\[([^\]]*)\]")
_READ_SCHEMA = "ReadSchema: struct<"


def _colunas_lidas(linha: str) -> int:
    """Conta os campos de topo do `ReadSchema` do scan.

    Não dá para regex simples: o struct aninha (`struct<a:int,b:struct<c:int>>`)
    e o `ReadSchema` nem sempre é o último campo da linha (no v2 vem
    `RuntimeFilters` depois). Anda pelo texto fechando os `<`.
    """
    inicio = linha.find(_READ_SCHEMA)
    if inicio < 0:
        return 0
    corpo_inicio = inicio + len(_READ_SCHEMA)
    nivel = 1
    for pos in range(corpo_inicio, len(linha)):
        if linha[pos] == "<":
            nivel += 1
        elif linha[pos] == ">":
            nivel -= 1
            if nivel == 0:
                corpo = linha[corpo_inicio:pos]
                return len(_split_top_level(corpo)) if corpo.strip() else 0
    return 0


def _lista(*campos: str, linha: str) -> List[str]:
    achado = None
    for campo in campos:
        achado = re.search(_LISTA.format(campo=campo), linha)
        if achado:
            break
    if not achado:
        return []
    corpo = achado.group(1).strip()
    if not corpo:
        return []
    return [item.strip() for item in _split_top_level(corpo) if item.strip()]


def _split_top_level(texto: str) -> List[str]:
    """Divide por vírgula ignorando as que estão dentro de () ou <>.

    `EqualTo(grupo,3)` é um item só, e `struct<a:int,b:struct<c:int>>` tem dois
    campos de primeiro nível.
    """
    partes: List[str] = []
    nivel = 0
    atual = []
    for char in texto:
        if char in "(<":
            nivel += 1
        elif char in ")>":
            nivel -= 1
        if char == "," and nivel == 0:
            partes.append("".join(atual))
            atual = []
            continue
        atual.append(char)
    partes.append("".join(atual))
    return partes


def parse_pushdown(plan: str) -> List[ScanPushdown]:
    """Extrai o pushdown declarado por cada nó de leitura de um plano físico.

    Recebe o texto de `queryExecution.executedPlan` (é o que `explain()` imprime)
    e devolve uma entrada por scan. Função pura de propósito: o parsing é o que
    tem risco de errar, e assim ele é testável sem SparkSession.

    O plano é a única fonte que sabe o que o Spark realmente empurrou — a opção
    pedida no JSON diz a intenção, não o resultado. Um `filter` sobre coluna
    partitionada aparece em `PartitionFilters`; sobre coluna comum, em
    `PushedFilters`; e o que não desceu fica num nó `Filter` acima do scan.
    """
    scans: List[ScanPushdown] = []
    for linha_bruta in plan.splitlines():
        # Tira os glifos da árvore e o marcador de codegen (`*(1) `).
        linha = re.sub(r"^[\s:+\-]*", "", linha_bruta)
        linha = re.sub(r"^\*\(\d+\)\s*", "", linha)
        inicio = _SCAN_START.match(linha)
        if not inicio:
            continue
        kind, resto = inicio.group(1), inicio.group(2)
        # `FileScan parquet [id#2L,...]` → source `parquet`; no v2 o nome da
        # relação já vem colado no colchete das colunas.
        source = resto.split("[")[0].strip() or kind
        # `Scan ExistingRDD` / `OneRowRelation` não são leitura de fonte: são
        # dados que já estão na memória do driver. Sem fonte, não há pushdown a
        # cobrar — ficam de fora para não gerar aviso falso.
        if source.startswith(("ExistingRDD", "OneRowRelation")):
            continue
        local = _LOCATION.search(linha)
        scans.append(
            ScanPushdown(
                kind=kind,
                source=source,
                partition_filters=_lista("PartitionFilters", linha=linha),
                pushed_filters=_lista("PushedFilters", linha=linha),
                pushed_aggregates=_lista("PushedAggregation", "PushedAggregates", linha=linha),
                pushed_group_by=_lista("PushedGroupBy", "PushedGroupByExpressions", linha=linha),
                runtime_filters=_lista("RuntimeFilters", linha=linha),
                columns_read=_colunas_lidas(linha),
                location=local.group(1) if local else "",
            )
        )
    return scans


def _plan_text(df: DataFrame) -> str:
    """Texto do plano físico sem passar pelo stdout do `explain()`."""
    return df._jdf.queryExecution().executedPlan().toString()


class DebugTransformation(BaseTransformation):
    """Executes inspection actions on the DataFrame without modifying it.

    The "show" action always uses df.show(). Databricks' display() is a notebook
    widget and only works reliably when called directly in a cell — from an
    imported module it emits DataFrame.__repr__() (the schema string) to stdout
    instead of the actual rows.

    JSON params:
      actions         – list of actions to run (default: ["show", "print_schema"])
      label           – optional label shown in the separator line
      transformations – optional list of transformations applied to a throwaway
                        copy of the df, JUST for this inspection. They do NOT touch
                        the pipeline df (debug always returns the original df).
                        Useful to focus the view (filter/select/group_by/…) without
                        changing what the next steps receive.
      show_rows       – rows for show/display (default: 20)
      truncate        – truncate show output (default: true)
      vertical        – vertical layout for show (default: false)
      extended        – extended plan for explain (default: false)

    Supported actions:
      show, print_schema, count, explain, pushdown, columns, dtypes

    A ação `pushdown` lê o plano físico e diz, por nó de leitura, o que desceu
    até a fonte: `PartitionFilters` (partição podada, nem abre arquivo),
    `PushedFilters` (predicado entregue ao Parquet/ORC ou ao banco),
    `PushedAggregates`, `RuntimeFilters` (DPP e bloom filter de join) e quantas
    colunas o scan devolve. Quando um scan não empurra nada, avisa — é a
    diferença entre "pedi pushdown" e "o Spark empurrou". Não dispara job: o
    plano físico é planejamento, não execução.

    Example:
      { "type": "debug", "label": "após join", "actions": ["count", "print_schema", "show"] }
      { "type": "debug", "actions": ["show"], "show_rows": 5, "truncate": false }
      { "type": "debug", "label": "leitura", "actions": ["pushdown"] }
      // inspeciona só as linhas de uma cessão, sem alterar o pipeline:
      { "type": "debug", "label": "cessão C1", "actions": ["count", "show"],
        "transformations": [ { "type": "filter", "condition": "id_cessao = 'C1'" },
                             { "type": "select", "columns": ["id_cessao", "numero_contrato"] } ] }
    """

    _SUPPORTED_ACTIONS = {
        "show",
        "print_schema",
        "count",
        "explain",
        "pushdown",
        "columns",
        "dtypes",
    }

    def apply(self, df: DataFrame) -> DataFrame:
        params = self.config.params
        label = params.get("label", "")
        actions: list[str] = params.get("actions", ["show", "print_schema"])

        # Transformações efêmeras: aplicadas num df à parte, só para a inspeção.
        # O df do pipeline não é alterado — no fim retornamos o `df` original.
        view = df
        raw_transforms = params.get("transformations", [])
        if raw_transforms:
            from sparquet.core.config import TransformationConfig
            from sparquet.transform.engine import TransformationEngine
            # Compartilha o runtime para que {{var}} coletadas antes resolvam aqui.
            engine = TransformationEngine(runtime=self.runtime)
            cfgs = [TransformationConfig.from_dict(t) for t in raw_transforms]
            view = engine.apply(view, cfgs)

        header = f"[DEBUG{f' — {label}' if label else ''}]"
        print(f"\n{'─' * 60}\n{header}\n{'─' * 60}")

        for raw in actions:
            action = raw.lower().replace("-", "_").replace("printschema", "print_schema")
            if action == "show":
                view.show(
                    n=params.get("show_rows", 20),
                    truncate=params.get("truncate", True),
                    vertical=params.get("vertical", False),
                )
            elif action == "print_schema":
                view.printSchema()
            elif action == "count":
                print(f"count: {view.count()}")
            elif action == "explain":
                view.explain(extended=params.get("extended", False))
            elif action == "pushdown":
                self._report_pushdown(view)
            elif action == "columns":
                print(f"columns: {view.columns}")
            elif action == "dtypes":
                print(f"dtypes: {view.dtypes}")
            else:
                print(
                    f"⚠️  ação desconhecida: '{raw}'. "
                    f"Disponíveis: {sorted(self._SUPPORTED_ACTIONS)}"
                )

        print(f"{'─' * 60}\n")
        return df

    def _report_pushdown(self, view: DataFrame) -> None:
        """Imprime o pushdown por nó de leitura e avisa o que ficou sem."""
        plan = _plan_text(view)
        scans = parse_pushdown(plan)
        if not scans:
            print(
                "pushdown: nenhum nó de leitura no plano — o df vem de memória "
                "(range/createDataFrame) ou de um checkpoint já materializado."
            )
            return

        # Um `Filter` acima do scan é predicado que o Spark decidiu (ou não pôde)
        # avaliar depois da leitura — o dado sobe do disco e só então é descartado.
        filtros_acima = len(re.findall(r"^[\s:+\-]*(?:\*\(\d+\)\s*)?Filter ", plan, re.M))

        for i, scan in enumerate(scans, start=1):
            alvo = f" — {scan.location}" if scan.location else ""
            print(f"scan {i}: {scan.kind} {scan.source}{alvo}")
            print(f"  colunas lidas: {scan.columns_read}")
            for rotulo, valores in (
                ("PartitionFilters", scan.partition_filters),
                ("PushedFilters", scan.pushed_filters),
                ("PushedAggregates", scan.pushed_aggregates),
                ("PushedGroupBy", scan.pushed_group_by),
                ("RuntimeFilters", scan.runtime_filters),
            ):
                if valores:
                    print(f"  {rotulo}: {', '.join(valores)}")
            if scan.pushes_nothing:
                print(
                    "  ⚠️  nada empurrado: este scan lê a fonte inteira. "
                    "Ponha o `filter` como primeira transformação; se o predicado "
                    "for sobre coluna de partição, aponte o path na raiz do dataset "
                    "(+ basePath) para ele virar PartitionFilters."
                )

        if filtros_acima:
            print(
                f"  nota: {filtros_acima} nó(s) `Filter` acima dos scans — predicado "
                f"avaliado depois de ler (o Spark não pôde descer, ou é expressão que "
                f"a fonte não entende)."
            )



class UnionTransformation(BaseTransformation):
    """Appends rows from a second source to the main DataFrame.

    JSON params:
      input                 – source config (format + path + options)
      allow_missing_columns – fill missing columns with null (default: false)

    Example:
      { "type": "union",
        "input": { "format": "parquet", "path": "/data/extra_orders" },
        "allow_missing_columns": true }
    """

    def apply(self, df: DataFrame) -> DataFrame:
        from sparquet.io.factory import ReaderFactory

        source_cfg = InputConfig.from_dict(_fonte(self.config.params, "union"))
        other = ReaderFactory.create(df.sparkSession, source_cfg).read()

        if self.config.params.get("allow_missing_columns", False):
            return df.unionByName(other, allowMissingColumns=True)
        return df.union(other)
