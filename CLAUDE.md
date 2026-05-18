# SparkFramework — Contexto para Claude

Framework Python/PySpark **orientado a configuração JSON** para pipelines de dados.
Toda a lógica de leitura, transformação e escrita é declarativa; Python só orquestra
(loops, parâmetros de runtime, cleanup). Objetivo: produto reutilizável para
qualquer caso de ingestão, transformação e qualidade de dados.

---

## Uso como biblioteca (ponto de entrada principal)

```python
from spark_framework import SparkFramework

fw = SparkFramework(spark={"app_name": "MeuJob", "master": "yarn"})

r1 = fw.run("pipeline_clientes.json")
r2 = fw.run("pipeline_pedidos.json", params={"data_ref": "2026-01-01"})
r3 = fw.run_from_dict({"name": "inline", "input": {...}, "output": {...}})

fw.drop_views(["view_intermediaria_1", "view_intermediaria_2"])
fw.stop()
```

`SparkFramework` em `framework.py` gerencia o singleton de SparkSession e
compartilha os engines de transformação/validação entre todas as execuções.

---

## Substituição de params (`${param}`)

Valores de runtime são passados via `fw.run(conf, params={...})` e substituem
ocorrências de `${param_name}` nas strings do JSON **antes** da execução.

**Duas sintaxes**:

| Sintaxe | Comportamento | Quando usar |
|---------|---------------|-------------|
| `${name}` | SQL value escapado (string com aspas, lista como `array(...)`, None como `NULL`, bool como `true/false`, numérico literal) | Em expressões SQL (`condition`, `expression`, `skip_if`) — caso mais comum |
| `${name!raw}` | Valor literal sem escape | Em paths, nomes de view, options de conexão (Kafka broker, tópico, etc.) |

**Exemplos** com `params = {"tipo": "NC", "lista_cessoes": ["a", "b"], "topico": "vertc-topic"}`:

| JSON fonte | Resultado |
|-----------|-----------|
| `"condition": "tipo_ativo = ${tipo}"` | `"tipo_ativo = 'NC'"` |
| `"condition": "array_contains(${lista_cessoes}, id)"` | `"array_contains(array('a', 'b'), id)"` |
| `"path": "${topico!raw}"` | `"vertc-topic"` |
| `"options": { "bootstrap_servers": "${broker!raw}" }` | substitui literal sem aspas |

**Listas vazias** (`[]`) ou `None` → viram `NULL`. Use `skip_if` para skipar
transformações quando o param está vazio:

```json
{ "type": "filter",
  "skip_if":   "${param_lista_cessoes} IS NULL",
  "condition": "array_contains(${param_lista_cessoes}, id_cessao)" }
```

A substituição é puramente **textual** e aplicada antes da deserialização da
conf — não há injeção de coluna no DataFrame. Para criar uma coluna a partir
de um valor de runtime, use `with_column` com a expressão substituída.

---

## Arquitetura

```
JSON/dict → substitute_params(${...}) → PipelineConfig → Pipeline.run()
                                                              │
                                                              ├─► ReaderFactory(input)  → DataFrame
                                                              ├─► TransformationEngine   → DataFrame
                                                              ├─► ValidationEngine       → ValidationResult[]
                                                              └─► para cada output em outputs:
                                                                      output.transformations (opcional)
                                                                      _project_columns(df, output)
                                                                      WriterFactory(output).write(df_projetado)
```

### Módulos

| Módulo | Arquivo | Responsabilidade |
|--------|---------|-----------------|
| `SparkFramework` | `framework.py` | Entry point como lib; substitui params; gerencia singleton Spark |
| `substitute_params` | `core/config.py` | Resolve `${name}` e `${name!raw}` recursivamente no dict da conf |
| `PipelineConfig` | `core/config.py` | Deserializa JSON em dataclasses |
| `SparkContextManager` | `core/context.py` | Singleton do SparkSession |
| `Pipeline` | `core/pipeline.py` | Orquestrador; `run() → PipelineResult` |
| `BaseReader/Writer` | `io/base.py` | Contratos abstratos para IO |
| `ParquetReader/Writer` | `io/parquet.py` | Implementação Parquet |
| `IcebergReader/Writer` | `io/iceberg.py` | Iceberg (com MERGE INTO + alias T/S) |
| `DeltaReader/Writer` | `io/delta.py` | Delta Lake (com MERGE INTO + alias T/S) |
| `CsvReader/Writer` | `io/csv.py` | CSV com defaults de header/encoding |
| `TxtReader/Writer` | `io/txt.py` | Texto bruto |
| `KafkaWriter` | `io/kafka.py` | Publicação Kafka (batch) |
| `ViewReader/Writer` | `io/view.py` | Spark temp views (com cache/checkpoint) |
| `ReaderFactory/WriterFactory` | `io/factory.py` | Registry de formatos; extensível |
| `TransformationEngine` | `transform/engine.py` | Aplica transformações em sequência + avalia `skip_if` |
| Transformações nativas | `transform/builtin.py` | Ver lista abaixo |
| `ValidationEngine` | `validation/engine.py` | Roda validators; respeita `on_failure` |
| Validators nativos | `validation/builtin.py` | not_null, unique, range, regex, row_count, custom_sql |

---

## Schema JSON do pipeline

```jsonc
{
  "name": "string",                    // obrigatório
  "description": "string",             // opcional
  "params": ["param_tipo_ativo", "param_lista_cessoes"],  // opcional — documenta os params esperados
  "spark": {                            // opcional
    "app_name": "string",
    "master": "string",
    "configs": { "spark.sql.*": "valor" }
  },

  "input": {                            // obrigatório — fonte principal
    "format": "csv|parquet|delta|iceberg|view|txt",
    "path": "string",
    "options": {}
  },

  "transformations": [                  // opcional — aplicadas em ordem
    // Cada transformação aceita opcionalmente:
    //   "skip_if": "<expressão SQL>"   — se avaliada como false/null, é skipada

    { "type": "filter", "condition": "SQL expr" },

    // select aceita strings (nomes) OU dicts {name, expression} (colunas computadas)
    // ESTA É A FORMA MAIS COMPACTA de criar colunas inline dentro de uma projeção:
    { "type": "select",
      "columns": [
        "id",
        "nome",
        { "name": "doc_padronizado", "expression": "lpad(regexp_replace(doc, '\\D', ''), 14, '0')" },
        { "name": "data_envio",      "expression": "current_timestamp()" }
      ] },

    { "type": "drop", "columns": ["x"] },
    { "type": "rename", "mappings": {"old": "new"} },
    { "type": "cast", "columns": {"col": "type"} },

    { "type": "with_column", "name": "col", "expression": "SQL expr" },

    // with_columns (plural): cria várias colunas em batch (mais legível
    // que N with_column quando há regras correlatas)
    { "type": "with_columns",
      "columns": [
        { "name": "doc_norm",     "expression": "regexp_replace(doc, '\\D', '')" },
        { "name": "doc_padded",   "expression": "lpad(doc_norm, 14, '0')" }   // pode referenciar coluna criada antes
      ] },

    { "type": "drop_duplicates", "columns": ["id"] },
    { "type": "sql", "query": "SELECT ...", "view_name": "_df" },
    { "type": "fill_na", "value": 0, "columns": ["col"] },
    { "type": "sort", "columns": ["col"], "ascending": true },

    {
      "type": "group_by",
      "by": ["col1", "col2"],
      "agg": [
        // funções nativas
        { "func": "min|max|sum|avg|count|first|last|count_distinct|collect_list|collect_set",
          "column": "col",
          "alias": "nome" },

        // expressão SQL arbitrária
        { "func": "expr",
          "expression": "count(distinct struct(tipo_ativo, registradora)) > 1",
          "alias": "multi_ativos" }
      ]
    },

    {
      "type": "join",
      // 'with' pode ser fonte externa OU "self" (auto-join — ver nota de performance)
      "with": { "format": "delta", "path": "/ref/table", "options": {} },
      "on": "join_key",             // coluna, ["key1","key2"] ou SQL expr com l./r.
      "how": "inner|left|right|full|leftanti|leftsemi|...",
      // with_transformations: aplica transformações no df da direita antes do join
      "with_transformations": [
        { "type": "filter", "condition": "status = 1" },
        { "type": "select", "columns": ["id", "val"] }
      ]
    },

    {
      "type": "union",
      "with": { "format": "parquet", "path": "/data/extra" },
      "allow_missing_columns": false
    },

    // Materialização em qualquer ponto da lista
    { "type": "checkpoint" },             // localCheckpoint — QUEBRA a lineage
    { "type": "checkpoint", "eager": false },  // lazy variant
    { "type": "cache" }                   // df.cache() + count() — MANTÉM lineage
  ],

  "validations": {                      // opcional
    "on_failure": "fail|warn|skip",
    "rules": [
      { "type": "not_null", "columns": ["id"] },
      { "type": "unique", "columns": ["id"] },
      { "type": "range", "column": "age", "min": 0, "max": 150 },
      { "type": "regex", "column": "email", "pattern": ".*@.*" },
      { "type": "row_count", "min": 1, "max": 1000000 },
      { "type": "custom_sql",
        "query": "SELECT COUNT(*) = 0 FROM _validation_df WHERE ...",
        "error_message": "msg" }
    ]
  },

  // Saída única (shorthand):
  "output": {
    "format": "csv|parquet|delta|iceberg|view|kafka",
    "path": "string",
    "mode": "overwrite|append|merge",
    "partition_by": ["col"],
    "columns": ["col_a", "col_b"],    // opcional: projeta só essas colunas

    // ViewWriter — options:
    //   cache:      "true" (default) | "false"
    //   checkpoint: "true" — usa localCheckpoint (mais agressivo que cache;
    //                        equivalente ao "type":"checkpoint" antes do output)
    "options": { "checkpoint": "true" }

    // Delta/Iceberg merge — options:
    //   T = target (tabela destino), S = source (DataFrame sendo escrito)
    // "options": { "merge_keys": ["id"], "merge_condition": "T.deleted = FALSE" }
  },

  // OU múltiplas saídas (cada uma com suas próprias transformations + columns):
  "outputs": [
    {
      "format": "view",
      "path": "payload_view",
      "mode": "overwrite"
    },
    {
      "format": "kafka",
      "path": "${param_topico!raw}",
      "transformations": [
        { "type": "with_column", "name": "value", "expression": "to_json(payload)" },
        { "type": "with_column", "name": "headers", "expression": "array(...)" }
      ],
      "columns": ["value", "headers"],
      "options": {
        "bootstrap_servers": "${param_kafka_broker!raw}",
        "value_column":      "value"
      }
    },
    {
      "format": "delta",
      "path": "lastros.silver_parcelas",
      "mode": "append",
      "transformations": [
        { "type": "with_column", "name": "parcela", "expression": "explode(parcelas)" },
        { "type": "select",
          "columns": [
            "id_cessao",
            { "name": "identificador", "expression": "parcela.id" },
            { "name": "valor",         "expression": "parcela.valor" }
          ] }
      ]
    }
  ]
}
```

---

## Skip condicional (`skip_if`)

Toda transformação aceita o campo opcional `skip_if`: uma **expressão SQL**
avaliada via `spark.sql()` em uma linha sintética. Se o resultado for `FALSE`
ou `NULL`, a transformação é skipada.

Combinado com substituição `${param}`, permite condicionais elegantes:

```json
{ "type": "filter",
  "skip_if":   "${param_lista_cessoes} IS NULL",
  "condition": "array_contains(${param_lista_cessoes}, id_cessao)" }
```

- Quando `param_lista_cessoes = None` ou `[]`: `${param_lista_cessoes}` vira `NULL`,
  `skip_if` é `NULL IS NULL = TRUE` → skipa o filter.
- Quando `param_lista_cessoes = ["a", "b"]`: vira `array('a', 'b')`, `IS NULL`
  é `FALSE` → aplica o filter.

Útil também para **outras condições** além de checagem de null:
```json
{ "type": "join",
  "skip_if": "${param_pular_anti_join} = true",
  "with": {...}, "on": [...], "how": "leftanti" }
```

---

## Self-join vs Window function — quando usar

`join` com `"with": "self"` permite auto-join, mas tem **custo alto** (shuffle
em ambos os lados pela join key). Para a maioria dos casos de "criar coluna
agregada", prefira **window functions** com `with_column`:

```json
// ❌ Self-join — caro (2 shuffles)
{ "type": "join",
  "with": "self",
  "with_transformations": [
    { "type": "select", "columns": ["id_operacao", "tipo_ativo", "registradora"] },
    { "type": "drop_duplicates" },
    { "type": "group_by", "by": ["id_operacao"], "agg": [
      { "func": "expr", "expression": "count(distinct struct(tipo_ativo, registradora)) > 1",
        "alias": "multi_ativos" }
    ] }
  ],
  "on": "id_operacao",
  "how": "left" }

// ✅ Window function — 1 shuffle, mesma semântica, mais legível
{ "type": "with_column",
  "name": "multi_ativos",
  "expression": "size(collect_set(struct(tipo_ativo, registradora)) over (partition by id_operacao)) > 1" }
```

**Use self-join apenas** quando precisa de filtros cruzados que window não cobre
(ex: anti-join contra versão filtrada do mesmo df).

---

## Materialização: cache vs checkpoint

Duas formas de materializar e influenciar a lineage:

| Operação | Mantém lineage? | Quando usar |
|----------|-----------------|-------------|
| `cache` | ✅ Sim | DAG pequeno; df reusado várias vezes; rápido recovery |
| `checkpoint` (localCheckpoint) | ❌ Quebra | DAG está crescendo demais; Catalyst está lento; risco de OOM no driver; reuso entre confs |

**Como transformação** (em qualquer ponto do pipeline):
```json
"transformations": [
  { "type": "join", "with": {...}, ... },
  { "type": "checkpoint" },          // quebra lineage; o que vem depois parte do zero
  { "type": "filter", ... }
]
```

**Como option no ViewWriter** (na escrita final):
```json
"output": {
  "format": "view",
  "path": "minha_view",
  "options": { "checkpoint": "true" }   // ou { "cache": "true" } (default)
}
```

---

## Validações vs Transformações — quando usar cada um

| Necessidade | Use |
|-------------|-----|
| Remover linhas nulas antes de gravar | `filter` em transformations |
| Saber quantas linhas nulas chegaram (sem remover) | `not_null` em validations |
| Descartar duplicatas | `drop_duplicates` em transformations |
| Falhar o pipeline se existirem duplicatas | `unique` em validations |
| Cleansing de dados | transformations |
| Observabilidade de qualidade (métricas, relatórios) | validations |

Transformações **mudam** os dados. Validações **reportam** sobre eles sem
modificá-los. O `PipelineResult.validation_results` expõe `failed_count` e
mensagem por regra — útil para dashboards de qualidade.

---

## Múltiplos outputs com transformations + projeção de colunas

Cada output pode ter suas próprias `transformations` (aplicadas ao df antes
da escrita) e `columns` (projeção final). Essencial para escrever **destinos
com granularidades diferentes a partir do mesmo pipeline**:

```json
"outputs": [
  // Kafka — 1 msg por linha do struct payload
  { "format": "kafka", "path": "${param_topico!raw}",
    "transformations": [
      { "type": "with_columns", "columns": [
          { "name": "value",   "expression": "to_json(payload)" },
          { "name": "headers", "expression": "array(...)" }
      ] }
    ],
    "columns": ["value", "headers"] },

  // Delta controle — distinct id_cessao por par (id_cessao, id_operacao)
  { "format": "delta", "path": "lastros.controle",
    "mode": "append",
    "transformations": [
      { "type": "select", "columns": ["id_cessao", "id_operacao"] },
      { "type": "drop_duplicates" }
    ] },

  // Delta parcelas — explode do array, 1 linha por parcela (cardinalidade diferente!)
  { "format": "delta", "path": "lastros.parcelas",
    "mode": "append",
    "transformations": [
      { "type": "with_column", "name": "parcela", "expression": "explode(parcelas)" },
      { "type": "select",
        "columns": [
          "id_cessao",
          { "name": "identificador", "expression": "parcela.id" },
          { "name": "numero",        "expression": "parcela.numero" }
        ] }
    ] }
]
```

As transformações são aplicadas em um **clone do df principal**, sem afetar
outros outputs. A projeção `columns` é aplicada **depois** das `transformations`.

---

## Padrões de extensão

### Novo formato IO

```python
fw = SparkFramework()
fw.register_reader("delta", DeltaReader)
fw.register_writer("delta", DeltaWriter)
```

### Nova transformação

```python
class NormalizeText(BaseTransformation):
    def apply(self, df):
        col = self.config.params["column"]
        return df.withColumn(col, F.trim(F.lower(F.col(col))))

fw.register_transformation("normalize_text", NormalizeText)
# JSON: { "type": "normalize_text", "column": "email" }
```

### Novo validator

```python
class NoFutureDateValidator(BaseValidator):
    def validate(self, df):
        col = self.rule.params["column"]
        failed = df.filter(F.col(col) > F.current_date()).count()
        if failed:
            return ValidationResult("no_future_date", False, f"{failed} datas futuras", failed)
        return ValidationResult("no_future_date", True)

fw.register_validator("no_future_date", NoFutureDateValidator)
```

---

## Convenções do projeto

- `input` (singular) como fonte principal; múltiplas fontes via `join`/`union` em transformations.
- `output` (singular) ou `outputs` (lista) — ambos aceitos; um único objeto é normalizado para lista internamente.
- `columns` em output = projeção de colunas por destino; sem `columns` = escreve todas.
- `transformations` em output = transforms específicas do destino (não afetam outros outputs).
- `params` na conf = lista declarativa dos params esperados (warning quando faltam no `fw.run(..., params={...})`).
- Factories são class-level registries — extensões em `SparkFramework` afetam todas as execuções.
- `Pipeline` recebe engines injetáveis — útil para testes ou para injetar engines com transformações customizadas.
- `PipelineResult` nunca lança exceção — erros ficam em `result.error`.
- Logger sempre JSON estruturado (`utils/logger.py`).

---

## Arquivos de teste / exemplos disponíveis

| Caminho | Descrição |
|---------|-----------|
| `tests/run_ingestion.py` + `tests/ingestion_csv_to_parquet.json` | CSV de clientes → Parquet |
| `tests/run_join.py` + `tests/join_orders_products.json` | JOIN orders×products → 3 outputs |
| `tests/registro_vert/` | Projeto completo: orquestrador Python + confs JSON declarativas para registro de cessões CERC/B3 (4 ativos: CCB, Duplicata, CPR, NC). Inclui testes unitários com fixtures CSV. |

---

## Roadmap pendente

- Modo dry-run (valida config sem executar)
- Variáveis de runtime no JSON via env var (`${ENV_VAR}` separado dos params)
- Métricas de tempo de execução por etapa
- Suporte a perfis (dev/staging/prod) no mesmo JSON
