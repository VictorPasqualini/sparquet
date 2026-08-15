# Sparquet — Contexto para Claude

Framework Python/PySpark orientado a configuração JSON para pipelines de dados.
Objetivo: produto reutilizável para qualquer caso de ingestão, transformação e qualidade de dados.

---

## Uso como biblioteca (ponto de entrada principal)

```python
from sparquet import Sparquet, Pipeline, PipelineResult, PipelineConfig

fw = Sparquet(spark={"app_name": "MeuJob", "master": "yarn"})

r1 = fw.run("pipeline_clientes.json")
r2 = fw.run("pipeline_pedidos.json")
r3 = fw.run_from_dict({"name": "inline", "input": {...}, "output": {...}})

# Injeção de DataFrame externo, colunas de runtime e parâmetros de template
r4 = fw.run("pipeline.json", input_df=df_existente, columns={"dt_ref": "2025-01-01"})
r5 = fw.run("pipeline.json", params={"tipo_ativo": "NC", "ids": ["A1", "A2"], "aplicar_filtro": True})

fw.stop()
```

`Sparquet` em `framework.py` gerencia o singleton de SparkSession e compartilha os engines de transformação/validação entre todas as execuções.

### API pública completa

```python
# Execução
fw.run(config_path: str, input_df=None, columns=None, params=None) → PipelineResult
fw.run_from_dict(config: dict, input_df=None, columns=None, params=None) → PipelineResult

# Extensão
fw.register_reader(format_name: str, reader_cls)
fw.register_writer(format_name: str, writer_cls)
fw.register_transformation(name: str, transformation_cls)
fw.register_validator(name: str, validator_cls)

# Ciclo de vida
fw.stop()
```

**`input_df`**: substitui a leitura do `input` — o pipeline começa a partir do DataFrame fornecido.  
**`columns`**: injeta colunas literais (`F.lit(value)`) antes das transformações, sem alterar o JSON.  
**`params`**: substitui placeholders `{chave}` no texto bruto do JSON antes do parse. Listas viram SQL IN (`'a', 'b'`); `True` → `"true"`; `False`/lista vazia → `""` (falsy, dispara `skip_if_false`).  
**`result.output_df`**: DataFrame após todas as transformações, disponível no resultado.

### Uso direto de Pipeline

```python
from sparquet import Pipeline

p = Pipeline.from_file("meu_pipeline.json")
result = p.run()

p2 = Pipeline.from_dict({...})
result2 = p2.run()
```

---

## Arquitetura

```
JSON/dict → apply_template(params) → resolve_includes → PipelineConfig → Pipeline.run()
                                                                               │
                                                                               ├─► ReaderFactory(input)  → DataFrame
                                                                               ├─► injeção de columns (F.lit)
                                                                               ├─► TransformationEngine   → DataFrame
                                                                               ├─► ValidationEngine       → ValidationResult[]
                                                                               └─► para cada output em outputs:
                                                                                       _project_columns(df, output)
                                                                                       WriterFactory(output).write(df_projetado)
```

### Módulos

| Módulo | Arquivo | Responsabilidade |
|--------|---------|-----------------|
| `Sparquet` | `framework.py` | Entry point como lib |
| `cli` | `cli.py` | Entry point de linha de comando |
| `PipelineConfig` | `core/config.py` | Deserializa JSON em dataclasses |
| `SparkContextManager` | `core/context.py` | Singleton do SparkSession; detecta Databricks/EMR/Dataproc/Synapse/local |
| `Pipeline` | `core/pipeline.py` | Orquestrador; `run() → PipelineResult` |
| `BaseReader/Writer` | `io/base.py` | Contratos abstratos para IO |
| `ParquetReader/Writer` | `io/parquet.py` | Implementação Parquet |
| `IcebergReader/Writer` | `io/iceberg.py` | Implementação Iceberg (com MERGE INTO) |
| `DeltaReader/Writer` | `io/delta.py` | Implementação Delta Lake (com MERGE, time travel) |
| `CsvReader/Writer` | `io/csv.py` | Implementação CSV com defaults de header/encoding |
| `TxtReader/Writer` | `io/txt.py` | Arquivos texto plano (coluna `value`) |
| `KafkaReader/Writer` | `io/kafka.py` | Leitura e publicação batch em tópico Kafka (MSK via SASL/IAM) |
| `ViewReader/Writer` | `io/view.py` | Spark temp views (auto-cache) |
| JDBC (`Postgres/MySql/MariaDb/SqlServer/Oracle`) | `io/jdbc.py` | Base JDBC + dialetos (driver/URL por banco) |
| `BigQueryReader/Writer` | `io/bigquery.py` | Google BigQuery (spark-bigquery-connector) |
| `SnowflakeReader/Writer` | `io/snowflake.py` | Snowflake (spark-snowflake) |
| `RedshiftReader/Writer` | `io/redshift.py` | Amazon Redshift (staging em S3) |
| `MongoReader/Writer` | `io/mongodb.py` | MongoDB e Amazon DocumentDB (mongo-spark) |
| `DynamoDbReader/Writer` | `io/dynamodb.py` | Amazon DynamoDB (spark-dynamodb) |
| `CassandraReader/Writer` | `io/cassandra.py` | Cassandra/ScyllaDB (spark-cassandra-connector) |
| `ElasticsearchReader/Writer` | `io/elasticsearch.py` | Elasticsearch/OpenSearch (es-hadoop) |
| `ReaderFactory/WriterFactory` | `io/factory.py` | Registry de formatos; extensível |
| `TransformationEngine` | `transform/engine.py` | Aplica transformações em sequência |
| Transformações nativas | `transform/builtin.py` | Ver lista abaixo |
| `ValidationEngine` | `validation/engine.py` | Roda validators; respeita `on_failure` |
| Validators nativos | `validation/builtin.py` | Ver lista abaixo |
| `apply_template` | `utils/template.py` | Substitui `{chave}` no JSON bruto antes do parse; formata listas/booleanos para SQL |
| `resolve_includes` | `utils/includes.py` | Expande diretivas `$include` em transformations |

---

## Template de variáveis no JSON (`params`)

Qualquer valor `{chave}` no JSON é substituído antes do parse quando `params` é passado para `fw.run()`.

```python
fw.run("pipeline.json", params={
    "tipo_ativo":   "NC",
    "registradora": "CERC",
    "ids_cessao":   ["C1", "C2", "C3"],   # lista → "'C1', 'C2', 'C3'"
    "aplicar_join": True,                  # bool True  → "true"
    "filtro_extra": False,                 # bool False → ""  (falsy)
})
```

Regras de formatação:

| Tipo Python | Resultado no JSON | Uso típico |
|-------------|-------------------|-----------|
| `str` / `int` / `float` | `str(value)` | caminho, nome, número |
| `bool True` | `"true"` | mantém a transformação (`skip_if_false`) |
| `bool False` | `""` | pula a transformação (`skip_if_false`) |
| `list` de strings | `"'a', 'b', 'c'"` | cláusula `IN (...)` no SQL |
| `list` de números | `"1, 2, 3"` | cláusula `IN (...)` no SQL |
| `list` vazia | `""` | falsy — pula transformação ou filtro vazio |

Chaves sem correspondência em `params` ficam literais no JSON — não causam erro.

### `skip_if_false`

Qualquer transformação aceita `"skip_if_false"`. Após a substituição de template, o engine decide em 3 casos:

| Valor pós-substituição | Comportamento |
|------------------------|---------------|
| `""` (string vazia) | **pula** (ex: bool `False`, lista vazia, param ausente) |
| expressão que avalia como **booleano** (ex: `'REGISTRO' in ('EMISSAO', ...)`) | **pula se `false`** |
| qualquer outro valor não-vazio (ex: `"CERC"`, `"'a','b'"`) | executa |

```jsonc
// Pula o join inteiro se params["aplicar_join"] == False  (valor vira "")
{ "type": "join", "skip_if_false": "{aplicar_join}", "with": {...}, "on": "id" }

// Filtro só aplicado se params["registradora"] != ""
{ "type": "filter", "skip_if_false": "{registradora}", "condition": "registradora = '{registradora}'" }

// Branch por valor (expressão booleana): roda só nos fluxos de emissão
{ "type": "struct", "skip_if_false": "'{fluxo_operacao}' in ('EMISSAO', 'EMISSAO_E_REGISTRO')", "column": "payload", "fields": {...} }
```

A expressão é avaliada sobre **literais** (já substituídos pelo template) — não enxerga colunas do df; serve para branchear por parâmetro.

---

## Variáveis de runtime (`{{var}}`)

Diferente de `{param}` (substituído **antes** do parse, com valores conhecidos na
chamada), `{{var}}` é resolvido **durante a execução**, com valores computados pelo
próprio pipeline. Serve para empurrar um filtro literal (`IN (...)`) nas leituras de
tabelas grandes carregadas depois — o equivalente declarativo ao `df.collect()` +
`col.isin(lista)` de jobs Spark (predicate pushdown / data skipping no Delta).

```jsonc
// 1) materializa o conjunto e coleta a chave numa variável de runtime
{ "type": "checkpoint" },
{ "type": "collect", "column": "id_cessao", "as": "cessoes_pendentes" },

// 2) usa {{cessoes_pendentes}} para filtrar a leitura de tabelas seguintes
{ "type": "join",
  "with": { "format": "delta", "path": "lastros.bronze_remessa" },
  "with_transformations": [
    { "type": "filter", "condition": "id_cessao IN ({{cessoes_pendentes}})" },  // pushdown
    { "type": "select", "columns": ["id_cessao", "numero_contrato", "tipo_contrato"] }
  ],
  "on": ["id_cessao", "numero_contrato"], "how": "left" }
```

Como funciona:

- **`collect`** roda `df.select(column).distinct().collect()` e guarda a lista em um
  *store* de runtime sob a chave `as`. Não altera o df, mas dispara uma action Spark
  (traz dados ao driver) — por isso use **após um `checkpoint`** (o df já materializado
  torna o collect barato e evita recomputar a linhagem).
- **`{{var}}`** é resolvido no momento de aplicar cada transformação. A formatação para
  SQL: lista de strings → `'a', 'b'` (aspas escapadas); lista de números → `1, 2`;
  lista vazia → `NULL` (`IN (NULL)` não casa nada — comportamento correto quando o
  conjunto apto está vazio); string → `'valor'`.
- O store é **compartilhado com os `with_transformations` aninhados** dos joins, então
  variáveis coletadas no escopo externo são visíveis nos reads de dentro.
- O store é **zerado a cada `fw.run(...)`** (o engine é reusado no `Sparquet`),
  evitando vazamento de variáveis entre execuções.
- `{{var}}` cuja variável ainda não foi coletada fica **literal** (não dá erro) — é
  resolvido quando/se a variável passar a existir num escopo aninhado.

Ordem de resolução: `{param}` (template, pré-parse) → parse do JSON → `{{var}}` (runtime,
durante as transformações).

---

## Reutilização de transformações com `$include`

Um item `{ "$include": "caminho/arquivo.json" }` na lista de `transformations` é substituído inline pelo conteúdo do arquivo referenciado. O caminho é relativo ao diretório do JSON principal.

O arquivo incluído pode ser um único objeto de transformação ou uma lista. Template `params` é aplicado antes do parse, então variáveis como `{tipo_ativo}` funcionam normalmente em arquivos compartilhados.

```jsonc
// pipeline_nc.json
{
  "transformations": [
    { "$include": "shared/filtro_tipo_ativo.json" },   // expande inline
    { "type": "with_column", "name": "payload", "expression": "..." }
  ]
}

// shared/filtro_tipo_ativo.json — objeto único ou lista
[
  { "type": "filter", "condition": "tipo_ativo = '{tipo_ativo}' AND registradora = '{registradora}'" }
]
```

Inclusions aninhadas (`$include` dentro de arquivo já incluído) não são suportadas.

---

## Schema JSON do pipeline

```jsonc
{
  "name": "string",                    // obrigatório
  "description": "string",             // opcional
  "spark": {                            // opcional
    "app_name": "string",
    "master": "string",
    "configs": { "spark.sql.*": "valor" }
  },

  "input": {                            // obrigatório (ignorado quando input_df é injetado)
    "format": "csv|parquet|iceberg|delta|txt|view",
    "path": "string",
    // Delta: suporta time travel via options
    "options": {
      "versionAsOf": "5",
      "timestampAsOf": "2025-05-10T10:00:00Z"
    }
  },

  "transformations": [                  // opcional — aplicadas em ordem
    { "type": "filter", "condition": "SQL expr" },
    // select: nomes simples ou expressões SQL completas com alias
    { "type": "select", "columns": ["a", "b", "to_json(payload) AS value", "CAST(1 AS INT) AS status"] },
    { "type": "drop", "columns": ["x"] },
    { "type": "rename", "mappings": {"old": "new"} },
    { "type": "cast", "columns": {"col": "type"} },
    // with_column: aceita "column" (ou "name", compat) + "expression", OU "columns"
    // (mapa nome→expr) para criar várias colunas num bloco, em ordem. add_column = alias.
    { "type": "with_column", "column": "col", "expression": "SQL expr" },
    { "type": "with_column", "columns": { "c1": "expr1", "c2": "expr2 usando c1" } },
    // struct: monta coluna struct aninhada a partir de um mapa campo→expressão
    // (valor string = expr SQL; valor mapa = struct aninhado). Mais legível que named_struct.
    // Chaves em dot-path ("data.nc.issuerName") auto-aninham → payload como tabela
    // plana (ótimo p/ ler/diff). Pode misturar dot-path e mapa aninhado.
    { "type": "struct", "column": "payload",
      "fields": { "id_externo": "id_vert", "data.nc.issuerName": "nome_sacado",
                  "data.nc.paymentMethod.indexCode": "lpad(cod, 4, '0')" } },
    { "type": "drop_duplicates", "columns": ["id"] },
    { "type": "distinct" },                                  // remove duplicatas usando todas as colunas
    // checkpoint: materializa e trunca o plano lógico (quebra a linhagem após joins pesados)
    // method: "localCheckpoint" (default) ou "checkpoint" (confiável); eager: true (default)
    // method inválido → transformação ignorada + warning no fim do pipeline
    { "type": "checkpoint", "method": "localCheckpoint", "eager": true },
    // collect: coleta valores distintos de uma coluna numa variável de runtime {{as}}
    // (não altera o df; dispara collect no driver — use após checkpoint). Ver seção
    // "Variáveis de runtime ({{var}})" abaixo.
    { "type": "collect", "column": "id_cessao", "as": "cessoes_pendentes" },
    // stop_if_empty: encerra o pipeline graciosamente se o df estiver vazio — não
    // roda as transformações seguintes nem escreve nas saídas. result.skipped = True,
    // success = True, rows_written = 0. Posicione logo após o filtro que define o
    // conjunto a processar (antes de joins/payloads pesados).
    { "type": "stop_if_empty", "message": "Sem dados a processar" },
    { "type": "sql", "query": "SELECT ...", "view_name": "_df" },
    { "type": "fill_na", "value": 0, "columns": ["col"] },
    { "type": "sort", "columns": ["col"], "ascending": true },
    // $include: expande inline o conteúdo de um arquivo JSON (caminho relativo ao pipeline)
    { "$include": "shared/filtro_tipo_ativo.json" },
    {
      "type": "debug",                          // não modifica o df — apenas inspeciona
      "label": "após join contratos",           // opcional, aparece no separador
      // show usa df.show() — display() do Databricks só funciona chamado diretamente na célula
      "actions": ["count", "print_schema", "show", "explain", "columns", "dtypes"],
      // transformations: opcional — aplicadas a um df descartável SÓ para esta
      // inspeção (filter/select/group_by/etc.); NÃO alteram o df do pipeline
      // (o debug sempre retorna o df original). Útil p/ focar a visualização.
      "transformations": [ { "type": "filter", "condition": "id_cessao = 'C1'" } ],
      "show_rows": 20,                          // linhas para show (default: 20)
      "truncate": true,                         // truncar show (default: true)
      "vertical": false,                        // layout vertical no show (default: false)
      "extended": false                         // plano estendido no explain (default: false)
    },
    {
      "type": "group_by",
      "by": ["col1", "col2"],
      // agg: lista de expressões SQL de agregação completas (strings) — qualquer
      // função/sintaxe SQL do Spark, com alias e expressões compostas.
      "agg": [
        "sum(valor) as total",
        "first(tipo_contrato) as tipo_contrato",
        "count(distinct struct(tipo_ativo, registradora)) > 1 as multi_ativos"
      ],
      // pivot (opcional): "coluna" ou { "column": "coluna", "values": [...] }
      "pivot": { "column": "mes", "values": ["jan", "fev"] }
    },
    {
      "type": "join",
      "with": { "format": "parquet", "path": "/ref/table", "options": {} },
      "on": "join_key",             // coluna, ["key1","key2"] ou SQL expr com l./r.
      "how": "inner|left|right|full|cross|leftsemi|leftanti|...",
      "skip_if_false": "{meu_param}",   // opcional: pula o join se o valor pós-substituição for ""
      // with_transformations: aplica transformações no df da direita antes do join
      // df esquerdo (principal) é alias 'l'; df direito é alias 'r'.
      "with_transformations": [
        { "type": "filter", "condition": "status = 1" },
        { "type": "sql", "view_name": "v", "query": "SELECT id, MIN(val) AS val FROM v GROUP BY id" },
        { "type": "select", "columns": ["id", "val"] }
      ]
    },
    {
      "type": "union",
      "with": { "format": "parquet", "path": "/data/extra" },
      "allow_missing_columns": false
    }
  ],

  "validations": {                      // opcional
    "on_failure": "fail|warn|skip",
    // report: opcional — grava 1 linha por regra (pipeline, rule_type, passed,
    // failed_count, message, validated_at) para análise de qualidade. Aceita
    // qualquer formato de saída. Gerado nos modos que não abortam (warn/skip)
    // ou quando todas as regras passam (em "fail" com violação, aborta antes).
    "report": { "format": "csv", "path": "/dq/validation_report", "mode": "overwrite" },
    "rules": [
      { "type": "not_null", "columns": ["id"] },
      { "type": "unique", "columns": ["id"] },
      { "type": "range", "column": "age", "min": 0, "max": 150 },
      { "type": "regex", "column": "email", "pattern": ".*@.*" },
      { "type": "row_count", "min": 1, "max": 1000000 },
      {
        "type": "custom_sql",
        "query": "SELECT COUNT(*) = 0 FROM _validation_df WHERE ...",
        "error_message": "msg"
      }
    ]
  },

  // Saída única (shorthand):
  "output": {
    "format": "csv|parquet|iceberg|delta|txt|kafka|view",
    "path": "string",
    "mode": "overwrite|append|merge",
    "partition_by": ["col"],
    "columns": ["col_a", "col_b"],    // opcional: projeta só essas colunas
    // transformations: opcional — transformações próprias deste destino aplicadas
    // sobre o df transformado, antes de columns/escrita, sem afetar as demais saídas.
    // Permite gravar formas diferentes do mesmo df (explode, to_json, join, etc.).
    "transformations": [ { "type": "with_column", "column": "value", "expression": "to_json(payload)" } ],
    // merge (Delta/Iceberg): T = target (tabela destino), S = source (DataFrame)
    "options": {
      "merge_keys": ["id"],
      "merge_condition": "T.deleted = FALSE",   // condição SQL extra — usa T./S.
      // Kafka:
      "bootstrap_servers": "broker:9092",
      "topic": "meu-topico",
      "value_column": "payload",   // default: "value"
      "key_column": "header"       // default: "key"
    }
  },

  // OU múltiplas saídas (cada uma pode ter "columns" e "transformations" diferentes):
  "outputs": [
    {
      "format": "parquet",
      "path": "/data/full",
      "mode": "overwrite"
    },
    {
      "format": "parquet",
      "path": "/data/analytics",
      "columns": ["id", "name", "total"]
    },
    {
      "format": "csv",
      "path": "/data/export",
      "columns": ["id", "total"]
    }
  ]
}
```

---

## Formatos IO suportados

| Formato | Leitura | Escrita | Notas |
|---------|---------|---------|-------|
| `parquet` | sim | sim | Parquet nativo Spark |
| `csv` | sim | sim | Defaults: `header=true`, `inferSchema=true` |
| `delta` | sim | sim | Unity Catalog ou path; time travel; MERGE |
| `iceberg` | sim | sim | MERGE INTO nativo |
| `txt` | sim | sim | Texto plano; coluna `value` |
| `view` | sim | sim | Spark temp views; auto-cache |
| `kafka` | sim | sim | Batch read/write; MSK via SASL/IAM; requer conector Kafka no classpath |
| `postgresql` | sim | sim | JDBC; `path`=tabela; `url` ou `host`+`database` em options |
| `mysql` | sim | sim | JDBC |
| `mariadb` | sim | sim | JDBC |
| `sqlserver` | sim | sim | JDBC |
| `oracle` | sim | sim | JDBC (service name na `database`) |
| `bigquery` | sim | sim | `path`=`projeto.dataset.tabela`; write via GCS/direct |
| `snowflake` | sim | sim | Opções `sfXxx`; `path`=tabela |
| `redshift` | sim | sim | Requer `url` + `tempdir` (S3) |
| `mongodb` | sim | sim | `path`=coleção; `connection.uri`+`database` em options |
| `documentdb` | sim | sim | Amazon DocumentDB (mesmo conector Mongo; URI com TLS) |
| `dynamodb` | sim | sim | `path`=tabela; write é upsert por chave (append) |
| `cassandra` | sim | sim | `path`=`keyspace.tabela`; append (upsert) |
| `elasticsearch` | sim | sim | `path`=índice; Elasticsearch/OpenSearch |

> Todos os conectores externos (JDBC, BigQuery, Snowflake, Redshift, Mongo,
> DynamoDB, Cassandra, Elasticsearch, Kafka) exigem o **JAR do driver/conector no
> classpath** do Spark (`spark.jars` / `spark.jars.packages`). O framework só monta a
> chamada `.format(...).options(...)`; não empacota drivers. Cada `io/<fmt>.py`
> documenta as opções de conexão; o catálogo do Studio (`formats.databases.ts`) as
> descreve para a UI e a IA.

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

Transformações **mudam** os dados. Validações **reportam** sobre eles sem modificá-los.  
O `PipelineResult.validation_results` expõe `failed_count` e mensagem por regra — útil para dashboards de qualidade.

---

## PipelineResult

```python
@dataclass
class PipelineResult:
    pipeline_name: str
    success: bool
    rows_read: int = 0
    rows_written: int = 0
    validation_results: List[ValidationResult] = []
    error: Optional[str] = None
    output_df: Optional[DataFrame] = None  # preenchido quando input_df é injetado
    skipped: bool = False                  # True quando encerrado por stop_if_empty (sem dados)

    def summary() -> str  # linha de status legível
```

`PipelineResult` nunca lança exceção — erros ficam em `result.error`.
`skipped=True` indica encerramento gracioso por `stop_if_empty` (success=True, rows_written=0).

---

## Múltiplos outputs com projeção de colunas

O campo `columns` em cada output permite escrever subconjuntos diferentes para destinos diferentes a partir do mesmo DataFrame transformado:

```json
"outputs": [
  { "format": "parquet", "path": "/dw/full" },
  { "format": "parquet", "path": "/bi/analytics", "columns": ["id", "revenue", "region"] },
  { "format": "csv",     "path": "/export/report",  "columns": ["id", "revenue"] }
]
```

A projeção é feita em `Pipeline._project_columns()` — o DataFrame principal não é alterado entre outputs.

### Transformações por output

Quando os destinos precisam de **formas diferentes** (não só subconjuntos de colunas) do mesmo df, use `transformations` por output. São aplicadas sobre o df transformado, antes de `columns`/escrita, sem afetar as demais saídas. Aceitam todos os tipos do engine (incl. `join`, `explode` via `with_column`, `to_json`, `{{var}}` de runtime).

```json
"outputs": [
  { "format": "kafka", "path": "topico",
    "transformations": [ { "type": "with_column", "column": "value", "expression": "to_json(payload)" } ],
    "options": { "bootstrap_servers": "broker:9092", "value_column": "value", "key_column": null } },

  { "format": "delta", "path": "schema.parcelas", "mode": "append",
    "transformations": [
      { "type": "join", "with": { "format": "delta", "path": "schema.silver_parcela" },
        "on": ["id_cessao", "numero_contrato"], "how": "inner" },
      { "type": "with_column", "column": "data_baixa", "expression": "cast(null as date)" }
    ] }
]
```

Cada output parte do **mesmo** df principal (materialize-o com `checkpoint` na última transformação para não recomputar a linhagem a cada destino). Aplicado em `Pipeline._write_outputs()`.

---

## Padrões de extensão

### Novo formato IO

```python
fw = Sparquet()
fw.register_reader("meu_formato", MeuReader)   # class MeuReader(BaseReader)
fw.register_writer("meu_formato", MeuWriter)   # class MeuWriter(BaseWriter)
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
- Factories são class-level registries — extensões em `Sparquet` afetam todas as execuções.
- `Pipeline` recebe engines injetáveis — útil para testes ou para injetar engines com transformações customizadas.
- `PipelineResult` nunca lança exceção — erros ficam em `result.error`.
- Logger sempre JSON estruturado (`utils/logger.py`).
- `SparkContextManager` detecta o ambiente automaticamente (Databricks reusa sessão ativa; outros criam via builder).

---

## Exemplos e caso de uso

| Caminho | Descrição |
|---------|-----------|
| `examples/` | Confs ilustrativas das capacidades (ingestão+validações, pushdown runtime, struct/multi-saída, merge). Ver `examples/README.md`. |
| `tests/case-of-success/` | Caso real completo: migração dos jobs de registro de lastros (base de cessões → registro por fluxo → commit). `job_registro.py` orquestra as confs. |
| `tests/case-of-success/old/` | Material-fonte: os jobs Spark originais (`.py`) que foram migrados. |

Padrão **staging → commit** do caso de uso: cada conf de registro grava no staging
genérico `view_registro_staging`; a `conf_commit_registro.json` verifica (via
`validations`) e grava os 3 destinos. Ver `tests/case-of-success/ROADMAP_CASE_OF_SUCCESS.md`.

---

## Roadmap

- Caso de uso (migração de registro): `tests/case-of-success/ROADMAP_CASE_OF_SUCCESS.md`
- Melhorias e pendências de desenvolvimento do framework (conectores, data
  quality/governança, dry-run, métricas, perfis): `BACKLOG.md`
- Deploy como biblioteca no PyPI: `docs/DEPLOY_PYPI.md`

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).

---

## Sparquet Studio (frontend)

Editor visual para os pipelines JSON, em `sparquet-studio/` (React 18 + TypeScript +
Vite + Tailwind + React Flow). É o ponto de entrada de uso do framework: o usuário
desenha o pipeline no canvas, o Studio compila para o mesmo JSON que o
`Sparquet` executa.

```bash
cd sparquet-studio && npm install && npm run dev     # http://localhost:5273
```

| Camada | Caminho | Responsabilidade |
|--------|---------|------------------|
| Catálogo | `src/catalog/` | Descreve a linguagem (transformações, formatos, validators) com campos, defaults, docs e gotchas. **Fonte única**: alimenta paleta, formulários, linter e o system prompt da IA. |
| Compilador | `src/lib/compiler/` | `compileGraph()` (grafo → JSON) e `pipelineToGraph()` (JSON → grafo) são inversos, com testes de round-trip sobre os confs de `examples/`. |
| Linter | `src/lib/validation/lint.ts` | Regras client-side (merge sem `merge_keys`, `{{var}}` sem `collect`, `{param}` não declarado, etc). |
| IA | `src/lib/ai/` | Cliente streaming multi-provider (Anthropic/OpenAI/Google/compatível), prompt gerado do catálogo, parser de proposta. |
| Runner | `sparquet-studio/server/` | Serviço FastAPI opcional que executa o pipeline com o `Sparquet` real e devolve contadores, validações, preview e logs. |
| Estado | `src/store/` | zustand: editor (grafo, histórico, autosave), library (projetos/workflows), settings. |

**Regra de ouro ao evoluir o framework**: toda transformação, formato ou validator
novo precisa de uma entrada correspondente no catálogo (`src/catalog/`), senão o
Studio não a oferece na paleta nem a descreve para a IA. Tipos desconhecidos ainda
são importados e preservados no round-trip, mas sem formulário dedicado.

Verificação: `npm run typecheck`, `npm run test` (vitest), `npm run lint` e
`npm run smoke` (end-to-end em Chrome real).
