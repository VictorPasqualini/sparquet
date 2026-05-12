# SparkFramework — Contexto para Claude

Framework Python/PySpark orientado a configuração JSON para pipelines de dados.
Objetivo: produto reutilizável para qualquer caso de ingestão, transformação e qualidade de dados.

---

## Uso como biblioteca (ponto de entrada principal)

```python
from spark_framework import SparkFramework, Pipeline, PipelineResult, PipelineConfig

fw = SparkFramework(spark={"app_name": "MeuJob", "master": "yarn"})

r1 = fw.run("pipeline_clientes.json")
r2 = fw.run("pipeline_pedidos.json")
r3 = fw.run_from_dict({"name": "inline", "input": {...}, "output": {...}})

# Injeção de DataFrame externo, colunas de runtime e parâmetros de template
r4 = fw.run("pipeline.json", input_df=df_existente, columns={"dt_ref": "2025-01-01"})
r5 = fw.run("pipeline.json", params={"tipo_ativo": "NC", "ids": ["A1", "A2"], "aplicar_filtro": True})

fw.stop()
```

`SparkFramework` em `framework.py` gerencia o singleton de SparkSession e compartilha os engines de transformação/validação entre todas as execuções.

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
from spark_framework import Pipeline

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
| `SparkFramework` | `framework.py` | Entry point como lib |
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
| `KafkaWriter` | `io/kafka.py` | Publicação batch em tópico Kafka |
| `ViewReader/Writer` | `io/view.py` | Spark temp views (auto-cache) |
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

Qualquer transformação aceita `"skip_if_false": "{chave}"`. Após substituição, se o valor resultante for string vazia a transformação é ignorada; qualquer outro valor a executa normalmente.

```jsonc
// Pula o join inteiro se params["aplicar_join"] == False
{ "type": "join", "skip_if_false": "{aplicar_join}", "with": {...}, "on": "id" }

// Filtro só aplicado se params["registradora"] != ""
{ "type": "filter", "skip_if_false": "{registradora}", "condition": "registradora = '{registradora}'" }
```

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
    { "type": "with_column", "name": "col", "expression": "SQL expr" },  // add_column também aceito
    { "type": "drop_duplicates", "columns": ["id"] },
    { "type": "sql", "query": "SELECT ...", "view_name": "_df" },
    { "type": "fill_na", "value": 0, "columns": ["col"] },
    { "type": "sort", "columns": ["col"], "ascending": true },
    // $include: expande inline o conteúdo de um arquivo JSON (caminho relativo ao pipeline)
    { "$include": "shared/filtro_tipo_ativo.json" },
    {
      "type": "debug",                          // não modifica o df — apenas inspeciona
      "label": "após join contratos",           // opcional, aparece no separador
      // show usa display(df) no Databricks; faz df.show() localmente
      "actions": ["count", "print_schema", "show", "explain", "columns", "dtypes"],
      "show_rows": 20,                          // linhas para show (default: 20)
      "truncate": true,                         // truncar show (default: true)
      "vertical": false,                        // layout vertical no show (default: false)
      "extended": false                         // plano estendido no explain (default: false)
    },
    {
      "type": "group_by",
      "by": ["col1", "col2"],
      "agg": [
        { "func": "min|max|sum|avg|count|first|last|count_distinct|collect_list|collect_set",
          "column": "col",   // opcional para count
          "alias": "nome"    // opcional
        }
      ]
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

  // OU múltiplas saídas (cada uma pode ter "columns" diferente):
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
| `kafka` | não | sim | Publicação batch; requer conector Kafka no classpath |

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

    def summary() -> str  # linha de status legível
```

`PipelineResult` nunca lança exceção — erros ficam em `result.error`.

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

---

## Padrões de extensão

### Novo formato IO

```python
fw = SparkFramework()
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
- Factories são class-level registries — extensões em `SparkFramework` afetam todas as execuções.
- `Pipeline` recebe engines injetáveis — útil para testes ou para injetar engines com transformações customizadas.
- `PipelineResult` nunca lança exceção — erros ficam em `result.error`.
- Logger sempre JSON estruturado (`utils/logger.py`).
- `SparkContextManager` detecta o ambiente automaticamente (Databricks reusa sessão ativa; outros criam via builder).

---

## Arquivos de teste disponíveis

| Script | Config | Descrição |
|--------|--------|-----------|
| `tests/run_ingestion.py` | `tests/ingestion_csv_to_parquet.json` | CSV de clientes → Parquet |
| `tests/run_join.py` | `tests/join_orders_products.json` | JOIN orders×products → 3 outputs |
| `tests/run_databricks.py` | `tests/databricks/` | Testes específicos para Databricks |
| `tests/run_cessoes.py` | — | Pipeline de cessões |

Dados em `tests/csv/`: `customers.csv`, `orders.csv`, `products.csv`.

---

## Roadmap pendente

- Modo dry-run (valida config sem executar)
- Testes unitários com dados mock
- Métricas de tempo de execução por etapa
- Suporte a perfis (dev/staging/prod) no mesmo JSON
