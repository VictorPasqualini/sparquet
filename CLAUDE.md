# SparkFramework — Contexto para Claude

Framework Python/PySpark orientado a configuração JSON para pipelines de dados.
Objetivo: produto reutilizável para qualquer caso de ingestão, transformação e qualidade de dados.

---

## Uso como biblioteca (ponto de entrada principal)

```python
from spark_framework import SparkFramework

fw = SparkFramework(spark={"app_name": "MeuJob", "master": "yarn"})

r1 = fw.run("pipeline_clientes.json")
r2 = fw.run("pipeline_pedidos.json")
r3 = fw.run_from_dict({"name": "inline", "input": {...}, "output": {...}})

fw.stop()
```

`SparkFramework` em `framework.py` gerencia o singleton de SparkSession e compartilha os engines de transformação/validação entre todas as execuções.

---

## Arquitetura

```
JSON/dict → PipelineConfig → Pipeline.run()
                                 │
                                 ├─► ReaderFactory(input)  → DataFrame
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
| `PipelineConfig` | `core/config.py` | Deserializa JSON em dataclasses |
| `SparkContextManager` | `core/context.py` | Singleton do SparkSession |
| `Pipeline` | `core/pipeline.py` | Orquestrador; `run() → PipelineResult` |
| `BaseReader/Writer` | `io/base.py` | Contratos abstratos para IO |
| `ParquetReader/Writer` | `io/parquet.py` | Implementação Parquet |
| `IcebergReader/Writer` | `io/iceberg.py` | Implementação Iceberg (com MERGE INTO) |
| `CsvReader/Writer` | `io/csv.py` | Implementação CSV com defaults de header/encoding |
| `ReaderFactory/WriterFactory` | `io/factory.py` | Registry de formatos; extensível |
| `TransformationEngine` | `transform/engine.py` | Aplica transformações em sequência |
| Transformações nativas | `transform/builtin.py` | Ver lista abaixo |
| `ValidationEngine` | `validation/engine.py` | Roda validators; respeita `on_failure` |
| Validators nativos | `validation/builtin.py` | Ver lista abaixo |

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

  "input": {                            // obrigatório — fonte principal
    "format": "csv|parquet|iceberg",
    "path": "string",
    "options": {}
  },

  "transformations": [                  // opcional — aplicadas em ordem
    { "type": "filter", "condition": "SQL expr" },
    { "type": "select", "columns": ["a", "b"] },
    { "type": "drop", "columns": ["x"] },
    { "type": "rename", "mappings": {"old": "new"} },
    { "type": "cast", "columns": {"col": "type"} },
    { "type": "add_column", "name": "col", "expression": "SQL expr" },
    { "type": "drop_duplicates", "columns": ["id"] },
    { "type": "sql", "query": "SELECT ...", "view_name": "_df" },
    { "type": "fill_na", "value": 0, "columns": ["col"] },
    { "type": "sort", "columns": ["col"], "ascending": true },
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
      "how": "inner|left|right|full|leftanti|leftsemi|...",
      // with_transformations: aplica transformações no df da direita antes do join
      // Suporta agregações, filtros, selects — qualquer tipo builtin.
      // O df esquerdo (principal) é alias 'l'; o da direita é alias 'r'.
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
    "format": "csv|parquet|iceberg",
    "path": "string",
    "mode": "overwrite|append|merge",
    "partition_by": ["col"],
    "columns": ["col_a", "col_b"],    // opcional: projeta só essas colunas
    // merge: T = target (tabela destino), S = source (DataFrame sendo escrito)
    "options": {
      "merge_keys": ["id"],
      "merge_condition": "T.deleted = FALSE"   // condição SQL extra — usa T./S.
    }
  },

  // OU múltiplas saídas (cada uma pode ter "columns" diferente):
  "outputs": [
    {
      "format": "parquet",
      "path": "/data/full",
      "mode": "overwrite"
                                      // sem "columns" = escreve tudo
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
fw.register_reader("delta", DeltaReader)   # class DeltaReader(BaseReader)
fw.register_writer("delta", DeltaWriter)   # class DeltaWriter(BaseWriter)
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

---

## Arquivos de teste disponíveis

| Script | Config | Descrição |
|--------|--------|-----------|
| `tests/run_ingestion.py` | `tests/ingestion_csv_to_parquet.json` | CSV de clientes → Parquet |
| `tests/run_join.py` | `tests/join_orders_products.json` | JOIN orders×products → 3 outputs |

Dados em `tests/csv/`: `customers.csv`, `orders.csv`, `products.csv`.

---

## Roadmap pendente

- Delta Lake como formato nativo
- Modo dry-run (valida config sem executar)
- Testes unitários com dados mock
- Variáveis de runtime no JSON (`${ENV_VAR}`)
- Métricas de tempo de execução por etapa
- Suporte a perfis (dev/staging/prod) no mesmo JSON
