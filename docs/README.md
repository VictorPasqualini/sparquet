# Sparquet — Documentação

Framework de pipelines Spark orientado a configuração JSON.  
Permite criar pipelines de ingestão, transformação e qualidade de dados **sem escrever código**, apenas editando um arquivo `.json`.

---

## Sumário

1. [Visão geral](#visão-geral)  
2. [Instalação](#instalação)  
3. [Estrutura do projeto](#estrutura-do-projeto)  
4. [Schema do JSON](#schema-do-json)  
5. [Sources (leitura)](#sources-leitura)  
6. [Transformações](#transformações)  
7. [Validações](#validações)  
8. [Sinks (escrita)](#sinks-escrita)  
9. [Executando um pipeline](#executando-um-pipeline)  
10. [Usando via código Python](#usando-via-código-python)  
11. [Estendendo o framework](#estendendo-o-framework)  
12. [Exemplos completos](#exemplos-completos)  

---

## Visão geral

```
JSON Config
    │
    ▼
Pipeline.from_file("config.json")
    │
    ├─► ReaderFactory  ──► DataFrame
    │
    ├─► TransformationEngine  ──► DataFrame transformado
    │
    ├─► ValidationEngine  ──► ValidationResult[]
    │
    └─► WriterFactory  ──► Parquet / Iceberg
```

O fluxo é sempre: **leitura → transformação → validação → escrita**.  
Cada etapa é controlada inteiramente pelo JSON.

---

## Instalação

```bash
pip install -e .
# ou somente as dependências
pip install -r requirements.txt
```

Para Iceberg, adicione o JAR ao Spark:

```bash
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2 main.py config.json
```

---

## Estrutura do projeto

```
framework-spark/
├── sparquet/
│   ├── core/
│   │   ├── config.py        # Dataclasses de configuração
│   │   ├── context.py       # Gerenciamento do SparkSession
│   │   └── pipeline.py      # Orquestrador principal
│   ├── io/
│   │   ├── base.py          # BaseReader / BaseWriter (abstratos)
│   │   ├── parquet.py       # Leitor/escritor Parquet
│   │   ├── iceberg.py       # Leitor/escritor Iceberg (com merge)
│   │   └── factory.py       # ReaderFactory / WriterFactory
│   ├── transform/
│   │   ├── base.py          # BaseTransformation (abstrato)
│   │   ├── builtin.py       # Transformações nativas
│   │   └── engine.py        # Motor de transformações
│   └── validation/
│       ├── base.py          # BaseValidator / ValidationResult
│       ├── builtin.py       # Validators nativos
│       └── engine.py        # Motor de validação
├── examples/                # Configs JSON de exemplo
├── docs/                    # Esta documentação
└── main.py                  # CLI
```

---

## Schema do JSON

```jsonc
{
  "pipeline": { ... },        // Obrigatório: metadados e config Spark
  "sources": [ ... ],         // Obrigatório: ao menos 1 source
  "transformations": [ ... ], // Opcional: lista de transformações em ordem
  "validations": { ... },     // Opcional: regras de qualidade de dados
  "sinks": [ ... ]            // Obrigatório: ao menos 1 sink
}
```

### pipeline

| Campo         | Tipo   | Padrão           | Descrição                          |
|---------------|--------|------------------|------------------------------------|
| `name`        | string | —                | Nome do pipeline (obrigatório)     |
| `description` | string | `""`             | Descrição livre                    |
| `version`     | string | `"1.0"`          | Versão do pipeline                 |
| `spark`       | object | veja abaixo      | Configurações do SparkSession      |

#### spark

| Campo      | Tipo   | Padrão        | Descrição                                      |
|------------|--------|---------------|------------------------------------------------|
| `app_name` | string | `Sparquet` | Nome da aplicação Spark                     |
| `master`   | string | `local[*]`    | Master URL (local, yarn, k8s://, etc.)         |
| `configs`  | object | `{}`          | Configs adicionais como `spark.sql.*`          |

---

## Sources (leitura)

```json
"sources": [
  {
    "alias": "main",
    "format": "parquet",
    "path": "/data/raw/tabela",
    "options": {}
  }
]
```

| Campo     | Tipo   | Descrição                                     |
|-----------|--------|-----------------------------------------------|
| `alias`   | string | Identificador interno (use `"main"` como primário) |
| `format`  | string | `parquet` ou `iceberg`                        |
| `path`    | string | Caminho ou tabela (`catalog.db.table`)        |
| `options` | object | Opções adicionais passadas ao leitor Spark    |

---

## Transformações

As transformações são aplicadas **em ordem** ao DataFrame principal.

### filter

Filtra linhas por uma expressão SQL booleana.

```json
{ "type": "filter", "condition": "status = 'active' AND age > 18" }
```

### select

Seleciona colunas específicas (projeção).

```json
{ "type": "select", "columns": ["id", "name", "email"] }
```

### drop

Remove colunas.

```json
{ "type": "drop", "columns": ["coluna_inutil", "pii_data"] }
```

### rename

Renomeia colunas.

```json
{
  "type": "rename",
  "mappings": { "dt_criacao": "created_at", "nm_cliente": "customer_name" }
}
```

### cast

Converte tipos de colunas.

```json
{
  "type": "cast",
  "columns": { "age": "integer", "price": "double", "created_at": "timestamp" }
}
```

### drop_duplicates

Remove linhas duplicadas. Se `columns` omitido, considera todas as colunas.

```json
{ "type": "drop_duplicates", "columns": ["id"] }
```

### sql

Executa SQL arbitrário. O DataFrame fica disponível na view `_df` (ou no alias configurado).

```json
{
  "type": "sql",
  "query": "SELECT id, name, SUM(amount) as total FROM _df GROUP BY id, name",
  "view_name": "_df"
}
```

### fill_na

Substitui valores nulos.

```json
{ "type": "fill_na", "value": 0, "columns": ["quantity", "price"] }
```

### sort

Ordena o DataFrame.

```json
{ "type": "sort", "columns": ["sale_date", "id"], "ascending": [false, true] }
```

### with_timestamp

Adiciona coluna com timestamp de ingestão.

```json
{ "type": "with_timestamp", "column_name": "ingestion_timestamp" }
```

---

## Validações

```json
"validations": {
  "on_failure": "fail",
  "rules": [ ... ]
}
```

| `on_failure` | Comportamento quando uma regra falha         |
|--------------|----------------------------------------------|
| `fail`       | Lança exceção e interrompe o pipeline (padrão) |
| `warn`       | Loga warning e continua                      |
| `skip`       | Ignora falhas silenciosamente               |

### not_null

```json
{ "type": "not_null", "columns": ["id", "email", "created_at"] }
```

### unique

```json
{ "type": "unique", "columns": ["id"] }
```

### range

```json
{ "type": "range", "column": "age", "min": 0, "max": 150 }
```

### regex

```json
{ "type": "regex", "column": "email", "pattern": "^[\\w.+\\-]+@[\\w\\-]+\\.[a-z]{2,}$" }
```

### row_count

```json
{ "type": "row_count", "min": 1, "max": 10000000 }
```

### sql

O DataFrame é exposto como `_validation_df`. A query deve retornar um único valor
booleano — semântica **pass-when-true** (expresse o invariante, não a violação).

```json
{
  "type": "sql",
  "query": "SELECT COUNT(*) = 0 FROM _validation_df WHERE total_amount < 0",
  "error_message": "Detectados valores negativos em total_amount"
}
```

> Também disponíveis: `check` (métrica + threshold warn/fail, estilo SODA Core) e
> `schema` (colunas/tipos). Ver a referência completa em [sparquet.dev/docs](https://sparquet.dev/docs) ou no
> [CLAUDE.md](../CLAUDE.md).

---

## Sinks (escrita)

```json
"sinks": [
  {
    "format": "parquet",
    "path": "/data/processed/tabela",
    "mode": "overwrite",
    "partition_by": ["ano", "mes"],
    "options": {}
  }
]
```

| Campo          | Tipo         | Padrão    | Descrição                                   |
|----------------|--------------|-----------|---------------------------------------------|
| `format`       | string       | —         | `parquet` ou `iceberg`                      |
| `path`         | string       | —         | Caminho ou `catalog.db.table`               |
| `mode`         | string       | `append`  | `append`, `overwrite`, `merge` (Iceberg)    |
| `partition_by` | string[]     | `[]`      | Colunas de particionamento                  |
| `options`      | object       | `{}`      | Opções adicionais. Para merge: `merge_keys` |

### Iceberg merge

O modo `merge` executa `MERGE INTO` (upsert). É necessário informar `merge_keys`:

```json
{
  "format": "iceberg",
  "path": "local.db.orders",
  "mode": "merge",
  "options": {
    "merge_keys": ["order_id"]
  }
}
```

---

## Executando um pipeline

### CLI

```bash
python main.py examples/basic_parquet.json
python main.py examples/iceberg_upsert.json --stop-spark
```

### spark-submit

```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2 \
  main.py \
  examples/iceberg_upsert.json
```

---

## Usando via código Python

```python
from sparquet import Pipeline

# A partir de um arquivo JSON
pipeline = Pipeline.from_file("meu_pipeline.json")
result = pipeline.run()

print(result.summary())
# Pipeline 'meu_pipeline' completed | read=15000 | written=14800 | validations=4/4

# A partir de um dicionário (útil para gerar configs programaticamente)
config_dict = { ... }
pipeline = Pipeline.from_dict(config_dict)
result = pipeline.run()

if not result.success:
    raise RuntimeError(result.error)
```

---

## Estendendo o framework

### Adicionar um novo formato de leitura/escrita

```python
from sparquet.io.base import BaseReader, BaseWriter
from sparquet.io.factory import ReaderFactory, WriterFactory

class DeltaReader(BaseReader):
    def read(self):
        return self.spark.read.format("delta").load(self.config.path)

class DeltaWriter(BaseWriter):
    def write(self, df):
        df.write.format("delta").mode(self.config.mode).save(self.config.path)

ReaderFactory.register("delta", DeltaReader)
WriterFactory.register("delta", DeltaWriter)
```

### Adicionar uma transformação customizada

```python
from sparquet.transform.base import BaseTransformation
from sparquet.transform.engine import TransformationEngine
from pyspark.sql import functions as F

class NormalizeTextTransformation(BaseTransformation):
    def apply(self, df):
        col = self.config.params["column"]
        return df.withColumn(col, F.trim(F.lower(F.col(col))))

engine = TransformationEngine()
engine.register("normalize_text", NormalizeTextTransformation)

pipeline = Pipeline(config, transform_engine=engine)
```

### Adicionar um validator customizado

```python
from sparquet.validation.base import BaseValidator, ValidationResult
from sparquet.validation.engine import ValidationEngine

class NoFutureDate(BaseValidator):
    def validate(self, df):
        from pyspark.sql import functions as F
        col = self.rule.params["column"]
        failed = df.filter(F.col(col) > F.current_date()).count()
        if failed:
            return ValidationResult("no_future_date", False, f"{failed} future dates found", failed)
        return ValidationResult("no_future_date", True)

engine = ValidationEngine()
engine.register("no_future_date", NoFutureDate)
```

---

## Exemplos completos

| Arquivo                                | Descrição                                     |
|----------------------------------------|-----------------------------------------------|
| `examples/basic_parquet.json`          | Ingestão simples com filtros e validações      |
| `examples/iceberg_upsert.json`         | Upsert incremental em tabela Iceberg           |
| `examples/etl_with_validations.json`   | ETL completo com múltiplos sinks               |
