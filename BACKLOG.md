# sparquet — Backlog de desenvolvimento

Melhorias e pendências de desenvolvimento **do framework** (não de um caso de uso
específico). Cada item é uma capacidade genérica, ortogonal e sem acoplamento de
domínio.

Atualizado em 2026-08-15.

---

## 0. Entregue recentemente (changelog)

Rodada de evolução — capacidades já no código, com testes e entrada no catálogo do Studio:

- **Rename do pacote**: `spark_framework` → `sparquet`; classe de entrada
  `SparkFramework` → **`Sparquet`** (`from sparquet import Sparquet`).
- **Licença**: MIT → **Apache 2.0** (+ arquivo `NOTICE`).
- **Conectores de IO** (read+write) — resolve boa parte do §2: JDBC (`postgresql`,
  `mysql`, `mariadb`, `sqlserver`, `oracle`), warehouses (`bigquery`, `snowflake`,
  `redshift`), NoSQL/busca (`mongodb`, `documentdb`, `dynamodb`, `cassandra`,
  `elasticsearch`) e **leitura Kafka** batch (MSK via SASL/IAM).
- **Validação estilo SODA Core** — resolve boa parte do §3: `check` (métrica +
  threshold warn/fail, incl. `freshness` e formatos nomeados) e `schema`
  (colunas/tipos); `ValidationResult` ganhou `severity`/`metric_value`/`check_name`;
  relatório enriquecido; severidade `warn` não aborta.
- **Broadcast join**: param `broadcast` no `join` (map-side, sem shuffle).
- **Métricas por output**: `PipelineResult.output_metrics` + contagem antes da
  escrita (`rows_written` = soma por destino).
- **Renome**: validação `custom_sql` → **`sql`**. **Removido**: alias `add_column`
  (use `with_column`).
- **Fix**: heurística path-vs-tabela do Delta agora é *scheme-agnostic* (cobre
  `s3a://`, `abfs://`, etc., não só `s3://`).

Pendências abaixo já descontam o que foi entregue.

---

## 1. Princípio

Núcleo fino e modular (registry de IO + transformações + validações + engine) que
transforma pipelines imperativos em **configuração declarativa**. Toda evolução
deve preservar: ortogonalidade (capacidades não se acoplam), extensibilidade via
`register_*`, e o limite saudável **"config declarativa, não código em JSON"**.

---

## 2. Conectores de IO (novos formatos)

Cada formato é um par `BaseReader`/`BaseWriter` registrado nas factories. Hoje
(read+write): `parquet`, `csv`, `delta`, `iceberg`, `txt`, `view`, `kafka`,
`postgresql`, `mysql`, `mariadb`, `sqlserver`, `oracle`, `bigquery`, `snowflake`,
`redshift`, `mongodb`, `documentdb`, `dynamodb`, `cassandra`, `elasticsearch`.

Pendentes / candidatos:

| Conector | Leitura | Escrita | Notas |
|---|---|---|---|
| `json` | ☐ | ☐ | multiline, schema inference, `to_json`/`from_json` já existem em transformações |
| `avro` | ☐ | ☐ | requer `spark-avro`; schema registry (futuro) |
| `orc` | ☐ | ☐ | nativo Spark |
| `rest`/`http` | ☐ | ☐ | ingestão de APIs (paginação, auth) — decidido adiar (não é fonte Spark nativa); via reader custom |
| `sqs` | ☐ | ☐ | não é fonte Spark nativa → reader/writer driver-side com boto3 (adiado) |
| `excel`/`xml` | ☐ | ☐ | nichos; via libs externas (`spark-excel`, `spark-xml`) |

> Kafka streaming (hoje só batch read/write) e o *stream* contínuo em geral seguem
> fora de escopo do modelo batch atual.

Diretriz: manter o reader/writer fino; opções específicas via `options`. Formatos
que exigem dependência extra entram como `optional-dependencies` no pyproject
(como `delta` hoje).

---

## 3. Data quality & governança  (eixo estratégico)

Hoje: bloco `validations` com `not_null`, `unique`, `range`, `regex`, `row_count`,
`sql`, e — estilo **SODA Core** — `check` (métrica + threshold warn/fail) e
`schema`; `on_failure` (fail/warn/skip); severidade `pass/warn/fail` (warn não
aborta); resultados em `PipelineResult.validation_results` com `severity`,
`metric_value`, `check_name` + relatório opcional enriquecido.

Já entregue (via `check`/`schema`): **completeness** (`missing_*`), **freshness**,
**accepted_values** (`valid_values`/`valid_format`), **validade** (invalid_*),
**métricas estruturadas** (`metric_value`) e um **data contract** básico (`schema`:
colunas obrigatórias/proibidas + tipos).

Evolução ainda proposta:

1. **`reference`/reconciliação cross-dataset** — comparar contagem/soma do df contra
   outra fonte (tabela/view) por chave; `failed_count` por grupo. (O `sql` cobre isso
   hoje de forma manual; falta o validator dedicado — SODA tem `reconciliation`.)
2. **Quarentena** — em vez de `fail`/`warn`, rotear linhas inválidas para um destino
   de quarentena (split good/bad) sem abortar.
3. **Data contract versionado** — evoluir o `schema` para nullability/constraints
   ricos, versionado por pipeline e validado antes de transformar.
4. **Lineage / observabilidade** — registrar fonte→destino, contagens/tempo por etapa
   (ver §4) e versão da config; expor para catálogo.
5. **Otimização de métricas** — hoje cada `check` roda sua própria action; batelar as
   agregações de vários checks numa passada só.

Princípio de DQ: **validações reportam, transformações mudam** — manter essa
separação ao evoluir.

---

## 4. Núcleo / DX

- [ ] **Dry-run** — valida a config (schema, tipos de transformação/validator,
      colunas referenciadas quando possível) sem executar Spark.
- [ ] **Métricas por etapa** — tempo e contagem de linhas por transformação;
      expor em `PipelineResult`.
- [ ] **Perfis (dev/staging/prod)** no mesmo JSON — overrides de path/options por
      ambiente, selecionados por param.
- [ ] **Testes unitários com dados mock** — cobrir transformações (`struct`,
      `collect`/`{{var}}`, `stop_if_empty`, `group_by`, outputs com `transformations`)
      e validators, com SparkSession local — base para CI.
- [ ] **`$include` aninhado** — hoje não suportado (um nível só).
- [ ] **Catálogo de erros** — mensagens de erro padronizadas e acionáveis
      (transformação desconhecida, coluna inexistente, etc.).

---

## 5. Empacotamento / distribuição / CI

- [ ] **CI de release** — rodar testes + build + publish no PyPI ao criar uma tag
      (ver [docs/DEPLOY_PYPI.md](docs/DEPLOY_PYPI.md)).
- [ ] **Matriz de versões** Python × PySpark no CI.
- Base atual: versão única via `__version__` (pyproject `dynamic`); publicação no
  PyPI já documentada.
