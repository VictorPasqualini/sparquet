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
- **`sparquet_cola`**: o motor de validação virou uma **biblioteca separável**
  (pacote top-level `sparquet_cola/`, só depende de pyspark, API `Cola`). O bloco
  JSON continua `validations`; `sparquet.validation.*` são shims de compat.
- **`sql` failed_rows**: além do invariante booleano, o `sql` aceita `failed_rows`
  (query que retorna as linhas ruins) + `output` próprio para gravá-las.
- **Quarentena** (§3 item resolvido): `validations.outputs` (`valid`/`invalid`)
  roteia linhas para destinos próprios, apartado da saída principal.
- **Formatos de arquivo**: `json`, `orc`, `avro`, `xml`, `binary` (só leitura) e
  `hudi` (upsert via `hoodie.*`).
- **OpenSearch separado** do Elasticsearch — conector próprio (`opensearch`,
  prefixo `opensearch.*`). Cassandra/ScyllaDB e ES/OpenSearch documentados.
- **Input em temp view** (`input_view` na chamada do Sparquet): registra e cacheia
  a entrada para self-join/SQL sem reler a base.
- **Temp view global vs sessão**: `view` ganhou `options.scope`
  (`session`/`global`); o `input_view` aceita `{"name": ..., "type": "session"|"global"}`.

- **CI/CD de release** (resolve §5): GitHub Actions `ci.yml` (testes em push/PR,
  matriz Python) + `publish.yml` (testes → build + `twine check` → publish no PyPI em
  release; TestPyPI em execução manual), via Trusted Publishing OIDC.
- **`input_view` unificado**: o escopo da temp view de entrada agora vai dentro de
  `input_view` (`"orders"` ou `{"name": "orders", "type": "global"}`); o antigo
  `input_view_scope` foi removido.
- **`sparquet-cola` extraído** (resolve §7): o motor de DQ virou um pacote/repo próprio
  (`../sparquet-cola`), com pyproject + CI/publish + README trilíngue + docs. O `sparquet`
  passou a declará-lo em `dependencies`; `packages.find` do sparquet agora empacota só
  `sparquet` (o `sparquet_cola` vem do PyPI).

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
(read+write, salvo indicado): `parquet`, `csv`, `delta`, `iceberg`, `txt`, `view`,
`json`, `orc`, `avro`, `xml`, `hudi`, `binary` (só leitura), `kafka` (batch),
`postgresql`, `mysql`, `mariadb`, `sqlserver`, `oracle`, `bigquery`, `snowflake`,
`redshift`, `mongodb`, `documentdb`, `dynamodb`, `cassandra`, `elasticsearch`,
`opensearch`.

Decisões tomadas:

- **`json` / `orc` / `avro` / `xml` / `binary` / `hudi`**: ✅ **feito**.
- **Elasticsearch × OpenSearch**: ✅ **separados** (conectores distintos, `es.*` vs `opensearch.*`).
- **Cassandra × ScyllaDB**: ✅ **uma classe só** — Scylla usa o MESMO spark-cassandra-connector (não há conector separado); deixado explícito na classe/doc.
- **DynamoDB via RDD**: ✅ **descartado** — mantido o conector DataFrame (spark-dynamodb), que integra ao modelo do framework.
- **Streaming (readStream/writeStream)**: mantido **batch-only** por ora — streaming exige caminho de execução próprio (ver §6).

Pendentes / candidatos:

| Conector | Notas |
|---|---|
| `rest`/`http` | ingestão de APIs — adiado (não é fonte Spark nativa); via reader custom/boto3 |
| `sqs` | não é fonte Spark nativa → reader/writer driver-side com boto3 (adiado) |
| `kinesis` | AWS Kinesis — via conector do provedor; é essencialmente **streaming** (ver §6) |
| `excel` | nicho; via `spark-excel` |

- [ ] **Credenciais cloud (AWS/GCP/Azure)** — hoje passa-se tudo por `spark.configs`
      (ex: `spark.hadoop.fs.s3a.access.key`, IAM role, credenciais GCS, `fs.azure.account.key...`).
      Falta um **helper de 1ª classe** para configurar chaves/roles por provedor de
      forma padronizada (perfil de credenciais reutilizável entre pipelines).

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
2. **Data contract versionado** — evoluir o `schema` para nullability/constraints
   ricos, versionado por pipeline e validado antes de transformar.
3. **Lineage / observabilidade** — registrar fonte→destino, contagens/tempo por etapa
   (ver §4) e versão da config; expor para catálogo.
4. **Otimização de métricas** — hoje cada `check` roda sua própria action; batelar as
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

- ✅ **CI de release** — GitHub Actions: `ci.yml` (testes em push/PR) e `publish.yml`
      (testes → build + `twine check` → publish). Release publicado → PyPI; execução
      manual → TestPyPI (ensaio). Trusted Publishing (OIDC), sem token manual. Ver
      [docs/DEPLOY_PYPI.md](docs/DEPLOY_PYPI.md) §8.
- [ ] **Matriz de versões** — CI já cobre Python (3.9/3.11/3.12); falta variar
      **PySpark** (ex: 3.4 × 3.5) na matriz.
- Base atual: versão única via `__version__` (pyproject `dynamic`); publicação no
  PyPI documentada e automatizada via CI.

---

## 6. Performance

Já entregue:

- ✅ **Input em temp view** (`input_view`, string ou `{"name","type"}`) — self-join / SQL sobre a entrada sem reler a base.
- ✅ **Broadcast join** (`broadcast` no `join`) — map-side, sem shuffle.
- ✅ **Doc: `filter`/`select` primeiro** — recomendação no guia de performance + convenção no CLAUDE.md.
- ✅ (parcial) pushdown já disponível: `collect` + `{{var}}` (IN literal → data skipping), `checkpoint`, `partitionColumn`/`fetchsize` (JDBC), `partition_by` / `compression` / `maxRecordsPerFile` via `options`, e a heurística de path do Delta corrigida.

Pendente:

- [ ] **Ler sempre com partição / estratégia de leitura** — orientar (e onde fizer sentido, automatizar) leitura particionada com *partition pruning* por padrão, em vez de scan total.
- [ ] **Hash para `partitionBy`** — particionamento por hash de coluna (buckets) para evitar skew e *small files* na escrita.
- [ ] **Avaliar Apache DataFusion Comet** — acelerador vetorizado do Spark; medir ganho real e o custo de dependência antes de recomendar.
- [ ] **Análise consolidada de opções de tuning** — mapear/expor/documentar e decidir o que vira opção declarativa vs recomendação de doc: `vacuum`, `optimize`, `z-order`, `repartition`, `coalesce`, `partitionBy`, `bucketBy`, `clustering`/`clusterBy`, `compression`, `persist`/`cache`, `checkpoint`, `maxRecordsPerFile`, broadcast automático (`spark.sql.autoBroadcastJoinThreshold`), *partition pruning*, *predicate pushdown*, problema de *small files*, `shuffle` (partitions/skew), *garbage collection* e `checkpointLocation` (streaming).

> Streaming (readStream/writeStream, `checkpointLocation`, output modes, Kinesis) é
> um eixo à parte — o modelo atual é batch. Decisão registrada em §2.

---

## 7. Produtos / estratégia

- ✅ **`sparquet-cola` como repositório/pacote separado** — extraído para repo próprio
      (`../sparquet-cola`, GitHub `VictorPasqualini/sparquet-cola`) com pyproject,
      CI/publish (mesmo padrão do sparquet), README trilíngue e docs. **Publicado no PyPI**
      como **`sparquet-cola`** 0.1.0 (import `sparquet_cola`). O `sparquet` o declara em
      `dependencies` (`sparquet-cola>=0.1.0`, sem cap — mantido retrocompatível) e é
      validado contra o pacote do PyPI;
      os shims `sparquet.validation.*` seguem reexportando dele.
- [ ] **`sparquet-lite`** — versão que roda puramente em Python **sem Spark**
      (duckdb / polars / pandas), para volumes pequenos e dev local rápido. Reusar o
      mesmo schema JSON de pipeline e, idealmente, o `sparquet_cola` nas validações.
