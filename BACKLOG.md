# sparquet — Backlog de desenvolvimento

Melhorias e pendências de desenvolvimento **do framework e do Studio** (não de um caso
de uso específico). Cada item é uma capacidade genérica, ortogonal e sem acoplamento de
domínio.

Atualizado em 2026-09-13.

**O formato de uma tarefa.** Toda tarefa deste arquivo, em qualquer seção e em
qualquer nível de aninhamento, é um item de lista com a mesma forma:

```
- <marca> **Título** — o corpo, em prosa, começando na mesma linha.
  Continuação com dois espaços de indentação; tabela, bloco de código e
  sub-item entram dentro do item, indentados junto.
```

| Marca | Significa |
|---|---|
| ✅ | entregue **e** verificado — existe no código e há teste ou verificação manual apontada no item |
| 🟡 | entregue em parte — o que falta está dito no próprio item |
| [ ] | pendente |

A marca fica na frente porque é ela, e não a posição na seção, que diz o estado: não há
lista de “entregue” separada de lista de “pendente”, que é o arranjo que apodrece no dia
em que um item muda de estado e ninguém o move. O título em negrito é o nome pelo qual a
tarefa é citada em outras seções; o corpo diz o porquê e, quando entregue, onde no
código e coberto por qual teste.

Cobertura de testes (o que está garantido e o que não está, conector por conector,
transformação por transformação): [docs/TEST_PLAN.md](docs/TEST_PLAN.md). Os itens de
§11 saem de lá — a ordem é a do plano.

**Índice**

1. [Princípio](#1-princípio)
2. [Entregue — framework](#2-entregue--framework-changelog)
3. [Entregue — Studio](#3-entregue--studio-changelog)
4. [Conectores de IO](#4-conectores-de-io-novos-formatos)
5. [Data quality & governança](#5-data-quality--governança--eixo-estratégico)
6. [Núcleo / DX](#6-núcleo--dx)
7. [Performance](#7-performance)
8. [Empacotamento / distribuição / CI](#8-empacotamento--distribuição--ci)
9. [Studio — plataforma](#9-studio--plataforma) (execução · IAM · billing · observabilidade · catálogo)
10. [Produtos / estratégia](#10-produtos--estratégia)
11. [Testes e cobertura](#11-testes-e-cobertura)

---

## 1. Princípio

Núcleo fino e modular (registry de IO + transformações + validações + engine) que
transforma pipelines imperativos em **configuração declarativa**. Toda evolução
deve preservar: ortogonalidade (capacidades não se acoplam), extensibilidade via
`register_*`, e o limite saudável **"config declarativa, não código em JSON"**.

Vocabulário: para o framework um JSON é um *pipeline* (classe `Pipeline`). No Studio o
mesmo arquivo é um **Job**, **Pipeline** é a sequência ordenada de Jobs e **Workflow** é
o container. A API Python **não** muda por causa disso.

---

## 2. Entregue — framework (changelog)

Capacidades já no código, com testes e entrada no catálogo do Studio:

- ✅ **Rename do pacote** — `spark_framework` → `sparquet`; classe de entrada
  `SparkFramework` → **`Sparquet`** (`from sparquet import Sparquet`).
- ✅ **Licença** — MIT → **Apache 2.0** (+ arquivo `NOTICE`).
- ✅ **Conectores de IO (read+write)** — resolve boa parte do §4: JDBC (`postgresql`,
  `mysql`, `mariadb`, `sqlserver`, `oracle`), warehouses (`bigquery`, `snowflake`,
  `redshift`), NoSQL/busca (`mongodb`, `documentdb`, `dynamodb`, `cassandra`,
  `elasticsearch`) e **leitura Kafka** batch (MSK via SASL/IAM).
- ✅ **Validação estilo SODA Core** — resolve boa parte do §5: `check` (métrica +
  threshold warn/fail, incl. `freshness` e formatos nomeados) e `schema`
  (colunas/tipos); `ValidationResult` ganhou `severity`/`metric_value`/`check_name`;
  relatório enriquecido; severidade `warn` não aborta.
- ✅ **Broadcast join** — param `broadcast` no `join` (map-side, sem shuffle).
- ✅ **Métricas por output** — `PipelineResult.output_metrics` (`rows_written` = soma
  por destino), lido do contador que a própria escrita apura — sem `count()` extra;
  ver `sparquet/core/write_metrics.py` e o benchmark no CHANGELOG 0.7.0. É também o que o billing do Studio
  conta como escrita bem-sucedida (§9.3) — sem isso a cobrança por escrita não teria
  fonte.
- ✅ **Renome** — validação `custom_sql` → **`sql`**. **Removido**: alias `add_column`
  (use `with_column`).
- ✅ **Fix** — heurística path-vs-tabela do Delta agora é *scheme-agnostic* (cobre
  `s3a://`, `abfs://`, etc., não só `s3://`).
- ✅ **`sparquet_cola`** — o motor de validação virou uma **biblioteca separável**
  (pacote top-level `sparquet_cola/`, só depende de pyspark, API `Cola`). O bloco
  JSON continua `validations`; `sparquet.validation.*` são shims de compat.
- ✅ **`sql` failed_rows** — além do invariante booleano, o `sql` aceita `failed_rows`
  (query que retorna as linhas ruins) + `output` próprio para gravá-las.
- ✅ **Quarentena (§5 item resolvido)** — `validations.outputs` (`valid`/`invalid`)
  roteia linhas para destinos próprios, apartado da saída principal.
- ✅ **Formatos de arquivo** — `json`, `orc`, `avro`, `xml`, `binary` (só leitura) e
  `hudi` (upsert via `hoodie.*`).
- ✅ **OpenSearch separado do Elasticsearch** — conector próprio (`opensearch`,
  prefixo `opensearch.*`). Cassandra/ScyllaDB e ES/OpenSearch documentados.
- ✅ **Input em temp view (`input_view` na chamada do Sparquet)** — registra e cacheia
  a entrada para self-join/SQL sem reler a base.
- ✅ **Temp view global vs sessão** — `view` ganhou `options.scope`
  (`session`/`global`); o `input_view` aceita `{"name": ..., "type": "session"|"global"}`.
- ✅ **`input_view` unificado** — o escopo da temp view de entrada agora vai dentro de
  `input_view` (`"orders"` ou `{"name": "orders", "type": "global"}`); o antigo
  `input_view_scope` foi removido.
- ✅ **Marcadores de etapa nos logs (v0.3.1)** — `Pipeline`/`TransformationEngine`
  emitem `step=True` com `scope` (`input`/`transformation`/`output`), `index` e `total`.
  Puramente aditivo — `apply(..., top_level=False)` mantém o comportamento anterior.
  É o que alimenta o status por etapa no canvas do Studio.
- ✅ **CI/CD de release (resolve §8)** — GitHub Actions `ci.yml` (testes em push/PR,
  matriz Python) + `publish.yml` (testes → build + `twine check` → publish no PyPI em
  release; TestPyPI em execução manual), via Trusted Publishing OIDC.
- ✅ **`sparquet-cola` extraído (resolve parte do §10)** — o motor de DQ virou um
  pacote/repo próprio (`../sparquet-cola`), com pyproject + CI/publish + README
  trilíngue + docs. O `sparquet` passou a declará-lo em `dependencies`;
  `packages.find` do sparquet agora empacota só `sparquet` (o `sparquet_cola` vem do
  PyPI).

---

## 3. Entregue — Studio (changelog)

O Studio compila o canvas para o **mesmo JSON** que o framework executa; nada aqui
muda a API Python.

- ✅ **Vocabulário** — `Workflow` (container) › `Job` (um JSON) › `Pipeline` (conjunto
  ordenado de Jobs, executado em sequência).
- ✅ **Execução em sequência** — `POST /run/flow/stream` no runner executa vários Jobs
  numa SparkSession compartilhada, com status/logs por estágio e `stop_on_error`.
- ✅ **Storage dos JSONs — padronização e versionamento em git** — a biblioteca do
  Studio passou a morar em arquivos reais, servidos pelo runner
  (`sparquet-studio/server/workspace.py`, endpoints `GET /workspace`,
  `PUT`/`DELETE /workspace/{kind}/{id}` e `PUT`/`DELETE /workspace/meta/{key}`).
  Cada registro vira dois arquivos: o revisável — `<workflow>/workflow.json`,
  `<workflow>/jobs/<slug>.json` (o **JSON compilado**, que `sparquet run` executa
  sem tradução) e `<workflow>/pipelines/<slug>.json` — e um sidecar em
  `.studio/<kind>/<id>.json` com o registro completo do editor (posições de canvas,
  parâmetros). Diretório padrão `sparquet-workspace/` na raiz, configurável por
  `SPARQUET_STUDIO_WORKSPACE`; renomear move o arquivo em vez de deixar cópia velha, e
  renomear um Workflow move tudo que está sob ele. O cliente escolhe o backend em
  cadeia — workspace, depois IndexedDB, depois `localStorage`, depois memória
  (`src/lib/storage/db.ts` + `remote.ts`) — e uma biblioteca que só existia no
  navegador é empurrada uma única vez para um workspace vazio. Fonte da verdade: o
  arquivo; o navegador virou cache de quando o runner não está no ar.
- ✅ **O produto não escreve dentro do próprio código-fonte** — um checkout é código:
  ele é puxado, resetado e apagado, então uma biblioteca dentro dele morre no primeiro
  `git clean` — ou é commitada por engano muito antes disso. O default virou o
  diretório de dados do usuário (`%APPDATA%\Sparquet\workspace` no Windows,
  `$XDG_DATA_HOME/sparquet/workspace` no resto; `SPARQUET_HOME` sobrescreve), e
  `GET`/`PUT /workspace/root` deixam escolher outro pela interface (Settings → Local
  runner → *Library location*). Precedência: `SPARQUET_STUDIO_WORKSPACE` (deployment
  decide, a interface não sobrepõe — `409` + `locked`) › escolha salva em
  `studio.json` › `sparquet-workspace/` antigo **que já tenha um `.studio/`**, adotado
  para ninguém perder biblioteca › default. Trocar a raiz **não copia nada**: adotar um
  diretório que já tem biblioteca é o caso de uso, e uma cópia pela metade sem volta é
  pior do que uma mudança que ninguém fez. Recusas: caminho relativo, diretório que não
  dá para criar ou escrever, e qualquer caminho dentro do código-fonte. A ação é
  `runner:Configure`, de propósito fora de `workspace:*` — o papel `editor` tem
  `workspace:*`, e decidir onde o runner escreve na máquina é decisão de administrador.
  `spark-warehouse/` e `sparquet-workspace/` saíram do git.

O que é execução, histórico, IAM e billing está em §9, com o que ainda falta em cada
um logo abaixo do que já existe.

---

## 4. Conectores de IO (novos formatos)

Cada formato é um par `BaseReader`/`BaseWriter` registrado nas factories. Hoje
(read+write, salvo indicado): `parquet`, `csv`, `delta`, `iceberg`, `txt`, `view`,
`json`, `orc`, `avro`, `xml`, `hudi`, `binary` (só leitura), `kafka` (batch),
`postgresql`, `mysql`, `mariadb`, `sqlserver`, `oracle`, `bigquery`, `snowflake`,
`redshift`, `mongodb`, `documentdb`, `dynamodb`, `cassandra`, `elasticsearch`,
`opensearch`.

- ✅ **`json` / `orc` / `avro` / `xml` / `binary` / `hudi`** — feito.
- ✅ **Elasticsearch × OpenSearch** — separados (conectores distintos, `es.*` vs
  `opensearch.*`).
- ✅ **Cassandra × ScyllaDB** — uma classe só — Scylla usa o MESMO
  spark-cassandra-connector (não há conector separado); deixado explícito na
  classe/doc.
- ✅ **DynamoDB via RDD** — descartado — mantido o conector DataFrame (spark-dynamodb),
  que integra ao modelo do framework.
- [ ] **Streaming (readStream/writeStream)** — mantido **batch-only** por ora: streaming
  exige caminho de execução próprio (ver §7).
- ✅ **`mariadb` passou a montar url de MySQL** — achado da camada de integração com
  serviço (`tests/io/integration/test_jdbc_services_spark.py`). O Spark 4.1.1 **não tem
  dialeto MariaDB**: existe só `MySQLDialect`, e ele reconhece apenas url começando em
  `jdbc:mysql`. A url `jdbc:mariadb://` que o `_MariaDbDialect` montava caía no dialeto
  default, que cita identificador com `"` — e o MariaDB recusa:

      java.sql.SQLSyntaxErrorException: You have an error in your SQL syntax; check the
      manual ... near '"id" INTEGER , "nome" TEXT , "valor" DOUBLE PRECISION'

  Valia para escrita **e** leitura (o SELECT que o Spark monta cita as colunas do mesmo
  jeito): o conector estava inteiro fora do ar contra um MariaDB de verdade. As duas
  rotas foram executadas contra o container e as duas funcionam — a do MySQL virou o
  default do conector:

  | Rota | Como | Preço |
  |---|---|---|
  | **default** — driver do MySQL | o dialeto monta `jdbc:mysql://host:3306/db` e usa `com.mysql.cj.jdbc.Driver` | exige o jar do Connector/J em vez do `mariadb-java-client`; o servidor não muda (o MariaDB fala o protocolo do MySQL) |
  | válvula — `sql_mode='ANSI_QUOTES'` | `url: jdbc:mariadb://...` à mão (o dialeto acompanha com `org.mariadb.jdbc.Driver`) + `options: {"sessionVariables": "sql_mode='ANSI_QUOTES'"}` | na mesma sessão `"..."` deixa de ser literal de string — importa para quem usa `query`; e o dialeto continua o default: sem pushdown de LIMIT/agregação no SQL do MariaDB, e `TEXT` no lugar de `VARCHAR` |

  O default é o do MySQL porque é o único que devolve o `MySQLDialect` de verdade:
  citação com crase, mapeamento de tipo e SQL de pushdown corretos. Leitura não piora —
  ela era exatamente o que estava quebrado antes. Quem informa `url` ou `driver` continua
  no comando: `driver` explícito vence sempre, url `jdbc:mariadb:` traz o driver do
  MariaDB por conta própria, e a mesma url sem `ANSI_QUOTES` sai com aviso dizendo as
  duas saídas. Coberto por `tests/io/test_connectors.py` e pelas duas classes de
  integração (`MariaDbTest`, `MariaDbUrlExplicitaTest`).
- ✅ **`opensearch` tem build para Spark 4 — e é por ele que o Elasticsearch chega** —
  achado da camada de integração com serviço
  (`tests/io/integration/test_nosql_services_spark.py`). O que existe publicado, medido
  no Spark 4.1.1:

  | Coordenada | Situação |
  |---|---|
  | `org.opensearch.client:opensearch-spark-40_2.13:2.0.0` | **funciona** — o pom declara Spark 4.1.1 e Scala 2.13.16 |
  | `org.opensearch.client:opensearch-spark-30_2.13:1.3.0` | quebra — era a coordenada que estava aqui |
  | `org.elasticsearch:elasticsearch-spark-30_2.13` 8.16.1, 9.0.3 e 9.5.3 (a última publicada) | quebra — a 9.5.3 ainda compila contra Spark 3.4.3 |
  | `elasticsearch-spark-40` | **não existe** no Maven Central |

  Quem quebra, quebra na escrita com `Dataset.sqlContext()`, que saiu da API no Spark 4:

      java.lang.NoSuchMethodError: 'org.apache.spark.sql.SQLContext org.apache.spark.sql.Dataset.sqlContext()'

  A saída para Elasticsearch é o conector do OpenSearch apontado para o servidor do
  Elasticsearch: o `opensearch-spark` é fork do `elasticsearch-hadoop` e continua
  falando a mesma API REST. Medido contra o container 8.16.1 — escrita, leitura e
  `mapping.id` passam, e os documentos aparecem no `_search` do próprio ES
  (`ElasticsearchViaOpenSearchTest`). Migrar um pipeline custa duas edições no JSON, e
  nenhuma das duas dá para pular:

  * `format: "elasticsearch"` → `"opensearch"`. O jar do OpenSearch não registra o nome
    antigo: `[DATA_SOURCE_NOT_FOUND] Failed to find the data source: es`.
  * opções `es.*` → `opensearch.*`. O prefixo antigo é ignorado e a escrita morre em
    `OpenSearchHadoopIllegalArgumentException`.

  Nada muda no servidor, no índice ou no schema. As outras saídas (rodar aquele pipeline
  em Spark 3.5, indexar pelo `_bulk` fora do Spark, ou ler pelo JDBC de Elasticsearch
  SQL, que é licença paga e só lê) estão em `docs/PIPELINE_SCHEMA.md`, na seção "Busca
  (Elasticsearch e OpenSearch) no Spark 4".

  Fica de acompanhamento: no dia em que sair um `elasticsearch-spark-40`, apagar o campo
  `incompativel` da linha do elasticsearch em `services.py` devolve o conector nativo e
  faz `ElasticsearchTest` rodar sem mais nenhuma mudança.
- [ ] **`cassandra`: o catálogo do conector não funciona em Spark 4 (só o caminho de
  dados)** — mesmo achado. Ler e escrever com `format("cassandra")` passa com
  `com.datastax.spark:spark-cassandra-connector_2.13:3.5.1` (a última publicada; não
  há build para Spark 4). O que não passa é registrar
  `spark.sql.catalog.<nome> = com.datastax.spark.connector.datasource.CassandraCatalog`
  e usar SQL: o primeiro `CREATE` morre com

      java.lang.NoClassDefFoundError: org/apache/spark/sql/catalyst/analysis/NoSuchNamespaceException

  — classe que existia no Spark 3.5 e saiu no Spark 4. Consequência prática para o
  usuário: **keyspace e tabela precisam existir antes** do pipeline, e o DDL sai de
  fora do Spark (cqlsh, driver DataStax, migration). Vale documentar isso no
  `sparquet-web` junto com o conector; e a tabela precisa ter a coluna
  `ingestion_ts timestamp`, que o `Pipeline` acrescenta depois do reader. Duas
  consequências para o nosso lado, além da doc: o jar do Cassandra **não** convive com
  `delta-spark` nem com `iceberg-spark-runtime` no mesmo `spark.jars.packages` (o
  `SparkContext` não sobe — ver `harness.USE_BASE_PACKAGES`), então um pipeline que
  escreve de Cassandra para Delta na mesma sessão precisa ser verificado antes de ser
  prometido.
- [ ] **`rest`/`http`** — ingestão de APIs; adiado, porque não é fonte Spark nativa:
  o caminho é um reader custom ou boto3.
- [ ] **`sqs`** — também não é fonte Spark nativa; sai como reader/writer driver-side
  com boto3. Adiado.
- [ ] **`kinesis`** — via conector do provedor; é essencialmente **streaming**, então
  depende do eixo de §7.
- [ ] **`excel`** — nicho; via `spark-excel`.
- ✅ **`IcebergWriter` não cria a tabela** — a escrita usava
  `df.write.format("iceberg").save(path)`, e no Spark 4 esse caminho exige que a
  tabela **já exista**: apontar um output para uma tabela nova devolvia
  `[TABLE_OR_VIEW_NOT_FOUND] The table or view db.x cannot be found`, sem criar
  nada. Corrigido: quando o `path` é identificador de catálogo, a escrita usa
  `saveAsTable`, que cria a tabela (com o `partition_by` declarado) na primeira
  carga; caminho físico continua em `save`. A regra path-vs-tabela virou
  `is_table_name` em `sparquet/io/base.py`, compartilhada com o `DeltaWriter`.
  O `merge` numa tabela que ainda não existe grava tudo em vez de falhar.
  Coberto por `tests/io/integration/test_lakehouse_spark.py` (`IcebergTest`).
- ✅ **O bloco `spark` do JSON não chega na criação da sessão** — `Sparquet.__init__`
  chamava `SparkContextManager.get_or_create` com a config **do construtor**, antes
  de `run_from_dict` ler o JSON; como a `SparkSession` é singleton de processo,
  `spark.configs` do JSON era morto — em especial `spark.jars.packages`, ou seja,
  **nenhum JSON conseguia pedir o jar de um conector**. Corrigido: o construtor não
  cria mais a sessão; ela nasce na primeira execução, em `pipeline.py`, já com a
  config do JSON (e com as do construtor por cima, via `_apply_spark_override`).
  Quem precisa da sessão antes de executar usa a propriedade `Sparquet.spark`.
  `tests/io/integration/harness.py` passa o bloco pelo JSON e é a prova disso.
- ✅ **`mode: merge` escrito à mão** — `sparquet/io/merge.py` monta o `MERGE INTO` a
  partir de duas opções **obrigatórias** em `output.options`: `on` (a condição ON
  inteira, sobre `T.` e `S.`) e `actions` (a lista de cláusulas `WHEN ...`, emitidas
  na ordem dada). Nada é gerado por conta própria: o upsert simples é
  `["WHEN MATCHED THEN UPDATE SET *", "WHEN NOT MATCHED THEN INSERT *"]`, e apagar é
  uma cláusula como qualquer outra — `WHEN MATCHED AND S.op = 'D' THEN DELETE`
  **antes** do UPDATE, porque a primeira cláusula que casa vence, e
  `WHEN NOT MATCHED BY SOURCE THEN DELETE` só contra um snapshot completo da origem.
  A forma declarativa antiga (`merge_keys`, `merge_condition`, `delete_when`,
  `delete_not_matched_by_source`) **deixou de existir**: cada chave é recusada pelo
  nome, dizendo qual cláusula a substitui. Catálogo do Studio atualizado; falta o PR
  no `sparquet-web`.
- [ ] **Credenciais cloud (AWS/GCP/Azure)** — hoje passa-se tudo por `spark.configs`
  (ex: `spark.hadoop.fs.s3a.access.key`, IAM role, credenciais GCS, `fs.azure.account.key...`).
  Falta um **helper de 1ª classe** para configurar chaves/roles por provedor de
  forma padronizada (perfil de credenciais reutilizável entre pipelines).

Diretriz: manter o reader/writer fino; opções específicas via `options`. Formatos
que exigem dependência extra entram como `optional-dependencies` no pyproject
(como `delta` hoje).

---

## 5. Data quality & governança  (eixo estratégico)

Hoje: bloco `validations` com `not_null`, `unique`, `range`, `regex`, `row_count`,
`sql`, e — estilo **SODA Core** — `check` (métrica + threshold warn/fail) e
`schema`; `on_failure` (fail/warn/skip); severidade `pass/warn/fail` (warn não
aborta); resultados em `PipelineResult.validation_results` com `severity`,
`metric_value`, `check_name` + relatório opcional enriquecido.

Já entregue (via `check`/`schema`): **completeness** (`missing_*`), **freshness**,
**accepted_values** (`valid_values`/`valid_format`), **validade** (invalid_*),
**métricas estruturadas** (`metric_value`) e um **data contract** básico (`schema`:
colunas obrigatórias/proibidas + tipos).

- [ ] **`reference`/reconciliação cross-dataset** — comparar contagem/soma do df contra
  outra fonte (tabela/view) por chave; `failed_count` por grupo. (O `sql` cobre isso
  hoje de forma manual; falta o validator dedicado — SODA tem `reconciliation`.)
- [ ] **Data contract versionado** — evoluir o `schema` para nullability/constraints
  ricos, versionado por pipeline e validado antes de transformar.
- [ ] **Lineage / observabilidade** — registrar fonte→destino, contagens/tempo por etapa
  (ver §6) e versão da config; expor para catálogo. **Parcial**: o Studio já persiste
  lineage por execução (`job_run.lineage`) e a versão da config (`job_run.config_hash`)
  — ver §9.1; falta o mesmo fora do runner do Studio.
- [ ] **Otimização de métricas** — hoje cada `check` roda sua própria action; batelar as
  agregações de vários checks numa passada só.

### 5.1 Validações candidatas no `sparquet_cola`

Hoje: `not_null`, `unique`, `range`, `regex`, `row_count`, `sql`, `schema` e o `check`
(métricas `row_count`, `distinct_count`, `missing_*`, `duplicate_*`, `invalid_*`,
`min`/`max`/`avg`/`sum`/`stddev`, `freshness`). Cada nova regra precisa de entrada no
catálogo do Studio (`src/catalog/`), senão o editor não a oferece.

**A. Já é possível com `sql`, mas merece ser declarativo.** O `sql` cobre tudo — e é
exatamente por isso que essas viram SQL copiado entre pipelines, sem nome nem
semântica no relatório:

- [ ] **`column_comparison`** — `col_a <= col_b` (`data_inicio` ≤ `data_fim`,
  `valor_liquido` ≤ `valor_bruto`). A checagem cross-column mais comum que existe.
- [ ] **`conditional_not_null`** — coluna obrigatória **quando** outra tem valor
  (`cnpj` obrigatório se `tipo = 'PJ'`). Row-level, entra na quarentena.
- [ ] **`accepted_values`** — hoje só via `check` + `valid_values`; verboso para o caso
  mais frequente de todos.
- [ ] **`mutually_exclusive`** — no máximo uma de N colunas preenchida.

**B. Cross-dataset.** Complementa o `reference`/reconciliação já proposto acima:

- [ ] **`foreign_key`** — todo valor de `col` existe em outra fonte. Hoje exige um join
  manual no pipeline, o que mistura validação com transformação.

**C. Estatística / outliers.** Thresholds sobre a *forma* do dado, não só extremos:

- [ ] **`quantile` como métrica do `check`** — p50/p95/p99 (`must_be: "< 1000"` no p95
  diz mais sobre latência/valor que `max`, que um único outlier distorce).
- [ ] **`outliers`** — contagem fora de N desvios (z-score) ou do IQR.
- [ ] **`cardinality` por grupo** — `distinct_count` particionado (distinct país por cliente).

**D. Completude segmentada:**

- [ ] **`missing_percent` com `group_by`** — hoje é global; responder "qual país tem 30%
  de cpf vazio" exige uma regra por país.

**E. Depende de histórico de execuções.** O histórico agora existe (§9.1), mas só
dentro do runner do Studio e sem série temporal por métrica — é isso que falta para
estas três:

- [ ] **`volume_anomaly`** — `row_count` contra a média móvel das últimas N execuções.
  Pega o caso clássico de "a fonte veio pela metade e ninguém percebeu".
- [ ] **`distribution_shift`** — desvio da distribuição de uma coluna vs. a execução
  anterior.
- [ ] **`freshness` por partição** — não só o máximo global da coluna.

Diretriz: manter cada check como uma classe `BaseCheck` com `run()` e, quando a regra
sabe apontar linhas, `violation()` — é o `violation()` que a coloca na quarentena.

Princípio de DQ: **validações reportam, transformações mudam** — manter essa
separação ao evoluir.

---

## 6. Núcleo / DX

- [ ] **Dry-run** — valida a config (schema, tipos de transformação/validator,
  colunas referenciadas quando possível) sem executar Spark.
- [ ] **Métricas por etapa** — tempo e contagem de linhas por transformação;
  expor em `PipelineResult`. (O Studio já mede isso por fora, no `StepRun` — §9.1;
  falta no `PipelineResult`, que é o que vale para quem usa como lib.)
- [ ] **Perfis (dev/staging/prod) no mesmo JSON** — overrides de path/options por
  ambiente, selecionados por param.
- [ ] **Testes unitários com dados mock** — cobrir transformações (`struct`,
  `collect`/`{{var}}`, `stop_if_empty`, `group_by`, outputs com `transformations`)
  e validators, com SparkSession local — base para CI. Ver §11.
- [ ] **Testes unitários estilo dbt (`unit_tests` do dbt 1.8+)** — declarar *dados de
  entrada fictícios* e a *saída esperada* de um Job e rodar a asserção sem tocar em
  fonte real: `given` (linhas mock por input) + `expect` (linhas esperadas). Cobre o
  que o item acima não cobre — hoje o teste é do framework, não **do pipeline que o
  usuário escreveu**. Valor: o autor de um Job testa a própria lógica (o `filter`
  certo, o `join` que não duplica, o `struct` com o payload esperado) em segundos e
  no CI, sem cluster nem dado de produção.
  Decisão a tomar: **depender do dbt** (traz o ecossistema, mas acopla o sparquet a
  outra ferramenta e ao modelo dele de *model/ref*) ou **replicar o conceito** em
  JSON/YAML próprio, como já foi feito com o SODA Core (o `check`/`thresholds` do
  `sparquet_cola` é a ideia do SODA sem a dependência). O precedente sugere replicar,
  mas medir antes o esforço de um runner de asserção com `assertDataFrameEqual`
  (PySpark 3.5+) e comparação sem ordem.
- [ ] **`$include` em qualquer nó do JSON** — hoje a diretiva só é expandida dentro de
  `transformations` (`sparquet/utils/includes.py:26`); em qualquer outra chave ela é
  ignorada em silêncio. O caso que motiva é o bloco `spark`: quem liga um conector
  precisa repetir `spark.jars.packages`, `spark.sql.extensions` e as configs de
  catálogo em **cada** JSON, porque via CLI (`Sparquet()` sem argumento) o JSON é o
  único lugar de onde essas configs podem vir. Como lib já existe saída —
  `Sparquet(spark={"configs": {...}})` vale para todas as execuções do processo — e
  em cluster o lugar natural do jar é o `spark-defaults.conf`/`--packages`; falta o
  caminho declarativo. Com a diretiva genérica: `"spark": {"$include":
  "shared/spark-lakehouse.json"}`. Serve também para `output` e `validations`
  repetidos entre pipelines. Precisa de entrada no catálogo do Studio (o editor tem
  que entender arquivo compartilhado) e de doc no `sparquet-web`.
- ✅ **`$include` aninhado** — `sparquet/utils/includes.py` expande em profundidade: um
  arquivo incluído pode conter novos `$include`, e o caminho deles é relativo ao
  **arquivo que os escreveu**, não ao pipeline principal (mover uma pasta de includes
  inteira não obriga a reescrever os caminhos de dentro). Ciclo (A inclui B que inclui
  A) levanta `ValueError` com o percurso (`a.json -> b.json -> a.json`) em vez de
  estourar a pilha; cadeia acima de 20 níveis é recusada. Coberto em
  `tests/utils/test_template_includes.py`.
- [ ] **Catálogo de erros** — mensagens de erro padronizadas e acionáveis
  (transformação desconhecida, coluna inexistente, etc.).

---

## 7. Performance

- ✅ **Input em temp view (`input_view`, string ou `{"name","type"}`)** — self-join / SQL sobre a entrada sem reler a base.
- ✅ **Broadcast join (`broadcast` no `join`)** — map-side, sem shuffle.
- ✅ **Doc: `filter`/`select` primeiro** — recomendação no guia de performance + convenção no CLAUDE.md.
- ✅ **Estratégia de leitura e escrita** — `docs/PIPELINE_SCHEMA.md` §*Estratégia de leitura
  e escrita*: ordem das alavancas de leitura (path direto na partição + `basePath`,
  `filter` como primeira transformação, `maxPartitionBytes`), leitura JDBC, contagem de
  arquivos na escrita, bucket por hash, skew e particionamento oculto do Iceberg.
  **Decisão registrada: não existirá `input.partition_filter`.** O Catalyst produz o
  mesmo plano físico a partir do `filter` (o predicado vira `PartitionFilters` +
  `PushedFilters` dentro do `FileScan`, verificável com `debug.explain`), então a chave
  seria só um segundo lugar para escrever a mesma condição — e um lugar a mais para ela
  divergir. Não há "ler sempre particionado" por default: particionamento é propriedade
  do layout dos dados, que o framework não descobre sozinho.
- ✅ **`repartition`** — transformação nativa: `num_partitions`, `columns` (aceita expressão
  SQL, não só nome de coluna), `coalesce` e `range` (`repartitionByRange`), com guards
  para toda combinação inválida. É a alavanca do problema de *small files*: arquivos
  gravados são pares *(task, diretório)*, então reparticionar pela mesma chave do
  `partition_by` dá exatamente um arquivo por valor de chave (o AQE funde partições
  vizinhas, nunca separa uma chave).
- ✅ **Hash para `partitionBy`** — duas formas, e a escolha é entre pruning e controle de
  arquivo. (a) *Bucket materializado*, em qualquer formato: `with_column` com
  `pmod(hash(col), N)` + `repartition` pela mesma coluna. **`pmod`, nunca `%`** — o
  `hash()` do Spark é Murmur3 de 32 bits com sinal, e `% N` cria diretórios
  `bucket=-17`; `hash(null)` é constante, então coluna nullable joga todos os nulos num
  bucket. **N vem do tamanho de arquivo alvo, não de primalidade**: Murmur3 já
  avalancha, `% 64` distribui igual a `% 61` — a regra do módulo primo vale para hash
  fraco ou identidade, não aqui. (b) *Transform do Iceberg* em `partition_by`:
  `bucket(16, id)`, `years/months/days/hours(col)`, via `writeTo` (DataFrameWriterV2).
  É a única forma que **mantém o pruning** — o Iceberg guarda a relação
  coluna→transform nos metadados, então `WHERE id = 'X'` poda os buckets sozinho,
  enquanto uma coluna `bucket` gravada à mão só é podada por filtro sobre `bucket`.
  **`bucketBy` do Spark não entra**: só funciona com `saveAsTable` (bucketing Hive),
  `save(path)` o recusa e o Delta não o suporta.
- ✅ **Leitura paralela JDBC** — o quarteto
  `partitionColumn`/`lowerBound`/`upperBound`/`numPartitions` é all-or-none e falhava
  com mensagem genérica do Spark; agora `partitionColumn` incompleto levanta
  `ValueError` nomeando o que falta, e o inverso (limites sem `partitionColumn`, que o
  Spark **ignora em silêncio** caindo para uma task/conexão) emite warning.
  `query` + `dbtable` e `query` + `partitionColumn` são recusados apontando a saída
  (subquery com alias em `dbtable`). `pushDownPredicate`/`Aggregate`/`Limit`,
  `fetchsize`, `sessionInitStatement` e `queryTimeout` expostos no catálogo do Studio.
- ✅ **Apache DataFusion Comet avaliado — funciona, fica opt-in** — plugin que troca
  operadores do plano físico por implementações nativas (Rust/Arrow). Não exige código
  no framework: `spark.configs` já passa `spark.plugins`,
  `spark.shuffle.manager` e o off-heap. Medido em Linux (Comet 1.0.0 + pyspark 4.1.1 +
  JDK 17) por `tests/io/integration/test_comet_spark.py`, que roda o mesmo pipeline em
  duas JVMs, com e sem as configs: as linhas saem idênticas e o plano acelerado fica
  nativo de ponta a ponta (`CometNativeScan`, `CometFilter`, `CometHashAggregate`,
  `CometExchange`/`CometNativeShuffle`, com `CometColumnarToRow` só na borda), contra
  zero nó `Comet` na execução sem elas. O que impede virar default está medido também:
  jar de 88 MB que precisa estar no `--driver-class-path` **antes** de a JVM subir
  (pelo builder dá `ClassNotFoundException: org.apache.spark.CometPlugin`), binário
  nativo só para Linux — em Windows/macOS o plugin se desabilita **em silêncio**, o
  pipeline passa e a aceleração simplesmente não acontece —, off-heap obrigatório e
  fallback por operador. Roda no CI no job `comet`; detalhes e configs em
  `docs/PIPELINE_SCHEMA.md`, "DataFusion Comet". O ganho de tempo também está medido,
  por `tests/io/integration/bench_comet.py` (mesmo pipeline nas duas configurações,
  JVMs separadas, aquecimento descartado, mediana de três repetições): em 40 milhões de
  linhas / 290 MB de Parquet em `local[4]`, a agregação caiu de 3,56s para 1,61s
  (**2,21x**) e o filtro com `count` de 1,02s para 0,83s (1,24x) — o ganho acompanha
  quanto do plano virou nativo, então o número é por forma de consulta, não do plugin.
  O benchmark fica fora do CI de propósito: asserção de tempo em runner compartilhado é
  teste instável.
- 🟡 **Pushdown** — já disponível: `collect` + `{{var}}` (IN literal → data skipping), `checkpoint`, `partitionColumn`/`fetchsize` (JDBC), `partition_by` / `compression` / `maxRecordsPerFile` via `options`, e a heurística de path do Delta corrigida.
- [ ] **Análise consolidada de opções de tuning** — já resolvido acima: `repartition`,
  `coalesce`, `partitionBy`, `bucketBy`, `maxRecordsPerFile`, *partition pruning*,
  *predicate pushdown*, *small files* e `shuffle` (partitions/skew). Falta mapear/expor/
  documentar e decidir o que vira opção declarativa vs recomendação de doc: `vacuum`,
  `optimize`, `z-order`, `clustering`/`clusterBy`, `compression`, `persist`/`cache`
  (hoje só `checkpoint`), broadcast automático (`spark.sql.autoBroadcastJoinThreshold`),
  *garbage collection* e `checkpointLocation` (streaming).
- [ ] **Skew de chave única na escrita** — nem bucket nem `repartition` resolvem: o valor é
  indivisível. As saídas hoje são manuais (*salting* com
  `concat(chave, '-', pmod(hash(rand()), 8))`, ou `maxRecordsPerFile`). Decidir se vale
  uma opção declarativa de salting ou se fica como recomendação de doc.
- [ ] **Doc pública da estratégia** — a seção nova está só em `docs/PIPELINE_SCHEMA.md`;
  falta o PR no `sparquet-web` (EN/PT/ES).

> Streaming (readStream/writeStream, `checkpointLocation`, output modes, Kinesis) é
> um eixo à parte — o modelo atual é batch. Decisão registrada em §4.

---

## 8. Empacotamento / distribuição / CI

- ✅ **CI de release** — GitHub Actions: `ci.yml` (testes em push/PR) e `publish.yml`
  (testes → build + `twine check` → publish). Release publicado → PyPI; execução
  manual → TestPyPI (ensaio). Trusted Publishing (OIDC), sem token manual. Ver
  [docs/DEPLOY_PYPI.md](docs/DEPLOY_PYPI.md) §8.
- ✅ **Matriz de versões** — Python (3.9/3.11/3.12) no job `test` e **linha do Spark**
  nos três jobs com JVM (`integration`, `services-tier`, `comet`): 4.1.1 (Scala 2.13,
  Python 3.12) e 3.5.9 (Scala 2.12, Python 3.11 — o pyspark 3.5 não é testado contra
  3.12 upstream). O custo real da matriz não é o YAML, é a coordenada: quase todo jar
  de conector é publicado por linha **e** por binário do Scala, e errar o sufixo não dá
  erro de versão, dá `NoSuchMethodError` no meio da execução. Por isso as coordenadas
  moram em `tests/io/integration/harness.py` (`_LINHAS`) e `services.py` (moldes com
  `{scala}`/`{spark}`, `por_linha` quando muda o **nome** do artefato e não só o
  sufixo, `incompativel_em` quando a incompatibilidade é de uma linha só), e o
  workflow só escolhe a versão do pyspark. Acrescentar uma linha nova é acrescentar
  uma entrada em `_LINHAS` e uma linha na matriz, nessa ordem.
- [ ] **Perna 3.5 do `services-tier` ainda com `continue-on-error: true`** — as
  coordenadas 2.12 (`opensearch-spark-30_2.12:1.3.0`,
  `elasticsearch-spark-30_2.12:8.16.1`, Mongo e Cassandra 2.12) foram verificadas como
  publicadas — o pom de cada uma diz contra que Spark foi compilada — mas foram
  escritas sem Docker na máquina, então quem as executou pela primeira vez foi o CI.
  A primeira execução falhou exatamente no ponto de atenção previsto, o par de
  conectores de busca: `ElasticsearchViaOpenSearchTest` é a saída para a linha em que
  o `elasticsearch-spark` não tem build, e na 3.5 ele tem — lá o conector do OpenSearch
  daquela linha recusa o servidor Elasticsearch com
  `Unsupported/Unknown OpenSearch version [8.16.1]. Highest supported version is [3.x]`.
  A classe agora se pula onde o jar nativo roda (o `ElasticsearchTest` é quem cobre o
  servidor ali). Falta o resto: duas execuções verdes seguidas e o `continue-on-error`
  sai.
- [ ] **Cache de jar por linha** — a chave do cache do Ivy e a do jar do Comet incluem a
  versão do pyspark de propósito. Uma chave só para as duas pernas faria cada uma
  restaurar o Scala da outra no mesmo diretório, e o sintoma seria erro de classe num
  job que não mudou.
- ✅ **Lint de Python no CI** — job `lint` com `ruff check` e configuração em
  `[tool.ruff.lint]` do `pyproject.toml`: `select = ["E","W","F"]`,
  `ignore = ["E501"]`, `target-version = "py39"`. O escopo é decisão, não preguiça —
  o conjunto default do ruff aponta 406 ocorrências neste repositório, dominadas por
  modernização de sintaxe (`UP006`/`UP045`/`UP035`) que reescreveria o código para
  3.10+ e quebraria o piso 3.9 declarado em `requires-python`. `ruff format` também
  fica fora: reformataria 56 arquivos de uma vez, enterrando o histórico num diff que
  ninguém revisou. Versão do ruff fixada no extra `dev` — linter que muda de regra
  sozinho deixa vermelho um PR que não tocou no código apontado.
- ✅ **`examples/` viaja dentro da wheel** — o Studio é instalado com `sparquet` como
  dependência, não clonando o monorepo, e os testes de round-trip do compilador liam
  as confs de exemplo por caminho relativo dentro do checkout: fora dele, a suíte
  silenciosamente não tinha o que testar. `[tool.setuptools.package-dir]` mapeia
  `examples/` para o pacote `sparquet.examples` e `sparquet.examples_path()` devolve o
  diretório empacotado (com fallback para o repositório, para quem trabalha no
  monorepo). Como `package-dir` fora da árvore impede o `packages.find`, a lista de
  pacotes virou explícita — e `tests/test_packaging.py` falha se um subpacote no disco
  não estiver declarado, que é o único jeito de uma lista manual não apodrecer. Do
  lado JS, `src/test/exampleConfigs.ts` resolve em ordem: `SPARQUET_EXAMPLES_DIR` ›
  pacote instalado (`python -c "import sparquet; print(sparquet.examples_path())"`) ›
  repositório; não achando, os casos se pulam — exceto onde `SPARQUET_EXAMPLES_REQUIRED`
  está ligado, que é o CI, porque uma suíte que se pula em CI não prova nada. A amarração
  de versão vem de graça: as fixtures são as do `sparquet` que está instalado.
- ✅ **Studio desacoplado do monorepo** — falta só mover o diretório. O framework virou
  dependência com faixa de versão (`server/requirements.txt`, declarada uma vez em
  `server/compat.py`), `/health` responde `framework_supported`/`framework_message` e
  **Settings → Local runner** mostra a frase quando a versão instalada está fora da
  faixa — divergência de versão tem que ser uma linha de texto, não uma execução que
  falha estranho meia hora depois. O runner acha o framework por import; o `sys.path`
  para a raiz do repositório só entra quando existe um `sparquet/` lá, e
  `SPARQUET_FRAMEWORK_PATH` cobre um checkout em qualquer lugar. `sparquet-studio/
  .github/workflows/ci.yml` já existe (inerte no monorepo, porque o GitHub só lê
  workflows na raiz) com as duas camadas, web e runner, instalando o framework pinado
  para as fixtures. O procedimento — `git subtree split`, o que verificar antes de
  anunciar, o que remover do lado do framework — está em
  [sparquet-studio/SPLIT.md](sparquet-studio/SPLIT.md). O que **não** vai junto são as
  `examples/`: elas chegam pelo pacote pinado, que é exatamente o acoplamento que deve
  existir.
Base atual: versão única via `__version__` (pyproject `dynamic`); publicação no PyPI
documentada e automatizada via CI.

---

## 9. Studio — plataforma

O runner (`sparquet-studio/server/`) é o único componente que **executa configuração
arbitrária**: tudo aqui é, no fim, postura de segurança e de operação. Ele fica preso a
`127.0.0.1` e o token é senha, não identificador.

### 9.1 Execução, histórico e canvas

- ✅ **Agendar Job e Pipeline no projeto local** — o runner dispara sozinho, e o
  `launched=SCHEDULED` que `server/history.py` já previa finalmente tem quem o escreva.
  `server/scheduling.py` (só biblioteca padrão) faz o parse e a aritmética de relógio como
  funções puras — mesma separação de `monitoring.py`, e por isso testam sem thread, sem
  banco e sem Spark. O que ficou de pé:

  1. **Onde a agenda mora.** No registro da biblioteca, ao lado do Job: bloco `schedule`
     com `cron` de cinco campos, `timezone`, `enabled` e `runAs`. Vai para o git junto do
     Job, aparece no diff da revisão e um clone do repositório já nasce agendado. Nenhum
     quinto SQLite, nenhuma segunda fonte da verdade para manter em dia. Expressão de seis
     campos é recusada em vez de interpretada — ler `0 0 6 * * *` como minuto-zero-da-hora-zero
     roda de minuto em minuto um job diário — e macro (`@daily`) também.
  2. **Quem dispara.** Thread daemon `schedule-sweep` no runner, no molde da varredura de
     monitores: acorda a cada `SPARQUET_STUDIO_SCHEDULER_INTERVAL` segundos (30 por
     padrão), resolve o que venceu e chama **o mesmo caminho de `/run`** — IAM, admissão de
     crédito, trava de execução e histórico idênticos aos da execução manual. Pipeline passa
     por `/run/flow/stream` drenado sem ninguém lendo, porque é o `finally` do gerador que
     solta a trava e liquida os créditos. `SPARQUET_STUDIO_SCHEDULER=off` desliga.
  3. **Identidade sem principal de serviço.** `runAs` nomeia um usuário existente e a
     execução carrega as permissões dele; runner sem usuário nenhum roda como o principal
     do token, como todo o resto. Resolveu §9.2 por outro caminho, e de propósito: uma
     identidade que não é de ninguém é uma identidade que ninguém percebe que ainda tem
     acesso. Desabilitar a pessoa para a agenda.
  4. **Não rodar duas vezes, não rodar em avalanche.** O runner guarda só a âncora em
     memória — ela diz o que *este* processo fez, e persisti-la seria afirmar algo sobre
     uma máquina que estava desligada. Sem catch-up além da janela de
     `SPARQUET_STUDIO_SCHEDULER_GRACE` (900 s): uma noite de execuções perdidas vira uma,
     nunca doze. Execução sobreposta é **pulada, não enfileirada** — o 409 vira o erro do
     relatório e a ocorrência se gasta.
  5. **Na tela.** `SchedulePopover` no cabeçalho do editor de Job e de Pipeline (expressão
     validada enquanto se digita, presets, timezone, `runAs`, pausar sem apagar) e a aba
     **Monitoring → Schedules** com próxima, última, pausada e "rodar o que venceu", que
     dispara a varredura sem deslocar nada. Agenda ilegível aparece com o motivo em vez de
     sumir: essa linha é um Job que nunca roda.
  6. **O limite, dito em voz alta.** A varredura é de um processo só; dois runners na mesma
     pasta disparam os dois e nada aqui os coordena. Laptop fechado não roda às 6h. É a
     fronteira com o **agendamento gerenciado** do `sparquet-cloud` (§10) — e o formato da
     agenda é o mesmo nos dois, para que subir seja mover o arquivo e não reescrevê-lo.

  7. **Agendar é executar, e a permissão é a de executar.** A agenda mora dentro do
     registro da biblioteca, então ela chegava pela rota de `workspace:Write` — quem podia
     *editar* um Job podia fazê-lo *rodar* todo dia às 6h sem ter `run:Execute`. Agora
     `PUT /workspace/{kind}/{id}` compara a agenda normalizada antes e depois
     (`cron`, `timezone`, `enabled`, `run_as`): salvar sem tocar nela não pede nada; criar,
     apagar, pausar ou re-apontar exige a mesma autorização de uma execução daquele Job ou
     Pipeline. `runAs` apontando para outra conta pede ainda `iam:ManageUsers` **e** que a
     conta nomeada passe ela própria no teste de execução — senão dava para criar uma
     agenda que falha calada toda madrugada. A checagem acontece **antes** da escrita, para
     que agenda recusada deixe o arquivo exatamente como estava; o runner sem usuário
     nenhum segue livre. O `SchedulePopover` desabilita os campos e diz o porquê quando
     falta permissão, mas quem decide é o runner. 17 testes em `test_schedule_guard.py`.

  `GET /schedules` (`workspace:Read`) e `POST /schedules/evaluate` (`run:Execute`),
  documentados em `server/README.md`. 59 testes novos no runner (`test_scheduling.py` 39,
  `test_schedule_routes.py` 20) e 20 no Studio (`cron.test.ts` 12, `scheduling.test.ts` 8,
  mais 8 em `SchedulesPanel.test.ts`).

- ✅ **`input_view` pelo Studio** — fechado; o que segue é o desenho original, mantido
  porque as três decisões no fim dele são o que o item resolveu. **Como ficou:**
  `PipelineConfig.input_view` é chave do JSON (string ou `{"name","type"}`, nome validado
  como identificador simples no parse), `Pipeline.__init__` cai para ela quando o
  argumento é `None` (**precedência argumento > JSON**), a CLI ganhou de graça. No Studio
  o campo vive no Inspector do nó de input ("Register as a temp view" + escopo), o
  compilador emite e relê (`toJson.ts` / `toGraph.ts`, round-trip testado) e o lint
  cobre as três armadilhas: nome com ponto é **erro**, escopo `global` **avisa** que a
  sessão é reusada entre execuções e outro Job com o mesmo nome atropela esta entrada, e
  view que ninguém lê **avisa** que é cache pago à toa. Documentado em
  `docs/PIPELINE_SCHEMA.md` e no prompt da IA. Falta só o PR no `sparquet-web` (EN/PT/ES).
  Testes: `tests/test_input_view_config.py` (9) e `lint.test.ts` (6).

  *Desenho original:* hoje `input_view` só existe como **argumento Python**
  (`Sparquet(input_view=...)`, `run(...)`, `run_from_dict(...)`, `sparquet/framework.py:49`),
  nunca como chave do JSON. O Studio compila JSON e o runner chama
  `framework.run_from_dict(body.pipeline, params=...)` (`server/main.py:1554`) sem esse
  argumento, então não há como chegar lá pelo editor. **Impacto no framework** (as duas
  primeiras linhas mexem no contrato do JSON):

  1. `PipelineConfig` ganha o campo `input_view: Optional[Union[str, Dict[str, Any]]] = None`
     (`sparquet/core/config.py:253`) e `from_dict` passa a lê-lo. É chave nova no JSON —
     precisa de doc em `docs/PIPELINE_SCHEMA.md` e no `sparquet-web` (EN/PT/ES).
  2. `Pipeline.__init__` (`sparquet/core/pipeline.py:113`) hoje recebe o valor só por
     argumento. Passa a cair para o do config quando o argumento for `None` — **precedência
     argumento > JSON**, igual à de `columns`/`input_df`, para que quem usa como lib não
     perca o controle. Nada quebra: JSON sem a chave continua com o comportamento atual.
  3. CLI ganha o caminho de graça (passa por `from_dict`); a API Python continua igual.
  4. Studio: campo no catálogo do nó de input (`src/catalog/`), `compileGraph` emitindo e
     `pipelineToGraph` lendo de volta — **o round-trip é o risco real**: chave que o
     compilador não conhece some ao reabrir o Job no canvas, e o usuário perde a
     configuração sem aviso. O teste de round-trip tem que cobrir.
  5. Opcional, e só depois de 1–4: expor também como **opção de execução** no diálogo de
     run (`run_from_dict(..., input_view=...)`), para experimentar sem gravar no Job. Sozinha
     essa opção não serve — o valor não ficaria no JSON, e a mesma conf rodada pela CLI se
     comportaria diferente.

  Custo estimado: pequeno no framework (um campo e uma precedência), médio no Studio
  (catálogo + compilador nos dois sentidos + round-trip).

  Três decisões que travam o item se ficarem implícitas: (a) **nome conflitante** —
  registrar uma temp view com o nome de uma tabela existente sombreia a tabela dentro
  daquela sessão, e um `sql` que parecia ler `orders` do catálogo passa a ler a entrada;
  o nome precisa ser identificador simples (sem ponto), validado no lint do Studio e não
  em runtime; (b) **escopo `global`** vive na SparkSession, e o runner reusa a sessão
  entre execuções — dois Jobs com a mesma global view se atropelam, então ou o runner
  prefixa o nome por execução, ou `global` fica fora do que o Studio oferece; (c)
  **cache** — o `input_view` registra *e cacheia* a entrada
  (`sparquet/core/pipeline.py:191`), o que numa base grande é memória que ninguém
  pediu; o campo no catálogo tem que dizer isso, ou o cache vira opção à parte. Aceite:
  round-trip do compilador cobrindo a chave, lint do nome, doc em
  `docs/PIPELINE_SCHEMA.md` e PR no `sparquet-web` (EN/PT/ES).
- ✅ **Histórico de execuções** — `PipelineRun`/`JobRun`/`StepRun` persistidos em SQLite
  (`server/history.py`, `ExecutionRepository`), sobrevivem a reiniciar o app.
  `GET /runs` (lista) e `GET /runs/{id}` (detalhe) servem o `ExecutionHistoryPanel` no
  Studio, decoupled do estado efêmero de `useEditorStore`/`usePipelineEditorStore`.
  Verificado em `server/test_history.py`.
- ✅ **Status por etapa no histórico** — `StepTracker` grava cada etapa
  (input/transformação/validação/output) como `StepRun` durante a execução; num Job com
  falha o step que quebrou fica `FAILED` com a mensagem de erro persistida, os demais
  Jobs de um Pipeline ficam `SKIPPED` — responde "onde quebrou da última vez" sem
  reexecutar.
- ✅ **Estado de cada caixa ao abrir uma execução** — abrir um Job pinta cada caixa com
  o status da última execução (ou da execução exata escolhida no histórico), com uma
  tarja dizendo *qual* run está na tela; o Inspector mostra duração, linhas e erro
  daquela caixa naquele run. Num Pipeline o mesmo vale para os stages, e abrir um stage
  cai no Job **fixado na execução que aquele stage rodou** — o drill-down estilo
  Databricks. Mapeamento em `lib/runner/stepNodes.ts` (step → caixa, `validation_sink`
  por `role`), `lib/runner/stageRuns.ts` (stage → `job_run`, casado por `job_id`, nunca
  por posição) e `lib/runner/runView.ts` (qual execução carregar).
- ✅ **Cancelar a execução de verdade** — o Stop chama `POST /runs/{id}/cancel`: uma
  flag interrompe o fluxo na próxima fronteira de estágio e `cancelAllJobs()` aborta o
  que o Spark estiver computando (sem isso uma escrita longa terminava mesmo depois do
  Stop). `cancelled` virou status de primeira classe — servidor, histórico, canvas e
  painéis o tratam como encerramento a pedido, nunca como falha.
- ✅ **Histórico de logs por execução** — cada linha que a execução imprimiu
  (framework, JVM, `stdout`) é persistida em `run_log` a partir da mesma fila que
  alimenta o SSE, então o histórico guarda exatamente o que o usuário viu ao vivo.
  `GET /job-runs/{id}/logs` pagina por `seq` (não por offset, que releria linhas de um
  run ainda em andamento); teto de 3000 linhas por execução, com uma linha `WARNING`
  dizendo quantas ficaram de fora.
- ✅ **Nova apresentação do histórico** — o painel virou lista de execuções com faixa de
  status das últimas 14 (altura proporcional à duração) e uma ação de canvas por linha;
  o detalhe saiu do acordeão apertado da lateral e virou um diálogo com os estágios, os
  passos e os logs completos, com filtro por nível e por origem, busca e paginação.
  Erros longos passam a viver em cartão rolável (`ErrorCard`), na lateral e nas caixas
  do canvas.
- ✅ **Fluxo e histórico lado a lado, estilo Databricks** — abrir um Job ou um Pipeline
  não pinta mais a última execução por cima do canvas: o centro da tela virou uma área
  de trabalho com abas (`Flow | JSON | Runs` no Job, `Flow | Runs` no Pipeline). A aba
  **Runs** é a tabela de execuções — status, *run id*, início, duração, *run as*,
  *launched* — e o *run id* é o link do drill-down (`Job/Pipeline → run id`), que abre o
  detalhe da execução e, de lá, pinta o canvas. O JSON deixou de existir só na lateral
  estreita: a mesma superfície (preview/edição Monaco) roda no centro. Detalhes de
  execução ganharam *job id*, *job run id*, *run as*, *launched*, início, fim, duração,
  status e **lineage** (o que a execução leria e escreveria, lido do JSON submetido —
  logo, existe mesmo em run que morreu antes de escrever). No servidor:
  `pipeline_run.run_as`/`launched`, `job_run.lineage` e `history.lineage_of()`.
- ✅ **Histórico aponta para a versão do JSON, não só para o Job** — cada execução grava
  a impressão digital do que rodou de verdade: `job_run.config_hash` (`sha256:<hex>` do
  JSON canônico — chaves ordenadas, sem espaços — já com `{param}` resolvido, pela mesma
  razão que o lineage resolve) e `job_run.config` com o JSON íntegro até 512 KB
  (`history.config_version()`, `MAX_STORED_CONFIG_BYTES`). Acima disso guarda-se só o
  hash, que continua respondendo "estas duas execuções rodaram o mesmo JSON?". O detalhe
  da execução mostra a versão abreviada e busca o JSON sob demanda em
  `GET /job-runs/{id}/config` — a config não entra na listagem porque é maior que a
  linha que descreve o run. Migração acrescenta as colunas em base existente; run
  anterior à mudança aparece como versão desconhecida, não como versão errada.
- ✅ **Retenção / rotação do SQLite de histórico** — expurgo em dois estágios, aplicado
  pelo runner uma vez por dia e sob demanda em `POST /runs/purge` (com `dry_run`).
  Passados `DETAIL_DAYS` (30) a execução perde logs, steps e a cópia do JSON, mas
  **mantém a linha** com status, tempos, contagens e `config_hash` — série histórica
  e comparação por impressão digital continuam de pé. Passados `MAX_DAYS` (365) a
  linha some, e só com `SPARQUET_STUDIO_HISTORY_DELETE` ligado. Nada expira duas
  coisas: execução fixada (`pinned`, marcada no histórico, ação IAM `history:Pin`) e
  as `KEEP_RUNS` (10) mais recentes de cada Job e de cada Pipeline. O ledger de
  créditos é outro banco e não é tocado — expurgar histórico nunca reescreve o que
  foi cobrado. `VACUUM` só quando saiu volume que justifique reescrever o arquivo.
- ✅ **Histórico de execução fora do Studio** — o framework roda em qualquer lugar sem
  depender de nada, e era justamente por isso que as execuções que mais importam
  (job noturno no Databricks, DAG no Airflow, `sparquet.cli` numa VM) não deixavam
  rastro nenhum. Agora o framework reporta as próprias execuções
  (`sparquet/observability/`) e elas aparecem no histórico como qualquer outra:
  mesmas etapas, mesmos logs, mesmas telas.

  Desligado por padrão e de graça quando desligado: sem `SPARQUET_HISTORY_URL` (e
  sem sink registrado em código) nada é instanciado e `Pipeline.run` não muda.
  Ligado, a execução é recolhida dos registros estruturados que o framework **já
  emite** e enviada **uma vez, no fim** — uma requisição por execução, não uma por
  etapa — como um documento JSON (`schema: "sparquet.run/1"`) por `urllib` da
  biblioteca padrão, sem dependência nova. Falha também é enviada, que é a execução
  que mais interessa. Enviar nunca afeta o pipeline: receptor fora do ar, token
  errado ou rede caída viram `warning` e o mesmo `PipelineResult`.

  Do lado do runner, `POST /runs/ingest` (ação IAM `history:Ingest`) reproduz os
  registros pelo **mesmo** `StepTracker` e pelo mesmo gravador de log de uma
  execução local — um caminho só, sem risco de as duas divergirem. A execução fica
  marcada `launched="external"` (quem lê distingue o que este runner executou do que
  apenas lhe contaram), os tempos vêm do documento e não do relógio daqui, e ela não
  consome crédito: o processamento não foi nosso. Identidade
  (`SPARQUET_HISTORY_JOB_ID`/`_WORKFLOW_ID`/`_PIPELINE_ID`/`_RUN_AS`/`_TAGS`) e
  `Sparquet.register_history_sink(sink)` completam a configuração. Framework 0.7.0.
- ✅ **SparkSession quente no start do runner** — a primeira requisição que precisa de
  Spark paga a JVM subindo, os jars sendo resolvidos e a sessão sendo configurada:
  dezenas de segundos antes de qualquer trabalho, cobrados de quem clicou Run primeiro.
  Com `SPARQUET_STUDIO_WARM_SPARK=on` o runner faz isso no start, numa thread daemon,
  em paralelo com alguém abrindo a interface (`_warm_spark` em `server/main.py`,
  `server/test_warm_spark.py`). Aquece **com o que a biblioteca declara**, não com uma
  sessão pelada: jar de conector e extensão de SQL só valem na *criação* da sessão,
  então uma sessão pelada seria derrubada e reconstruída pela primeira consulta Delta e
  a espera voltaria exatamente para onde deveria ter sumido. O runner lê o
  `spark.configs` de cada Job salvo e monta a união — as chaves de lista
  (`spark.jars.packages`, `spark.sql.extensions`, …) são fundidas, então uma biblioteca
  com um Job Delta e um Job Iceberg aquece uma sessão que abre os dois. Fica **desligado
  por padrão**, ao contrário do expurgo do histórico, e a assimetria é deliberada:
  importar o módulo não pode custar uma JVM — os testes o importam, e qualquer
  ferramenta que inspecione o app também. Falha de aquecimento é logada e engolida, que
  é o que mantém de pé um runner numa máquina sem Java. Ganha junto o marcador de
  sintaxe do editor SQL (§9.5), que só verifica quando já existe sessão.

### 9.2 IAM — identidade e permissão

- ✅ **Usuários, login e permissionamento estilo IAM** — o runner ganhou identidade
  (`server/auth.py`, SQLite próprio em `SPARQUET_STUDIO_AUTH_DB`). Dois modos, decididos
  por existir usuário ou não: sem usuário nada muda — o token compartilhado é a
  identidade e ninguém fica trancado do lado de fora ao atualizar; criado o primeiro
  usuário, o runner passa a exigir sessão **além** do token. Política no formato
  `{effect, actions, resources}` com ação `service:Verb` (`workspace:Write`,
  `run:Execute`, `iam:ManageUsers`) e recurso `kind/id`, `*` em qualquer posição, **deny
  explícito vence** e o padrão é negar; papéis nativos
  `admin`/`editor`/`operator`/`viewer`. Senha em scrypt (com PBKDF2 de reserva), sessão
  guardada como hash — cópia do arquivo não é um conjunto de logins vivos. Desabilitar
  conta e trocar senha derrubam as sessões abertas, e o último administrador ativo não
  pode ser rebaixado, desabilitado nem removido. Toda rota declara a ação que exige;
  `run as` deixou de ser texto livre quando há usuário — quem executou é fato, não
  rótulo. No cliente: `store/auth.ts`, tela de login e a seção **Access & IAM** em
  Settings. Verificado em `server/test_auth.py`.
- ✅ **Recuperação de senha por código de uso único** — em vez de e-mail, porque o
  runner não tem servidor de e-mail e não deveria ganhar um. Um administrador
  (`iam:ManageUsers`, em **Settings → Access & IAM → Recovery code**) ou quem opera a
  máquina (`python server/auth.py recovery-code <user>`) emite o código; a pessoa o
  gasta na tela de login em *I have a recovery code* e escolhe a própria senha. Tabela
  `recovery` no SQLite de identidade guardando apenas o SHA-256 do código; emitir
  invalida o anterior não usado; vale uma vez e expira em
  `SPARQUET_STUDIO_RECOVERY_MINUTES` (padrão 30); resgatar derruba todas as sessões da
  conta; a senha nova é validada **antes** de o código ser queimado (senha curta não
  gasta o código); conta desabilitada não é recuperável; e toda recusa tem a mesma
  mensagem — o endpoint não diz se o código era desconhecido, expirado ou já usado.
  Rotas `POST /auth/users/{id}/recovery` e `POST /auth/recover` (esta não exige sessão,
  já que vem da tela de login, nem token quando o runner tem usuários — ver o item do
  laço fechado abaixo).
- ✅ **O laço fechado entre o token e a tela de login** — o token compartilhado se digita
  em **Settings → Local runner**, Settings fica atrás do login, e o login exigia o
  token: trocar o token do runner (ou reiniciá-lo sem `SPARQUET_STUDIO_TOKEN`, já que
  cada processo sorteia o seu) trancava todo mundo fora da única tela capaz de receber o
  valor novo, sem saída pela interface — só editando o `localStorage` na mão. Três
  mudanças, em `server/main.py` e `components/auth/LoginGate.tsx`:
  1. **Sessão vale como token.** `require_token` aceita uma sessão viva no lugar do
     token. Compra a mesma coisa — viaja em header próprio, que o navegador não anexa
     cross-origin sem o preflight que o check de `Origin` recusa — e é a credencial que
     a pessoa logada de fato tem. Sessão presente mas inválida responde
     `SESSION_EXPIRED_HELP`, não "falta token": mandar quem tem sessão vencida procurar
     um token é mandar procurar a coisa errada. O principal resolvido na porta é
     reaproveitado por `current_principal`, sem segunda leitura do store.
  2. **As três rotas de saída deixam de exigir token quando há usuário.**
     `GET /auth/status`, `POST /auth/login` e `POST /auth/recover` passam por
     `require_token_unless_users`: o check de `Origin` continua, a senha vira a parede.
     Runner sem usuário segue exigindo o token nas três — não há mais nada a exigir.
  3. **Rate limit no que ficou alcançável sem segredo.** `_LoginThrottle`, janela
     deslizante em memória, conta **só falhas**, por chamador **e** por conta (uma delas
     sozinha tem buraco: por conta, um chamador varre todas; por IP, uma botnet espalha).
     Acerto esquece as falhas anteriores. `SPARQUET_STUDIO_LOGIN_ATTEMPTS` (10) por
     `SPARQUET_STUDIO_LOGIN_WINDOW` segundos (300); estouro responde `429` com
     `Retry-After`. No `/auth/recover` conta só por chamador, porque o código não nomeia
     conta nenhuma antes de ser resgatado.

  A tela de login ganhou também o campo do token, recolhido atrás de *The runner is
  asking for a token*, como rede de segurança para runner em modo token-only ou token
  rotacionado com o navegador segurando o anterior. Verificado em
  `server/test_login_guard.py` (23 testes).
- ✅ **Step-up para emitir código de recuperação** — emitir um código é, na prática,
  virar a conta de outra pessoa; por isso a rota exige **a senha de quem está pedindo**,
  além da sessão e da permissão `iam:ManageUsers`. Sessão roubada ou máquina destravada
  não basta. A senha checada é a do emissor, nunca a do alvo (`POST
  /auth/users/{id}/recovery`, corpo `{"password": ...}`).
- ✅ **Papéis customizados pela interface** — **Settings → Access & IAM → Roles** cria,
  edita e remove papéis com um editor de statements (efeito, ações agrupadas por
  serviço, recursos). Os papéis nativos aparecem como somente-leitura e são reescritos a
  cada start (corrigir a política no código corrige em toda instalação); os customizados
  nunca são tocados por upgrade. Um papel ainda em uso — por usuário **ou** por equipe —
  não pode ser removido, porque a redução de permissão seria invisível para quem o
  detém. `GET /auth/policy` publica o vocabulário de ações para a UI não ter uma cópia
  desatualizada.
- ✅ **Equipes (permissão e cobrança por grupo)** — `team` é ao mesmo tempo a unidade de
  cobrança (§9.3) e uma segunda fonte de papéis: os papéis da equipe **somam** aos
  pessoais, e um `deny` de qualquer lado continua vencendo — equipe concede, nunca tira.
  Todo usuário pertence a uma equipe; a equipe padrão não pode ser removida e apagar uma
  equipe move os membros de volta para ela em vez de deixá-los órfãos. **Settings →
  Access & IAM → Teams**; rotas `/auth/teams*`; `team` no cadastro e na edição de
  usuário.
- ✅ **Escopo por Workflow/Pipeline/Job nas rotas de execução** — `/run`, `/run/stream` e
  `/run/flow/stream` não podem ser autorizados pela dependência de rota (ela roda antes
  de o corpo ser lido, e é o corpo que diz o alvo). Passaram a autorizar **dentro do
  handler**, depois do parse, via `_authorize_run`: basta um `allow` entre
  `workflow/<id>`, `pipeline/<id>` e `job/<id>`, mas um `deny` em qualquer um deles
  encerra — senão "pode rodar tudo do w1, menos o j1" seria contornável por um grant mais
  largo. Job não salvo não nomeia nada e cai em `*`. Verificado em
  `server/test_run_scope.py`.
- ✅ **UI ciente de permissão** — `lib/auth/usePermission.ts` (`usePermission`,
  `usePermissionReason`) assina o *principal*, não a função `can` — cujo endereço não
  muda e por isso nunca re-renderizava a tela de quem teve o papel alterado com o Studio
  aberto. Botões de executar/validar/parar consultam `run:Execute`/`run:Validate`/
  `run:Cancel` **no recurso certo** (`job/<id>`, `pipeline/<id>`), e criar/duplicar/
  renomear/apagar consultam `workspace:Write`/`workspace:Delete` no Dashboard, no
  Workflow e nos Templates. Todo controle desabilitado carrega no `title` o motivo — um
  botão cinza sem explicação é o pior dos dois mundos. Isto **não** é fronteira de
  segurança: o runner reavalia a mesma política e responde 403 de qualquer jeito.
- ✅ **Log de auditoria** — `server/audit.py` + middleware: toda requisição que muda
  estado vira uma linha com quem, o quê, sobre qual recurso e com que desfecho —
  inclusive as **recusadas**, que são as que interessam ler. Filtros por ator,
  recurso, desfecho, prefixo de ação (`iam:*`) e data em `GET /audit`, atrás de
  `iam:ReadAudit`; corpo de requisição nunca é gravado, só os campos nomeados. Na
  interface: **Access & IAM › Audit log**.
- ✅ **Dono e concessões por recurso, e permissão padrão para o que acaba de nascer** —
  papel responde "esta pessoa pode executar alguma coisa"; concessão responde "quais".
  São duas camadas de propósito (`server/grants.py`, `src/lib/iam/grants.ts`):
  `grants.evaluate()` devolve `Decision(governed, level, owned, source)` percorrendo a
  cadeia de escopo, nomear um dono passa a **governar** o recurso, e `deny` explícito
  fecha de novo o que uma concessão mais larga tinha aberto. `OWNABLE_KINDS` é
  `dataset`, `job`, `pipeline`, `workflow` e `secret`. O que estava faltando era o
  começo: recurso criado nascia sem dono, então ou ficava aberto a todos ou dependia de
  alguém lembrar de configurá-lo. Agora nasce com dono (quem criou) e leitura/escrita
  para o time, controlado por `SPARQUET_STUDIO_NEW_RESOURCE_DEFAULT`
  (`creator+team`, o default; `creator`; `off`) — com `read` em vez de `write` no time
  quando é `secret`. Quatro guardas: pessoa real (token compartilhado não vira dono),
  id novo, nada acima já governando, e no catálogo só o que é novo.
- ✅ **IAM dividido em seções com endereço, e o log de auditoria em página própria** —
  a tela empilhava cinco assuntos sem relação numa coluna só (pessoas, papéis, regras
  por recurso, simulador e o log), e o log ficava embaixo: toda visita para cadastrar
  um usuário buscava cem eventos que ninguém tinha pedido. Viraram cinco rotas
  (`/access`, `/access/roles`, `/access/rules`, `/access/simulator`, `/access/audit`)
  sob o mesmo cabeçalho, com uma faixa de abas nova — `components/layout/PageTabs.tsx`,
  um `nav` de `NavLink` e **não** um `tablist`, porque aba que navega mente para o
  leitor de tela sobre o que acontece ao ser apertada. São rotas, e não estado local,
  justamente porque o log é a coisa que uma pessoa manda para outra por link. O log
  ganhou a largura da página e o que faltava para ser lido: janela de tempo (`since`),
  limite de linhas, busca livre sobre o que já veio (filtrar no cliente, sem uma
  requisição por tecla), linha que abre mostrando `detail`/`ip`/`roles`/`resource`, e
  export CSV do que está na tela — no formato RFC 4180 que o framework lê e escreve
  (`lib/utils/csv.ts`).
- [ ] **SSO / OIDC e senha gerenciada fora do runner** — para instalação corporativa,
  onde criar mais um usuário/senha local é justamente o que não se quer.
- [ ] **Expiração e rotação de sessão por política** — hoje é só
  `SPARQUET_STUDIO_SESSION_HOURS`, global; falta prazo por papel, revogação de uma
  sessão específica e lista de sessões abertas por usuário.
- [ ] **Segundo fator** — o step-up de recuperação já mostra o padrão; falta TOTP para
  login e para operações sensíveis.
- [ ] **Testes da camada HTTP** — **parcial**: `requires(...)` agora carrega `action` e
  `resource` na própria closure, e `server/test_monitoring.py` varre `app.routes`
  afirmando que toda rota de monitoramento declara permissão e que `PATCH`/`DELETE`
  exigem `Manage`. Falta estender a varredura ao app inteiro — hoje nada afirma que é o
  `PUT /workspace` que cobra `workspace:Write` — com uma tabela esperada de rota → ação
  revisável num diff, e não só a checagem de que *alguma* permissão existe. Ver §11.
- [ ] **Principal de serviço** — quem chama `/run` sem ser uma pessoa (o agendador de
  §9.1, um CI, um script de madrugada) só tem o token compartilhado: tudo-ou-nada, sem
  papel, sem equipe que pague a conta e sem identidade própria no audit log, onde a
  conta aparece como o literal `token`. Falta um principal não-humano com papel, equipe,
  token próprio, revogação individual e rastro. É pré-requisito do agendamento e o que
  hoje impede recusar um token antigo sem trocar o de todo mundo.
- [ ] **Dono dito em dois lugares** — `OwnerPicker`
  (`src/components/catalog/OwnerPicker.tsx`) grava o dono de dataset, Job, Pipeline e
  Workflow como **principal do IAM** — quem detém todo privilégio sobre o objeto e não
  pode ser barrado por um deny. A ficha do catálogo tem, em paralelo, um campo `owner`
  de **texto livre** (`src/lib/datacatalog/catalog.ts`), que é o que a maioria preenche
  primeiro porque está na tela onde se documenta. São duas respostas para "quem responde
  por isso" e nenhuma é autoritativa. Decidir: ou o texto livre vira rótulo de exibição
  quando existe dono de IAM, ou a ficha passa a oferecer o picker e o texto sobra para
  quem não é usuário deste runner (um time terceiro, um e-mail de plantão). Ver §9.5.

### 9.3 Billing e créditos de execução

Modelo atual, implementado em `server/credits.py` (SQLite próprio em
`SPARQUET_STUDIO_CREDITS_DB`).

**Escopo: para execução local está fechado; o produto de cobrança é do
`sparquet-cloud`.** Quem roda o runner na própria máquina não tem pendência aqui —
`spark.master` local não custa crédito nenhum, e a localidade sai da configuração do
Job e nunca de um campo da requisição, então não há como se declarar local; a
medição vem desligada por padrão (`SPARQUET_STUDIO_CREDITS` ausente registra sem
bloquear), e nesse modo nenhuma rota de execução exige saldo. Tudo o que segue em
aberto abaixo — planos, assinatura, fatura, gateway, excedente, painel multi-equipe,
preço por tamanho, conciliação com o custo do cluster — é produto **hospedado** e vai
para o backlog do `sparquet-cloud` (§10). O motivo não é organizacional: no
self-hosted quem opera a máquina edita o SQLite de créditos, então cobrança de
verdade exige um serviço de billing fora do runner. O que fica neste repositório é o
mecanismo — razão, franquia, reserva, rateio por dimensão e o slot
`SPARQUET_STUDIO_CREDITS_PROVIDER` — para o cloud consumir sem fork. Os itens abaixo
ficam listados para não se perderem, marcados com o escopo a que pertencem:

- ✅ **1 crédito por escrita bem-sucedida, não por Job** — a régua saiu de "um Job = uma
  moeda" para o que o usuário efetivamente obteve. A contagem vem de
  `PipelineResult.output_metrics`, que só tem entrada para destino que terminou de
  escrever: **erro não gasta crédito**, e um Job que grava três destinos custa três.
  Preço configurável em `SPARQUET_STUDIO_CREDITS_PER_WRITE`.
- ✅ **Cobrança depois da execução (era na admissão)** — antes de começar só se checa o
  mínimo — `precheck` recusa com **HTTP 402** a equipe que não pode pagar nem uma escrita
  —, e o débito real acontece com o run terminado, quando o número de escritas é fato.
- ✅ **Só execução remota custa** — a localidade sai da configuração do Job, nunca de um
  campo da requisição, senão bastaria o cliente declarar-se local: `spark.master` (ou
  `spark.configs["spark.master"]`) começando com `local` é grátis; `spark.remote` cobra
  mesmo com master local ao lado; `yarn`/`spark://`/`k8s://` cobram; e runner rodando em
  Databricks/EMR/Dataproc/Synapse cobra todo Job, qualquer que seja o master.
- ✅ **40 escritas grátis por mês (`SPARQUET_STUDIO_CREDITS_FREE_MONTHLY`)** — por
  período `YYYY-MM` em UTC. A franquia é gasta **antes** do saldo concedido, vira zero
  sozinha na virada do mês e **não acumula** — é franquia, não estoque.
- ✅ **Conta por equipe, equipe com N usuários** — quem paga é o time, não a pessoa
  (`account_for(principal)` resolve pela equipe; em modo sem usuários a conta é o
  literal `token`). Trocar alguém de equipe muda quem paga **daqui para frente**: lançamento
  já escrito fica com a equipe que pagou na hora, porque fatura passada não se muda.
- ✅ **Medir e cobrar são coisas separadas** — por padrão o livro-razão registra e não
  bloqueia nada; só `SPARQUET_STUDIO_CREDITS=on|1|true|yes|enforce` faz o saldo barrar.
  Sob medição a franquia **não** é queimada e o lançamento fica `applied = false`, então
  ligar a cobrança parte do que foi concedido e não da dívida acumulada.
- ✅ **Num Pipeline, cada Job é cobrado quando termina** — `/run`, `/run/stream` e cada
  estágio de `/run/flow/stream` chamam a cobrança por conta própria, então um flow que
  quebra no quarto Job pagou o que os três primeiros escreveram — e o extrato mostra
  isso linha a linha, com `job_run_id` e `pipeline_run_id`.
- ✅ **Sem saldo negativo** — se a execução escreveu mais do que a conta podia pagar, a
  diferença é gravada como `shortfall` no lançamento em vez de virar dívida silenciosa.
- ✅ **Visível na tela e no histórico** — aba **Settings → Billing** (`CreditsPanel`)
  com franquia usada/restante, saldo, escritas e cobrança do mês, outras equipes e o
  extrato; e o custo de cada execução no detalhe do run (`credits` em `JobRunRecord`,
  `GET /runs/{id}`), dizendo quantas escritas, quanto veio da franquia, quanto ficou em
  aberto e se foi só medição.
- ✅ **Ações e rotas de crédito** — `credits:Read` / `credits:Manage`; rotas `/credits/me`, `/credits`,
  `/credits/{id}/ledger`, `/credits/{id}/grant`; `credits_enforced` no `/health`.
  Verificado em `server/test_credits.py` e `src/lib/runner/credits.test.ts`.
- ✅ **Tags como dimensão de rateio** — Workflow, Pipeline e Job carregam tags
  (`catalog_tag` no banco do histórico, editor `TagsPopover` nas três telas) e a cobrança
  congela no lançamento a união das tags do Job, do seu Pipeline e do seu Workflow
  (`effective_tags`), mais o que o chamador mandar em `tags` no `POST /run`. Congelar é o
  que preserva a fatura: retaguear um Job muda o que ele custa **daqui para frente** e não
  reescreve mês fechado. Marcar o Workflow marca tudo que está dentro — repetir o centro
  de custo em quarenta Jobs garante que um fique de fora e apareça sem tag na fatura.
  Tags são a **única** dimensão que não particiona o mês: um run com duas tags conta
  inteiro nas duas, então as linhas somam mais que o total — daí `totals()` contar cada
  lançamento uma vez e a resposta trazer `overlapping` para a tela poder dizer isso.
- ✅ **Tela de análise, não só extrato** — Billing agora abre com seis meses em barras
  (`SpendTrend`, `GET /credits/timeline`), onde a barra é também o seletor do mês, e o
  rateio (`SpendBreakdown`) lê o mês escolhido por Workflow, Job, **Tag**, usuário ou
  equipe, em lista ordenada com barra proporcional ao maior — não à soma, que deixaria
  toda barra invisível com vinte linhas. Sem biblioteca de gráfico.
- [ ] **Produto de cobrança — escopo `sparquet-cloud`** — o que existe aqui é medição e
  débito interno, e isso basta para execução local. O produto de cobrança em volta
  **mora no backlog do `sparquet-cloud`** e está listado aqui só enquanto aquele
  repositório não o absorve. A cobrir, em ordem de
  dependência: **planos** (free/pro/enterprise como objeto de primeira classe, com a
  franquia e o preço saindo do plano em vez de variável de ambiente); **assinatura e
  ciclo de faturamento** (data de renovação, upgrade/downgrade no meio do período,
  proração); **fatura por período** (fechar o mês, congelar o extrato, gerar
  documento); **meio de pagamento** (gateway — Stripe/Pagar.me —, ninguém guarda
  cartão aqui; webhook de pagamento confirmado/recusado); **compra de créditos
  avulsos** e recarga automática; **cobrança de excedente** vs bloqueio (hoje só há
  bloqueio); **limites e alçadas por equipe** (teto mensal, quem pode conceder);
  **aviso de saldo baixo** antes do 402 (e-mail/tela); **exportação do extrato**
  (CSV/JSON) para conciliação contábil; **impostos e emissão fiscal**, se houver
  venda direta; e **painel administrativo multi-equipe** (hoje o extrato é por conta,
  um de cada vez). Decidir também o que é *self-hosted* (runner na máquina do
  cliente, cobrança desligada por padrão) e o que é *hospedado* — os dois não podem
  ter o mesmo modelo de confiança: no self-hosted quem opera a máquina pode editar o
  SQLite de créditos, então cobrança de verdade exige um serviço de billing fora do
  runner.
- [ ] **Preço por tamanho** (escopo `sparquet-cloud`) — toda escrita remota custa igual, independentemente de
  gravar dez linhas ou dez bilhões. O caminho natural é ponderar por duração ou por
  linhas escritas, dados que o histórico já tem (`rows_written`, duração por step).
- ✅ **Reserva antes de executar, com estorno** — o run segura o custo estimado antes
  de começar e liquida no fim (`reserve` → `settle`/`release`). `available` desconta
  o que está preso, então duas execuções paralelas não gastam o mesmo saldo; a
  liberação é idempotente e uma queda do runner deixa reserva órfã, varrida por
  `release_stale()` na subida.
- [ ] **Conciliação com o custo real do cluster** (escopo `sparquet-cloud`) — o crédito
  é unidade interna, sem
  relação com o que a nuvem cobrou pelo mesmo run.
- [ ] **Cobrar execução que não passa pelo runner** (escopo `sparquet-cloud`) —
  `sparquet.cli`, job agendado,
  Databricks: hoje é invisível para o razão. Sem isso, "conta da equipe" é a conta
  *do que rodou pelo Studio*.
- ✅ **Rateio por Workflow, usuário e Job** — resolvido **sem** conta por Workflow:
  quem paga continua sendo a equipe, e o Workflow virou *dimensão de leitura*. Cada
  lançamento do razão carrega `workflow_id` e `actor`, e `GET /credits/usage`
  agrupa por equipe, usuário, workflow ou job. A decisão é deliberada: um Workflow
  é uma pasta — ele é renomeado e muda de equipe —, então um orçamento preso a ele
  quebraria no dia em que alguém arrastasse um Job para fora. O nome do Workflow é
  resolvido na leitura, a partir do catálogo do histórico, de modo que renomear
  reetiqueta também as faturas passadas. Na interface: **Billing › Spending**.

### 9.4 Monitoramento e observabilidade

- ✅ **Painel de saúde do conjunto de Jobs** — `GET /health/jobs`
  (`server/history.py: job_health`) devolve **uma linha por Job do catálogo**,
  tenha ele rodado ou não: último run, último run *bem-sucedido*, falhas
  consecutivas, e duração e volume dos últimos runs bem-sucedidos. A lista de
  execuções é ordenada por tempo e por isso responde "o que aconteceu hoje à
  tarde" e é estruturalmente incapaz de responder "qual Job parou de rodar na
  terça" — um Job que não rodou não está no topo de uma lista ordenada por tempo,
  ele não está nela. Na interface: **Monitoring › Job health**
  (`src/components/monitoring/JobHealthPanel.tsx`), com filtro
  Falhando/Ocioso/Nunca rodou e o sparkline das durações — o mesmo dado que uma
  regra de mediana compara, para que um alerta possa ser lido contra a forma que o
  produziu.
- ✅ **Alerta** — `server/monitoring.py`: quatro perguntas sobre esses fatos —
  `failed` (últimos N runs falharam), `late` (nenhum run bem-sucedido há N
  minutos), `duration` e `volume` (acima do teto / abaixo do piso, em número
  absoluto ou em múltiplo da mediana do próprio Job). `job_id: "*"` vale para toda
  a biblioteca, **inclusive Jobs criados depois**. O `late` mede desde o último
  *sucesso*, não desde o último run: um Job que falha de dez em dez minutos está
  rodando o tempo todo e não está bem. Uma varredura roda em thread daemon a cada
  `SPARQUET_STUDIO_MONITOR_INTERVAL` (60 s), ligada por padrão
  (`SPARQUET_STUDIO_MONITORS=off` desliga) — a assimetria com o warm-up do Spark é
  deliberada: varrer custa uma consulta a um SQLite, e regra que só é avaliada
  quando alguém abre a tela não é monitor. Só **transição** vira evento, então o
  log lê "começou a disparar / limpou" e não uma linha por minuto enquanto algo
  está quebrado. Webhook opcional em `SPARQUET_STUDIO_ALERT_WEBHOOK` (JSON via
  `urllib`, sem dependência nova, falha engolida — canal de alerta que derruba o
  runner é pior que canal nenhum). Estado em `server/data/monitors.sqlite3`,
  permissões `monitoring:Read` e `monitoring:Manage`, com `POST
  /monitors/evaluate` deliberadamente em `Read`: é o botão de quem acabou de
  consertar um Job. Na interface: **Monitoring › Alerts**.
- ✅ **Métricas exportáveis (Prometheus)** — `GET /metrics` no formato texto, sem
  dependência: por Job, sucesso do último run, timestamps do último run e do
  último sucesso, duração, linhas lidas e escritas, falhas consecutivas, runs e
  falhas registrados; por regra, se está disparando. **Tudo é gauge, inclusive as
  contagens** — `sparquet_job_runs_recorded` é quantos runs o histórico *ainda
  guarda*, e a retenção apaga linhas: um `_total` que desce em silêncio faz
  `rate()` mentir, e taxa errada é pior que taxa ausente. Protegido por
  `monitoring:Read`, então o scraper carrega token como todo o resto.
- [ ] **OpenTelemetry e traço distribuído por execução** — o que foi feito é
  pull-based e do runner para fora. Falta o inverso: o pipeline emitindo spans
  (um por etapa, com `rows_read`/`rows_written`/duração nos atributos) para um
  coletor, de modo que uma execução do Studio e uma do `sparquet.cli` apareçam no
  mesmo traço que o resto da plataforma do cliente. É onde `sparquet/observability/`
  (§9.1) vira exportador em vez de só emissor de `POST /runs/ingest`. Depende de §6
  (métricas por etapa) para ter o que pendurar no span.
- [ ] **Alerta sobre qualidade, não só sobre execução** — hoje uma regra pergunta
  se o run falhou, demorou ou escreveu pouco. Não pergunta se *passou nas
  validações*: um run verde com 30% de linhas em quarentena não dispara nada. O
  `PipelineResult` já traz o resultado por validação e o histórico guarda
  `step_run` de validação; falta a regra (`quality`, sobre taxa de falha de
  validação) e o fato correspondente em `job_health`.
- [ ] **Agrupar alertas** — uma regra com `job_id: "*"` numa biblioteca de 50 Jobs
  e uma fonte fora do ar dispara 50 vezes. Falta a noção de incidente: agrupar por
  causa comum (mesmo endereço, mesma janela) e mandar um webhook, não cinquenta.

### 9.5 Catálogo de dados

- [ ] **Revisar e implementar** — cada Job já declara o que lê e o que escreve, e desde o
  histórico isso é persistido por execução (`job_run.lineage`). **Parcial**: a aba
  **Catalog** (`/catalog`, `src/screens/Catalog.tsx`; `/lineage` redireciona) reúne as
  duas metades.

  *Linhagem, derivada.* `src/lib/lineage/` lê o inventário direto do canvas da
  biblioteca — endereço normalizado, formatos, Jobs que produzem e consomem, os
  workflows que tocam cada dataset (`LineageDataset.workflowIds`) e a classificação
  `external`/`intermediate`/`terminal`/`isolated`, que é onde aparecem endereço com
  typo e handoff que ninguém lê. A visão padrão é o **grafo** (React Flow + dagre,
  `src/lib/lineage/flow.ts`), bipartido dataset→Job→dataset, com clique traçando o
  caminho inteiro para cima e para baixo (`traceFrom`) e duplo-clique abrindo o Job
  ou a ficha do dataset; a outra visão é a lista. O escopo por **workflow** filtra
  quais Jobs são lidos, não quais datasets aparecem — o dataset cujo outro lado mora
  em outro workflow continua na tela, marcado com o número de workflows, porque é
  exatamente a fronteira que ninguém enxerga de dentro do próprio workflow. É
  derivado do canvas, não do JSON compilado, para que um Job incompleto também
  apareça. O botão *Load example* cria os três Jobs Medallion
  (`LINEAGE_EXAMPLE_IDS` em `src/data/templates.ts`) encadeados só por endereço; o
  `union` foi acrescentado ao `history.lineage_of()` no mesmo passo, porque a segunda
  fonte dele é uma leitura como a do `join` e não estava sendo registrada.

  *Catálogo, digitado.* `src/lib/datacatalog/` guarda uma anotação por **endereço**
  de dataset (a mesma chave normalizada com que a linhagem junta dois Jobs, não um id
  de registro: apagar o Job não pode levar a descrição da tabela junto): descrição,
  dono, domínio, classificação (`public`/`internal`/`confidential`/`restricted`) e
  tags. Store em `src/store/catalog.ts` (carregado sob demanda, só por esta tela),
  persistência em um único registro sob a chave `meta:catalog` — rota que o backend
  de workspace **já** escreve como arquivo (`.studio/meta.json`), então o catálogo
  grava no repositório sem um quarto tipo de registro no servidor. `orphanAnnotations`
  aponta anotação cujo endereço nenhum Job menciona mais (o caso normal é endereço
  renomeado), e ela é mantida, não apagada.

  *Hierarquia, deduzida do endereço.* `src/lib/datacatalog/namespace.ts` lê cada
  endereço na forma que Glue, Hive e Unity Catalog usam: `describeAsset` decide o
  **tipo** pelo formato (delta/iceberg/hudi/JDBC são `table` mesmo morando num path;
  parquet/csv/json são `directory`; kafka é `topic`; view é `view`) e o **lugar** pelo
  endereço — `analytics.gold.revenue` vira catálogo/database/tabela, `/lake/gold/revenue`
  vira bucket/pasta/tabela, `s3://warehouse/...` vira o próprio bucket,
  `jdbc:postgresql://db:5432/app` tem o esquema empilhado descascado antes de parsear.
  Identificador pontuado só é quebrado em níveis quando o formato diz tabela: `orders.csv`
  é arquivo, não tabela `csv` dentro do database `orders`. `buildNamespaceTree` monta a
  árvore que o `CatalogBrowser` (`src/components/lineage/CatalogBrowser.tsx`) desenha —
  buckets primeiro (ícone de balde), depois catálogos, depois streams e views. Nada disso
  é metastore: não resolve endereço para o engine nem concede acesso; quem resolve em
  runtime continua sendo Spark/Glue/Unity Catalog.

O que falta, na ordem em que dói:

- ✅ **Coluna como objeto do catálogo, com classificação de acesso por coluna** — a
  anotação deixou de ser só por endereço de dataset. Cada coluna carrega descrição,
  classificação e tags próprias, e é um *securable* do IAM.

  1. *Onde a coluna mora.* Dentro da entrada do dataset (`DatasetAnnotation.columns`),
     com a chave em minúsculas e a grafia digitada guardada no registro. É deliberado:
     o endereço do dataset é a chave de junção do catálogo inteiro e uma coluna não tem
     endereço que sobreviva à sua tabela. Isso reaproveita o saneamento, o
     armazenamento e a máquina de `tag:` que já existiam — `normalizeColumns`,
     `withColumnAnnotation`, `columnAnnotationOf` em
     `sparquet-studio/src/lib/datacatalog/catalog.ts`.
  2. *Como o acesso é endereçado.* Um novo tipo de recurso `column`, escrito
     `<chave do dataset>#<coluna>` (`/lake/silver/orders#cpf`). O `#` em vez de `/` ou
     `.` porque o endereço do dataset já aninha com esses dois, e a coluna precisa
     continuar sendo coluna de `main.silver.orders`, não um quarto nível do nome.
  3. *A regra só estreita.* A cadeia é a coluna, depois todas as colunas, depois a
     cadeia inteira do dataset — tabela, pastas acima, tags dos dois. Um `deny` na
     coluna vence um `allow` na tabela, que é o que torna dizível "a tabela toda menos
     o CPF"; um `allow` na coluna não reabre tabela negada acima, porque negativa em
     qualquer ponto da cadeia continua ganhando.
  4. *As tags da coluna entram na mesma cadeia.* `tag/pii` e
     `tag/classification:restricted` valem para coluna e para tabela, então uma regra
     só governa as duas — inclusive as classificadas mês que vem.
  5. *Coluna não tem dono.* Propriedade é admin que negativa nenhuma alcança, e coluna
     se entrega junto com a tabela. O dono se atribui no dataset.
  6. *A classificação sobe.* `effectiveClassification` responde o que a tabela vale de
     fato: ela é tão restrita quanto a coluna mais restrita dentro dela, e nunca
     desce. O badge do dataset — na ficha e na lista do catálogo — mostra o valor
     efetivo, e a linha da coluna diz qual coluna levantou. Era exatamente o problema
     que impedia alguém de marcar qualquer coisa: marcar a tabela por causa de uma
     coluna restringia as outras trinta.

  Onde se edita: na ficha do dataset, aba *Shape*, cada linha do schema abre com
  descrição, classificação e tags da coluna (salva na hora, porque a linha colapsa).
  Onde se governa: `ResourceGrantsPanel` ganhou o tipo *Columns*, que só oferece as
  colunas que alguém descreveu — regra sobre nome que ninguém assumiu é regra sobre
  nome que a próxima mudança de schema leva embora. Espelhado no runner em
  `server/grants.py` (`column_resource`, `parse_column_resource`, cadeia própria em
  `scope_chain`) e em `server/main.py` (`_parents_of` lê as tags da coluna dentro da
  entrada do catálogo), documentado em *Columns as securables* no `server/README.md`.
- [ ] **Schema observado, não declarado** — nenhuma tela diz quais colunas e tipos o
  dataset **tem**. O caminho barato já está aberto: a amostra da ficha (`SampleRows` em
  `src/components/lineage/DatasetSheet.tsx`) passa pelo runner e volta com o schema do
  resultado — guardar esse schema por endereço, com a data da leitura, responde "que
  colunas essa tabela tinha da última vez" sem metastore nenhum. O que vem depois, e é
  mais caro, é **detectar mudança de schema** entre duas leituras e avisar: é a mesma
  varredura de §9.4, com o dataset no lugar do Job.
- [ ] **Frescor e tamanho** — a ficha não diz quando o dataset foi escrito pela última
  vez nem quantas linhas tinha. O histórico sabe: `job_run.lineage` diz quem escreveu o
  quê e `rows_written` diz quanto, então "última escrita" e "linhas na última escrita"
  por endereço se derivam sem tocar no storage. É também o fato que falta para uma regra
  de monitoramento por **dataset** em vez de por Job — "essa tabela não é atualizada há
  dois dias" é a pergunta que o time de negócio faz, e hoje ela só existe na forma "esse
  Job não roda há dois dias".
- [ ] **Busca e glossário** — o catálogo é navegável (árvore de namespace, filtro por
  domínio e classificação) e não é **pesquisável** por termo de negócio: quem procura
  "receita bruta" não chega em `analytics.gold.revenue`. Falta índice de texto sobre
  nome, descrição e tags, e um glossário de termos ligado a datasets. É o que separa
  inventário técnico de coisa que alguém fora de engenharia abre por vontade própria.
- [ ] **Cobertura como fila, não como número** — `CatalogStats` já calcula a fração
  documentada, com dono e classificada. Falta o que se faz com ela: dataset novo sem
  dono virando pendência de alguém, com nome e data, em vez de um percentual que ninguém
  persegue. Ligado ao dono duplicado de §9.2 — não dá para cobrar dono enquanto houver
  dois campos de dono.
- [ ] **Importar de um metastore existente** — o catálogo só conhece endereço que algum
  Job da biblioteca menciona. Ler tabelas de Glue, Hive ou Unity Catalog que ninguém
  ainda toca (para documentar antes de usar, e para descobrir o que já existe) é item
  próprio, sem desenho ainda, e não muda o princípio: continua sem resolver endereço em
  runtime nem conceder acesso.

  Sobre a **decisão de fundo**: Unity Catalog (e Glue, Hive Metastore, Polaris) é
  *runtime* — fica no caminho da leitura, resolve nome para path e aplica ACL, então
  adotá-lo muda o `PipelineConfig` e vira dependência de execução. DataHub e
  OpenMetadata são *observadores* fora do caminho crítico, e tiram lineage de scan +
  parse de SQL, que é justamente onde erram. Aqui o lineage é **declarado**, não
  inferido: o JSON já diz `input.path` e `outputs[].path`. A escolha é catálogo próprio
  com saída planejada — o modelo `{key, description, owner, domain, classification,
  tags}` mapeia direto para `GlueTable.Parameters`, para `COMMENT`/`OWNER`/`TAG` do
  Unity Catalog e para `DatasetProperties` do DataHub, então falta só um exportador.
  O que **não** fazer é virar metastore de runtime.

  *Schema, derivado do canvas.* `src/lib/datacatalog/schema.ts` percorre a cadeia de
  cada Job e devolve as colunas de cada endereço, com o tipo quando alguém o afirma:
  `cast` prova nome e tipo, `with_column`/`struct` criam, `group_by` devolve
  exatamente chaves + agregados, a lista `columns` do destino é a projeção final e uma
  regra de validação prova que a coluna existe. A confiança é dita na tela
  (`complete`/`partial`/`unknown`) porque um leitor CSV traz o resto das colunas só em
  runtime; entre duas leituras do mesmo endereço vence a escrita sobre a leitura e a
  completa sobre a parcial. Um passo `sql` torna a cadeia opaca e o derivador para de
  afirmar ordem. A ficha do dataset mostra a tabela; a lista mostra o badge `N col`.

  *Schema real, lido pelo runner.* `POST /dataset/schema` (`server/main.py`) monta um
  `InputConfig`, cria o reader pelo `ReaderFactory` e devolve `df.schema` — nenhuma
  linha é lida e nada é escrito. Ação própria `catalog:Inspect` (`server/auth.py`,
  concedida a `editor` e `operator`), cliente em `fetchDatasetSchema`
  (`src/lib/runner/client.ts`). O corpo aceita um bloco `spark`, que só vale enquanto o
  processo ainda não tem SparkSession — `spark.jars.packages` e `spark.sql.extensions`
  são lidos na criação da sessão e ignorados depois, então um runner que já executou
  algo continua com a sessão que tem. A comparação vive em
  `src/lib/datacatalog/drift.ts`: tipos normalizados antes de confrontar (`bigint` e
  `long`, `integer` e `int`, `smallint`, `tinyint`, `real`, `numeric` são grafias do
  mesmo tipo; `int` e `long` continuam diferentes, que é justamente o drift que
  interessa), nomes casados sem diferenciar maiúsculas, como o próprio Spark resolve.
  Coluna a mais **não** é drift enquanto o schema derivado for parcial — ali ela é o
  estado esperado do mundo, não um achado.

  *Coluna: linhagem e impacto.* O mesmo passeio que deriva o schema registra a
  procedência (`analyzeJob` devolve `{schemas, links, uses}`), e
  `src/lib/datacatalog/columns.ts` monta o grafo: `impactOf` responde "o que quebra se
  eu mexer nesta coluna", `originsOf` responde "de onde vem este número". São dois
  registros diferentes de propósito — uma **aresta** é valor que flui
  (`/lake/silver/orders.amount` alimenta `revenue`), um **uso** é passo que só cita a
  coluna (filtro, chave de join, regra de DQ) — porque um rename quebra os dois e uma
  troca de tipo quebra só o primeiro. Cada coluna em voo carrega as raízes de onde
  veio; coluna intocada tem como raiz implícita o dataset de entrada; `union` empilha
  o head do outro lado, então a coluna somada aponta para as duas fontes; depois de um
  `sql` a cadeia fica opaca e nenhuma aresta é inventada. A busca é em largura, com
  limite de profundidade e à prova de ciclo, e diz quando parou no limite em vez de no
  fim. Na ficha, cada linha do schema abre em *Comes from* / *Feeds* / *Named by*.

  Falta: **persistir o schema observado** (hoje a leitura morre ao fechar a ficha, e
  guardá-la é o que permite comparar duas datas) e frescor/última atualização (vêm do
  `job_run`, não do canvas); **lineage de execução** (`job_run.lineage`, com `{param}`
  já resolvido) sobreposto ao declarado; cruzamento com **Pipelines** (hoje as arestas
  são Job→Job pelo endereço, e a ordem declarada no Pipeline não é confrontada com
  ela); busca por nome de coluna no catálogo; **diff de schema entre revisões** do Job, para ver a mudança antes de ela
  virar drift; resultado das validações do `sparquet_cola` anexado ao dataset;
  o catálogo servido pelo runner como **API** (hoje ele é lido e escrito só pelo
  cliente) e usado como contexto pela IA e pela paleta; export/import do bundle
  levando o catálogo junto (`exportAll`/`importAll` ainda ignoram `meta:catalog`);
  tela própria de catálogo com busca por domínio/dono/classificação, se a árvore da
  aba não bastar; **glossário e política** (dono obrigatório por domínio, termo de
  negócio ligado à coluna); e os **exportadores** que a decisão acima deixa em aberto —
  `GlueTable`/`CreateTable`, MCE do DataHub, eventos OpenLineage e `schema.yml` do dbt.
- ✅ **Editor SQL sobre o catálogo (`/sql`, `src/screens/SqlEditor.tsx`)** — escreve-se
  `SELECT` contra os endereços que o catálogo já conhece e o runner executa, abrindo
  cada dataset pelo **`ReaderFactory` do próprio framework** — o mesmo caminho de um
  Job, que é o que torna Delta e Iceberg consultáveis aqui sem esta tela saber nada
  sobre nenhum dos dois. `POST /query` com ação própria `catalog:Query`, só leitura
  por construção (SELECT/WITH/EXPLAIN/DESCRIBE/SHOW; os endereços viram temp views
  derrubadas no fim), **cap de linhas sempre imposto** pelo servidor e timeout, que é
  o controle que de fato limita custo — `LIMIT 20` num `GROUP BY` ainda varre o ano
  inteiro. O bloco `spark` viaja com a consulta (`sparkForDatasets`), porque jars e
  extensões só são lidos na criação da sessão.
  - ✅ **Consulta é arquivo (estilo Databricks/Athena)** — quarto tipo de registro no
    workspace, `queries/<slug>.sql` com o SQL cru em UTF-8 mais o sidecar
    `.studio/query/<id>.json` com nome, cap e datas. Várias abas abertas ao mesmo
    tempo, rascunho não salvo preservado em `localStorage`, Ctrl/Cmd+S salva. Ficam
    fora de `list_files()` de propósito: consulta não é Job e não pode ser oferecida
    como algo a executar.
  - ✅ **Executar só a seleção** — o que está selecionado roda sozinho, e o botão passa a
    dizer *Run selection*; é o que permite manter vários statements num buffer só.
  - ✅ **Autocomplete no Monaco** — provider registrado na linguagem `sql` e lido por
    ref, então sobrevive a qualquer mudança de catálogo sem re-registrar: tabelas,
    colunas qualificadas depois de `alias.`, todas as colunas e as palavras-chave,
    nessa ordem de prioridade.
  - ✅ **Cancelar consulta em andamento** — `cancelQuery` primeiro, `abort()` depois:
    abortar a requisição só fecha esta ponta do socket, e o Spark continuaria
    calculando uma consulta que ninguém espera.
  - ✅ **Exportar o resultado** — CSV (RFC 4180, `lib/utils/csv.ts`) e JSON (um objeto
    por linha, para ser lido por programa). Exporta o que voltou, já limitado pelo
    cap: exportar mais seria uma segunda consulta invisível, e um preview de 20 linhas
    viraria varredura completa sem ninguém pedir.
  - ✅ **Histórico de execuções por consulta (`lib/sql/history.ts`)** — cada execução
    guarda o statement exato enviado, quando, quanto demorou, quantas linhas vieram e
    o erro quando houve; as que falharam também, que é metade das perguntas. A chave é
    o arquivo quando a consulta tem um, e a aba enquanto não tem — salvar pela primeira
    vez carrega o histórico junto em vez de descartá-lo. Nasceu em `localStorage` e
    mudou para o runner no item abaixo.
  - ✅ **Grid melhor (`components/panels/RunResultTable.tsx`, tudo opcional para não
    mexer no preview de execução)** — tipo de cada coluna no cabeçalho vindo do schema
    que o runner devolve, número alinhado à direita, ordenação client-side com nulo
    sempre no fim (nulo é dado ausente; enterrar os extremos sob uma página de branco
    é o que torna coluna ordenada inútil) e painel de célula para ler o valor que não
    coube, com JSON formatado e copiar. A ordenação acontece **depois** do corte:
    continuam sendo as linhas que o runner mandou, reordenadas, e não "as 50 maiores".
  - ✅ **EXPLAIN como árvore (`lib/sql/plan.ts` + `components/sql/PlanTree.tsx`)** — o
    desenho de `+-`/`:-` e três espaços por nível é lido de volta como a árvore que
    ele representa, com subárvore colapsável e marcação do que se procura num plano:
    onde os dados são lidos e onde há shuffle. Ordem preservada (plano do Spark se lê
    de baixo para cima), `EXPLAIN EXTENDED` mantido como seções separadas, e o que o
    parser não entende (`FORMATTED`, `COST`) volta como texto — errar sobre um plano é
    pior do que mostrá-lo cru. Resultado, plano e histórico dividem uma faixa de abas
    (`WorkspaceTabs`) abaixo do editor.
  - ✅ **Criar Job a partir da consulta (`lib/sql/toJob.ts`,
    `components/sql/CreateJobFromQuery.tsx`)** — a consulta vira um JSON com `input`,
    uma transformação `sql` e um `output`, que abre no canvas e passa a ser versionado
    como qualquer outro Job. O statement atravessa **sem uma edição**: a transformação
    `sql` registra o DataFrame de entrada sob o `view_name` que se pedir, e esse nome é
    o mesmo alias com que o editor já abriu o dataset — nada de reescrever o SQL de
    alguém por regex. Só o **primeiro** dataset viaja assim, porque um pipeline tem um
    `input` e trazer os outros exige join ou union com chaves que nada aqui pode
    adivinhar: os demais voltam em `unattached`, ditos na tela antes de o Job nascer, e
    não numa view que falta ao executar.
  - ✅ **Consulta como securable do IAM** — `query` entrou em `RESOURCE_KINDS` e em
    `OWNABLE_KINDS` (`server/grants.py`, `src/lib/iam/grants.ts`), então uma consulta
    tem dono, herda a cadeia de escopo e é governada pelas mesmas concessões que Job,
    Pipeline, Workflow, dataset e segredo — inclusive o dono que nasce com o recurso
    (§9.2). O histórico de uma consulta salva responde à mesma regra: quem não pode ler
    a consulta não lê o que rodaram nela.
  - ✅ **Gráfico simples sobre o resultado (`components/sql/ResultChart.tsx`,
    `lib/sql/chartScale.ts`)** — barra e linha como quarta superfície ao lado de
    resultado, plano e histórico: escolhe-se a coluna do eixo e a da medida e
    desenha-se sobre as linhas que já voltaram, sem reexecutar e sem tirar o limite,
    pelo mesmo motivo da exportação. SVG cru, sem biblioteca de gráfico — uma
    dependência que desenha um gráfico de barras é uma dependência para atualizar pelo
    resto da vida —, e as cores saem dos mesmos tokens do resto, então clara e escura
    vêm juntas.
  - ✅ **Formatar SQL e marcar erro de sintaxe antes de rodar** — são duas coisas
    respondidas por lados diferentes de propósito. Formatar é *layout*, então mora no
    cliente (`lib/sql/format.ts`), atua sobre o buffer ou sobre a seleção como a
    execução, e **nunca reescreve o statement**: decide espaço em branco e a caixa das
    palavras-chave que reconhece, e literal, identificador, comentário e número saem
    byte a byte como entraram — é essa invariante que torna seguro apertar Format na
    consulta de outra pessoa, e ela é pinada por teste. O erro de sintaxe vem do parser
    que vai executar: `POST /query/validate` (`catalog:Query`) chama `parsePlan` e para
    ali, sem tocar em storage, sem iniciar job e sem exigir que as tabelas existam — o
    editor valida enquanto se digita, muito antes de as views serem registradas. Sessão
    é usada **só se já houver uma**; sem sessão a resposta é "não verificado", não um
    palpite, e o Monaco não marca nada.
  - ✅ **Histórico compartilhado pelo runner** — os runs saíram do navegador para a
    tabela `query_run` no SQLite do histórico (`server/history.py`, `GET`/`DELETE
    /query/history`, `POST /query/history/move`). Quem grava é o runner, dentro do
    próprio `/query`: só ele sabe tempo, linhas, truncamento e o texto da falha, e é
    por isso que a falha também entra. Consulta salva é chave `q:<id>` e o histórico é
    **compartilhado** — a mesma consulta é um arquivo que duas pessoas abrem, e cada run
    diz quem o executou; rascunho é `t:<principal>:<aba>`, montado assim pelo runner em
    vez de filtrado na saída, então o rascunho de alguém não é endereçável por outra
    pessoa. Salvar pela primeira vez move os runs da aba para o arquivo. Teto de 100
    runs por chave.
  - ✅ **Amostra do dataset na ficha do catálogo** — *Read 20 rows* em `DatasetSheet`
    reaproveita o mesmo `POST /query`, montando `SELECT * FROM <alias>` com o formato
    provável do dataset e o bloco `spark` que aquele endereço exige. A requisição não
    nomeia consulta nem aba, e uma requisição assim é executada **e não registrada** —
    é o que mantém leitura de storage fora do histórico de consulta de alguém.

### 9.6 Biblioteca e arquivos

- ✅ **Apontar um estágio do Pipeline para um arquivo JSON existente** — uma caixa do
  Pipeline nomeia *ou* um Job da biblioteca *ou* um `.json` que já existe — gerado
  por outro time, versionado em outro repositório, escrito à mão — e o executa sem
  importar. O estágio guarda `path`, relativo à raiz da biblioteca; o runner lê o
  arquivo no momento em que o estágio começa, então nada é importado nem
  cacheado e uma edição feita fora do Studio vale na execução seguinte. Endpoints
  `GET /workspace/files` e `GET /workspace/files/{path}` (`workspace:Read`); a
  resolução acontece **antes** de cobrar, travar ou executar, então arquivo que
  falta é `400` e não Pipeline que morre no meio com estágios já escritos.
  Caminho absoluto é recusado — nomeia um diretório que existe em uma máquina só.
  Fica em aberto: (a) o histórico registra o estágio sem `job_id`, o que basta para
  a linha do tempo mas deixa o catálogo sem dono do arquivo; (b) montar uma
  biblioteca inteira a partir de um diretório, em vez de estágio a estágio.
- ✅ **Abrir um diretório como biblioteca** — aba **Library files** (`/workflow/files` e
  `/workflow/files/<pasta>`, porque um caminho três níveis dentro do diretório de outra
  pessoa é exatamente o que se manda por mensagem). O runner já listava os `.json` da raiz da
  biblioteca (`GET /workspace/files`) numa lista plana, o que serve a um seletor de
  estágio e não serve a quem tem duzentas confs; a árvore de pastas é **derivada** dos
  caminhos (`src/lib/library/tree.ts`) — uma pasta na verdade não existe no disco, ela é
  o que os caminhos têm em comum. Pasta que só contém outras pastas aparece assim mesmo,
  com a contagem do que há abaixo: biblioteca organizada como `domínio/camada/job.json`
  é toda "vazia" no primeiro nível, e um navegador que não mostrasse nada ali seria um
  navegador do qual não se começa. A busca varre a biblioteca inteira, não a pasta
  aberta, e casa **todas** as palavras em qualquer ordem — é assim que se acha um arquivo
  entre duzentos, lembrando dois pedaços dele.

  Selecionar um arquivo lê o JSON pelo runner e passa pelo mesmo compilador do editor:
  o painel mostra o que o Studio entende dali (incluindo os avisos de chave desconhecida
  preservada em `extras`) **antes** de rodar, e não depois de quebrar no cluster. E dá
  para executar o arquivo como está, num flow de um estágio apontado pelo caminho — o
  que o arquiva sob `file:<caminho>` no catálogo, então a execução entra na saúde por
  Job, pode virar alerta e pode ser etiquetada.

  Nada é importado e nada é cacheado, e isso é o desenho, não uma limitação: o arquivo no
  disco continua sendo a verdade, relido a cada abertura e de novo a cada execução, então
  edição feita fora — num editor, por um gerador, por um `git pull` — vale sem ninguém
  precisar reimportar. Ler é o modo normal da tela: ela não renomeia e não salva por cima.
  As duas exceções — apagar do disco e copiar para um canvas — são do item seguinte, e
  cada uma pergunta antes. 24 testes em `src/lib/library/tree.test.ts`.

- ✅ **A biblioteca como aba, não como item de menu** — a tela deixou de ser uma entrada
  da barra lateral e virou aba em dois lugares, sobre a mesma biblioteca: na visão geral
  (**Workflow**, renomeada de *Overview* e movida para `/workflow` — ver §9.7), ao lado da
  aba Workflow; e dentro de `/workflows/:id`, ao lado dos Jobs e Pipelines. O motivo é que toda resposta que ela dá
  termina em outro lugar do Studio — rodar este arquivo, ver o que o linter acha dele,
  abrir o canvas dele — e uma tela solta obrigaria a escolher o destino num diálogo depois
  de a pessoa já ter decidido. As rotas são splats (`/workflow/files/*` e
  `/workflows/:workflowId/files/*`), que casam também com a cauda vazia: a aba aberta e a
  pasta navegada saem do mesmo match, e `/workflow/files` e `/workflow/files/vendas/gold`
  são uma rota só.

  Cada arquivo agora diz **de quem ele é**. O runner responde isso a partir de
  `.studio/index.json` (`owners()`), não do formato do caminho: derivar do layout exigiria
  re-slugificar nomes, e dois registros que slugificam igual — ou um renomeado desde então
  — dariam donos errados, e é esse dado que decide se o arquivo pode ser apagado. Vem como
  `owner_kind`/`owner_id` em cada item de `GET /workspace/files`.

  Com o dono conhecido, **Open canvas** tem dois sentidos. Arquivo que o Studio escreveu
  leva ao registro dele (`/jobs/:id`, `/pipelines/:id`, `/workflows/:id`). Arquivo que
  ninguém aqui escreveu não tem canvas atrás, então abrir num canvas **copia** o JSON para
  um Job novo — um fork, não uma importação: o arquivo original fica onde está e daí em
  diante os dois são separados, e a descrição do Job registra `Copied from <caminho>`. O
  destino é o Workflow aberto; em `/workflow`, onde não há um, é o Workflow mexido mais
  recentemente. O diálogo **nomeia** o destino antes de escrever — fallback dito em voz
  alta vale como escolha; fallback silencioso vira Job perdido em workflow errado.

  E **apagar**: `DELETE /workspace/files/{path}` com `workspace:Delete`, que remove o
  `.json` do disco sem lixeira e sem desfazer. Só vale para arquivo sem dono. Apagar o
  arquivo de um Job é **recusado** com o nome do registro a apagar no lugar — o arquivo
  legível é metade de um registro, e removê-lo sozinho deixaria o sidecar apontando para
  nada e o próximo save o reescreveria. A rota é declarada **antes** de
  `/workspace/{kind}/{record_id}`, porque um arquivo na raiz da biblioteca tem dois
  segmentos e seria lido como um registro; há teste que trava essa ordem, para que uma
  reordenação futura quebre alto. 40 testes em `server/test_library_files.py`, 17 em
  `src/lib/runner/libraryFiles.test.ts`.
- ✅ **Dono do arquivo avulso no catálogo** — o arquivo passou a ser o próprio registro.
  Um estágio que roda um `.json` por caminho e que nenhum Job reivindica ganha a
  identidade `file:<caminho relativo>` (`_file_job_id`), derivada do caminho justamente
  para que **duas execuções do mesmo arquivo sejam o mesmo objeto** — normalizada em
  barras, sem barra inicial, sem espaço em volta, de modo que um cliente Windows e um
  Linux não criem dois donos para o mesmo arquivo. O prefixo `file:` mantém a identidade
  distinguível de um id que o Studio gerou, e diz de onde a linha veio.

  Essa identidade **não** vai para `stage.job_id`: `job_id` é contra o que as regras de
  IAM por recurso são escritas, e cunhar uma ali começaria a recusar flows que hoje
  rodam sob uma política que nunca teve esse id para nomear. Ela viaja num campo próprio
  (`FlowStageRequest.file_job_id`), e todo lugar que arquiva a execução — `_ensure_catalog`,
  `create_job_run`, os três `skip_job_run` e as tags de cobrança — passa por
  `_stage_job_id(stage)`, que é o Job quando existe um e o arquivo quando não existe.

  O registro é escrito **depois** da autorização do flow (`_register_file_jobs`), não na
  leitura do arquivo: uma execução que ninguém tinha permissão de começar não deixa para
  trás um registro dizendo que começou. E é **criado, nunca sobrescrito** — a mesma regra
  que o resto do catálogo segue: a execução conhece um id, quem curou o registro depois
  conhece o nome. Então a segunda execução não desfaz o nome, a descrição nem as tags que
  alguém pôs. Nasce chamado pelo `name` que o próprio JSON declara (o nome do arquivo se
  não declara) e com o `path` — `ensure_run_targets` ganhou `path` para isso.

  Consequência, que era o ponto: esses runs aparecem em `GET /health/jobs` com nome, contam
  falhas consecutivas, podem ser alvo de regra de alerta e podem ser etiquetados para cair
  no centro de custo certo. 23 testes em `server/test_file_job_catalog.py`.
- ✅ **Duas máquinas escrevendo a mesma pasta** — cada registro tem agora uma
  **revisão**: o SHA-256 do sidecar **e** do arquivo legível lidos juntos
  (`FileWorkspaceStore._revision`). Conteúdo, não mtime — cliente de sincronização
  reescreve arquivo que não mudou, e o hash não se move por isso. `read`/`snapshot`
  devolvem a revisão, o `PUT` a informa, e `workspace.write` recusa a escrita com
  `WorkspaceConflict` quando o que está no disco não é mais aquilo (HTTP 409 levando a
  mensagem, a revisão atual e o **registro que está lá**). No Studio o 409 vira
  `WorkspaceConflictError`, a escrita **não** entra no cache e `dirty` continua
  verdadeiro — a edição não está salva e o editor diz isso numa faixa
  (`components/library/ConflictBanner.tsx`) com as duas únicas respostas honestas:
  *carregar o que está no disco* (relê o registro pelo runner, revisão junto) ou
  *sobrescrever com o meu* (`StorageBackend.overwriteNext`, que solta a guarda de uma
  escrita só). Nenhuma é o default: tentar de novo sozinho depois de um aviso que
  ninguém leu é exatamente como o trabalho da outra máquina desaparece. Vale para Job e
  para Pipeline. Testes: `server/test_workspace_conflict.py` (16) e
  `src/lib/storage/remote.test.ts` (20).
- ✅ **Chave que o Studio não sabe reabrir** — `pipelineToGraph` guarda o que não
  conhece num campo opaco `extras` (no topo do pipeline, no `input`, no `output` e nas
  `validations`) e `compileGraph` o devolve na escrita. O `extras` é **espalhado antes**
  do que o compilador sabe escrever, então uma sobra velha nunca sobrescreve uma chave
  que o Studio conhece. Abrir um `.json` com chave desconhecida também levanta um aviso
  nomeando a chave, para que a preservação seja visível e não silenciosa. Cobre extensão
  registrada só no framework — e cobriu `input_view` enquanto §9.1 estava aberto.

### 9.7 Assistente e agente

- ✅ **O Studio abre numa conversa** — `/` passou a ser a tela **Assistant**
  (`src/screens/Assistant.tsx`) e a visão geral que morava ali foi para `/workflow`, com
  as abas Workflow e Library files (`/workflow/files/*`). A lista de workflows que
  terminava o menu lateral saiu: ela duplicava, a um clique de distância, o que a aba
  Workflow já lista, e crescia sem limite — o menu agora é fixo, e workflow se acha onde
  ele está listado. `/files` virou redirect para `/workflow/files`, porque era rota
  pública enquanto a biblioteca foi tela.

  O motivo de a porta de entrada ser uma pergunta: toda outra tela começa de algo que a
  pessoa já tem — um canvas, um workflow, um diretório de confs. Quem chega sabendo o que
  quer que o dado faça, e não qual das onze transformações faz isso, não tem por onde
  começar numa tela de inventário.

  A tela **não** é o painel de IA do canvas. Aquele edita o Job aberto: manda o pipeline,
  a seleção e os avisos do linter em toda requisição e responde com proposta para
  aplicar. Esta não tem Job nenhum, então não sai do navegador nada além do que é
  digitado. Provider, modelo e chave são os mesmos de Settings — nem segundo lugar para
  configurar, nem segunda conta para pagar.

- [ ] **Omnigent como motor do assistente** — hoje a tela faz uma requisição e devolve
  texto. O que falta é **agir**: criar o Job que acabou de descrever, rodar, ler o
  catálogo de volta, corrigir o que o linter apontou. Isso é laço de agente com
  ferramentas, e a decisão é usar o Omnigent (open source) em vez de crescer um segundo
  motor dentro do Studio — o que existe em `src/lib/ai/` é cliente de provider, não
  orquestrador, e não deve virar um.

  O formato do transcript da tela já é o que um agente precisaria (turnos, streaming por
  token, cancelamento via `AbortController`), então a troca é de motor, não de interface.

  **O que o projeto é**, verificado em <https://github.com/omnigent-ai/omnigent>: framework
  de agentes em Python sob **Apache 2.0**, instalável como `pip install omnigent`. Sobe um
  servidor HTTP com SSE (`localhost:6767` por padrão) e também expõe SDK Python, então dá
  para embuti-lo no runner em vez de rodar um serviço à parte. Um agente é declarado em
  YAML e as ferramentas são funções Python referenciadas por caminho de import
  (`type: function` / `callable: pacote.modulo.funcao`), além de servidores MCP e
  sub-agentes. É esse ponto que torna a integração barata: as ferramentas do Studio já
  existem como funções Python no runner (`workspace.py`, o linter, o disparo de execução),
  e declarar uma delas é uma entrada de YAML, não um adaptador.

  **Duas das questões abaixo já têm resposta.** Licença: Apache 2.0, compatível com a
  distribuição do Studio. Empacotamento: entra como extra opcional, porque
  `requirements.txt` do runner não deve arrastar a árvore do agente para quem não usa o
  assistente — mas o piso de Python do Omnigent é **3.12+**, contra
  `requires-python = ">=3.9"` do framework, então instalar o extra estreita a versão de
  Python do ambiente e isso precisa estar dito no README do runner.

  As demais, a definir antes de começar:

  | Questão | Por que decide o desenho |
  |---|---|
  | Onde o agente roda | Ferramenta que cria Job, lê o disco ou dispara execução **não** pode viver no navegador: ela precisa do runner. Provavelmente um endpoint novo em `server/`, com `requires(...)` como todo o resto — agente sem IAM é escalada de privilégio com interface de chat. |
  | Embutido ou ao lado | O Omnigent roda como servidor próprio (`:6767`) ou como SDK dentro de outro processo. Servidor à parte é uma segunda porta a proteger e um segundo lugar aonde a identidade precisa chegar; SDK dentro do runner herda o IAM que já existe. A segunda opção parece certa, e o que decide é quanto do laço o Omnigent quer tomar do processo hospedeiro. |
  | Chave de quem | O navegador hoje chama o provider com a chave do usuário, e nada fica no servidor. Um agente no runner inverte isso; ou o runner passa a receber a chave por requisição, ou passa a ter uma própria — e aí é ele que aparece na fatura. |
  | Superfície de ferramentas | Começar com leitura (listar workflows, ler Job, ler catálogo, rodar o linter) e só depois escrita. Toda ferramenta que escreve precisa de confirmação na tela, pelo mesmo motivo que apagar arquivo da biblioteca tem: o usuário aprova o efeito, não a intenção. |
  | Custo por execução | Billing já cobra execução de pipeline (§9.3). Turno de agente é um custo novo, e sem medida ele entra como surpresa na fatura. |

---

## 10. Produtos / estratégia

- ✅ **`sparquet-cola` como repositório/pacote separado** — extraído para repo próprio
  (`../sparquet-cola`, GitHub `VictorPasqualini/sparquet-cola`) com pyproject,
  CI/publish (mesmo padrão do sparquet), README trilíngue e docs. **Publicado no PyPI**
  como **`sparquet-cola`** 0.1.0 (import `sparquet_cola`). O `sparquet` o declara em
  `dependencies` (`sparquet-cola>=0.1.0`, sem cap — mantido retrocompatível) e é
  validado contra o pacote do PyPI; os shims `sparquet.validation.*` seguem
  reexportando dele.
- ✅ **As três peças substituíveis do runner** — crédito, identidade e biblioteca são
  *política*, não mecanismo: o runner aberto responde as três com SQLite e arquivos
  no disco do operador, o que é certo para uma equipe numa máquina e errado para um
  serviço hospedado. Em vez de fork, cada uma virou **slot**: um `Protocol` no módulo
  dono (`credits.CreditLedger`, `auth.IdentityStore`, `workspace.WorkspaceStore`), a
  classe local como default, e uma variável de ambiente nomeando a fábrica
  alternativa (`SPARQUET_STUDIO_{CREDITS,AUTH,WORKSPACE}_PROVIDER`, no formato
  `module:factory`). Carregador em `server/providers.py`; `GET /health` diz o que
  carregou. **Provider configurado que falha de carregar derruba o processo** — cair
  no default local calado colocaria o ledger e a identidade de todos os tenants num
  arquivo só. Ver `sparquet-studio/server/README.md` → *Replaceable pieces*.
- [ ] **`sparquet-cloud`** — o que só faz sentido hospedado (multi-tenancy real,
  cobrança, identidade federada, execução gerenciada, segredos, agendamento,
  observabilidade agregada, colaboração, IA com chave da plataforma, catálogo como
  serviço) vive em repositório privado próprio (`../sparquet-cloud`), consumindo o
  runner aberto pelos slots acima. Escopo e backlog lá, em `SCOPE.md` e `BACKLOG.md`.
- [ ] **`sparquet-lite` — um segundo motor, sem JVM** — nome provisório; ver *Nome* no fim
  do item. Hoje todo JSON precisa de Spark, e Spark cobra caro pelo que não usa: uns
  poucos segundos de JVM subindo, Java instalado, jar do Maven baixado na primeira
  execução. Para um preview de 10 mil linhas no SQL editor isso é o custo inteiro da
  operação. A ideia é um **backend alternativo**, não um fork: os mesmos contratos
  `BaseReader` / `BaseWriter` / `BaseTransformation`, registrados nas mesmas factories,
  implementados sobre **pyarrow + DuckDB**. O `PipelineConfig`, o `TransformationEngine`,
  o `$include`, o `{param}` e o `sparquet_cola` continuam sendo os mesmos objetos —
  só o executor por baixo muda.

  **Onde paga a conta:**
  - preview do SQL editor e do catálogo no Studio (o caso que motivou o item);
  - testes do framework que hoje se pulam sozinhos quando não há Java — passariam a
    rodar de verdade no CI, cobrindo transformação e validação;
  - demo hospedada em `sparquet.web.app` sem JVM nenhuma no servidor;
  - CLI em arquivo pequeno, onde subir Spark é mais caro que o trabalho.

  **Como escolher o motor.** Por execução, nunca por instalação, e só quando *todas*
  as pontas couberem: formato em `parquet`, `csv`, `json` ou `delta`, e caminho local
  (ou `file://`). Qualquer outra coisa — Kafka, JDBC, BigQuery, Snowflake, Mongo,
  Cassandra, caminho remoto — cai para o Spark sem perguntar. O Studio mostra em qual
  motor a consulta rodou com um badge e oferece **"rodar no Spark"** em um clique, para
  o caso de o resultado divergir.

  **O custo real é a suíte de compatibilidade**, não o motor. Sem ela o `lite` é uma
  armadilha: responde parecido e erra no caso de borda, sempre em produção. O que
  torna o item confiável é uma suíte que roda **o mesmo JSON nos dois motores** e faz
  o diff dos resultados (schema, ordem, tipos, nulos), rodando no CI a cada mudança de
  transformação. Sem isso, não entregar.

  **Riscos conhecidos:**
  - *Divergência de dialeto* — `date_format`, `to_date`, casts de decimal e ordenação
    de nulos não batem entre Spark SQL e DuckDB. Cada um precisa de tradução explícita
    e de um caso na suíte de compatibilidade.
  - *"Python puro" quer dizer **sem JVM**, não sem binário nativo* — DuckDB é C++,
    `deltalake` é Rust. O ganho é não depender de Java e de download de jar, não é
    virar dependência zero.

  **Nome.** `lite` sugere "versão capada", e não é isso: o motor não tem menos
  framework, tem menos máquina. Sugestão principal **`sparquet-solo`** — roda sozinho,
  no chão, sem cluster, e faz par com `sparquet-cloud` no extremo oposto da mesma
  linha. Alternativas: **`sparquet-brasa`** (brasa queima sem chama; Spark é a
  faísca), **`sparquet-seta`** (Arrow, que é o substrato de verdade) e
  **`sparquet-lume`**.

---

## 11. Testes e cobertura

Inventário completo, com o porquê de cada lacuna, em
[docs/TEST_PLAN.md](docs/TEST_PLAN.md).

O que está pinado hoje: os 14 conectores de banco/warehouse/stream (montagem de
opções), o DSL de threshold, a severidade, o parse da quarentena, a expansão de
`targets`, o dialeto CSV, o round-trip dos seis formatos nativos, o **runner (724
testes**: identidade e auditoria 106, histórico 118, créditos 89, concessões 87,
workspace e biblioteca 70, agendamento 59, monitoramento e alerta 57, segredos 51,
escopo de execução 16, conectores 18, warm-up 13, validação de sintaxe 11, compat e
providers 21, formatos com Spark 8) e o **Studio (852 testes** + 19 checagens de smoke
em Chrome real).

Na ordem do plano — a primeira é a lacuna que ainda pesa, e o item logo abaixo dela é
a que fechou:

- [ ] **Métricas contra dados reais** — `avg`, `min`, `max`, `sum`, `stddev`,
  `distinct_count`, `duplicate_*`, `missing_*`, `invalid_*` e `freshness` **nunca
  rodaram sobre um DataFrame** em teste: a suíte Spark do cola cobre `not_null`,
  `unique`, `range`, `regex` e `row_count`. Uma expressão de agregação errada passa
  verde hoje. Um arquivo local-Spark com uma métrica de cada família sobre um CSV
  fixo fecha isso.
- ✅ **Comportamento das transformações** — `tests/transform/test_builtin_behavior_spark.py`,
  58 testes sobre as 19 transformações mais o `skip_if_false` do engine: o alias da
  expressão de `select`, o mapa em ordem do `with_column` (a segunda coluna enxerga a
  primeira), o dot-path do `struct` aninhando de verdade e o conflito folha-vs-mapa
  recusado, o `pivot` do `group_by` nas duas formas, o `broadcast` do `join` como
  dica no plano físico (a sessão sobe com `autoBroadcastJoinThreshold=-1`, senão
  qualquer fixture pequena vira broadcast e o teste não prova nada), o `{{var}}` do
  `collect` (lista → literal de `IN`, **lista vazia → `NULL`**, aspas escapadas), o
  `debug` inspecionando a cópia e devolvendo o original, e o `repartition` provando
  que a mesma chave cai na mesma partição. As fixtures são `spark.sql(... VALUES ...)`,
  não `createDataFrame`, pelo mesmo motivo do round-trip de formatos. Fica pendente o
  que a tabela do plano lista: vazamento da view do `sql`, aliases de tipo do `cast`,
  `{{var}}` dentro de `with_transformations` e o reset do runtime entre execuções.
- ✅ **Round-trip dos 6 formatos nativos (`parquet`, `csv`, `json`, `orc`, `txt`,
  `view`)** — `tests/test_formats_roundtrip_spark.py`, 11 casos: escreve pelo
  `WriterFactory` e relê pelo `ReaderFactory`, conferindo schema e linhas em parquet
  e orc, valores em json e csv (inclusive nulo que continua nulo e campo com aspas e
  vírgula inteiro), ordem das linhas em txt, e a view nos dois escopos — a `global`
  alcançável com e sem o prefixo `global_temp.`. O fixture é montado com
  `spark.sql(... VALUES ...)`, não com `createDataFrame`: o segundo sobe um Python
  worker, e num master local com `PYSPARK_PYTHON` divergente o arquivo inteiro
  falharia por um motivo que nada tem a ver com formato. Fica pendente do que a
  tabela do plano lista para esses formatos: os modos de escrita
  `append`/`error`/`ignore`, `multiLine` no json, `sep`/`encoding` no csv e o
  auto-cache da view.
- ✅ **Escopo de autorização das rotas de execução** — `server/test_run_scope.py`
  (16 casos): quais recursos um run nomeia, `allow` no Workflow cobrindo o Job de
  dentro, `deny` que não é alargado por um grant mais amplo, Job não salvo caindo em
  `*`, e a recusa dizendo quem foi recusado e com que papéis.
- [ ] **Ferramenta de IA — testar de ponta a ponta** — hoje os 28 testes cobrem só o
  transporte e o parse (`lib/ai/client.ts`, `lib/ai/parse.ts`): que o streaming é lido
  e que um JSON de proposta vira grafo. Nada afirma que a proposta **serve**. Falta:
  (a) o prompt montado por `lib/ai/prompt.ts` conter de fato o catálogo — uma
  transformação, formato ou validator sem entrada em `src/catalog/` some do prompt e a
  IA passa a inventar `type` que não existe, e isso quebra em silêncio a cada
  capacidade nova; (b) a proposta aceita sobreviver a `compileGraph` e passar no
  linter, que é a promessa real da ferramenta ("o que a IA gera roda"); (c)
  `lib/ai/providers.ts` — cada provider tem endpoint, cabeçalho de autenticação e
  formato de evento próprios, e um deles pode quebrar sem que os outros percebam;
  (d) as respostas ruins: JSON truncado no meio do stream, texto fora do bloco,
  `type` inexistente, chave de API recusada — o usuário tem de ver uma mensagem, não
  um canvas vazio.
  Os itens (a) e (b) são unitários e baratos (o catálogo e o compilador já estão em
  memória no teste); (c) e (d) pedem provider dublê. Chamada real a provider fica
  atrás de env var, nunca na execução default.
- ✅ **Semântica de `on_failure`** — 13 testes em `tests/validation/test_on_failure.py`,
  sem Spark: `fail` (o default, inclusive quando a chave está ausente) aborta sem
  escrever **nada** — nem os destinos nem o relatório, porque o engine levanta antes
  de o pipeline chegar lá — e volta como `PipelineResult(success=False)` com as
  falhas em `error`, nunca como exceção; `warn` e `skip` seguem e escrevem tudo, com
  os resultados falhos ainda no objeto; validação com `skip` **não** liga
  `PipelineResult.skipped`, que significa outra coisa (`stop_if_empty`); e resultado
  de **severidade** `warn` nunca aborta, nem sob `fail`.
- ✅ **`apply_template` e `$include`** — 20 testes em
  `tests/utils/test_template_includes.py`, sem Spark: a tabela de formatação
  (texto, número, `bool` como `"true"`/`""`, lista de texto com aspas, lista de
  número sem, lista vazia falsy), o primeiro item decidindo a lista inteira, chave
  ausente ficando literal, o padrão `\w+`, a interpolação crua, e a separação entre
  `{param}` e o `{{var}}` de runtime (`apply_template("{{var}}", {"var": "x"})` devolve
  `"{{var}}"` — as duas sintaxes não colidem mais, os lookarounds em `_PARAM` excluem a
  chave dupla). Do `$include`: objeto, lista expandida na ordem, `params` valendo dentro
  do incluído, caminho relativo a quem escreveu o include, aninhamento, ciclo com o
  percurso na mensagem, `FileNotFoundError` nomeando o arquivo, e o limite que resta —
  só vale em `transformations`.
- [ ] **Camada HTTP do runner (`sparquet-studio/server/main.py`)** — os módulos de apoio
  já têm teste (`history.py`, `auth.py`, `workspace.py`, `credits.py`) e o escopo de
  execução também (`test_run_scope.py`), mas a **camada HTTP em si não tem nenhum**:
  token, allow-list de origem, a sequência de eventos SSE, o status por estágio do
  `/run/flow/stream` e a dependência `requires(...)` que liga cada rota à ação que
  ela exige — o avaliador de política está pinado, mas nada afirma que é o
  `PUT /workspace` que cobra `workspace:Write`. É o único componente que executa
  conf arbitrária, e a postura de segurança dele quebra em silêncio. `TestClient` do
  FastAPI cobre sem Spark — **exige `httpx` no ambiente de teste**, que hoje não
  está instalado (foi o que impediu de já fechar esta lacuna).
- ✅ **`mode: merge` (Delta e Iceberg)** — em
  `tests/io/integration/test_lakehouse_spark.py`, com jar de verdade e pulando-se
  sozinho quando ele falta: upsert (atualiza o que casa, insere o que não casa),
  `on` ausente falhando antes de qualquer chamada Spark, e os dois DELETEs escritos
  como cláusula (`WHEN MATCHED AND S.op = 'D' THEN DELETE` sobre um CSV de CDC com
  coluna `op`, `WHEN NOT MATCHED BY SOURCE THEN DELETE` sobre um snapshot).
  15 testes no arquivo.
- [ ] **Orquestração do `Pipeline`** — `input_df`, `columns`, `input_view`, projeção por
  output, `transformations` por output, `PipelineResult` nunca levantando exceção.
- ✅ **`opensearch`** — conector próprio (opções `opensearch.*`) afirmado ao lado do
  caso `elasticsearch` de que foi separado, no unitário e na camada de integração.
  O teste com serviço existe e **pula**: nenhuma versão publicada do conector roda
  em Spark 4 (item acima).
- ✅ **Integração com containers** — três arquivos em `tests/io/integration/`, atrás de
  `SPARQUET_IT=1` e pulando-se sozinhos quando a porta não responde, com a razão
  nomeando o `docker compose ... up -d <serviço>` que falta:
  `test_jdbc_services_spark.py` (25 casos: os cinco dialetos contra Postgres, MySQL,
  MariaDB, SQL Server e Oracle de verdade), `test_kafka_spark.py` (4) e
  `test_nosql_services_spark.py` (10: Mongo e Cassandra passam, ES e OpenSearch
  pulam pelo item acima). O `docker-compose.yml` publica tudo em `127.0.0.1` e não
  declara volume nomeado — teste que depende de estado sobrevivente é teste que
  mente, e `down -v` limpa. Um serviço por vez: uma JVM Spark local mais vários
  containers estoura a memória da máquina, e o sintoma engana (o mesmo laço de
  `idWithoutTopologyInfo` do conflito de jar). O que cada execução descobriu está em
  [docs/TEST_PLAN.md](docs/TEST_PLAN.md) → *The service tier*.
- ✅ **Camada de serviço no CI** — job `services-tier`, separado do `integration` para o
  tier de jar continuar rápido. Sobe sete containers do próprio `docker-compose.yml`
  com `up -d --wait` (Postgres, MySQL, MariaDB, Mongo, Kafka, OpenSearch e
  Elasticsearch) e, antes de rodar, **verifica alcance de cada porta e falha se
  faltar**: teste que se pula sozinho é a coisa certa na máquina de quem não tem
  Docker e é um job verde provando nada no CI. Em falha, despeja
  `docker compose logs --tail 40` — é o único lugar onde um boot quebrado se explica.
- [ ] **Três serviços ainda de fora do `services-tier`** — SQL Server, Oracle e Cassandra
  ficaram no run local: cada um leva minutos para subir, contra menos de um minuto dos
  sete atuais. `test_jdbc_services_spark.py` cobre os cinco dialetos, então no CI dois
  dos cinco passam pulando. Entram quando houver medição dizendo que o PR aguenta o
  tempo — ou num job noturno, que é o formato natural para eles.
- [ ] **Runner do Studio sem job** — o `sparquet-studio/server/` (FastAPI que executa o
  Job e faz streaming do resultado) não é importado por nenhum job: o `studio` cobre o
  frontend (typecheck, lint, testes, build e smoke em Chrome) e o `test` cobre o
  framework. Um import + um `TestClient` no endpoint de execução fecharia a lacuna sem
  precisar de Spark.
- ✅ **Cobertura medida no CI** — o job `test` roda cada arquivo sob
  `coverage run` (modo paralelo, um processo por arquivo), faz `combine` e publica o
  total no `$GITHUB_STEP_SUMMARY` junto com a contagem de arquivos e de testes pulados.
  O piso é `--fail-under=65`, deliberadamente abaixo do medido: esse job **não tem
  Java**, então todo ramo que precisa de SparkSession viva é inalcançável ali — 69,6%
  sem JVM contra 76,9% com uma. O piso existe para pegar módulo que saiu da suíte
  inteiro, não para policiar ponto percentual; apertá-lo até o número atual deixaria
  vermelho um refactor honesto.
- [ ] **`dynamodb` sem serviço** — é o único conector NoSQL sem container na
  `docker-compose.yml`. `amazon/dynamodb-local` fecharia a lacuna sem conta AWS.
