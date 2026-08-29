# sparquet — Backlog de desenvolvimento

Melhorias e pendências de desenvolvimento **do framework e do Studio** (não de um caso
de uso específico). Cada item é uma capacidade genérica, ortogonal e sem acoplamento de
domínio.

Atualizado em 2026-08-28.

**Como ler as marcas:**

| Marca | Significa |
|---|---|
| ✅ | entregue **e** verificado — existe no código e há teste ou verificação manual apontada no item |
| 🟡 | entregue em parte — o que falta está dito no próprio item |
| [ ] | pendente |

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

- ✅ **Rename do pacote**: `spark_framework` → `sparquet`; classe de entrada
  `SparkFramework` → **`Sparquet`** (`from sparquet import Sparquet`).
- ✅ **Licença**: MIT → **Apache 2.0** (+ arquivo `NOTICE`).
- ✅ **Conectores de IO** (read+write) — resolve boa parte do §4: JDBC (`postgresql`,
  `mysql`, `mariadb`, `sqlserver`, `oracle`), warehouses (`bigquery`, `snowflake`,
  `redshift`), NoSQL/busca (`mongodb`, `documentdb`, `dynamodb`, `cassandra`,
  `elasticsearch`) e **leitura Kafka** batch (MSK via SASL/IAM).
- ✅ **Validação estilo SODA Core** — resolve boa parte do §5: `check` (métrica +
  threshold warn/fail, incl. `freshness` e formatos nomeados) e `schema`
  (colunas/tipos); `ValidationResult` ganhou `severity`/`metric_value`/`check_name`;
  relatório enriquecido; severidade `warn` não aborta.
- ✅ **Broadcast join**: param `broadcast` no `join` (map-side, sem shuffle).
- ✅ **Métricas por output**: `PipelineResult.output_metrics` (`rows_written` = soma
  por destino), lido do contador que a própria escrita apura — sem `count()` extra;
  ver `sparquet/core/write_metrics.py` e o benchmark no CHANGELOG 0.7.0. É também o que o billing do Studio
  conta como escrita bem-sucedida (§9.3) — sem isso a cobrança por escrita não teria
  fonte.
- ✅ **Renome**: validação `custom_sql` → **`sql`**. **Removido**: alias `add_column`
  (use `with_column`).
- ✅ **Fix**: heurística path-vs-tabela do Delta agora é *scheme-agnostic* (cobre
  `s3a://`, `abfs://`, etc., não só `s3://`).
- ✅ **`sparquet_cola`**: o motor de validação virou uma **biblioteca separável**
  (pacote top-level `sparquet_cola/`, só depende de pyspark, API `Cola`). O bloco
  JSON continua `validations`; `sparquet.validation.*` são shims de compat.
- ✅ **`sql` failed_rows**: além do invariante booleano, o `sql` aceita `failed_rows`
  (query que retorna as linhas ruins) + `output` próprio para gravá-las.
- ✅ **Quarentena** (§5 item resolvido): `validations.outputs` (`valid`/`invalid`)
  roteia linhas para destinos próprios, apartado da saída principal.
- ✅ **Formatos de arquivo**: `json`, `orc`, `avro`, `xml`, `binary` (só leitura) e
  `hudi` (upsert via `hoodie.*`).
- ✅ **OpenSearch separado** do Elasticsearch — conector próprio (`opensearch`,
  prefixo `opensearch.*`). Cassandra/ScyllaDB e ES/OpenSearch documentados.
- ✅ **Input em temp view** (`input_view` na chamada do Sparquet): registra e cacheia
  a entrada para self-join/SQL sem reler a base.
- ✅ **Temp view global vs sessão**: `view` ganhou `options.scope`
  (`session`/`global`); o `input_view` aceita `{"name": ..., "type": "session"|"global"}`.
- ✅ **`input_view` unificado**: o escopo da temp view de entrada agora vai dentro de
  `input_view` (`"orders"` ou `{"name": "orders", "type": "global"}`); o antigo
  `input_view_scope` foi removido.
- ✅ **Marcadores de etapa nos logs** (v0.3.1): `Pipeline`/`TransformationEngine`
  emitem `step=True` com `scope` (`input`/`transformation`/`output`), `index` e `total`.
  Puramente aditivo — `apply(..., top_level=False)` mantém o comportamento anterior.
  É o que alimenta o status por etapa no canvas do Studio.
- ✅ **CI/CD de release** (resolve §8): GitHub Actions `ci.yml` (testes em push/PR,
  matriz Python) + `publish.yml` (testes → build + `twine check` → publish no PyPI em
  release; TestPyPI em execução manual), via Trusted Publishing OIDC.
- ✅ **`sparquet-cola` extraído** (resolve parte do §10): o motor de DQ virou um
  pacote/repo próprio (`../sparquet-cola`), com pyproject + CI/publish + README
  trilíngue + docs. O `sparquet` passou a declará-lo em `dependencies`;
  `packages.find` do sparquet agora empacota só `sparquet` (o `sparquet_cola` vem do
  PyPI).

---

## 3. Entregue — Studio (changelog)

O Studio compila o canvas para o **mesmo JSON** que o framework executa; nada aqui
muda a API Python.

- ✅ **Vocabulário**: `Workflow` (container) › `Job` (um JSON) › `Pipeline` (conjunto
  ordenado de Jobs, executado em sequência).
- ✅ **Execução em sequência**: `POST /run/flow/stream` no runner executa vários Jobs
  numa SparkSession compartilhada, com status/logs por estágio e `stop_on_error`.
- ✅ **Storage dos JSONs — padronização e versionamento em git**. A biblioteca do
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
- ✅ **O produto não escreve dentro do próprio código-fonte.** Um checkout é código:
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

Decisões tomadas:

- ✅ **`json` / `orc` / `avro` / `xml` / `binary` / `hudi`**: feito.
- ✅ **Elasticsearch × OpenSearch**: separados (conectores distintos, `es.*` vs
  `opensearch.*`).
- ✅ **Cassandra × ScyllaDB**: uma classe só — Scylla usa o MESMO
  spark-cassandra-connector (não há conector separado); deixado explícito na
  classe/doc.
- ✅ **DynamoDB via RDD**: descartado — mantido o conector DataFrame (spark-dynamodb),
  que integra ao modelo do framework.
- **Streaming (readStream/writeStream)**: mantido **batch-only** por ora — streaming
  exige caminho de execução próprio (ver §7).

Pendentes / candidatos:

| Conector | Notas |
|---|---|
| `rest`/`http` | ingestão de APIs — adiado (não é fonte Spark nativa); via reader custom/boto3 |
| `sqs` | não é fonte Spark nativa → reader/writer driver-side com boto3 (adiado) |
| `kinesis` | AWS Kinesis — via conector do provedor; é essencialmente **streaming** (ver §7) |
| `excel` | nicho; via `spark-excel` |

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

Evolução ainda proposta:

1. [ ] **`reference`/reconciliação cross-dataset** — comparar contagem/soma do df contra
   outra fonte (tabela/view) por chave; `failed_count` por grupo. (O `sql` cobre isso
   hoje de forma manual; falta o validator dedicado — SODA tem `reconciliation`.)
2. [ ] **Data contract versionado** — evoluir o `schema` para nullability/constraints
   ricos, versionado por pipeline e validado antes de transformar.
3. [ ] **Lineage / observabilidade** — registrar fonte→destino, contagens/tempo por etapa
   (ver §6) e versão da config; expor para catálogo. **Parcial**: o Studio já persiste
   lineage por execução (`job_run.lineage`) e a versão da config (`job_run.config_hash`)
   — ver §9.1; falta o mesmo fora do runner do Studio.
4. [ ] **Otimização de métricas** — hoje cada `check` roda sua própria action; batelar as
   agregações de vários checks numa passada só.

### 5.1 Validações candidatas no `sparquet_cola`

Hoje: `not_null`, `unique`, `range`, `regex`, `row_count`, `sql`, `schema` e o `check`
(métricas `row_count`, `distinct_count`, `missing_*`, `duplicate_*`, `invalid_*`,
`min`/`max`/`avg`/`sum`/`stddev`, `freshness`). Cada nova regra precisa de entrada no
catálogo do Studio (`src/catalog/`), senão o editor não a oferece.

**A. Já é possível com `sql`, mas merece ser declarativo.** O `sql` cobre tudo — e é
exatamente por isso que essas viram SQL copiado entre pipelines, sem nome nem
semântica no relatório:

- [ ] `column_comparison` — `col_a <= col_b` (`data_inicio` ≤ `data_fim`,
      `valor_liquido` ≤ `valor_bruto`). A checagem cross-column mais comum que existe.
- [ ] `conditional_not_null` — coluna obrigatória **quando** outra tem valor
      (`cnpj` obrigatório se `tipo = 'PJ'`). Row-level, entra na quarentena.
- [ ] `accepted_values` — hoje só via `check` + `valid_values`; verboso para o caso
      mais frequente de todos.
- [ ] `mutually_exclusive` — no máximo uma de N colunas preenchida.

**B. Cross-dataset.** Complementa o `reference`/reconciliação já proposto acima:

- [ ] `foreign_key` — todo valor de `col` existe em outra fonte. Hoje exige um join
      manual no pipeline, o que mistura validação com transformação.

**C. Estatística / outliers.** Thresholds sobre a *forma* do dado, não só extremos:

- [ ] `quantile` como métrica do `check` — p50/p95/p99 (`must_be: "< 1000"` no p95
      diz mais sobre latência/valor que `max`, que um único outlier distorce).
- [ ] `outliers` — contagem fora de N desvios (z-score) ou do IQR.
- [ ] `cardinality` por grupo — `distinct_count` particionado (distinct país por cliente).

**D. Completude segmentada:**

- [ ] `missing_percent` com `group_by` — hoje é global; responder "qual país tem 30%
      de cpf vazio" exige uma regra por país.

**E. Depende de histórico de execuções.** O histórico agora existe (§9.1), mas só
dentro do runner do Studio e sem série temporal por métrica — é isso que falta para
estas três:

- [ ] `volume_anomaly` — `row_count` contra a média móvel das últimas N execuções.
      Pega o caso clássico de "a fonte veio pela metade e ninguém percebeu".
- [ ] `distribution_shift` — desvio da distribuição de uma coluna vs. a execução
      anterior.
- [ ] `freshness` por partição, não só o máximo global da coluna.

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
- [ ] **Perfis (dev/staging/prod)** no mesmo JSON — overrides de path/options por
      ambiente, selecionados por param.
- [ ] **Testes unitários com dados mock** — cobrir transformações (`struct`,
      `collect`/`{{var}}`, `stop_if_empty`, `group_by`, outputs com `transformations`)
      e validators, com SparkSession local — base para CI. Ver §11.
- [ ] **Testes unitários estilo dbt** (`unit_tests` do dbt 1.8+) — declarar *dados de
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
- [ ] **`$include` aninhado** — hoje não suportado (um nível só).
- [ ] **Catálogo de erros** — mensagens de erro padronizadas e acionáveis
      (transformação desconhecida, coluna inexistente, etc.).

---

## 7. Performance

Já entregue:

- ✅ **Input em temp view** (`input_view`, string ou `{"name","type"}`) — self-join / SQL sobre a entrada sem reler a base.
- ✅ **Broadcast join** (`broadcast` no `join`) — map-side, sem shuffle.
- ✅ **Doc: `filter`/`select` primeiro** — recomendação no guia de performance + convenção no CLAUDE.md.
- 🟡 **Pushdown** — já disponível: `collect` + `{{var}}` (IN literal → data skipping), `checkpoint`, `partitionColumn`/`fetchsize` (JDBC), `partition_by` / `compression` / `maxRecordsPerFile` via `options`, e a heurística de path do Delta corrigida.

Pendente:

- [ ] **Ler sempre com partição / estratégia de leitura** — orientar (e onde fizer sentido, automatizar) leitura particionada com *partition pruning* por padrão, em vez de scan total.
- [ ] **Hash para `partitionBy`** — particionamento por hash de coluna (buckets) para evitar skew e *small files* na escrita.
- [ ] **Avaliar Apache DataFusion Comet** — acelerador vetorizado do Spark; medir ganho real e o custo de dependência antes de recomendar.
- [ ] **Análise consolidada de opções de tuning** — mapear/expor/documentar e decidir o que vira opção declarativa vs recomendação de doc: `vacuum`, `optimize`, `z-order`, `repartition`, `coalesce`, `partitionBy`, `bucketBy`, `clustering`/`clusterBy`, `compression`, `persist`/`cache`, `checkpoint`, `maxRecordsPerFile`, broadcast automático (`spark.sql.autoBroadcastJoinThreshold`), *partition pruning*, *predicate pushdown*, problema de *small files*, `shuffle` (partitions/skew), *garbage collection* e `checkpointLocation` (streaming).

> Streaming (readStream/writeStream, `checkpointLocation`, output modes, Kinesis) é
> um eixo à parte — o modelo atual é batch. Decisão registrada em §4.

---

## 8. Empacotamento / distribuição / CI

- ✅ **CI de release** — GitHub Actions: `ci.yml` (testes em push/PR) e `publish.yml`
      (testes → build + `twine check` → publish). Release publicado → PyPI; execução
      manual → TestPyPI (ensaio). Trusted Publishing (OIDC), sem token manual. Ver
      [docs/DEPLOY_PYPI.md](docs/DEPLOY_PYPI.md) §8.
- [ ] **Matriz de versões** — CI já cobre Python (3.9/3.11/3.12); falta variar
      **PySpark** (ex: 3.4 × 3.5) na matriz.
- Base atual: versão única via `__version__` (pyproject `dynamic`); publicação no
  PyPI documentada e automatizada via CI.

---

## 9. Studio — plataforma

O runner (`sparquet-studio/server/`) é o único componente que **executa configuração
arbitrária**: tudo aqui é, no fim, postura de segurança e de operação. Ele fica preso a
`127.0.0.1` e o token é senha, não identificador.

### 9.1 Execução, histórico e canvas

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

Pendente aqui:

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
  Rotas `POST /auth/users/{id}/recovery` e `POST /auth/recover` (esta exige o token
  compartilhado e **nenhuma** sessão, já que vem da tela de login).
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

Pendente aqui:

- ✅ **Log de auditoria** — `server/audit.py` + middleware: toda requisição que muda
      estado vira uma linha com quem, o quê, sobre qual recurso e com que desfecho —
      inclusive as **recusadas**, que são as que interessam ler. Filtros por ator,
      recurso, desfecho, prefixo de ação (`iam:*`) e data em `GET /audit`, atrás de
      `iam:ReadAudit`; corpo de requisição nunca é gravado, só os campos nomeados. Na
      interface: **Access & IAM › Audit log**.
- [ ] **SSO / OIDC** e senha gerenciada fora do runner — para instalação corporativa,
      onde criar mais um usuário/senha local é justamente o que não se quer.
- [ ] **Expiração e rotação de sessão por política** — hoje é só
      `SPARQUET_STUDIO_SESSION_HOURS`, global; falta prazo por papel, revogação de uma
      sessão específica e lista de sessões abertas por usuário.
- [ ] **Segundo fator** — o step-up de recuperação já mostra o padrão; falta TOTP para
      login e para operações sensíveis.
- [ ] **Testes da camada HTTP** — `requires(...)` liga cada rota a uma ação, e nada
      afirma que é o `PUT /workspace` que cobra `workspace:Write`. Ver §11.

### 9.3 Billing e créditos de execução

Modelo atual, implementado em `server/credits.py` (SQLite próprio em
`SPARQUET_STUDIO_CREDITS_DB`):

- ✅ **1 crédito por escrita bem-sucedida**, não por Job — a régua saiu de "um Job = uma
  moeda" para o que o usuário efetivamente obteve. A contagem vem de
  `PipelineResult.output_metrics`, que só tem entrada para destino que terminou de
  escrever: **erro não gasta crédito**, e um Job que grava três destinos custa três.
  Preço configurável em `SPARQUET_STUDIO_CREDITS_PER_WRITE`.
- ✅ **Cobrança depois da execução** (era na admissão). Antes de começar só se checa o
  mínimo — `precheck` recusa com **HTTP 402** a equipe que não pode pagar nem uma escrita
  —, e o débito real acontece com o run terminado, quando o número de escritas é fato.
- ✅ **Só execução remota custa** — a localidade sai da configuração do Job, nunca de um
  campo da requisição, senão bastaria o cliente declarar-se local: `spark.master` (ou
  `spark.configs["spark.master"]`) começando com `local` é grátis; `spark.remote` cobra
  mesmo com master local ao lado; `yarn`/`spark://`/`k8s://` cobram; e runner rodando em
  Databricks/EMR/Dataproc/Synapse cobra todo Job, qualquer que seja o master.
- ✅ **40 escritas grátis por mês** (`SPARQUET_STUDIO_CREDITS_FREE_MONTHLY`), por
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
- ✅ **Num Pipeline, cada Job é cobrado quando termina** (`/run`, `/run/stream` e cada
  estágio de `/run/flow/stream` chamam a cobrança por conta própria), então um flow que
  quebra no quarto Job pagou o que os três primeiros escreveram — e o extrato mostra
  isso linha a linha, com `job_run_id` e `pipeline_run_id`.
- ✅ **Sem saldo negativo** — se a execução escreveu mais do que a conta podia pagar, a
  diferença é gravada como `shortfall` no lançamento em vez de virar dívida silenciosa.
- ✅ **Visível na tela e no histórico** — aba **Settings → Billing** (`CreditsPanel`)
  com franquia usada/restante, saldo, escritas e cobrança do mês, outras equipes e o
  extrato; e o custo de cada execução no detalhe do run (`credits` em `JobRunRecord`,
  `GET /runs/{id}`), dizendo quantas escritas, quanto veio da franquia, quanto ficou em
  aberto e se foi só medição.
- ✅ Ações `credits:Read` / `credits:Manage`; rotas `/credits/me`, `/credits`,
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

Pendente aqui:

- [ ] **Revisão e implementação de TODA a parte de billing** — o que existe é medição e
      débito interno; falta o produto de cobrança em volta. A cobrir, em ordem de
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
- [ ] **Preço por tamanho** — toda escrita remota custa igual, independentemente de
      gravar dez linhas ou dez bilhões. O caminho natural é ponderar por duração ou por
      linhas escritas, dados que o histórico já tem (`rows_written`, duração por step).
- ✅ **Reserva antes de executar, com estorno** — o run segura o custo estimado antes
      de começar e liquida no fim (`reserve` → `settle`/`release`). `available` desconta
      o que está preso, então duas execuções paralelas não gastam o mesmo saldo; a
      liberação é idempotente e uma queda do runner deixa reserva órfã, varrida por
      `release_stale()` na subida.
- [ ] **Conciliação com o custo real do cluster** — o crédito é unidade interna, sem
      relação com o que a nuvem cobrou pelo mesmo run.
- [ ] **Cobrar execução que não passa pelo runner** — `sparquet.cli`, job agendado,
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

- [ ] **Revisar e implementar.** Hoje existe a matéria prima e não o produto: log
      estruturado em JSON (`sparquet/utils/logger.py`), `PipelineResult` com o resultado
      de cada validação, e o histórico do Studio
      (`pipeline_run`/`job_run`/`step_run`/`run_log`) com duração e status por etapa. Não
      existe nada disso *fora* de uma execução isolada. A cobrir: métricas exportáveis
      (OpenTelemetry/Prometheus — duração, linhas lidas/escritas, taxa de falha, por
      Job/etapa), traço distribuído por execução, alerta (run falhou, run não rodou,
      duração fora da faixa, queda de volume), painel de saúde do conjunto de Jobs (não
      de um por vez), e o mesmo caminho valendo
      para execução fora do Studio — esta última parte está resolvida em §9.1
      (`sparquet/observability/` + `POST /runs/ingest`), e o que falta aqui é o resto:
      exportação de métricas, traço, alerta e painel. Ver §5 (lineage) e §6 (métricas
      por etapa), que são pré-requisitos parciais.

### 9.5 Catálogo de dados

- [ ] **Revisar e implementar.** Cada Job já declara o que lê e o que escreve, e desde o
      histórico isso é persistido por execução (`job_run.lineage`). Falta transformar
      essas arestas num catálogo: inventário de datasets (endereço, formato, schema
      observado, dono, Jobs que produzem e que consomem), grafo de lineage cruzando Jobs
      e Pipelines, última atualização e frescor, e o resultado das validações do
      `sparquet_cola` anexado ao dataset — a resposta para "de onde veio isto, quem
      depende disto, e dá para confiar". Decidir se é catálogo próprio (servido pelo
      runner, consumível pelo Studio e pela IA como contexto) ou integração com um
      existente (Unity Catalog, DataHub, OpenMetadata) — e, se próprio, se ele também
      alimenta a paleta do Studio com fontes já conhecidas.

### 9.6 Biblioteca e arquivos

- [ ] **Apontar um estágio do Pipeline para um arquivo JSON existente.** Hoje uma caixa
      do Pipeline referencia um Job da biblioteca, e o Job é o dono do arquivo. Falta o
      caminho inverso: apontar a caixa para um `.json` que já existe no disco — gerado
      por outro time, versionado em outro repositório, escrito à mão — e executá-lo sem
      importar para dentro da biblioteca. Desenho proposto: o estágio ganha uma origem
      alternativa (`{ "source": "file", "path": "vendas/jobs/ingestao.json" }`, relativa
      à raiz da biblioteca), o runner resolve na hora de executar, e o Studio mostra o
      JSON em modo leitura com o linter rodando em cima — editar continua sendo pelo
      Job. Pontos a decidir antes: (a) caminho relativo à raiz **sempre**, para não
      vazar caminho absoluto de uma máquina para outra e para manter a recusa de
      escapar da raiz que `workspace.py` já faz; (b) o que a execução registra no
      histórico quando não há `job.id` — provavelmente um Job sintético identificado
      pelo caminho, senão o catálogo fica com órfão; (c) o que acontece quando o arquivo
      some ou deixa de compilar — falhar no lint do Pipeline, antes de rodar, não no
      meio da execução; (d) se o mesmo mecanismo vale para um Workflow inteiro
      (montar a biblioteca a partir de um diretório) ou só para estágio.

---

## 10. Produtos / estratégia

- ✅ **`sparquet-cola` como repositório/pacote separado** — extraído para repo próprio
      (`../sparquet-cola`, GitHub `VictorPasqualini/sparquet-cola`) com pyproject,
      CI/publish (mesmo padrão do sparquet), README trilíngue e docs. **Publicado no PyPI**
      como **`sparquet-cola`** 0.1.0 (import `sparquet_cola`). O `sparquet` o declara em
      `dependencies` (`sparquet-cola>=0.1.0`, sem cap — mantido retrocompatível) e é
      validado contra o pacote do PyPI; os shims `sparquet.validation.*` seguem
      reexportando dele.
- [ ] **`sparquet-lite`** — versão que roda puramente em Python **sem Spark**
      (duckdb / polars / pandas), para volumes pequenos e dev local rápido. Reusar o
      mesmo schema JSON de pipeline e, idealmente, o `sparquet_cola` nas validações.

---

## 11. Testes e cobertura

Inventário completo, com o porquê de cada lacuna, em
[docs/TEST_PLAN.md](docs/TEST_PLAN.md).

O que está pinado hoje: os 14 conectores de banco/warehouse/stream (montagem de
opções), o DSL de threshold, a severidade, o parse da quarentena, a expansão de
`targets`, o dialeto CSV, o round-trip dos seis formatos nativos, o **runner (180
testes**: histórico 48, identidade 64, workspace 10, créditos 42, escopo de execução
16) e o **Studio (455 testes** + 19 checagens de smoke em Chrome real).

As duas lacunas que pesam, e por que pesam:

- [ ] **Métricas contra dados reais** — `avg`, `min`, `max`, `sum`, `stddev`,
      `distinct_count`, `duplicate_*`, `missing_*`, `invalid_*` e `freshness` **nunca
      rodaram sobre um DataFrame** em teste: a suíte Spark do cola cobre `not_null`,
      `unique`, `range`, `regex` e `row_count`. Uma expressão de agregação errada passa
      verde hoje. Um arquivo local-Spark com uma métrica de cada família sobre um CSV
      fixo fecha isso.
- [ ] **Nenhuma transformação tem teste de comportamento** — o `test_examples.py`
      confere que o `type` existe, não o que sai do DataFrame. São 19 transformações;
      as de maior risco são `select` (expressão com alias), `with_column` (mapa em
      ordem), `struct` (dot-path aninhando), `join` (`on` como SQL, `broadcast`,
      `with_transformations`), `group_by` (pivot), `collect` + `{{var}}` (lista vazia →
      `NULL`) e `skip_if_false` (os três casos).

Restante, na ordem do plano:

- ✅ **Round-trip dos 6 formatos nativos** (`parquet`, `csv`, `json`, `orc`, `txt`,
      `view`) — `tests/test_formats_roundtrip_spark.py`, 11 casos: escreve pelo
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
- [ ] **`apply_template` e `$include`** — puros e rápidos; a tabela de formatação do
      CLAUDE.md já é a lista de casos.
- [ ] **Camada HTTP do runner** (`sparquet-studio/server/main.py`) — os módulos de apoio
      já têm teste (`history.py`, `auth.py`, `workspace.py`, `credits.py`) e o escopo de
      execução também (`test_run_scope.py`), mas a **camada HTTP em si não tem nenhum**:
      token, allow-list de origem, a sequência de eventos SSE, o status por estágio do
      `/run/flow/stream` e a dependência `requires(...)` que liga cada rota à ação que
      ela exige — o avaliador de política está pinado, mas nada afirma que é o
      `PUT /workspace` que cobra `workspace:Write`. É o único componente que executa
      conf arbitrária, e a postura de segurança dele quebra em silêncio. `TestClient` do
      FastAPI cobre sem Spark — **exige `httpx` no ambiente de teste**, que hoje não
      está instalado (foi o que impediu de já fechar esta lacuna).
- [ ] **`mode: merge`** (Delta e Iceberg) — precisa do `delta-spark`, então vai num
      arquivo que se pula quando o jar falta.
- [ ] **Orquestração do `Pipeline`** — `input_df`, `columns`, `input_view`, projeção por
      output, `transformations` por output, `PipelineResult` nunca levantando exceção.
- [ ] **`opensearch`** — conector próprio (opções `opensearch.*`), nunca afirmado, ao
      lado do caso `elasticsearch` de que foi separado.
- [ ] **Integração com containers** — Postgres, MySQL, Mongo, Cassandra, Elasticsearch e
      Kafka têm imagem oficial. Atrás de env var, **nunca** na execução default (que
      tem de continuar offline e de um segundo).
