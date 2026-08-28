# sparquet — Backlog de desenvolvimento

Melhorias e pendências de desenvolvimento **do framework** (não de um caso de uso
específico). Cada item é uma capacidade genérica, ortogonal e sem acoplamento de
domínio.

Atualizado em 2026-08-27.

Cobertura de testes (o que está garantido e o que não está, conector por conector,
transformação por transformação): [docs/TEST_PLAN.md](docs/TEST_PLAN.md). Os itens de
§8 saem de lá — a ordem é a do plano.

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

- **Marcadores de etapa nos logs** (framework, v0.3.1): `Pipeline`/`TransformationEngine`
  emitem `step=True` com `scope` (`input`/`transformation`/`output`), `index` e `total`.
  Puramente aditivo — `apply(..., top_level=False)` mantém o comportamento anterior.
  É o que alimenta o status por etapa no canvas do Studio.
- **Studio — vocabulário**: `Workflow` (container) › `Job` (um JSON) › `Pipeline`
  (conjunto ordenado de Jobs, executado em sequência). O framework **não** mudou:
  para ele um JSON continua sendo um *pipeline* (classe `Pipeline`).
- **Studio — execução em sequência**: `POST /run/flow/stream` no runner executa vários
  Jobs numa SparkSession compartilhada, com status/logs por estágio e `stop_on_error`.
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

### Validações candidatas no `sparquet_cola`

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

**E. Depende de histórico de execuções** (ver §7 — sem persistir execuções não há
linha de base para comparar):

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
- ✅ **Studio — histórico de execuções** — `PipelineRun`/`JobRun`/`StepRun` persistidos
      em SQLite (`server/history.py`, `ExecutionRepository`), sobrevivem a reiniciar o
      app. `GET /runs` (lista) e `GET /runs/{id}` (detalhe) servem o novo
      `ExecutionHistoryPanel` no Studio, decoupled do estado efêmero de
      `useEditorStore`/`usePipelineEditorStore`.
- ✅ **Studio — histórico de status por etapa** — `StepTracker` grava cada etapa
      (input/transformação/validação/output) como `StepRun` durante a execução; num Job
      com falha o step que quebrou fica `FAILED` com a mensagem de erro persistida, os
      demais Jobs de um Pipeline ficam `SKIPPED` — responde "onde quebrou da última vez"
      sem reexecutar.
- ✅ **Studio — estado de cada caixa ao abrir uma execução** — abrir um Job pinta cada
      caixa com o status da última execução (ou da execução exata que o usuário
      escolheu no histórico), com uma tarja dizendo *qual* run está na tela; o
      Inspector mostra duração, linhas e erro daquela caixa naquele run. Num Pipeline o
      mesmo vale para os stages, e abrir um stage cai no Job **fixado na execução que
      aquele stage rodou** — o drill-down estilo Databricks. Mapeamento em
      `lib/runner/stepNodes.ts` (step → caixa, `validation_sink` por `role`),
      `lib/runner/stageRuns.ts` (stage → `job_run`, casado por `job_id`, nunca por
      posição) e `lib/runner/runView.ts` (qual execução carregar).
- ✅ **Studio — cancelar a execução de verdade** — o Stop agora chama
      `POST /runs/{id}/cancel`: uma flag interrompe o fluxo na próxima fronteira de
      estágio e `cancelAllJobs()` aborta o que o Spark estiver computando (sem isso uma
      escrita longa terminava mesmo depois do Stop). `cancelled` virou status de
      primeira classe — servidor, histórico, canvas e painéis o tratam como
      encerramento a pedido, nunca como falha.
- ✅ **Studio — histórico de logs por execução** — cada linha que a execução imprimiu
      (framework, JVM, `stdout`) é persistida em `run_log` a partir da mesma fila que
      alimenta o SSE, então o histórico guarda exatamente o que o usuário viu ao vivo.
      `GET /job-runs/{id}/logs` pagina por `seq` (não por offset, que reliria linhas de
      um run ainda em andamento); teto de 3000 linhas por execução, com uma linha
      `WARNING` dizendo quantas ficaram de fora.
- ✅ **Studio — nova apresentação do histórico** — o painel virou lista de execuções
      com faixa de status das últimas 14 (altura proporcional à duração) e uma ação de
      canvas por linha; o detalhe saiu do acordeão apertado da lateral e virou um
      diálogo com os estágios, os passos e os logs completos, com filtro por nível e
      por origem, busca e paginação. Erros longos passam a viver em cartão rolável
      (`ErrorCard`), na lateral e nas caixas do canvas.
- ✅ **Studio — fluxo e histórico lado a lado, estilo Databricks** — abrir um Job ou um
      Pipeline não pinta mais a última execução por cima do canvas: o centro da tela virou
      uma área de trabalho com abas (`Flow | JSON | Runs` no Job, `Flow | Runs` no
      Pipeline). A aba **Runs** é a tabela de execuções — status, *run id*, início,
      duração, *run as*, *launched* — e o *run id* é o link do drill-down
      (`Job/Pipeline → run id`), que abre o detalhe da execução e, de lá, pinta o canvas.
      O JSON deixou de existir só na lateral estreita: a mesma superfície (preview/edição
      Monaco) roda no centro. Detalhes de execução ganharam *job id*, *job run id*,
      *run as*, *launched*, início, fim, duração, status e **lineage** (o que a execução
      leria e escreveria, lido do JSON submetido — logo, existe mesmo em run que morreu
      antes de escrever). No servidor: `pipeline_run.run_as`/`launched`,
      `job_run.lineage` e `history.lineage_of()`.
- [ ] **Studio — consumo de tokens por execução** — contabilizar uso na régua
      **1 token = 1 Job executado** (um Pipeline de N Jobs consome N tokens), com o
      total por execução visível no histórico.
- [x] **Storage dos JSONs — padronização e versionamento em git** — feito. A biblioteca
      do Studio passou a morar em arquivos reais, servidos pelo runner
      (`sparquet-studio/server/workspace.py`, endpoints `GET /workspace`,
      `PUT`/`DELETE /workspace/{kind}/{id}` e `PUT`/`DELETE /workspace/meta/{key}`).
      Cada registro vira dois arquivos: o revisável — `<workflow>/workflow.json`,
      `<workflow>/jobs/<slug>.json` (o **JSON compilado**, que `sparquet run` executa
      sem tradução) e `<workflow>/pipelines/<slug>.json` — e um sidecar em
      `.studio/<kind>/<id>.json` com o registro completo do editor (posições de
      canvas, parâmetros). Diretório padrão `sparquet-workspace/` na raiz, configurável
      por `SPARQUET_STUDIO_WORKSPACE`; renomear move o arquivo em vez de deixar cópia
      velha, e renomear um Workflow move tudo que está sob ele. O cliente escolhe o
      backend em cadeia — workspace, depois IndexedDB, depois `localStorage`, depois
      memória (`src/lib/storage/db.ts` + `remote.ts`) — e uma biblioteca que só existia
      no navegador é empurrada uma única vez para um workspace vazio. Fonte da verdade:
      o arquivo; o navegador virou cache de quando o runner não está no ar.
- ✅ **Studio — histórico aponta para a versão do JSON, não só para o Job** — cada
      execução grava a impressão digital do que rodou de verdade: `job_run.config_hash`
      (`sha256:<hex>` do JSON canônico — chaves ordenadas, sem espaços — já com
      `{param}` resolvido, pela mesma razão que o lineage resolve) e `job_run.config`
      com o JSON íntegro até 512 KB (`history.config_version()`,
      `MAX_STORED_CONFIG_BYTES`). Acima disso guarda-se só o hash, que continua
      respondendo "estas duas execuções rodaram o mesmo JSON?". O detalhe da execução
      mostra a versão abreviada e busca o JSON sob demanda em
      `GET /job-runs/{id}/config` — a config não entra na listagem porque é maior que a
      linha que descreve o run. Migração acrescenta as colunas em base existente; run
      anterior à mudança aparece como versão desconhecida, não como versão errada.
- ✅ **Studio — usuários, login e permissionamento estilo IAM** — o runner ganhou
      identidade (`sparquet-studio/server/auth.py`, SQLite próprio em
      `SPARQUET_STUDIO_AUTH_DB`). Dois modos, decididos por existir usuário ou não: sem
      usuário nada muda — o token compartilhado é a identidade e ninguém fica trancado
      do lado de fora ao atualizar; criado o primeiro usuário, o runner passa a exigir
      sessão **além** do token. Política no formato `{effect, actions, resources}` com
      ação `service:Verb` (`workspace:Write`, `run:Execute`, `iam:ManageUsers`) e
      recurso `kind/id`, `*` em qualquer posição, **deny explícito vence** e o padrão é
      negar; papéis nativos `admin`/`editor`/`operator`/`viewer`. Senha em scrypt (com
      PBKDF2 de reserva), sessão guardada como hash — cópia do arquivo não é um conjunto
      de logins vivos. Desabilitar conta e trocar senha derrubam as sessões abertas, e o
      último administrador ativo não pode ser rebaixado, desabilitado nem removido.
      Toda rota passou a declarar a ação que exige; `run as` deixou de ser texto livre
      quando há usuário — quem executou é fato, não rótulo. No cliente: `store/auth.ts`,
      tela de login e a seção **Access** em Settings.
- ✅ **Studio — recuperação de senha** — código de uso único em vez de e-mail, porque
      o runner não tem servidor de e-mail e não deveria ganhar um. Um administrador
      (`iam:ManageUsers`, em **Settings → Access → Recovery code**) ou quem opera a
      máquina (`python server/auth.py recovery-code <user>`) emite o código; a pessoa o
      gasta na tela de login em *I have a recovery code* e escolhe a própria senha. Isso
      não cria autoridade nova — quem roda a CLI já é dono do host —, só evita que a
      senha seja ditada por chat. Tabela `recovery` no SQLite de identidade guardando
      apenas o SHA-256 do código; emitir invalida o anterior não usado; vale uma vez e
      expira em `SPARQUET_STUDIO_RECOVERY_MINUTES` (padrão 30); resgatar derruba todas as
      sessões da conta; a senha nova é validada **antes** de o código ser queimado (senha
      curta não gasta o código); conta desabilitada não é recuperável; e toda recusa tem
      a mesma mensagem — o endpoint não diz se o código era desconhecido, expirado ou já
      usado. Rotas `POST /auth/users/{id}/recovery` e `POST /auth/recover` (esta exige o
      token compartilhado e **nenhuma** sessão, já que vem da tela de login).
- ✅ **Studio — créditos de execução** — uma moeda por Job, cobrada **só quando o Job
      não roda na máquina do runner** (`sparquet-studio/server/credits.py`, SQLite
      próprio em `SPARQUET_STUDIO_CREDITS_DB`). A localidade sai da configuração do Job,
      nunca de um campo da requisição, senão bastaria o cliente declarar-se local:
      `spark.master` (ou `spark.configs["spark.master"]`) começando com `local` é grátis;
      `spark.remote` cobra mesmo com master local ao lado; `yarn`/`spark://`/`k8s://`
      cobram; e runner rodando em Databricks/EMR/Dataproc/Synapse cobra todo Job,
      qualquer que seja o master. **Medir e cobrar são coisas separadas**: por padrão o
      livro-razão registra e não bloqueia nada, e só `SPARQUET_STUDIO_CREDITS=on` faz o
      saldo barrar execução (**HTTP 402** antes de começar) — por isso a conta tem
      `balance`, que só se move sob cobrança, e `spent`, que sempre sobe: ligar a
      cobrança parte do que foi concedido, não da dívida acumulada. Cobrança na admissão
      e **sem estorno**: run recusado antes de começar não paga, run que começou e falhou
      paga. Flow cobra por etapa conforme cada uma começa, então flow que acaba o saldo na
      quarta rodou três de verdade e o razão diz isso. Conta = usuário, ou a conta
      literal `token` em modo sem usuários. Ações `credits:Read` e `credits:Manage`,
      rotas `/credits/me`, `/credits`, `/credits/{id}/ledger`, `/credits/{id}/grant`,
      `credits_enforced` no `/health`, painel em **Settings → Access**.
- [ ] **Créditos — o que ficou pendente** — o que existe cobra e registra; falta o que
      uma cobrança de verdade pede depois: **cota por período** (hoje o saldo é um
      estoque, não "N execuções por mês" com recarga automática), **preço por tamanho**
      (todo Job remoto custa igual, independentemente de rodar dois minutos ou seis
      horas — o caminho natural é cobrar por duração ou por linha processada, dados que
      o histórico já tem), **reserva antes de executar em vez de débito** (com estorno
      quando o run é recusado pelo cluster, não pelo runner), **conta por equipe/
      Workflow** e não só por pessoa, **aviso de saldo baixo** antes do 402, e
      **conciliação com o custo real** do cluster (o crédito hoje é uma unidade interna,
      não tem relação com o que a nuvem cobrou). Também falta cobrar execução que não
      passa pelo runner do Studio (`sparquet.cli`, job agendado) — hoje ela é invisível
      para o razão.
- [ ] **IAM — o que ficou pendente** — o que existe cobre autenticar e autorizar; falta
      o que uma instalação com várias pessoas cobra depois: **papéis customizados pela
      interface** (o modelo já aceita, só o `admin` por CLI/SQL cria), **escopo por
      Workflow nas rotas de execução** (`/run`, `/run/stream`, `/run/flow/stream` só
      sabem o alvo pelo corpo da requisição, que a dependência de permissão não lê —
      hoje autorizam `run:Execute` em `*`), **log de auditoria** (quem mudou o quê e
      quando; o histórico registra execução, não alteração), **SSO/OIDC** e senha
      gerenciada fora do runner, **expiração/rotação de sessão configurável por
      política** (hoje só `SPARQUET_STUDIO_SESSION_HOURS`), e **UI ciente de permissão em
      toda a tela** (os painéis de Access e de créditos escondem o que não é permitido; o
      resto do Studio ainda oferece botões que voltam 403).
- [ ] **Monitoramento e observabilidade** — revisar e implementar. Hoje existe a matéria
      prima e não o produto: log estruturado em JSON (`sparquet/utils/logger.py`),
      `PipelineResult` com o resultado de cada validação, e o histórico do Studio
      (`pipeline_run`/`job_run`/`step_run`/`run_log`) com duração e status por etapa. Não
      existe nada disso *fora* de uma execução isolada. A cobrir: métricas exportáveis
      (OpenTelemetry/Prometheus — duração, linhas lidas/escritas, taxa de falha, por
      Job/etapa), traço distribuído por execução, alerta (run falhou, run não rodou,
      duração fora da faixa, queda de volume), painel de saúde do conjunto de Jobs
      (não de um por vez), retenção/rotação do SQLite de histórico, e o mesmo caminho
      valendo para execução fora do Studio (`sparquet.cli`, Databricks, EMR) — hoje o
      histórico só existe quando o runner do Studio é quem executa. Ver §3 (lineage) e §4
      (métricas por etapa), que são pré-requisitos parciais.
- [ ] **Catálogo de dados** — revisar e implementar. Cada Job já declara o que lê e o que
      escreve, e desde o item de histórico acima isso é persistido por execução
      (`job_run.lineage`). Falta transformar essas arestas num catálogo: inventário de
      datasets (endereço, formato, schema observado, dono, Jobs que produzem e que
      consomem), grafo de lineage cruzando Jobs e Pipelines, última atualização e
      frescor, e o resultado das validações do `sparquet_cola` anexado ao dataset — a
      resposta para "de onde veio isto, quem depende disto, e dá para confiar". Decidir
      se é catálogo próprio (servido pelo runner, consumível pelo Studio e pela IA como
      contexto) ou integração com um existente (Unity Catalog, DataHub, OpenMetadata) —
      e, se próprio, se ele também alimenta a paleta do Studio com fontes já conhecidas.

- [ ] **`sparquet-lite`** — versão que roda puramente em Python **sem Spark**
      (duckdb / polars / pandas), para volumes pequenos e dev local rápido. Reusar o
      mesmo schema JSON de pipeline e, idealmente, o `sparquet_cola` nas validações.

---

## 8. Testes e cobertura

Inventário completo, com o porquê de cada lacuna, em
[docs/TEST_PLAN.md](docs/TEST_PLAN.md). O que está pinado hoje: os 14 conectores de
banco/warehouse/stream (montagem de opções), o DSL de threshold, a severidade, o parse
da quarentena, a expansão de `targets`, o dialeto CSV, o round-trip dos seis formatos
nativos, o runner (91 testes: histórico, identidade, workspace) e o Studio (440 testes +
19 checagens de smoke em Chrome real).

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
- [ ] **Semântica de `on_failure`** — que `fail` aborta **antes** de qualquer escrita e
      antes do relatório é promessa de segurança de dado, e nada verifica.
- [ ] **`apply_template` e `$include`** — puros e rápidos; a tabela de formatação do
      CLAUDE.md já é a lista de casos.
- [ ] **Serviço runner** (`sparquet-studio/server/main.py`) — os módulos de apoio já têm
      teste (`history.py`, `auth.py`, `workspace.py`); a **camada HTTP não tem nenhum**:
      token, allow-list de origem, o `403` no `/run`, a sequência de eventos SSE, o
      status por estágio do `/run/flow/stream` e a dependência `requires(...)` que liga
      cada rota à ação que ela exige — o avaliador de política está pinado, mas nada
      afirma que é o `PUT /workspace` que cobra `workspace:Write`. É o único componente
      que executa conf arbitrária, e a postura de segurança dele quebra em silêncio.
      `TestClient` do FastAPI cobre sem Spark.
- [ ] **`mode: merge`** (Delta e Iceberg) — precisa do `delta-spark`, então vai num
      arquivo que se pula quando o jar falta.
- [ ] **Orquestração do `Pipeline`** — `input_df`, `columns`, `input_view`, projeção por
      output, `transformations` por output, `PipelineResult` nunca levantando exceção.
- [ ] **`opensearch`** — conector próprio (opções `opensearch.*`), nunca afirmado, ao
      lado do caso `elasticsearch` de que foi separado.
- [ ] **Integração com containers** — Postgres, MySQL, Mongo, Cassandra, Elasticsearch e
      Kafka têm imagem oficial. Atrás de env var, **nunca** na execução default (que
      tem de continuar offline e de um segundo).
