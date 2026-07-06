# Case of Success — Registro de Lastros (`tests/case-of-success/`)

Trilha **específica deste caso de uso**: a migração dos jobs Spark de registro
(NC, CPR, Duplicata, CCB) para confs declarativas do spark_framework. Acompanha o que já
foi migrado, as decisões de arquitetura do caso e o status por fluxo.

> Para planejamento/evolução **estrutural do framework** (novos conectores, data
> quality/governança, etc.), ver [ROADMAP.md](ROADMAP.md).

Atualizado em 2026-06-16.

---

## 1. Princípio norteador

O framework é um **núcleo fino e modular** (registry de transformações + IO + engine)
que transforma pipelines de dados imperativos em **configuração declarativa (JSON)**.
A meta do caso de uso de registro: **o Python só orquestra** as confs; a lógica de
dados vive nas confs (ou em transformações registradas).

A pergunta de governança permanente:
> *Isto ainda é configuração declarativa, ou virou código Spark escrito em JSON?*

---

## 2. Já entregue (capacidades do core adicionadas)

Capacidades genéricas de dados, ortogonais e sem acoplamento de domínio:

| Capacidade | Tipo / API | Observação |
|---|---|---|
| `distinct` | transformação | registrada (estava órfã) |
| `checkpoint` | transformação | `method` (localCheckpoint/checkpoint), `eager`; método inválido → ignora + warning adiado |
| `collect` + `{{var}}` | transformação + runtime store | coleta valores no driver → placeholder de runtime para pushdown (`IN (...)`) |
| `group_by` | só expressões SQL (string) + `pivot` | removida a forma dict |
| `struct` | transformação | monta struct aninhado a partir de mapa campo→expressão (recursivo) |
| `with_column` | `column` (além de `name`) + `columns` (mapa, várias colunas em ordem) | |
| `transformations` por output | `OutputConfig.transformations` | cada destino aplica sua própria forma sobre o df principal |
| `stop_if_empty` | transformação + `PipelineResult.skipped` | encerra graciosamente sem dados (sem escrever saídas) |
| warnings adiados | `logger.defer_warning` / `flush_deferred_warnings` | emitidos no fim do pipeline |

**Arquitetura staging → commit (adotada):** cada conf de registro só monta o payload
e grava no **staging genérico** `view_registro_staging` (colunas comuns: `id_operacao,
id_vert, id_cessao, numero_contrato, payload`). Uma **conf de commit genérica**
(`conf_commit_registro.json`), rodando no loop 1×/fluxo, lê o staging, **verifica** os
contratos (via `validations`) e grava os **3 destinos** (Kafka, silver_registro_contratos,
silver_registro_parcelas). O Kafka deduplica por `id_vert` (grão contrato no registro,
lote na emissão). Isso elimina a duplicação dos 3 outputs em cada conf.

**Data quality:** a verificação de "não houve perda" é uma `validation` `custom_sql`
(`on_failure: warn`) na conf de commit, que cruza o staging com `view_cessoes_pendentes`
(elegíveis). Resultado em `PipelineResult.validation_results` (mensagem + `failed_count`).

Confs entregues no caso de uso (`tests/case-of-success/`):
- `conf_cessoes_pendentes_registro.json` — base de cessões pendentes (ETAPAS 2–6 do job antigo).
- `conf_registro_b3_nota_comercial.json`, `conf_registro_b3_cpr.json`, `conf_registro_vert_duplicata.json`, `conf_registro_cerc_duplicata.json` — montam payload → staging.
- `conf_commit_registro.json` — verifica (validations) + grava os 3 destinos.
- `job_registro.py` — orquestrador (base → loop: registro → commit).

**Branching de fluxo numa conf só** (Duplicata/CERC): os 3 fluxos (REGISTRO/EMISSAO/
EMISSAO_E_REGISTRO) vivem num único arquivo. O orquestrador passa só `fluxo_operacao`;
a conf seleciona blocos com `skip_if_false` por **expressão booleana** (ex:
`"'{fluxo_operacao}' in ('EMISSAO', 'EMISSAO_E_REGISTRO')"`). Janela unificada
`(id_cessao, lote_index)` com `lote_index` específico por fluxo (dense_rank por
contrato no REGISTRO; lotes de 300 na emissão).

---

## 3. Decisões de arquitetura em aberto (próximos passos)

### 3.1. Onde fica a lógica bespoke complexa
Decisão até aqui: **conf pura** para Duplicata (VERT e CERC) — viável e legível com
`struct` aninhado + `skip_if_false` por expressão. **Falta CCB/CERC**, que é o mais
pesado (resolução de coluna por parametrização `usa_dados_auxiliares`/`usa_dados_do_sistema_emissor`,
construção de IPOC, dezenas de colunas `_checked`, `agentes` com devedores/credores).

Opções para o CCB:
- **(A) Conf pura** — coerente com o resto; conf grande (~250+ linhas) com muitos
  `with_column` de CASE WHEN e a montagem do IPOC em SQL.
- **(B) Híbrido** — o payload/IPOC vira uma **transformação registrada**
  (`fw.register_transformation`, ex: `{ "type": "payload_ccb" }`), Python testável,
  e a conf cuida de IO/joins/pushdown/staging. Recomendado se a lógica de
  parametrização do CCB ficar ilegível em JSON.

> A decidir ao montar o CCB — "partir disso para pensar em outras estruturas".

> Status: aguardando escolha do usuário antes de implementar.

### 3.2. Duplicata/CERC tem 3 fluxos
REGISTRO / EMISSAO / EMISSAO_E_REGISTRO produzem payloads e janelas diferentes.
Encaminhamento: **uma conf de registro por fluxo** (cada uma grava no staging), e o
`CONFS_REGISTRO` mapeia cada fluxo ao seu tópico (`fluxos: {fluxo: topico}`). A conf
de commit continua genérica.

### 3.3. Reduzir duplicação entre confs  ✅ RESOLVIDO (staging → commit)
A duplicação dos 3 outputs (Kafka headers, silver_registro_contratos,
silver_registro_parcelas) foi eliminada: agora vivem só na `conf_commit_registro`.
As confs de registro entregam no staging genérico. `$include` deixa de ser necessário
para isso (continua disponível para fragmentos de transformação se útil no futuro).

### 3.4. Tornar explícito o que hoje é implícito
- `to_json` depende do default `ignoreNullFields=true` para o `paymentMethod`
  condicional do CPR sumir quando nulo. Expor como opção visível no output Kafka
  (ex: `"options": { "ignore_null_fields": true }`) em vez de herdar o default.

### 3.5. Diferenças de comportamento assumidas (confirmar)
- **CPR `paymentMethod`**: feito **por linha** (`case when tipo_codigo_cpr='F'...`),
  enquanto o `.py` decidia **por lote** (`.first()`). Mais correto, mas diferente.
- **`lpad numero_contrato`**: removido conforme edição do usuário no `.py` do CPR.

---

## 4. Diretrizes de uso (boas práticas)

**Cabe bem na conf:** input/IO, filtros, joins, pushdown (`collect`+`{{var}}`),
shaping de colunas, multi-output com `transformations`, `stop_if_empty`, structs
moderados via `struct`.

**Começa a doer na conf (considerar transformação registrada):** payload bespoke
com 30+ expressões; lógica condicional de *schema*; algo que se queira **testar
isoladamente**; helpers reutilizados entre fluxos (ex: `retorna_coluna_baseada_na_parametrizacao`).

**Regras de ouro:**
- Materialize o df com `checkpoint` antes de múltiplos outputs (evita recomputar a linhagem).
- `collect` sempre **após** `checkpoint` (df já materializado → collect barato).
- Sem truncamento/limite silencioso: se algo for cortado, logar (warning adiado).

---

## 5. Backlog estrutural do framework

Movido para o roadmap do framework → [ROADMAP.md](ROADMAP.md) (dry-run, testes
unitários, métricas por etapa, perfis dev/staging/prod, conectores, governança).

---

## 6. Migração do caso de registro — status

| Fluxo | Origem | Status |
|---|---|---|
| Base cessões pendentes | `job_registro_old.py` (ETAPAS 2–6) | ✅ `conf_cessoes_pendentes_registro.json` |
| NOTA_COMERCIAL / B3 | `registro_b3_nota_comercial.py` | ✅ `conf_registro_b3_nota_comercial.json` |
| CPR / B3 | `registro_b3_cpr.py` | ✅ `conf_registro_b3_cpr.json` → staging |
| DUPLICATA / VERT (emissão) | `old/registro_vert_duplicata.py` | ✅ `conf_registro_vert_duplicata.json` → staging |
| DUPLICATA / CERC (3 fluxos) | `old/registro_cerc_duplicata` | ✅ `conf_registro_cerc_duplicata.json` (1 conf, branch por `skip_if_false`) → staging |
| Commit (verifica + grava 3 destinos) | loop do `job_registro_old.py` | ✅ `conf_commit_registro.json` (genérico) |
| CCB / CERC | `old/registro_cerc_ccb` | ⏳ pendente — conf pura ou híbrido (ver 3.1) → staging |
