# sparquet — Backlog de desenvolvimento

Melhorias e pendências de desenvolvimento **do framework** (não de um caso de uso
específico). Cada item é uma capacidade genérica, ortogonal e sem acoplamento de
domínio.

Atualizado em 2026-07-08.

---

## 1. Princípio

Núcleo fino e modular (registry de IO + transformações + validações + engine) que
transforma pipelines imperativos em **configuração declarativa**. Toda evolução
deve preservar: ortogonalidade (capacidades não se acoplam), extensibilidade via
`register_*`, e o limite saudável **"config declarativa, não código em JSON"**.

---

## 2. Conectores de IO (novos formatos)

Cada formato é um par `BaseReader`/`BaseWriter` registrado nas factories. Hoje:
`parquet`, `csv`, `delta`, `iceberg`, `txt`, `view`, `kafka` (write).

Pendentes / candidatos:

| Conector | Leitura | Escrita | Notas |
|---|---|---|---|
| `json` | ☐ | ☐ | multiline, schema inference, `to_json`/`from_json` já existem em transformações |
| `jdbc` | ☐ | ☐ | bancos relacionais; particionamento por coluna, `fetchsize`, upsert |
| `avro` | ☐ | ☐ | requer `spark-avro`; schema registry (futuro) |
| `orc` | ☐ | ☐ | nativo Spark |
| `kafka` (read) | ☐ | n/a | hoje só escrita; leitura batch/stream (offsets, schema do value) |
| `rest`/`http` | ☐ | ☐ | ingestão de APIs (paginação, auth) — provavelmente via reader custom |
| `excel`/`xml` | ☐ | ☐ | nichos; via libs externas (`spark-excel`, `spark-xml`) |

Diretriz: manter o reader/writer fino; opções específicas via `options`. Formatos
que exigem dependência extra entram como `optional-dependencies` no pyproject
(como `delta` hoje).

---

## 3. Data quality & governança  (eixo estratégico)

Hoje: bloco `validations` com `not_null`, `unique`, `range`, `regex`, `row_count`,
`custom_sql`; `on_failure` (fail/warn/skip); resultados em
`PipelineResult.validation_results` (`failed_count` + mensagem).

Evolução proposta:

1. **Validators reutilizáveis de alto nível** (em vez de repetir `custom_sql`):
   - `reconciliation`/`count_match` — compara contagem do df contra outra fonte
     (tabela/view) por chave; retorna `failed_count` por grupo.
   - `completeness` — % de não-nulos por coluna com limiar.
   - `freshness` — idade máxima de uma coluna de data.
   - `accepted_values` — valor em conjunto/domínio (enum).
   - `referential` — FK existe em tabela de referência (anti-join).
2. **Métricas de qualidade estruturadas** — `validation_results` ganham métrica
   numérica padronizada (não só pass/fail) para alimentar dashboards; emissão
   opcional para uma tabela de auditoria (`data_quality_results`).
3. **Data contracts / expectations declarativas** — schema esperado (colunas,
   tipos, nullability) versionado por pipeline; valida antes de transformar.
4. **Quarentena** — em vez de `fail`/`warn`, rotear linhas inválidas para um
   destino de quarentena (split good/bad) sem abortar.
5. **Lineage / observabilidade** — registrar fonte→destino, contagens por etapa,
   tempo por etapa (ver §4) e versão da config; expor para catálogo.

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
