# Changelog

Histórico de mudanças do SparkFramework. Segue o formato [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/).

**Para Claude/contribuidores**: sempre que fizer alterações no framework (capacidades de transformação, IO, validação, ou contrato de uso), **atualize a seção `[Unreleased]` abaixo** com uma descrição curta orientada ao usuário do framework. Quando uma versão for cortada, mova as entradas de `[Unreleased]` para a nova seção versionada com a data.

Categorias usadas (de Keep a Changelog):
- **Added** — recurso novo
- **Changed** — mudança em recurso existente
- **Deprecated** — recurso marcado para remoção futura
- **Removed** — recurso removido
- **Fixed** — correção de bug
- **Security** — vulnerabilidade tratada

---

## [Unreleased]

### Changed
- `SelectTransformation`: cada item em `columns` agora aceita Spark SQL com alias direto via `"expr as nome"`. Internamente passa por `F.expr()`, que resolve tanto nomes simples (`"id"`) quanto expressões com alias (`"a * b as produto"`, `"current_timestamp() as ts"`). O formato antigo `{"name": "...", "expression": "..."}` continua suportado para retrocompat.

### Added
- `CHANGELOG.md` (este arquivo) para manter contexto histórico do framework. Atualizado a cada mudança.

### Changed (registro_vert)
- **Refatoração**: 4 `<ativo>/cessoes_pendentes.json` consolidadas em **1 conf genérica** `confs/cessoes_pendentes.json` que filtra por `(tipo_ativo, registradora, fluxo_operacao)` + compatibilidade `multi_ativos` × `tipo_contrato` (via `${param_codigo_tipo_contrato}`).
- Os joins específicos de cada ativo (bronze_remessa, silver_sacado, silver_dados_auxiliares, silver_emissao_ccb, silver_parcela, silver_lastros_relacionamento) migraram para cada `<ativo>/payload.json`.
- Para Duplicata (3 subfluxos compartilham os joins): nova conf intermediária `duplicata_cerc/enriquecimento.json` evita duplicação 3×.
- `cessoes_pendentes.json` aceita `param_fluxo_operacao` `null` para skipar o filtro de fluxo (usado pela Duplicata, onde o filtro acontece em cada `payload_*` específico).
- Total de confs: 10 → 7.

---

## [0.4.0] — 2026-05-18 — Refactor: substituição `${param}` + skip_if SQL + checkpoint/cache como transformação

### Changed
- **Breaking**: `fw.run(conf, columns={...})` → `fw.run(conf, params={...})`. O parâmetro `columns` foi renomeado para `params`. Não injeta mais colunas literais no DataFrame; apenas substitui `${param_name}` em strings do JSON.
- **Breaking**: substituição `${param}` agora retorna **SQL value** apropriado:
  - string → `'value'` com aspas escapadas
  - lista → `array('a', 'b')` para uso com `array_contains`
  - `None` / lista vazia → `NULL`
  - bool → `true`/`false`
  - numérico → `123`
- Novo modificador `${name!raw}` retorna valor literal sem escape (para paths, options, nomes de view, broker Kafka, etc.)
- **Breaking**: `skip_if_null` → `skip_if`. Agora aceita uma **expressão SQL** avaliada via `spark.sql()`. Se o resultado for `FALSE` ou `NULL`, a transformação é skipada. Combinado com `${param}`, permite condicionais como `"skip_if": "${param_lista} IS NULL"`.
- Self-join (`"with": "self"`) marcado como caro (2 shuffles); docstring recomenda usar **window functions** (`collect_set/collect_list().over(partition by ...)`) quando o objetivo é criar coluna agregada. Mantido para casos onde window não cobre (anti-join cruzado).

### Added
- `SelectTransformation` aceita expressões além de nomes (`{"name", "expression"}` dentro de `columns`)
- `CheckpointTransformation` — `{ "type": "checkpoint" }` materializa via `localCheckpoint` e quebra a lineage em qualquer ponto do pipeline (não só no output)
- `CacheTransformation` — `{ "type": "cache" }` materializa em memória mantendo lineage

### Removed
- `Pipeline._inject_column` e `_is_empty_param` (não há mais injeção de colunas literais)
- `PipelineResult.output_df` (resultado vai para view/Delta/Kafka declarado)

---

## [0.3.0] — 2026-05-18 — Transformações específicas por output + params declarativos

### Added
- `OutputConfig.transformations` — lista de transformações aplicadas ANTES da escrita deste output específico (não afeta os demais). Essencial para output múltiplo com granularidades diferentes (ex: Kafka 1 msg por contrato + Delta parcelas via `explode` na mesma conf).
- `WithColumnsTransformation` (plural) — cria múltiplas colunas em batch. Mais legível que N `with_column` quando há regras correlatas (ex: 15+ regras `api_*` no fluxo CCB).
- `PipelineConfig.params` — lista declarativa dos params esperados. Pipeline loga warning quando algum param declarado está faltando.
- `ViewWriter` option `"checkpoint": "true"` — força `localCheckpoint` em vez do cache default (essencial em DAGs longos).

---

## [0.2.0] — 2026-05-18 — Substituição `${param}` em strings + auto-join + group_by expr

### Added
- Substituição de `${param_name}` em strings do JSON via `substitute_params` (paths, options, expressions). Aplicada antes da deserialização da conf.
- `JoinTransformation` com `"with": "self"` — reusa o df corrente como right-side. Útil para criar colunas agregadas via auto-join + `with_transformations`.
- `GroupByTransformation` com `"func": "expr"` — agregações arbitrárias via Spark SQL (`count(distinct struct(a, b)) > 1`).
- `SparkFramework.drop_views([...])` — helper para cleanup de temp views (com unpersist) no fim do orquestrador.
- `skip_if_null` em qualquer transformação — skipa quando o param de runtime está ausente/None/vazio.

### Removed
- `fw.run(conf, input_df=df)` — `input_df` não é mais aceito. Cada conf deve ler do seu `input` declarado (use temp views via `ViewWriter` para encadear pipelines).

---

## [0.1.0] — 2026-05-XX — Versão inicial

### Added
- Pipeline declarativo via JSON: `input` → `transformations` → `validations` → `outputs`
- Transformações nativas: `filter`, `select`, `drop`, `rename`, `cast`, `with_column`, `drop_duplicates`, `sql`, `fill_na`, `sort`, `group_by`, `join`, `union`
- Readers/Writers: CSV, Parquet, Delta, Iceberg, Kafka, TXT, View (temp view do Spark)
- `JoinTransformation` com `with_transformations` (transforms aplicadas no right-side antes do join) e aliases `l`/`r`
- Delta/Iceberg merge com aliases `T` (target) / `S` (source)
- Validators nativos: `not_null`, `unique`, `range`, `regex`, `row_count`, `custom_sql`
- `ValidationConfig.on_failure`: `fail` / `warn` / `skip`
- `OutputConfig.columns` — projeção de colunas por destino (sem afetar o df principal)
- `SparkFramework`: singleton de SparkSession + factories registráveis para readers/writers/transformations/validators customizados
- `PipelineResult` com `success`, `rows_read`, `rows_written`, `validation_results`, `error` (nunca lança exceção)
- Logger JSON estruturado
