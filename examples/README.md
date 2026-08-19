# Exemplos de pipelines (`examples/`)

Confs de demonstração das capacidades do framework. São **ilustrativas** (os
caminhos/tabelas são fictícios) — copie e ajuste para o seu caso. Para um caso
real e completo, veja [`tests/case-of-success/`](../tests/case-of-success).

Como executar (como biblioteca):
```python
from sparquet import Sparquet
fw = Sparquet()
print(fw.run("examples/01_ingestao_validacoes.json").summary())
fw.stop()
```

| Exemplo | Demonstra |
|---|---|
| [01_ingestao_validacoes.json](01_ingestao_validacoes.json) | Ingestão + limpeza (`filter`, `select`, `cast`, `rename`, `with_column`, `drop_duplicates`) + bloco `validations` |
| [02_join_e_pushdown_runtime.json](02_join_e_pushdown_runtime.json) | `checkpoint` + `collect` + placeholder `{{var}}` para empurrar filtro literal na leitura antes do `join` (predicate pushdown) |
| [03_payload_struct_multi_saida.json](03_payload_struct_multi_saida.json) | `stop_if_empty`, `struct` (struct aninhado), `with_column` múltiplo, e `transformations` por output (formas diferentes do mesmo df) |
| [04_merge_delta.json](04_merge_delta.json) | `group_by` com expressões SQL + escrita `mode: merge` (MERGE INTO no Delta) |
| [05_data_quality_soda.json](05_data_quality_soda.json) | `broadcast` join (map-side) + validações estilo **SODA** (`check` com métrica/threshold warn/fail, `valid_format`, `freshness`) e `schema`; relatório com `severity`/`metric_value` |
| [06_quarentena_validacoes.json](06_quarentena_validacoes.json) | `validations.outputs` (quarentena por linha: `valid`/`invalid`) + `validations.report`. As saídas de validação são **laterais**: `_write_validation_outputs(df)` e `_write_outputs(df)` recebem o **mesmo df completo**, então o `output` principal continua gravando **todas** as linhas |

Referência completa do schema das confs: [CLAUDE.md](../CLAUDE.md).
