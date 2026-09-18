# Exemplos de pipelines (`examples/`)

Confs de demonstração das capacidades do framework. A maioria é **ilustrativa** (os
caminhos/tabelas são fictícios) — copie e ajuste para o seu caso. Os exemplos **07** e
**08** rodam localmente sobre o CSV de `examples/data/`, sem cluster nem credencial:
`sparquet examples/08_validacao_multi_alvo.json`.

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
| [05_data_quality_soda.json](05_data_quality_soda.json) | `broadcast` join (map-side) + validações estilo **SODA** (cada métrica é um `type` de regra, com threshold `must_be`/`warn`, `valid_format`, `freshness`) e `schema`; relatório com `severity`/`metric_value` |
| [06_quarentena_validacoes.json](06_quarentena_validacoes.json) | `validations.outputs` (quarentena por linha: `valid`/`invalid`) + `validations.report`. As saídas de validação são **laterais**: `_write_validation_outputs(df)` e `_write_outputs(df)` recebem o **mesmo df completo**, então o `output` principal continua gravando **todas** as linhas |
| [07_quarentena_codigos.json](07_quarentena_codigos.json) | **Códigos de falha por linha**: `code` na regra (e o código derivado da expressão quando ele é omitido), `validations.outputs.invalid.rules` para escopar a quarentena a alguns códigos e `annotate` para gravar em cada linha rejeitada o `array<string>` com os códigos que a rejeitaram. **Rodável localmente** (CSV → CSV + JSON) |
| [08_validacao_multi_alvo.json](08_validacao_multi_alvo.json) | **Uma entrada de regra, vários alvos** (`targets`): tudo fora de `targets` é default compartilhado (o `min` do `range`), cada alvo sobrescreve o que quiser (o `max`), e o resultado são N regras **independentes** — 3 entradas viram 7 linhas no relatório, cada uma com seu alvo e seu código. O escopo da quarentena cita **um** alvo do `range`, então as violações de `id` ficam fora dela. **Rodável localmente** (CSV → CSV + JSON) |

## Dados de exemplo

`seed_iceberg_demo.py` cria um warehouse Iceberg local (catálogo Hadoop, ou seja,
diretórios comuns) com `sales.orders` e `sales.order_items`, para abrir no SQL
editor do Studio. Precisa de Java e `pip install pyspark`; o jar do Iceberg é
baixado do Maven na primeira execução.

```bash
python examples/seed_iceberg_demo.py                  # /data/iceberg
python examples/seed_iceberg_demo.py E:/data/iceberg  # outro diretório
```

Os `customer_id` batem com a tabela Delta de clientes, então dá para testar join
entre Iceberg e Delta na mesma query.

Referência completa do schema das confs: [CLAUDE.md](../CLAUDE.md).
