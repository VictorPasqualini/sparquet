# Sparquet

Framework Python/PySpark orientado a JSON para pipelines de dados. É um **produto
reutilizável**, não um job pontual: toda mudança precisa valer para qualquer caso de
ingestão/transformação/qualidade, não só para o caso que motivou o pedido.

---

## Regras de trabalho

### Exploração — grafo antes de busca ampla

Existe um grafo do codebase em `graphify-out/` (AST local, sem API). Use-o **antes** de
`Grep`/`Glob` amplos ou de abrir arquivos "para entender":

```bash
python -m graphify explain "ValidationEngine"        # símbolo conhecido: vizinhos + file:line  (~350 tok)
python -m graphify affected "BaseWriter"             # o que quebra se eu mudar isto            (~150 tok)
python -m graphify query "<pergunta>" --budget 1200  # pergunta aberta; SEMPRE com --budget
python -m graphify god-nodes --top 10                # hubs arquiteturais
python -m graphify update .                          # após editar código (rápido, sem LLM)
```

`explain` e `affected` são precisos e baratos — prefira-os. `query` faz BFS a partir de
match difuso e traz ruído; use só para perguntas abertas e sempre com `--budget`.
Nome ambíguo (bate em `.md`/`.toml` também) → repita com o `id` que a saída sugere.
**Nunca leia** os artefatos brutos: `graph.json` (3 MB), `graph.html` (2,5 MB),
`GRAPH_REPORT.md` (30 KB) — só os comandos acima, que já vêm com orçamento.

Se o grafo não responder, aí sim busque — mas com alvo: `Grep` com `path`/`glob`
restrito ao módulo, nunca no repo inteiro.

### Leitura de arquivos

- Abra somente os arquivos que a tarefa exige. Não leia "o vizinho" por completude.
- Em arquivo grande, leia o trecho (offset/limit, `sed -n 'A,Bp'`), não o arquivo todo.
- Não releia o que já está no contexto desta conversa.

### Escopo

- Mexa só no que o pedido implica. Não refatore, renomeie nem reformate de passagem.
- Reutilize as abstrações existentes (`BaseReader/Writer`, `BaseTransformation`,
  factories, engines injetáveis) antes de criar estrutura nova.
- Não adicione dependência sem necessidade real; as existentes estão em `pyproject.toml`.

### Testes

- Rode primeiro o teste diretamente ligado à mudança: `python tests/<arquivo>.py`.
- Suíte inteira só quando a mudança for transversal (config, engine, factory).
- Studio: `npm run typecheck` no arquivo/área tocada antes de `npm run test`.

### Output de comandos e respostas

- Filtre a saída antes que ela entre no contexto: `| tail -30`, `| head -40`,
  `grep` do erro. Nunca despeje log/teste/build inteiro.
- Se um comando devolver milhares de linhas, resuma o resultado — não cole.
- Na resposta: resultado primeiro, sem narrar cada passo executado.
- Não repita código já mostrado no diff/arquivo; referencie `arquivo.py:linha`.
- **Mantenha na íntegra** o que serve para debugging: mensagem de erro, stacktrace,
  nome de config, valor divergente. Curto ≠ incompleto.

---

## Mapa do código

| Caminho | Conteúdo |
|---|---|
| `sparquet/framework.py` | `Sparquet` — entry point como lib; singleton SparkSession; registries de extensão |
| `sparquet/cli.py` | entry point de linha de comando |
| `sparquet/core/` | `config.py` (JSON→dataclasses) · `context.py` (SparkSession; detecta Databricks/EMR/Dataproc/Synapse/local) · `pipeline.py` (orquestrador, `run() → PipelineResult`) |
| `sparquet/io/` | `base.py` (contratos) · `factory.py` (registry de formatos) · um arquivo por formato (`parquet`, `delta`, `iceberg`, `csv`, `kafka`, `jdbc`, `bigquery`, …) |
| `sparquet/transform/` | `engine.py` (aplica em sequência, resolve `{{var}}`) · `builtin.py` (transformações nativas) |
| `sparquet/validation/` | `engine.py` (adaptador fino); os demais são **shims** que reexportam do pacote externo `sparquet_cola` |
| `sparquet/utils/` | `template.py` (`{param}`) · `includes.py` (`$include`) · `logger.py` (JSON estruturado) |
| `sparquet-studio/` | editor visual React — ver seção abaixo |
| `tests/` | `python tests/<arquivo>.py`; os que precisam de Spark se pulam sozinhos sem Java |
| `examples/` | `07` e `08` rodam local sem cluster |

Fluxo: `JSON → apply_template(params) → resolve_includes → PipelineConfig → Pipeline.run()`
→ reader → injeção de `columns` → TransformationEngine → ValidationEngine → writers.

---

## Sparquet Studio

React 18 + TS + Vite + Tailwind + React Flow, em `sparquet-studio/`. Compila o canvas
para o **mesmo JSON** que o framework executa.

| Camada | Caminho |
|---|---|
| Catálogo (fonte única: paleta, formulários, linter, prompt da IA) | `src/catalog/` |
| Compilador (`compileGraph` ↔ `pipelineToGraph`, inversos, round-trip testado) | `src/lib/compiler/` |
| Linter client-side | `src/lib/validation/lint.ts` |
| IA (streaming multi-provider) | `src/lib/ai/` |
| Runner FastAPI opcional | `server/` |
| Estado (zustand) | `src/store/` |

**Vocabulário — os dois lados divergem de propósito.** No framework um JSON é um
*pipeline* (`Pipeline`, `PipelineResult`, `PipelineConfig`). No Studio esse mesmo arquivo
é um **Job**, e *Pipeline* é a sequência ordenada de vários Jobs. **Não renomeie a API
Python.** Container = Workflow.

---

## Gotchas (não inferíveis do código)

- **Toda transformação/formato/validator novo precisa de entrada em
  `sparquet-studio/src/catalog/`** — senão o Studio não o oferece na paleta nem o
  descreve para a IA — **e de um PR no repo `sparquet-web`** (docs EN/PT/ES em
  `src/content/docs/docs/reference/`).
- **`sparquet_cola` é repo/pacote separado** (`../sparquet-cola`, PyPI, dependência
  `sparquet-cola>=0.4.0`). Mudança no motor de DQ se faz **lá** e publica antes de o
  sparquet consumir. Import é `sparquet_cola` (underscore); nome PyPI tem hífen.
- **`PYSPARK_PYTHON` em master local**: `context.py` faz `os.environ.setdefault` para
  `sys.executable`. Sem isso o worker sobe com outro Python e morre com
  `Python worker exited unexpectedly`. O sintoma engana — etapas puramente JVM
  (CSV→Parquet) não criam worker, então só quebra na primeira que cria (UDF, `sql` com
  função Python, `createDataFrame` a partir de linhas do driver). **Só em master local.**
- **CSV em dialeto RFC 4180**: leitura e escrita usam `escape='"'` (aspas dobradas), não
  o `\"` default do Spark. Os dois lados mudam juntos. Para ler arquivos no dialeto
  antigo: `options: {"escape": "\\"}`.
- **`filter`/`select` primeiro** na cadeia de `transformations`, antes de
  join/struct/group_by.
- **`PipelineResult` nunca lança exceção** — erro fica em `result.error`;
  `skipped=True` é encerramento gracioso por `stop_if_empty`.
- Factories e registries são **class-level** — extensão registrada no `Sparquet` afeta
  todas as execuções.
- Transformações **mudam** os dados; validações **reportam** sem modificar. Para saber
  *quais linhas* falharam, use a quarentena com `annotate`, não o agregado.

---

## Verificação

```bash
python tests/<arquivo>.py                 # teste alvo
cd sparquet-studio && npm run typecheck    # depois: npm run test | npm run lint | npm run smoke
python -m graphify update .                # após editar código, atualiza o grafo
```

---

## Documentação (carregar só quando a tarefa exigir)

| Arquivo | Quando |
|---|---|
| `docs/PIPELINE_SCHEMA.md` | schema JSON completo, `{param}`/`{{var}}`/`$include`, formatos IO, API pública, padrões de extensão |
| `docs/TEST_PLAN.md` | o que está coberto e o que falta |
| `BACKLOG.md` | pendências e melhorias do framework |
| `docs/DEPLOY_PYPI.md` | publicação como lib |
| `examples/README.md` | confs ilustrativas |
| [sparquet-web](https://github.com/VictorPasqualini/sparquet-web) | site e docs públicas (EN/PT/ES) — repo separado |
