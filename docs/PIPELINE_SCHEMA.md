# Referência do pipeline Sparquet

> Extraído do `CLAUDE.md` para reduzir o contexto carregado em toda conversa.
> **Carregue este arquivo só quando a tarefa exigir o schema/API em detalhe.**
> A documentação pública equivalente (EN/PT/ES) vive no repo
> [sparquet-web](https://github.com/VictorPasqualini/sparquet-web), em
> `src/content/docs/docs/reference/`. O catálogo do Studio
> (`sparquet-studio/src/catalog/`) descreve o mesmo vocabulário para a UI e a IA.

---

## Uso como biblioteca (ponto de entrada principal)

```python
from sparquet import Sparquet, Pipeline, PipelineResult, PipelineConfig

fw = Sparquet(spark={"app_name": "MeuJob", "master": "yarn"})

r1 = fw.run("pipeline_clientes.json")
r2 = fw.run("pipeline_pedidos.json")
r3 = fw.run_from_dict({"name": "inline", "input": {...}, "output": {...}})

# Injeção de DataFrame externo, colunas de runtime e parâmetros de template
r4 = fw.run("pipeline.json", input_df=df_existente, columns={"dt_ref": "2025-01-01"})
r5 = fw.run("pipeline.json", params={"tipo_ativo": "NC", "ids": ["A1", "A2"], "aplicar_filtro": True})

fw.stop()
```

`Sparquet` em `framework.py` gerencia o singleton de SparkSession e compartilha os engines de transformação/validação entre todas as execuções.

### API pública completa

```python
# Execução
fw.run(config_path: str, input_df=None, columns=None, params=None) → PipelineResult
fw.run_from_dict(config: dict, input_df=None, columns=None, params=None) → PipelineResult

# Extensão
fw.register_reader(format_name: str, reader_cls)
fw.register_writer(format_name: str, writer_cls)
fw.register_transformation(name: str, transformation_cls)
fw.register_validator(name: str, validator_cls)

# Ciclo de vida
fw.stop()
```

**`input_df`**: substitui a leitura do `input` — o pipeline começa a partir do DataFrame fornecido.  
**`columns`**: injeta colunas literais (`F.lit(value)`) antes das transformações, sem alterar o JSON.  
**`params`**: substitui placeholders `{chave}` no texto bruto do JSON antes do parse. Listas viram SQL IN (`'a', 'b'`); `True` → `"true"`; `False`/lista vazia → `""` (falsy, dispara `skip_if_false`).  
**`result.output_df`**: DataFrame após todas as transformações, disponível no resultado.

### Uso direto de Pipeline

```python
from sparquet import Pipeline

p = Pipeline.from_file("meu_pipeline.json")
result = p.run()

p2 = Pipeline.from_dict({...})
result2 = p2.run()
```


---

## Template de variáveis no JSON (`params`)

Qualquer valor `{chave}` no JSON é substituído antes do parse quando `params` é passado para `fw.run()`.

```python
fw.run("pipeline.json", params={
    "tipo_ativo":   "NC",
    "registradora": "CERC",
    "ids_cessao":   ["C1", "C2", "C3"],   # lista → "'C1', 'C2', 'C3'"
    "aplicar_join": True,                  # bool True  → "true"
    "filtro_extra": False,                 # bool False → ""  (falsy)
})
```

Regras de formatação:

| Tipo Python | Resultado no JSON | Uso típico |
|-------------|-------------------|-----------|
| `str` / `int` / `float` | `str(value)` | caminho, nome, número |
| `bool True` | `"true"` | mantém a transformação (`skip_if_false`) |
| `bool False` | `""` | pula a transformação (`skip_if_false`) |
| `list` de strings | `"'a', 'b', 'c'"` | cláusula `IN (...)` no SQL |
| `list` de números | `"1, 2, 3"` | cláusula `IN (...)` no SQL |
| `list` vazia | `""` | falsy — pula transformação ou filtro vazio |

Chaves sem correspondência em `params` ficam literais no JSON — não causam erro.

### `skip_if_false`

Qualquer transformação aceita `"skip_if_false"`. Após a substituição de template, o engine decide em 3 casos:

| Valor pós-substituição | Comportamento |
|------------------------|---------------|
| `""` (string vazia) | **pula** (ex: bool `False`, lista vazia, param ausente) |
| expressão que avalia como **booleano** (ex: `'REGISTRO' in ('EMISSAO', ...)`) | **pula se `false`** |
| qualquer outro valor não-vazio (ex: `"CERC"`, `"'a','b'"`) | executa |

```jsonc
// Pula o join inteiro se params["aplicar_join"] == False  (valor vira "")
{ "type": "join", "skip_if_false": "{aplicar_join}", "input": {...}, "on": "id" }

// Filtro só aplicado se params["registradora"] != ""
{ "type": "filter", "skip_if_false": "{registradora}", "condition": "registradora = '{registradora}'" }

// Branch por valor (expressão booleana): roda só nos fluxos de emissão
{ "type": "struct", "skip_if_false": "'{fluxo_operacao}' in ('EMISSAO', 'EMISSAO_E_REGISTRO')", "column": "payload", "fields": {...} }
```

A expressão é avaliada sobre **literais** (já substituídos pelo template) — não enxerga colunas do df; serve para branchear por parâmetro.

---

## Variáveis de runtime (`{{var}}`)

Diferente de `{param}` (substituído **antes** do parse, com valores conhecidos na
chamada), `{{var}}` é resolvido **durante a execução**, com valores computados pelo
próprio pipeline. Serve para empurrar um filtro literal (`IN (...)`) nas leituras de
tabelas grandes carregadas depois — o equivalente declarativo ao `df.collect()` +
`col.isin(lista)` de jobs Spark (predicate pushdown / data skipping no Delta).

```jsonc
// 1) materializa o conjunto e coleta a chave numa variável de runtime
{ "type": "checkpoint" },
{ "type": "collect", "column": "id_cessao", "as": "cessoes_pendentes" },

// 2) usa {{cessoes_pendentes}} para filtrar a leitura de tabelas seguintes
{ "type": "join",
  "input": { "format": "delta", "path": "lastros.bronze_remessa" },
  "with_transformations": [
    { "type": "filter", "condition": "id_cessao IN ({{cessoes_pendentes}})" },  // pushdown
    { "type": "select", "columns": ["id_cessao", "numero_contrato", "tipo_contrato"] }
  ],
  "on": ["id_cessao", "numero_contrato"], "how": "left" }
```

Como funciona:

- **`collect` tem teto**: acima de `max_values` valores distintos (default 10000) ele
  falha em vez de montar um `IN (...)` gigante — nesse tamanho o literal degrada o plano
  em vez de ajudar, e a saída é um join semi/inner contra a lista como DataFrame. O teto
  é aplicado na consulta, então a coleta nunca chega a estourar o driver. `0` desliga.
- **`collect`** roda `df.select(column).distinct().collect()` e guarda a lista em um
  *store* de runtime sob a chave `as`. Não altera o df, mas dispara uma action Spark
  (traz dados ao driver) — por isso use **após um `checkpoint`** (o df já materializado
  torna o collect barato e evita recomputar a linhagem).
- **`{{var}}`** é resolvido no momento de aplicar cada transformação. A formatação para
  SQL: lista de strings → `'a', 'b'` (aspas escapadas); lista de números → `1, 2`;
  lista vazia → `NULL` (`IN (NULL)` não casa nada — comportamento correto quando o
  conjunto apto está vazio); string → `'valor'`.
- O store é **compartilhado com os `with_transformations` aninhados** dos joins, então
  variáveis coletadas no escopo externo são visíveis nos reads de dentro.
- O store é **zerado a cada `fw.run(...)`** (o engine é reusado no `Sparquet`),
  evitando vazamento de variáveis entre execuções.
- `{{var}}` cuja variável ainda não foi coletada fica **literal** (não dá erro) — é
  resolvido quando/se a variável passar a existir num escopo aninhado.

Ordem de resolução: `{param}` (template, pré-parse) → parse do JSON → `{{var}}` (runtime,
durante as transformações).

As duas sintaxes **não colidem**: `{{nome}}` fica intacto mesmo quando `params` tem uma chave
`nome`. São as chaves duplas que distinguem as duas — o template só substitui `{nome}` sozinha.

---

## Reutilização de transformações com `$include`

Um item `{ "$include": "caminho/arquivo.json" }` na lista de `transformations` é substituído inline pelo conteúdo do arquivo referenciado. O caminho é relativo ao diretório do JSON principal.

O arquivo incluído pode ser um único objeto de transformação ou uma lista. Template `params` é aplicado antes do parse, então variáveis como `{tipo_ativo}` funcionam normalmente em arquivos compartilhados.

```jsonc
// pipeline_nc.json
{
  "transformations": [
    { "$include": "shared/filtro_tipo_ativo.json" },   // expande inline
    { "type": "with_column", "name": "payload", "expression": "..." }
  ]
}

// shared/filtro_tipo_ativo.json — objeto único ou lista
[
  { "type": "filter", "condition": "tipo_ativo = '{tipo_ativo}' AND registradora = '{registradora}'" }
]
```

Inclusões aninhadas são expandidas: um arquivo incluído pode conter novos `$include`, e o
caminho deles é relativo ao **arquivo que os escreveu**, não ao pipeline principal — é o que
permite mover uma pasta de includes inteira sem reescrever os caminhos de dentro. Um ciclo
(A inclui B que inclui A) levanta `ValueError` nomeando o percurso, e uma cadeia com mais de
20 níveis é recusada.

---

## Schema JSON do pipeline

```jsonc
{
  "name": "string",                    // obrigatório
  "description": "string",             // opcional
  "spark": {                            // opcional
    "app_name": "string",
    "master": "string",
    "configs": { "spark.sql.*": "valor" }
  },

  "input": {                            // obrigatório (ignorado quando input_df é injetado)
    "format": "csv|parquet|iceberg|delta|txt|view",
    "path": "string",
    // Delta: suporta time travel via options
    "options": {
      "versionAsOf": "5",
      "timestampAsOf": "2025-05-10T10:00:00Z"
    }
  },

  "transformations": [                  // opcional — aplicadas em ordem
    { "type": "filter", "condition": "SQL expr" },
    // select: nomes simples ou expressões SQL completas com alias
    { "type": "select", "columns": ["a", "b", "to_json(payload) AS value", "CAST(1 AS INT) AS status"] },
    { "type": "drop", "columns": ["x"] },
    { "type": "rename", "mappings": {"old": "new"} },
    { "type": "cast", "columns": {"col": "type"} },
    // with_column: aceita "column" (ou "name", compat) + "expression", OU "columns"
    // (mapa nome→expr) para criar várias colunas num bloco, em ordem.
    { "type": "with_column", "column": "col", "expression": "SQL expr" },
    { "type": "with_column", "columns": { "c1": "expr1", "c2": "expr2 usando c1" } },
    // struct: monta coluna struct aninhada a partir de um mapa campo→expressão
    // (valor string = expr SQL; valor mapa = struct aninhado). Mais legível que named_struct.
    // Chaves em dot-path ("data.nc.issuerName") auto-aninham → payload como tabela
    // plana (ótimo p/ ler/diff). Pode misturar dot-path e mapa aninhado.
    { "type": "struct", "column": "payload",
      "fields": { "id_externo": "id_vert", "data.nc.issuerName": "nome_sacado",
                  "data.nc.paymentMethod.indexCode": "lpad(cod, 4, '0')" } },
    { "type": "drop_duplicates", "columns": ["id"] },
    { "type": "distinct" },                                  // remove duplicatas usando todas as colunas
    // checkpoint: materializa e trunca o plano lógico (quebra a linhagem após joins pesados)
    // method: "localCheckpoint" (default) ou "checkpoint" (confiável); eager: true (default)
    // method inválido → transformação ignorada + warning no fim do pipeline
    { "type": "checkpoint", "method": "localCheckpoint", "eager": true },
    // collect: coleta valores distintos de uma coluna numa variável de runtime {{as}}
    // (não altera o df; dispara collect no driver — use após checkpoint). Ver seção
    // "Variáveis de runtime ({{var}})" abaixo.
    // max_values: teto de valores distintos (default 10000; 0 desliga). Acima disso a
    // transformação falha: lista grande em IN (...) degrada o plano em vez de ajudar.
    { "type": "collect", "column": "id_cessao", "as": "cessoes_pendentes", "max_values": 10000 },
    // stop_if_empty: encerra o pipeline graciosamente se o df estiver vazio — não
    // roda as transformações seguintes nem escreve nas saídas. result.skipped = True,
    // success = True, rows_written = 0. Posicione logo após o filtro que define o
    // conjunto a processar (antes de joins/payloads pesados).
    { "type": "stop_if_empty", "message": "Sem dados a processar" },
    { "type": "sql", "query": "SELECT ...", "view_name": "_df" },
    { "type": "fill_na", "value": 0, "columns": ["col"] },
    { "type": "sort", "columns": ["col"], "ascending": true },
    // repartition: muda o CUSTO, nunca os dados — quantas tasks (e portanto quantos
    // arquivos por diretório de partition_by) existem daqui para frente.
    // Pelo menos um de num_partitions/columns. columns aceita expressão SQL.
    // coalesce: true → df.coalesce(n), só REDUZ e sem shuffle (exige num_partitions,
    // recusa columns). range: true → repartitionByRange (divide por faixa, exige columns).
    { "type": "repartition", "num_partitions": 200 },
    { "type": "repartition", "num_partitions": 64, "columns": ["pmod(hash(id), 64)"] },
    { "type": "repartition", "num_partitions": 1, "coalesce": true },
    // $include: expande inline o conteúdo de um arquivo JSON (caminho relativo ao pipeline)
    { "$include": "shared/filtro_tipo_ativo.json" },
    {
      "type": "debug",                          // não modifica o df — apenas inspeciona
      "label": "após join contratos",           // opcional, aparece no separador
      // show usa df.show() — display() do Databricks só funciona chamado diretamente na célula
      // pushdown: lê o plano e diz o que desceu até a fonte (não dispara job)
      "actions": ["count", "print_schema", "show", "explain", "pushdown", "columns", "dtypes"],
      // transformations: opcional — aplicadas a um df descartável SÓ para esta
      // inspeção (filter/select/group_by/etc.); NÃO alteram o df do pipeline
      // (o debug sempre retorna o df original). Útil p/ focar a visualização.
      "transformations": [ { "type": "filter", "condition": "id_cessao = 'C1'" } ],
      "show_rows": 20,                          // linhas para show (default: 20)
      "truncate": true,                         // truncar show (default: true)
      "vertical": false,                        // layout vertical no show (default: false)
      "extended": false                         // plano estendido no explain (default: false)
    },
    {
      "type": "group_by",
      "by": ["col1", "col2"],
      // agg: lista de expressões SQL de agregação completas (strings) — qualquer
      // função/sintaxe SQL do Spark, com alias e expressões compostas.
      "agg": [
        "sum(valor) as total",
        "first(tipo_contrato) as tipo_contrato",
        "count(distinct struct(tipo_ativo, registradora)) > 1 as multi_ativos"
      ],
      // pivot (opcional): "coluna" ou { "column": "coluna", "values": [...] }
      "pivot": { "column": "mes", "values": ["jan", "fev"] }
    },
    {
      "type": "join",
      "input": { "format": "parquet", "path": "/ref/table", "options": {} },
      // input: a segunda fonte — mesma forma do input principal (obrigatório)
      "on": "join_key",             // coluna, ["key1","key2"] ou SQL expr com l./r.
      // Nome presente nos dois lados sai renomeado à DIREITA com sufixo `_r`
      // (`nome` e `nome_r`; `_r2`, `_r3` se já ocupado) — nunca duas colunas de mesmo
      // nome. A projeção é montada DEPOIS do join, então um `on` em SQL pode citar r.nome.
      // Como ela desfaz os aliases, l./r. não resolvem nos nós seguintes ao join.
      "how": "inner|left|right|full|cross|leftsemi|leftanti|...",
      "broadcast": true,                // opcional: true/"right" faz broadcast do lado
                                        // direito (dimensão/lookup pequeno) — map-side
                                        // join sem shuffle; "left" faz broadcast do principal
      "skip_if_false": "{meu_param}",   // opcional: pula o join se o valor pós-substituição for ""
      // with_transformations: aplica transformações no df da direita antes do join
      // df esquerdo (principal) é alias 'l'; df direito é alias 'r'.
      "with_transformations": [
        { "type": "filter", "condition": "status = 1" },
        { "type": "sql", "view_name": "v", "query": "SELECT id, MIN(val) AS val FROM v GROUP BY id" },
        { "type": "select", "columns": ["id", "val"] }
      ]
    },
    {
      "type": "union",
      "input": { "format": "parquet", "path": "/data/extra" },
      "allow_missing_columns": false
    }
  ],

  "validations": {                      // opcional
    "on_failure": "fail|warn|skip",
    // report: opcional — grava 1 linha por regra, num arquivo único (coalesce(1)):
    // pipeline, rule_type, check_name, target (coluna[s] da regra), rule_params
    // (JSON do que foi afirmado: min/max, pattern, metric, must_be…), severity,
    // passed, failed_count, rows_read (denominador — é a contagem da LEITURA, não
    // do df validado), metric_value, message, validated_at. Aceita
    // qualquer formato de saída. Gerado nos modos que não abortam (warn/skip)
    // ou quando todas as regras passam (em "fail" com violação, aborta antes).
    "report": { "format": "csv", "path": "/dq/validation_report", "mode": "overwrite" },
    "rules": [
      // "code" (opcional, em QUALQUER regra): o identificador da regra usado para
      // escopar (`outputs.*.rules`) e para rotular a linha (`outputs.invalid.annotate`).
      // Omitido, o código é a PRÓPRIA EXPRESSÃO da validação, renderizada de forma
      // compacta e determinística — ele vai para dentro dos dados, então a mesma regra
      // sempre rende a mesma string:
      //   not_null(email) · not_null(id,cpf) · unique(id,dt) · range(age,1,99)
      //   range(valor,0,*)  (* = lado sem limite) · regex(email,^.+@.+$)
      //   missing_percent(cpf) · invalid_count(email) · row_count · schema · sql
      // regras de coluna aceitam "columns" (lista) OU "column" (singular)
      { "type": "not_null", "columns": ["id"] },
      { "type": "unique", "columns": ["id"] },
      { "type": "range", "column": "age", "min": 0, "max": 150, "code": "AGE_RANGE" },
      { "type": "regex", "column": "email", "pattern": ".*@.*" },
      { "type": "row_count", "min": 1, "max": 1000000 },
      // sql: dois modos. "query" = invariante pass-when-true (retorna booleano).
      { "type": "sql", "query": "SELECT COUNT(*) = 0 FROM _validation_df WHERE ...",
        "error_message": "msg" },
      // "failed_rows" = retorna as LINHAS ruins (estilo SODA "failed rows"); falha se
      // vier alguma. "output" (opcional) grava essas linhas num destino.
      { "type": "sql", "failed_rows": "SELECT * FROM _validation_df WHERE valor < 0",
        "output": { "format": "delta", "path": "dq.linhas_ruins", "mode": "overwrite" } },

      // MÉTRICAS (estilo SODA Core): a métrica É o tipo da regra — não existe wrapper
      // `check`. must_be é a condição de aprovação; warn (opcional) rebaixa para aviso
      // (não aborta em on_failure="fail"). Threshold: > < >= <= = != , between X and Y,
      // com sufixo % (percentual) ou duração (1d/2h/30m, para freshness).
      // Métricas: row_count, distinct_count, missing_count/percent,
      // duplicate_count/percent, invalid_count/percent, min, max, avg, mean, sum,
      // stddev, freshness. As row-level (missing_*/invalid_*) entram na quarentena;
      // as agregadas descrevem a tabela e não rotulam linha.
      { "type": "row_count", "must_be": "> 0" },
      { "type": "missing_percent", "name": "cpf completo",
        "column": "cpf", "must_be": "< 1%", "warn": "= 0" },
      { "type": "duplicate_count", "columns": ["id"], "must_be": "= 0" },
      // invalid_* usa as configs de validade: valid_values / valid_format (email, uuid,
      // cpf, cnpj, date, …) / valid_regex / valid_min / valid_max / valid_length
      { "type": "invalid_percent", "column": "email",
        "valid_format": "email", "must_be": "< 5%" },
      { "type": "freshness", "column": "atualizado_em", "must_be": "< 1d" },

      // targets: uma entrada vira N regras INDEPENDENTES (um resultado, um código e
      // uma contribuição à quarentena por alvo). Chaves fora de targets são defaults
      // compartilhados. Vale para qualquer tipo. Expandido no parse da config.
      { "type": "regex", "targets": [
          { "column": "cpf",  "pattern": "^[0-9]{11}$" },
          { "column": "cnpj", "pattern": "^[0-9]{14}$" } ] },

      // schema: colunas obrigatórias/proibidas e tipos (aliases: long→bigint, integer→int)
      { "type": "schema", "required_columns": ["id", "valor"],
        "column_types": { "id": "bigint", "valor": "double" } }
    ],
    // report ganha as colunas check_name, severity (pass|warn|fail) e metric_value.

    // outputs (quarentena): roteia LINHAS apartado da(s) saída(s) principal(is). Uma
    // linha é "invalid" quando viola qualquer check row-level (not_null, range, regex,
    // unique, e o `check` de missing/invalid). Ex: bronze → silver_ok + silver_quarentena.
    // Duas chaves só existem aqui (em qualquer outro destino são erro de config):
    //   • "rules"    – lista de CÓDIGOS: só essas regras alimentam este destino.
    //                  Ausente = todas as row-level. Permite quarentena por
    //                  severidade (as regras críticas numa tabela, o resto fora dela).
    //   • "annotate" – SÓ no "invalid": nome da coluna array<string> com os códigos das
    //                  regras que rejeitaram cada linha, na ordem das regras. Não
    //                  aparece nas saídas principais, nem no "valid" (seria vazia por
    //                  definição), nem no report (que tem 1 linha por REGRA).
    //                  Se o destino tiver "columns", inclua a coluna de códigos nela.
    "outputs": {
      "valid":   { "format": "delta", "path": "silver.ok",         "mode": "overwrite" },
      "invalid": { "format": "delta", "path": "silver.quarentena", "mode": "overwrite",
                   "rules": ["AGE_RANGE", "regex(email,.*@.*)"], "annotate": "dq_codes" }
    },
    // cache (default false): materializa o df antes de validar. As regras agregáveis são
    // medidas numa passada só, então o cache não amortiza mais nada por si — ligue só
    // quando a linhagem for cara E houver muitas actions depois dela (regras `sql`,
    // várias quarentenas com escopos diferentes, muitos destinos).
    "cache": false
  },

  // Saída única (shorthand):
  "output": {
    "format": "csv|parquet|iceberg|delta|txt|kafka|view",
    "path": "string",
    "mode": "overwrite|append|merge",
    // partition_by: nomes de coluna. No Iceberg aceita também os transforms de
    // particionamento oculto: bucket(16, id), years/months/days/hours(ts).
    "partition_by": ["col"],
    "columns": ["col_a", "col_b"],    // opcional: projeta só essas colunas
    // transformations: opcional — transformações próprias deste destino aplicadas
    // sobre o df transformado, antes de columns/escrita, sem afetar as demais saídas.
    // Permite gravar formas diferentes do mesmo df (explode, to_json, join, etc.).
    "transformations": [ { "type": "with_column", "column": "value", "expression": "to_json(payload)" } ],
    // merge (Delta/Iceberg): T = target (tabela destino), S = source (DataFrame)
    "options": {
      // As duas são obrigatórias no merge: sem uma delas o writer levanta ValueError
      // antes de qualquer chamada Spark.
      "on": "S.id = T.id AND S.loja = T.loja",   // a condição ON inteira
      "actions": [
        "WHEN MATCHED AND S.op = 'D' THEN DELETE",
        "WHEN MATCHED THEN UPDATE SET T.status = S.status",
        "WHEN NOT MATCHED THEN INSERT (id, loja, status) VALUES (S.id, S.loja, S.status)"
      ],
      // Cada item começa com WHEN e sai na ordem dada. Dentro de um grupo (MATCHED,
      // NOT MATCHED, NOT MATCHED BY SOURCE) a primeira cláusula que casa é a que vale,
      // então a incondicional só pode ser a última do grupo — o writer recusa a lista
      // dizendo qual cláusula ficou inalcançável.
      // O upsert simples é ["WHEN MATCHED THEN UPDATE SET *",
      //                     "WHEN NOT MATCHED THEN INSERT *"].
      // No Delta, `SET *`/`INSERT *` exigem as MESMAS colunas dos dois lados: uma origem
      // de CDC com coluna `op` que o destino não tem falha ao resolver, e a cláusula
      // precisa listar as colunas do destino. O Iceberg tolera a coluna extra.
      // WHEN NOT MATCHED BY SOURCE THEN DELETE só vale contra um snapshot COMPLETO da
      // origem — numa carga incremental apaga tudo que ela não repetiu.
      // Kafka:
      "bootstrap_servers": "broker:9092",
      "topic": "meu-topico",
      "value_column": "payload",   // default: "value"
      "key_column": "header"       // default: "key"
    }
  },

  // OU múltiplas saídas (cada uma pode ter "columns" e "transformations" diferentes):
  "outputs": [
    {
      "format": "parquet",
      "path": "/data/full",
      "mode": "overwrite"
    },
    {
      "format": "parquet",
      "path": "/data/analytics",
      "columns": ["id", "name", "total"]
    },
    {
      "format": "csv",
      "path": "/data/export",
      "columns": ["id", "total"]
    }
  ]
}
```

---

## Formatos IO suportados

| Formato | Leitura | Escrita | Notas |
|---------|---------|---------|-------|
| `parquet` | sim | sim | Parquet nativo Spark |
| `csv` | sim | sim | Defaults: `header=true`, `inferSchema=true`, `escape="` (RFC 4180) |
| `delta` | sim | sim | Unity Catalog ou path; time travel; MERGE |
| `iceberg` | sim | sim | MERGE INTO nativo |
| `txt` | sim | sim | Texto plano; coluna `value` |
| `view` | sim | sim | Spark temp views; auto-cache; `scope` session/global |
| `json` | sim | sim | JSON nativo (JSON Lines; `multiLine` p/ 1 doc por arquivo) |
| `orc` | sim | sim | ORC colunar nativo |
| `avro` | sim | sim | Requer `spark-avro` no classpath |
| `xml` | sim | sim | `rowTag` obrigatório. Nativo a partir do Spark 4; no 3.x requer `com.databricks:spark-xml`, que **não** suporta `mode: append` (`Append mode is not supported by com.databricks.spark.xml.DefaultSource`) |
| `binary` | sim | **não** | `binaryFile` (só leitura): path/modificationTime/length/content |
| `hudi` | sim | sim | Requer bundle Hudi; upsert/partição via opções `hoodie.*` |
| `kafka` | sim | sim | Batch read/write; MSK via SASL/IAM; requer conector Kafka no classpath |
| `postgresql` | sim | sim | JDBC; `path`=tabela; `url` ou `host`+`database` em options |
| `mysql` | sim | sim | JDBC |
| `mariadb` | sim | sim | JDBC |
| `sqlserver` | sim | sim | JDBC |
| `oracle` | sim | sim | JDBC (service name na `database`) |
| `bigquery` | sim | sim | `path`=`projeto.dataset.tabela`; write via GCS/direct |
| `snowflake` | sim | sim | Opções `sfXxx`; `path`=tabela |
| `redshift` | sim | sim | Requer `url` + `tempdir` (S3) |
| `mongodb` | sim | sim | `path`=coleção; `connection.uri`+`database` em options |
| `documentdb` | sim | sim | Amazon DocumentDB (mesmo conector Mongo; URI com TLS) |
| `dynamodb` | sim | sim | `path`=tabela; write é upsert por chave (append) |
| `cassandra` | sim | sim | `path`=`keyspace.tabela`; append (upsert); **mesma classe atende Cassandra e ScyllaDB** |
| `elasticsearch` | sim | sim | `path`=índice; opções `es.*`; **sem build para Spark 4** — ver a nota abaixo |
| `opensearch` | sim | sim | `path`=índice; opções `opensearch.*`; conector próprio, **não** é o do Elasticsearch |

> Conectores externos exigem o **JAR do driver/conector no classpath** do Spark
> (`spark.jars` / `spark.jars.packages`): JDBC, BigQuery, Snowflake, Redshift, Mongo,
> DynamoDB, Cassandra, Elasticsearch, OpenSearch, Kafka, **Avro (`spark-avro`),
> XML no Spark 3.x (`spark-xml`) e Hudi (`hudi-spark-bundle`)**.
> `parquet`/`csv`/`delta`/`iceberg`/`txt`/`view`/`json`/`orc`/`binary` são nativos, e o
> `xml` passou a ser a partir do Spark 4.0. O framework só monta `.format(...).options(...)`; não
> empacota drivers. Cada `io/<fmt>.py` documenta as opções; o catálogo do Studio
> (`formats.databases.ts`, `formats.files.ts`) as descreve para a UI e a IA.

### Busca (Elasticsearch e OpenSearch) no Spark 4

Os dois formatos existem no framework desde sempre; o que mudou com o Spark 4 é quais
conectores ainda compilam. Medido contra container, em Spark 4.1.1:

| Formato | Coordenada | Situação |
|---|---|---|
| `opensearch` | `org.opensearch.client:opensearch-spark-40_2.13:2.0.0` | **funciona** — o pom declara Spark 4.1.1 / Scala 2.13.16; ida e volta e `opensearch.mapping.id` passam |
| `elasticsearch` | `org.elasticsearch:elasticsearch-spark-30_2.13:9.5.3` (a última publicada) | **não funciona** — ainda compila contra Spark 3.4.3 e chama `Dataset.sqlContext()`, que saiu da API |

O sintoma do lado do Elasticsearch é este, na escrita:

```
java.lang.NoSuchMethodError: 'org.apache.spark.sql.SQLContext org.apache.spark.sql.Dataset.sqlContext()'
```

Não existe artefato `elasticsearch-spark-40` no Maven Central, e o `opensearch-spark-40`
não serve de substituto: o cliente do OpenSearch 2.x recusa um servidor Elasticsearch 8+
na verificação de versão. Quem precisa de Elasticsearch tem quatro saídas, em ordem de
preferência:

1. **Rodar aquele pipeline em Spark 3.5** — o `elasticsearch-spark-30` funciona lá, e o
   JSON do pipeline não muda. É a única saída que mantém o conector nativo.
2. **Escrever num formato intermediário** (Parquet/Delta) e indexar fora do Spark, com o
   `_bulk` da API REST. É o caminho que não depende de nenhum build de terceiro.
3. **Ler por JDBC** com o driver de Elasticsearch SQL (`format: "jdbc"`, url
   `jdbc:es://`): serve para leitura, é recurso de licença paga (Platinum/Enterprise) e
   não escreve.
4. **Trocar o destino para OpenSearch**, que é fork do Elasticsearch 7.10 e tem o build
   de Spark 4 — decisão de infraestrutura, não de pipeline.

O `opensearch-spark-40` declara as próprias dependências de Spark, e `spark.jars.packages`
resolve a árvore inteira: sem `spark.jars.excludes` a sessão baixa um segundo jogo de
jars do Spark (`spark-core`, `spark-sql`, `spark-sql-api`, `spark-common-utils`,
`spark-streaming`, `spark-yarn`) e os põe no classpath do driver. Funciona assim mesmo
(o pyspark instalado ganha na ordem do classpath), mas o certo é excluí-los — como
`tests/io/integration/services.py` faz.

---

## Estratégia de leitura e escrita

Particionamento não é uma opção do pipeline: é uma propriedade do **layout dos dados**.
O framework não descobre esse layout sozinho — não há como "ler sempre particionado" por
default. O que existe são alavancas explícitas, e a ordem em que compensam.

### Leitura — não trazer o que não vai ser usado

**1. Path direto na partição** — o mais barato: nem lista o resto da tabela.

```jsonc
{ "format": "parquet", "path": "/lake/vendas/dt=2026-09-01" }
{ "format": "parquet", "path": "/lake/vendas/dt=2026-09-0*" }   // glob também funciona
```

Gotcha: **a coluna de partição desaparece do schema.** O Spark deduz as colunas de
partição dos diretórios *abaixo* do caminho apontado — apontando para dentro de
`dt=...` não sobra nada para deduzir, e `dt` não existe no DataFrame. `basePath`
devolve a coluna:

```jsonc
{ "format": "parquet", "path": "/lake/vendas/dt=2026-09-01",
  "options": { "basePath": "/lake/vendas" } }
```

**2. `filter` como primeira transformação** — é o que dá *partition pruning* e
*predicate pushdown*, os dois automáticos:

```jsonc
"input": { "format": "parquet", "path": "/lake/vendas" },
"transformations": [
  { "type": "filter", "condition": "dt >= '2026-09-01' AND uf = 'SP'" },
  { "type": "select", "columns": ["id", "valor", "dt"] }
]
```

Não existe `input.partition_filter`, e não vai existir: seria um segundo lugar para
escrever a mesma coisa. O Catalyst empurra o predicado para dentro do scan — dá para
conferir com `{ "type": "debug", "actions": ["pushdown"] }`, que lista
`PartitionFilters` (diretórios descartados sem abrir arquivo) e `PushedFilters`
(row-groups Parquet descartados sem descomprimir) por nó de leitura — veja
[Pushdown](#pushdown--o-que-a-leitura-deixa-de-trazer). Um `input.partition_filter`
produziria exatamente o mesmo plano.

O que **não** é automático: o pruning só acontece se a coluna filtrada for coluna de
partição na origem. Filtro sobre coluna comum lê todos os diretórios e conta apenas
com o pushdown de row-group. Vale o mesmo para `{{var}}` de `collect`, que virou
`IN (literal, literal, …)` — poda se a coluna for de partição.

**3. Paralelismo do scan** — quantas tasks leem, via `spark.configs`:

```jsonc
"spark": { "configs": {
  "spark.sql.files.maxPartitionBytes": "134217728",   // default 128 MB por task
  "spark.sql.shuffle.partitions": "200"               // partições após cada shuffle
} }
```

### JDBC — a leitura mais fácil de errar

Sem `partitionColumn` o Spark abre **uma conexão e uma task**: a tabela inteira passa
por um executor, não importa quantos existam no cluster. Três alavancas, nesta ordem:

**a) Filtre no banco.** `query` (SELECT completo) ou `dbtable` com subquery — o que não
sai do banco não custa rede nem memória:

```jsonc
{ "format": "postgresql", "path": "public.vendas",
  "options": { "host": "db.internal", "database": "app", "user": "sparquet",
               "password": "{db_password}",
               "query": "SELECT id, valor, dt FROM public.vendas WHERE dt >= '{data_corte}'" } }
```

**b) Paralelize o que sobrou.** `partitionColumn` + os outros três — o Spark gera
`numPartitions` queries com `WHERE col >= a AND col < b`:

```jsonc
{ "format": "postgresql", "path": "public.vendas",
  "options": { "host": "db.internal", "database": "app",
               "dbtable": "(SELECT id, valor, dt FROM public.vendas WHERE dt >= '2026-09-01') t",
               "partitionColumn": "id", "lowerBound": "1",
               "upperBound": "50000000", "numPartitions": "16",
               "fetchsize": "10000" } }
```

- **`query` e `partitionColumn` são exclusivos** no Spark. Para os dois juntos, mova o
  SELECT para `dbtable` como subquery **com alias** (o `t` do exemplo). O reader recusa
  a combinação com uma mensagem que diz isso, em vez de deixar o Spark falhar depois.
- **O quarteto é all-or-none**: `partitionColumn` sem `lowerBound`/`upperBound`/
  `numPartitions` levanta `ValueError` nomeando o que falta. O inverso — limites sem
  `partitionColumn` — o Spark **ignora em silêncio** e lê em uma task; aí o framework
  emite warning em vez de recusar.
- `lowerBound`/`upperBound` **não filtram nada**: são só a régua da divisão. Linhas fora
  do intervalo entram nas partições das pontas, que ficam gordas. Para filtrar, use
  `query`/`dbtable`.
- A coluna precisa ser numérica/data **e indexada**: são N queries de faixa — sem índice
  são N full scans, e paralelizar fica mais lento que não paralelizar.

**c) Ajuste o transporte.** `fetchsize` (linhas por round-trip) — o default do driver
raramente serve: o Postgres traz tudo de uma vez (risco de OOM no executor), o Oracle
traz 10 por vez (um round-trip a cada 10 linhas). `pushDownPredicate` (default `true`),
`pushDownAggregate`, `pushDownLimit`, `pushDownOffset` e `pushDownTableSample` empurram
filtro, agregação, `LIMIT`, `OFFSET` e `TABLESAMPLE` para o banco; passam direto ao Spark
via `options`. `pushDownOffset` só chega ao banco junto do `LIMIT` (o par vai inteiro ou
não vai), e `pushDownTableSample` depende do dialeto ter `TABLESAMPLE` — onde não tem, a
opção é no-op e o Spark amostra depois de ler tudo. Todas valem no caminho por tabela; com
`query`, o SELECT já é o recorte. Para conferir o que de fato desceu, veja
[Pushdown](#pushdown--o-que-a-leitura-deixa-de-trazer).

### Escrita — quantos diretórios, quantos arquivos

São duas decisões independentes, e confundi-las é a origem do problema de *small files*:

| Decisão | Quem controla | Erro típico |
|---|---|---|
| **Quais diretórios existem** | `partition_by` do output | particionar por coluna de alta cardinalidade → um diretório por linha |
| **Quantas tasks escrevem** (⇒ arquivos por diretório) | `repartition` na cadeia | nenhuma → cada task abre um arquivo em cada diretório que toca |

Arquivos gravados = pares *(task, diretório)* com linhas. Com 200 partições de shuffle
e `partition_by: ["dt"]` sobre 30 dias, isso são até **6.000 arquivos**. Reparticionando
pela mesma chave, cada valor de `dt` cai numa única task e saem **30**:

```jsonc
"transformations": [
  { "type": "repartition", "columns": ["dt"] }
],
"output": { "format": "parquet", "path": "/lake/vendas",
            "mode": "overwrite", "partition_by": ["dt"] }
```

A garantia vale porque um valor de chave nunca se divide entre tasks (o AQE funde
partições vizinhas, nunca as separa) — por isso `num_partitions` aqui só regula o
paralelismo da escrita, não a contagem de arquivos. Teto complementar, para o caso de
um diretório muito grande: `"options": { "maxRecordsPerFile": "2000000" }`.

**`coalesce` vs `repartition`.** `coalesce` funde partições vizinhas sem shuffle, mas
reduz o paralelismo **de tudo para trás**: `coalesce(1)` no fim faz o estágio anterior
inteiro rodar numa única task. Para gravar um arquivo só depois de um cálculo paralelo,
`repartition` com `num_partitions: 1` paga um shuffle e mantém o cálculo distribuído.
E `coalesce` com `n` maior que o número atual de partições é **no-op silencioso** — ele
só reduz.

### Bucket por hash

Quando a chave natural tem cardinalidade alta demais para `partition_by` (um `id`), o
padrão é materializar um bucket e particionar por ele:

```jsonc
"transformations": [
  { "type": "with_column", "column": "bucket", "expression": "pmod(hash(id), 64)" },
  { "type": "repartition", "num_partitions": 64, "columns": ["bucket"] }
],
"output": { "format": "parquet", "path": "/lake/eventos",
            "mode": "overwrite", "partition_by": ["bucket"] }
```

- **`pmod`, nunca `%`.** `hash()` é Murmur3 de 32 bits **com sinal**: `hash(id) % 64`
  devolve negativo para metade das chaves e cria diretórios `bucket=-17`.
- **`hash(null)` é constante** — coluna nullable manda todos os nulos para o mesmo
  bucket. Trate antes (`coalesce(id, '')`) ou filtre.
- **N vem do tamanho de arquivo alvo** (`total_bytes / 128 MB…1 GB`), não de
  primalidade. Murmur3 já avalancha: `% 64` distribui igual a `% 61`. A regra do
  "módulo primo" vem de tabela hash com hash fraco ou identidade (`Integer.hashCode`
  devolve o próprio valor, e aí ids múltiplos de 10 concentram em módulo potência
  de 2) — não é o caso aqui. Primo não atrapalha; só não ajuda.
- **O que bucket resolve:** cardinalidade limitada e espalhamento uniforme entre
  diretórios. **O que não resolve:** (a) *skew* de chave única — um `id` com 40% das
  linhas cai inteiro num bucket; (b) pruning por `id` — quem lê filtrando `id = 'X'` só
  poda se filtrar `bucket = pmod(hash('X'), 64)` também. Bucket materializado troca
  pruning por controle de arquivo.

### Skew

- **Em join:** o AQE já divide a partição gorda (`spark.sql.adaptive.skewJoin.enabled`,
  ligado por default no Spark 3.2+). Uma partição é considerada torta quando passa dos
  dois critérios ao mesmo tempo: `skewedPartitionFactor` × a mediana (default `5.0`) **e**
  `skewedPartitionThresholdInBytes` (default `268435456b`, 256 MB). Skew abaixo de 256 MB
  não é tratado — em dataset pequeno o AQE simplesmente não age, e o ajuste é baixar o
  limiar. Para dimensão pequena, `"broadcast": true` no `join` elimina o shuffle inteiro.
- **O AQE só enxerga shuffle.** Ele reparte a partição gorda *depois* do embaralhamento,
  para o join. Escrita não passa por lá: o diretório de saída de uma chave dominante
  continua sendo um arquivo grande, e nenhuma conf resolve.
- **Na escrita, com uma chave dominante:** nem bucket nem `repartition` ajudam — o valor
  é indivisível por definição. As saídas são *salting* (reparticionar por
  `concat(chave, '-', pmod(hash(rand()), 8))`) ou aceitar o diretório grande e limitar o
  arquivo com `maxRecordsPerFile`.
- **Diagnóstico:** `{ "type": "debug", "actions": ["count"] }` não mostra skew; a
  distribuição aparece num `group_by` da chave suspeita com `count(*)`, ou na aba Stages
  da Spark UI (duração máxima ≫ mediana).

### Iceberg — particionamento oculto

No Iceberg, `partition_by` aceita também os *transforms* de particionamento, e essa é a
**única** forma que mantém o pruning:

```jsonc
"output": { "format": "iceberg", "path": "catalogo.vendas.eventos", "mode": "overwrite",
            "partition_by": ["days(data_evento)", "bucket(16, id)"] }
```

O Iceberg guarda a relação coluna→transform nos metadados da tabela, então
`WHERE id = 'X'` poda os buckets e `WHERE data_evento = '2026-09-01'` poda os dias sem
que quem lê saiba que existe transform — o oposto do bucket materializado à mão.

- Transforms aceitos: `bucket(n, col)`, `years(col)`, `months(col)`, `days(col)`,
  `hours(col)`. O `truncate` do Iceberg não tem função equivalente no Spark: declare-o
  no DDL da tabela e deixe `partition_by` vazio.
- **Exige tabela de catálogo** (`catalogo.schema.tabela`). Com transform a escrita passa
  pelo `writeTo` (DataFrameWriterV2), porque o `partitionBy` do writer V1 só aceita nome
  de coluna — e `writeTo` não aceita caminho físico. Para destino em path, materialize o
  bucket numa coluna (receita acima).
- **A spec pertence à tabela, não à escrita:** ela é aplicada em `create` /
  `createOrReplace`. `mode: "append"` numa tabela existente usa a spec que a tabela já
  tem, e trocar `partition_by` num pipeline de append não reparticiona o que já está
  gravado. No `mode: "merge"`, a primeira carga (quando a tabela ainda não existe) cria
  a tabela com o `partition_by` declarado — é a única execução em que ele tem efeito.
- **`bucketBy` do Spark não é alternativa:** só funciona com `saveAsTable` (bucketing
  Hive), `save(path)` o recusa, e o Delta não o suporta. É por isso que bucket
  declarativo existe só no Iceberg.

---

## Pushdown — o que a leitura deixa de trazer

Pushdown é empurrar trabalho para quem guarda o dado: em vez de o Spark trazer tudo e
descartar depois, a fonte já devolve menos. São cinco recortes, e cada um aparece com
nome próprio no plano físico:

| Recorte | Nome no plano | O que economiza |
|---|---|---|
| Diretórios inteiros | `PartitionFilters` | nem abre o arquivo: o descarte acontece na listagem |
| Linhas | `PushedFilters` | Parquet/ORC pulam row-groups pela estatística min/max; banco recebe `WHERE` |
| Colunas | `ReadSchema` | formato colunar lê só as colunas pedidas (*column pruning*) |
| Agregação | `PushedAggregation` (v2 de arquivo) / `PushedAggregates` (JDBC) | `COUNT`/`MIN`/`MAX` saem do footer ou do banco, sem varrer linha |
| Recorte descoberto em runtime | `RuntimeFilters` | *dynamic partition pruning* e bloom filter de join: o filtro só existe depois que o outro lado do join roda |

Nada disso é opção do pipeline, e nada disso precisa ser pedido: o Catalyst empurra
sozinho o que consegue. O que o JSON controla são as condições em que ele *pode* — o
layout dos dados, a ordem da cadeia (`filter`/`select` antes de join/group_by), as opções
do conector e algumas confs de formato.

Duas coisas que pushdown **não** é:

- **Não é garantia de leitura mais rápida.** `PushedFilters` diz que o predicado saiu do
  Spark; o Parquet ainda precisa de estatística de row-group que case com ele para pular
  alguma coisa, e o banco ainda precisa de índice. Predicado empurrado para uma tabela
  sem índice vira full scan do lado de lá.
- **Não é o mesmo que partition pruning.** Poda de partição descarta arquivos pela
  estrutura de diretórios, antes de abrir qualquer um; pushdown de predicado abre o
  arquivo e pula pedaços dele. A poda é uma ordem de grandeza mais barata, e só existe
  se a coluna filtrada for coluna de partição na origem.

### Conferindo: a ação `pushdown` do `debug`

`explain` mostra o plano inteiro e deixa a conclusão por conta de quem lê.
`{ "type": "debug", "actions": ["pushdown"] }` lê o mesmo plano e responde a pergunta
direta — o que desceu até a fonte, por nó de leitura:

```jsonc
"transformations": [
  { "type": "filter", "condition": "dia = 1 AND grupo = 3" },
  { "type": "debug", "label": "leitura da fato", "actions": ["pushdown"] }
]
```

```
scan 1: FileScan parquet — file:/lake/vendas
  colunas lidas: 2
  PartitionFilters: isnotnull(dia#5), (dia#5 = 1)
  PushedFilters: IsNotNull(grupo), EqualTo(grupo,3)
  nota: 1 nó(s) `Filter` acima dos scans — predicado avaliado depois de ler (o Spark não
  pôde descer, ou é expressão que a fonte não entende).
```

- Um scan que não empurrou nada recebe o aviso `⚠️ nada empurrado: este scan lê a fonte
  inteira`.
- No JDBC, o `*` antes do predicado (`PushedFilters: [*IsNotNull(id)]`) é o Spark dizendo
  que ele foi **inteiramente** traduzido para o `WHERE`. Sem o `*`, o Spark reavalia o
  predicado depois de buscar as linhas.
- Não dispara job: o plano físico é planejamento, não execução. Diferente de `count`,
  `show` e `explain` em plano estendido, essa ação é grátis.
- Sem nó de leitura (dado vindo de `createDataFrame`, `range` ou de um `checkpoint` já
  materializado) a ação diz isso, em vez de reportar zero.

### Alavancas de arquivo (`spark.configs`)

Todas passam por `"spark": { "configs": { … } }` e são globais da sessão. Os defaults
abaixo foram lidos do Spark 4.1.1 — em geral já estão do lado certo, e a razão de estarem
documentadas é o caso oposto: precisar **desligar** uma delas, ou descobrir que a que
importa está desligada.

| Conf | Default | Para quê |
|---|---|---|
| `spark.sql.parquet.filterPushdown` | `true` | predicado para o Parquet (row-group min/max) |
| `spark.sql.parquet.filterPushdown.{date,timestamp,decimal,stringPredicate}` | `true` | recorta o pushdown por tipo — desligue um deles ao caçar bug de conversão |
| `spark.sql.parquet.filterPushdown.string.startsWith` | `true` | `LIKE 'abc%'` empurrado |
| `spark.sql.parquet.aggregatePushdown` | **`false`** | `MIN`/`MAX`/`COUNT` resolvidos no footer, sem ler dado |
| `spark.sql.orc.filterPushdown` | `true` | equivalente do Parquet, no ORC |
| `spark.sql.orc.aggregatePushdown` | **`false`** | idem, no ORC |
| `spark.sql.avro.filterPushdown.enabled` | `true` | descarta registro antes de desserializar |
| `spark.sql.csv.filterPushdown.enabled` | `true` | descarta a linha sem converter todos os campos |
| `spark.sql.json.filterPushdown.enabled` | `true` | idem, no JSON |
| `spark.sql.optimizer.nestedSchemaPruning.enabled` | `true` | lê só os campos usados **dentro** de um struct |
| `spark.sql.optimizer.dynamicPartitionPruning.enabled` | `true` | poda partições da fato com as chaves que sobraram na dimensão |
| `spark.sql.optimizer.runtime.bloomFilter.enabled` | `true` | bloom filter injetado no lado grande do join |
| `spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold` | `10485760b` | lado pequeno maior que isso não gera bloom filter |
| `spark.sql.files.maxPartitionBytes` | `134217728b` | tamanho alvo da task de leitura (não é pushdown, mas é a outra metade do custo do scan) |

**`aggregatePushdown` é o único que costuma valer a pena ligar — e é o mais cheio de
condição.** Medido no Spark 4.1.1:

- Só vale no caminho **DataSource v2** dos arquivos. O default de
  `spark.sql.sources.useV1SourceList` é `avro,csv,json,kafka,orc,parquet,text`, ou seja,
  Parquet é lido pelo v1 e a opção nem é consultada. Para usá-la, tire o formato da
  lista: `"spark.sql.sources.useV1SourceList": ""`.
- Só desce quando a consulta é **só** a agregação. Com `WHERE` (`PushedAggregation: []`)
  ou com `GROUP BY` (`PushedGroupBy: []`, agregação vazia junto) o Spark volta a ler os
  dados.
- Com as duas condições atendidas o plano mostra
  `PushedAggregation: [MAX(id), COUNT(*)]` e o scan devolve uma linha por arquivo, lida
  do footer.
- Trocar o Parquet para o v2 muda mais do que a agregação (o nó vira `BatchScan`, e o
  conjunto de otimizações é outro). Ligue por consulta, num pipeline de contagem/
  min/max, não como default do cluster.

### Alavancas por conector

O que o Spark empurra para arquivo, ele **não** empurra para conector: cada um traduz (ou
não) os filtros por conta própria. A coluna "explícito" é o que se escreve em `options`;
o resto o conector decide sozinho.

| Formato | Explícito em `options` | Automático |
|---|---|---|
| `postgresql`, `mysql`, `oracle`, `sqlserver`, … (JDBC) | `query` / `dbtable` com subquery, `pushDownPredicate`, `pushDownAggregate`, `pushDownLimit`, `pushDownOffset`, `pushDownTableSample` | predicado e projeção viram `WHERE`/`SELECT`; veja a seção JDBC acima |
| `cassandra` (e ScyllaDB) | `pushdown` (`true` por default) | predicado em *partition key* e *clustering column* vira CQL; filtro em coluna comum fica no Spark (o CQL exigiria `ALLOW FILTERING`) |
| `mongodb` (e DocumentDB) | `aggregation.pipeline` — `$match`/`$project` escritos à mão | o conector v10 traduz filtro e projeção para o estágio inicial do pipeline |
| `bigquery` | `filter`, `viewsEnabled` + `materializationDataset` (para `query`) | filtro e projeção vão para a Storage Read API; `maxParallelism` decide o número de streams |
| `snowflake` | `autopushdown` (`on` por default), `query` | com `autopushdown`, fragmentos inteiros do plano (join, agregação) são traduzidos para SQL Snowflake |
| `redshift` | `query`, `dbtable` com subquery | filtro e projeção entram no `UNLOAD`; o Spark lê do `tempdir` só o que sobrou |
| `elasticsearch` | `es.query` (DSL), `es.read.field.include` / `.exclude` | o es-hadoop traduz filtros do Spark para query DSL |
| `opensearch` | `opensearch.query`, mesmas chaves com o prefixo `opensearch.*` | idem |
| `dynamodb` | `filterPushdown` (`true` por default) | vira `FilterExpression` do Scan — cobra RCU do mesmo jeito, só economiza rede |
| `delta` | — | *data skipping* por estatística de arquivo (min/max das primeiras colunas indexadas) + poda de partição; `OPTIMIZE`/`ZORDER` melhoram o skipping |
| `iceberg` | — | poda por metadado e *hidden partitioning*: o filtro na coluna original poda a partição derivada (`days(dt)`, `bucket(16, id)`) |
| `kafka` | `startingOffsets`, `endingOffsets`, `assign`, `subscribePattern` | **nenhum**: filtro em `partition`/`offset`/`timestamp` roda depois da leitura. O recorte é o offset. |

Nome de opção acompanha a versão do conector no classpath — confirme no README da versão
que você fixou.

## DataFusion Comet — acelerador nativo (medido; opt-in, não default)

[Apache DataFusion Comet](https://datafusion.apache.org/comet/) é um plugin que troca
operadores do plano físico do Spark por implementações nativas (Rust/Arrow), operador a
operador, caindo de volta para o Spark no que não suporta. Não muda API nem resultado:
o mesmo pipeline JSON roda igual, mais rápido em scan/filtro/projeção/agregação/sort de
Parquet.

O framework **não** precisa de código para isso: `spark.configs` já é passagem livre de
conf. O que existe é uma lista de pré-requisitos que não são óbvios.

**Por que usar.** O benefício não é "Rust é rápido"; são quatro propriedades concretas:

1. **Execução vetorizada sobre Arrow.** O operador nativo trabalha em lotes colunares, com
   instruções SIMD, em vez de linha a linha na JVM. É por isso que o ganho aparece onde há
   varredura, filtro, projeção, hash aggregate e sort — e some onde o trabalho é de rede.
2. **Memória fora do heap.** Os buffers de execução saem do heap da JVM para o off-heap, o
   que tira do coletor de lixo justamente a parte que mais aloca. Em job com pausa de GC
   visível no *timeline* do Spark, essa metade do ganho é a que aparece primeiro.
3. **Nenhuma mudança no pipeline.** É configuração de sessão, não API: o mesmo JSON, os
   mesmos readers, writers, transformações e validações. Nada no arquivo do pipeline diz
   que existe Comet — o teste de integração afirma exatamente isso, comparando linha a
   linha o resultado das duas execuções.
4. **Adoção parcial e reversível.** A troca é operador a operador; o que o Comet não
   suporta continua no Spark, no mesmo plano. Não há migração, não há "porta única": tirar
   as configs volta ao estado anterior na próxima execução.

**Quando compensa:** leitura volumosa de Parquet com filtro, projeção, agregação, sort ou
shuffle no caminho — o perfil da maior parte da ingestão em lote. **Quando não compensa:**
pipeline cujo tempo está do outro lado da rede (JDBC, APIs), UDF em Python (força fallback
do trecho), pipeline dominado por escrita, e volume pequeno, onde o custo fixo de
dimensionar off-heap e carregar 88 MB de jar não se paga. Fora do Linux não há aceleração
nenhuma — ver "binário nativo" abaixo.

**Medido**, e não só avaliado: `tests/io/integration/test_comet_spark.py` roda o mesmo
pipeline (Parquet, `filter`, `group_by`) duas vezes em JVMs separadas, uma com as configs
abaixo e uma sem, e compara. Em Linux com Comet 1.0.0 + pyspark 4.1.1 + JDK 17 as linhas
saem idênticas e o plano acelerado fica nativo de ponta a ponta — só a borda de volta
para o Spark sobra:

```
*(1) CometColumnarToRow
+- CometHashAggregate [nome, sum, count], [Final], [nome], [sum(valor), count(1)]
   +- CometExchange hashpartitioning(nome, 1), ENSURE_REQUIREMENTS, CometNativeShuffle
      +- CometHashAggregate [nome, valor], [Partial], [nome], [partial_sum(valor), partial_count(1)]
         +- CometFilter [nome, valor], isnotnull(valor)
            +- CometNativeScan parquet [nome,valor] ... PushedFilters: [IsNotNull(valor)]
```

Sem as configs, o mesmo pipeline não produz nenhum nó `Comet` — é esse contraste que o
teste afirma, porque o plugin se desabilita em silêncio (ver "binário nativo" abaixo) e
um pipeline que passa não prova que houve aceleração.

```jsonc
"spark": { "configs": {
  "spark.plugins": "org.apache.spark.CometPlugin",
  "spark.shuffle.manager": "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager",
  "spark.memory.offHeap.enabled": "true",
  "spark.memory.offHeap.size": "4g",
  "spark.comet.explain.fallback.enabled": "true"
} }
```

- **Coordenada:** `org.apache.datafusion:comet-spark-spark4.1_2.13:1.0.0` — o artefato é
  por linha do Spark (`spark3.4`, `spark3.5`, `spark4.0`, `spark4.1`). O `org.apache.comet`
  que aparece em alguns tutoriais **não existe** no Maven Central. O jar tem ~88 MB.
- **O jar precisa estar no classpath do driver antes de a JVM subir.** Passar
  `spark.jars` pelo builder não basta — o plugin é carregado na construção do
  `SparkContext`, antes de o jar ser resolvido:

  ```
  java.lang.ClassNotFoundException: org.apache.spark.CometPlugin
    at org.apache.spark.internal.plugin.PluginContainer$.apply(PluginContainer.scala:210)
    at org.apache.spark.SparkContext.<init>(SparkContext.scala:594)
  ```

  Funciona via `spark-submit --jars <comet.jar> --driver-class-path <comet.jar>`, via
  `PYSPARK_SUBMIT_ARGS` equivalente, ou com o jar instalado no cluster.
- **Databricks (e qualquer host com sessão pré-existente):** `spark.configs` do JSON é
  ignorado, porque a sessão já existe quando o pipeline começa. Comet ali se configura na
  Spark config do cluster + a biblioteca instalada, nunca pelo pipeline.
- **Binário nativo só para Linux.** O jar embarca
  `org/apache/comet/linux/{amd64,aarch64}/libcomet.so` e mais nada. Em Windows/macOS o
  plugin carrega e se desabilita sozinho:

  ```
  WARN CometSparkSessionExtensions: Comet extension is disabled because of error when loading native lib. Falling back to Spark
  java.lang.UnsupportedOperationException: Unsupported OS/arch, cannot find /org/apache/comet/win32/amd64/comet.dll. Please try building from source.
  ```

  A degradação é graciosa — a consulta roda e o resultado é o mesmo, sem nenhum operador
  Comet no plano. Isso é bom para portabilidade e ruim para diagnóstico: a aceleração
  pode simplesmente não estar acontecendo em silêncio.
- **Versões:** Comet 1.0.0 cobre Spark 3.4 a 4.1 (4.1 com Java 17 ou 21); 4.2 é
  experimental.
- **Como saber se está ativo:** os nós do plano ganham prefixo `Comet` — na 1.0.0,
  `CometNativeScan` (não `CometScan`), `CometFilter`, `CometHashAggregate`,
  `CometExchange`/`CometNativeShuffle` e o `CometColumnarToRow` da borda —, e
  `{ "type": "debug", "actions": ["explain"] }` mostra. Com `spark.comet.explain.fallback.enabled=true`, cada operador que caiu de
  volta para o Spark é registrado com o motivo.
- **O que não acelera:** JDBC e os conectores (o trabalho está do outro lado da rede),
  UDF Python (força fallback do trecho) e escrita — o ganho está no caminho de leitura
  colunar e nos operadores intermediários.

**Ganho medido.** `tests/io/integration/bench_comet.py` roda o mesmo pipeline do framework
(`run_from_dict`) nas duas configurações, em JVMs separadas, com aquecimento descartado e
mediana de três repetições cronometradas. Em 40.000.000 de linhas / 290 MB de Parquet,
`local[4]`, off-heap de 4 GB, WSL2 com 16 núcleos:

| Forma da consulta | Sem Comet | Com Comet | Ganho | Nós nativos no plano |
|---|---|---|---|---|
| `filter` + `group_by` com `sum`/`count` | 3,56s | 1,61s | **2,21x** | `CometNativeScan`, `CometFilter`, `CometHashAggregate`, `CometExchange`, `CometNativeShuffle` |
| `filter` + `count` | 1,02s | 0,83s | 1,24x | `CometNativeScan`, `CometFilter`, `CometProject`, `CometColumnarToRow` |

O ganho acompanha quanto do plano virou nativo: a agregação, que tem shuffle e hash
aggregate para trocar, ganha mais que o dobro; a contagem, que é quase só varredura,
ganha pouco. Duas ressalvas de método: as saídas do benchmark usam
`"options": {"cache": "false"}`, porque o `ViewWriter` faz `cache()` + `count()` por
default e a repetição mediria o cache do Spark, não o Comet; e o número é deste hardware
e deste volume — reproduza com
`python tests/io/integration/bench_comet.py --linhas 40000000 --repeticoes 3` no seu.

**Posição atual:** funciona como opt-in, sem mudança no framework, em cluster Linux com
o jar instalado — e é assim que deve ser usado por enquanto. Não vira default: são 88 MB de
jar, exigência de off-heap dimensionado e fallback silencioso por operador, contra um
ganho que depende do formato e da forma da consulta — 2,21x numa agregação e 1,24x numa
contagem, na mesma máquina e nos mesmos dados. Vale medir no seu pipeline antes de adotar:
`test_comet_spark.py` prova correção e aceleração efetiva, `bench_comet.py` mede o tempo.

---

## Validações vs Transformações — quando usar cada um

| Necessidade | Use |
|-------------|-----|
| Remover linhas nulas antes de gravar | `filter` em transformations |
| Saber quantas linhas nulas chegaram (sem remover) | `not_null` em validations |
| Descartar duplicatas | `drop_duplicates` em transformations |
| Falhar o pipeline se existirem duplicatas | `unique` em validations |
| Cleansing de dados | transformations |
| Observabilidade de qualidade (métricas, relatórios) | validations |

Transformações **mudam** os dados. Validações **reportam** sobre eles sem modificá-los.  
O `PipelineResult.validation_results` expõe `failed_count` e mensagem por regra — útil para dashboards de qualidade.

Para saber **quais linhas** e **por quê**, o agregado não serve: use a quarentena com
`annotate` (`validations.outputs.invalid`), que grava numa coluna `array<string>` o
código de cada regra que rejeitou a linha (o `code` declarado ou a expressão da regra).
Ver o bloco `validations` do schema acima.

---

## PipelineResult

```python
@dataclass
class PipelineResult:
    pipeline_name: str
    success: bool
    rows_read: int = 0
    rows_written: int = 0
    validation_results: List[ValidationResult] = []
    error: Optional[str] = None
    output_df: Optional[DataFrame] = None  # preenchido quando input_df é injetado
    skipped: bool = False                  # True quando encerrado por stop_if_empty (sem dados)

    def summary() -> str  # linha de status legível
```

`PipelineResult` nunca lança exceção — erros ficam em `result.error`.
`skipped=True` indica encerramento gracioso por `stop_if_empty` (success=True, rows_written=0).

---

## Múltiplos outputs com projeção de colunas

O campo `columns` em cada output permite escrever subconjuntos diferentes para destinos diferentes a partir do mesmo DataFrame transformado:

```json
"outputs": [
  { "format": "parquet", "path": "/dw/full" },
  { "format": "parquet", "path": "/bi/analytics", "columns": ["id", "revenue", "region"] },
  { "format": "csv",     "path": "/export/report",  "columns": ["id", "revenue"] }
]
```

A projeção é feita em `Pipeline._project_columns()` — o DataFrame principal não é alterado entre outputs.

### Transformações por output

Quando os destinos precisam de **formas diferentes** (não só subconjuntos de colunas) do mesmo df, use `transformations` por output. São aplicadas sobre o df transformado, antes de `columns`/escrita, sem afetar as demais saídas. Aceitam todos os tipos do engine (incl. `join`, `explode` via `with_column`, `to_json`, `{{var}}` de runtime).

```json
"outputs": [
  { "format": "kafka", "path": "topico",
    "transformations": [ { "type": "with_column", "column": "value", "expression": "to_json(payload)" } ],
    "options": { "bootstrap_servers": "broker:9092", "value_column": "value", "key_column": null } },

  { "format": "delta", "path": "schema.parcelas", "mode": "append",
    "transformations": [
      { "type": "join", "input": { "format": "delta", "path": "schema.silver_parcela" },
        "on": ["id_cessao", "numero_contrato"], "how": "inner" },
      { "type": "with_column", "column": "data_baixa", "expression": "cast(null as date)" }
    ] }
]
```

Cada output parte do **mesmo** df principal (materialize-o com `checkpoint` na última transformação para não recomputar a linhagem a cada destino). Aplicado em `Pipeline._write_outputs()`.

---

## Padrões de extensão

### Novo formato IO

```python
fw = Sparquet()
fw.register_reader("meu_formato", MeuReader)   # class MeuReader(BaseReader)
fw.register_writer("meu_formato", MeuWriter)   # class MeuWriter(BaseWriter)
```

### Nova transformação

```python
class NormalizeText(BaseTransformation):
    def apply(self, df):
        col = self.config.params["column"]
        return df.withColumn(col, F.trim(F.lower(F.col(col))))

fw.register_transformation("normalize_text", NormalizeText)
# JSON: { "type": "normalize_text", "column": "email" }
```

### Novo validator

```python
class NoFutureDateValidator(BaseValidator):
    def validate(self, df):
        col = self.rule.params["column"]
        failed = df.filter(F.col(col) > F.current_date()).count()
        if failed:
            return ValidationResult("no_future_date", False, f"{failed} datas futuras", failed)
        return ValidationResult("no_future_date", True)

fw.register_validator("no_future_date", NoFutureDateValidator)
```


---

## Convenções do projeto (referência completa)

> Os itens não-inferíveis mais críticos estão resumidos em `CLAUDE.md`; a lista completa fica aqui.

- `input` (singular) como fonte principal; múltiplas fontes via `join`/`union` em transformations.
- `output` (singular) ou `outputs` (lista) — ambos aceitos; um único objeto é normalizado para lista internamente.
- `columns` em output = projeção de colunas por destino; sem `columns` = escreve todas.
- Factories são class-level registries — extensões em `Sparquet` afetam todas as execuções.
- `Pipeline` recebe engines injetáveis — útil para testes ou para injetar engines com transformações customizadas.
- `PipelineResult` nunca lança exceção — erros ficam em `result.error`.
- Logger sempre JSON estruturado (`utils/logger.py`).
- `SparkContextManager` detecta o ambiente automaticamente (Databricks reusa sessão ativa; outros criam via builder).
- **Workers Python em master local**: com `master=local*`, o `SparkContextManager` faz
  `os.environ.setdefault("PYSPARK_PYTHON", sys.executable)` (idem `PYSPARK_DRIVER_PYTHON`).
  Sem isso o Spark lança o worker com o `python` do PATH; se for outro build que o driver,
  o worker morre com `Python worker exited unexpectedly (crashed)`. O sintoma engana:
  etapas puramente JVM (CSV → Parquet) não criam worker, então só quebra na primeira que
  cria — uma UDF, um `sql` com função Python, um `createDataFrame` a partir de linhas do
  driver. **Só em master
  local** (driver e executor na mesma máquina); em cluster a variável é da plataforma, e
  apontar executor remoto para um caminho do driver quebraria o job. `setdefault` nunca
  sobrescreve escolha explícita.
- **CSV em dialeto RFC 4180**: leitura e escrita usam `escape='"'` por default — aspas
  dentro de um campo saem **dobradas** (`""`), não escapadas com `\"` como no default do
  Spark. O Spark relê o dialeto dele, mas o `csv` do Python, o pandas e o Excel não: o
  `validations.report`, cuja coluna `rule_params` é um JSON cheio de aspas, saía partido
  no meio do campo justamente na ferramenta em que é analisado. Os dois lados mudam
  juntos (senão o framework escreveria um CSV que ele mesmo não relê). Para ler arquivos
  gravados no dialeto antigo, declare `options: {"escape": "\\"}`.
- **`filter`/`select` primeiro**: comece a cadeia de `transformations` reduzindo linhas (`filter`) e colunas (`select`) antes de joins/structs/group_by pesados — menos dados por todo o resto do pipeline (o Spark empurra parte, mas colocar explícito ajuda o planner e a legibilidade).
- **Self-join sem reler a base**: `fw.run(..., input_view="entrada")` registra (e cacheia) o df de entrada como temp view; um `join`/`sql` seguinte referencia `entrada` sem reler a fonte. Para uma global temp view, passe um dict: `input_view={"name": "entrada", "type": "global"}` (default `"type": "session"`).
- **temp view (`view`) global vs sessão**: `options.scope` = `session` (default) ou `global` (`global_temp.<nome>`, visível a toda a aplicação Spark).
- **sparquet_cola** é um pacote/repo separado (`../sparquet-cola`), publicado no PyPI e declarado em `dependencies` do sparquet como `sparquet-cola>=0.4.0` (piso, sem cap: a 0.3.0 trouxe as métricas como tipos de regra e o `expand_targets` de que o parse da config depende; a 0.4.0, a medição das regras agregáveis numa passada única). Nome PyPI com hífen (`sparquet-cola`); o import é sempre `sparquet_cola` (underscore — convenção Python). Alterações no motor de DQ são feitas no repo `sparquet-cola` (publique uma nova versão lá antes de o sparquet a consumir).

---

