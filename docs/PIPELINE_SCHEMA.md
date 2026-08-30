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
    { "type": "collect", "column": "id_cessao", "as": "cessoes_pendentes" },
    // stop_if_empty: encerra o pipeline graciosamente se o df estiver vazio — não
    // roda as transformações seguintes nem escreve nas saídas. result.skipped = True,
    // success = True, rows_written = 0. Posicione logo após o filtro que define o
    // conjunto a processar (antes de joins/payloads pesados).
    { "type": "stop_if_empty", "message": "Sem dados a processar" },
    { "type": "sql", "query": "SELECT ...", "view_name": "_df" },
    { "type": "fill_na", "value": 0, "columns": ["col"] },
    { "type": "sort", "columns": ["col"], "ascending": true },
    // $include: expande inline o conteúdo de um arquivo JSON (caminho relativo ao pipeline)
    { "$include": "shared/filtro_tipo_ativo.json" },
    {
      "type": "debug",                          // não modifica o df — apenas inspeciona
      "label": "após join contratos",           // opcional, aparece no separador
      // show usa df.show() — display() do Databricks só funciona chamado diretamente na célula
      "actions": ["count", "print_schema", "show", "explain", "columns", "dtypes"],
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
| `xml` | sim | sim | Requer `spark-xml`; `rowTag` obrigatório |
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
| `elasticsearch` | sim | sim | `path`=índice; **mesma classe atende Elasticsearch e OpenSearch** |

> Conectores externos exigem o **JAR do driver/conector no classpath** do Spark
> (`spark.jars` / `spark.jars.packages`): JDBC, BigQuery, Snowflake, Redshift, Mongo,
> DynamoDB, Cassandra, Elasticsearch, Kafka, **Avro (`spark-avro`), XML (`spark-xml`)
> e Hudi (`hudi-spark-bundle`)**. `parquet`/`csv`/`delta`/`iceberg`/`txt`/`view`/`json`/
> `orc`/`binary` são nativos. O framework só monta `.format(...).options(...)`; não
> empacota drivers. Cada `io/<fmt>.py` documenta as opções; o catálogo do Studio
> (`formats.databases.ts`, `formats.files.ts`) as descreve para a UI e a IA.

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

