/**
 * Landing page copy, one object per locale.
 *
 * Keeping the words out of the components means a translation never touches
 * markup, and a missing key is a type error rather than an English string
 * leaking into another language.
 */

import type { Locale } from './ui'

export interface LandingCopy {
  meta: { title: string; description: string }
  hero: {
    eyebrow: string
    title: string
    titleAccent: string
    subtitle: string
    primaryCta: string
    secondaryCta: string
    note: string
    stats: { value: string; label: string }[]
    aiNote: string
    fileName: string
    canvasCaption: string
    canvasStatus: string
  }
  problem: {
    eyebrow: string
    title: string
    body: string
    points: { title: string; body: string }[]
  }
  ways: {
    eyebrow: string
    title: string
    subtitle: string
    items: { step: string; title: string; body: string; caption: string }[]
    footnote: string
  }
  features: {
    eyebrow: string
    title: string
    subtitle: string
    items: { title: string; body: string }[]
    cta: string
  }
  studio: {
    eyebrow: string
    title: string
    body: string
    vocabulary: { term: string; body: string }[]
    bullets: string[]
    cta: string
    imageAlt: string
  }
  ai: {
    eyebrow: string
    title: string
    body: string
    prompt: string
    answer: string
    bullets: string[]
    cta: string
  }
  connectors: {
    eyebrow: string
    title: string
    body: string
    groups: { label: string; items: string[] }[]
    cta: string
  }
  runs: {
    eyebrow: string
    title: string
    body: string
    targets: { name: string; body: string }[]
  }
  cta: {
    title: string
    body: string
    primary: string
    secondary: string
    install: string
  }
}

const EN: LandingCopy = {
  meta: {
    title: 'Sparquet, the standard for data engineering',
    description:
      'A framework that standardizes data engineering: every pipeline is one declarative JSON contract. Write it, generate it with AI, or design it in Sparquet Studio. Open source, Apache 2.0.',
  },
  hero: {
    eyebrow: 'The best open-source data engineering framework',
    title: 'Every pipeline your team writes,',
    titleAccent: 'written the same way.',
    subtitle:
      'Sparquet standardizes data engineering: ingestion, transformation, quality and delivery live in one declarative JSON contract instead of a thousand bespoke scripts. And because a pipeline is just JSON, an LLM writes it as well as you do: ask the assistant inside Sparquet Studio, or ChatGPT, Claude or Copilot anywhere else, review the diff and run it on any Spark.',
    primaryCta: 'Get started',
    secondaryCta: 'Read the docs',
    note: 'Apache 2.0 · No account · Runs on your infrastructure',
    stats: [
      { value: '27', label: 'connectors' },
      { value: '20', label: 'transformations' },
      { value: '1', label: 'artifact' },
    ],
    aiNote: 'A pipeline is JSON, so any model can write one, and the linter proves it before Spark ever starts.',
    fileName: 'orders_curated.json',
    canvasCaption: 'The same contract, as a Job on the Studio canvas',
    canvasStatus: '{count} nodes · valid · compiles to {file}',
  },
  problem: {
    eyebrow: 'Why it exists',
    title: 'Ten engineers, ten ways to read a CSV.',
    body: 'Data teams rarely lack tools, what they lack is a shared shape. Every job invents its own structure, its own naming, its own idea of what "validated" means, until nobody can safely review a pipeline they did not write. Sparquet replaces that with one contract every job obeys.',
    points: [
      {
        title: 'One contract',
        body: 'Source, transformations, quality rules and destinations are fields of the same document. A new engineer reads any pipeline on day one.',
      },
      {
        title: 'Reviewable by design',
        body: 'A change shows up as a diff a reviewer can reason about, not as archaeology across notebook cells and cluster state.',
      },
      {
        title: 'Reused, not copied',
        body: 'Parameters turn one contract into every region, date and client. Values change; the logic stays in a single place.',
      },
    ],
  },
  ways: {
    eyebrow: 'How you build one',
    title: 'Three ways to author it. One artifact.',
    subtitle:
      'The standard is the file. How it gets written is your call, and the three paths are interchangeable, in both directions.',
    items: [
      {
        step: '01',
        title: 'Write it',
        body: 'Author the JSON directly. The schema is small enough to keep in your head and strict enough to review in a pull request.',
        caption: 'orders_curated.json',
      },
      {
        step: '02',
        title: 'Generate it',
        body: 'Describe it in plain language, to the assistant inside Studio or to any model you already pay for. The whole language fits in a prompt, so what comes back is valid JSON, not code you have to run to trust.',
        caption: 'Any LLM',
      },
      {
        step: '03',
        title: 'Draw it',
        body: 'Open Sparquet Studio and build it on a canvas. Nodes are the entries of the file, connections are their order, and the file is what actually runs.',
        caption: 'Sparquet Studio',
      },
    ],
    footnote:
      'Import a hand-written file into the canvas, or export a drawn one to git. Nothing is lost either way.',
  },
  features: {
    eyebrow: 'What the standard covers',
    title: 'A complete pipeline language.',
    subtitle:
      'Twenty transformations, twenty-seven connectors and a data quality engine, every one of them documented, typed in the editor and understood by the assistant.',
    items: [
      {
        title: 'Transformations',
        body: 'filter, select, cast, with_column, struct, group_by, join, union, sort, distinct and more, applied in the order you write them.',
      },
      {
        title: 'Data quality',
        body: 'Null, uniqueness, range, regex, row count, SQL invariants and schema checks, with fail / warn / skip policies and a per-rule report.',
      },
      {
        title: 'Connectors',
        body: 'Lakehouse tables, files, relational databases, warehouses, NoSQL, search and streams, the same node model for all of them.',
      },
      {
        title: 'Quarantine',
        body: 'Route the rows that fail a rule to their own destination, so the clean set ships while the bad set stays inspectable.',
      },
      {
        title: 'Parameters',
        body: 'One file, many runs: {param} placeholders formatted for SQL, and skip_if_false to switch whole steps on and off per execution.',
      },
      {
        title: 'Runtime pushdown',
        body: 'collect a key set into a {{variable}} and push it into a later read as a literal IN (...), data skipping without hand-written glue.',
      },
      {
        title: 'Multiple destinations',
        body: 'One DataFrame, many shapes: per-destination transformations and column projections in a single pass over the data.',
      },
      {
        title: 'Extensible',
        body: 'Register your own readers, writers, transformations and validators. Studio keeps unknown node types intact when it opens the file.',
      },
    ],
    cta: 'See the full reference',
  },
  studio: {
    eyebrow: 'Sparquet Studio',
    title: 'The visual interface for your workflows.',
    body: 'Studio is where the contract becomes something a team can see. Drag a source, connect transformations, wire a destination, then press ⌘J and read the exact JSON that will run. No proprietary project format, no hidden generation step.',
    vocabulary: [
      { term: 'Workflow', body: 'the container, usually one per domain: Sales, Billing, CRM.' },
      { term: 'Job', body: 'one pipeline JSON, drawn on the canvas.' },
      { term: 'Pipeline', body: 'an ordered set of Jobs, executed in sequence in one session.' },
    ],
    bullets: [
      'Every field documented in place, with the behaviors that usually live only in the source code',
      'Live linting that catches the mistakes Spark only reports after an hour of compute',
      'Run a Job or a whole Pipeline from the canvas and read counters, quality results and a data preview',
      'Everything stored in your browser, no account, no server, no telemetry',
    ],
    cta: 'Explore Studio',
    imageAlt: 'Sparquet Studio showing a Job on the canvas with the inspector open',
  },
  ai: {
    eyebrow: 'AI-native by design',
    title: 'The format is what makes the AI reliable.',
    body: 'Ask a model for a PySpark job and you get a script nobody can verify without running it on real data. Ask for a Sparquet pipeline and you get a short JSON document: every field exists in the catalog, the linter checks it in place, and the diff is reviewable line by line. Studio ships an assistant that uses your own key, and any model outside it works just as well, because the entire language fits in a prompt.',
    prompt:
      'Read orders from the Delta table sales.orders, keep the confirmed ones, drop duplicates by id, fail the run if id has nulls, and upsert revenue per customer into analytics.customer_revenue',
    answer: 'Proposed a Job with 6 nodes · 1 destination · 2 quality rules',
    bullets: [
      'Generate a complete Job, or modify the one already on the canvas',
      'Works outside Studio too: paste the reference into ChatGPT, Claude, Copilot or your own agent',
      'Ask it to explain, optimize or fix exactly the issues the linter found',
      'Every proposal is reviewed before it touches your work, and undo is one keystroke',
    ],
    cta: 'How the assistant works',
  },
  connectors: {
    eyebrow: 'Connectors',
    title: 'Twenty-seven sources and destinations.',
    body: 'Swapping where the data comes from is a field, not a rewrite. The same node model covers lakehouse tables, plain files, operational databases, warehouses, NoSQL stores and streams.',
    groups: [
      { label: 'Lakehouse', items: ['Delta Lake', 'Iceberg', 'Hudi', 'Parquet', 'ORC', 'Avro'] },
      { label: 'Files', items: ['CSV', 'JSON', 'XML', 'Text', 'Binary'] },
      { label: 'Databases', items: ['PostgreSQL', 'MySQL', 'MariaDB', 'SQL Server', 'Oracle'] },
      { label: 'Warehouses', items: ['BigQuery', 'Snowflake', 'Redshift'] },
      {
        label: 'NoSQL & search',
        items: ['MongoDB', 'DocumentDB', 'DynamoDB', 'Cassandra', 'Elasticsearch', 'OpenSearch'],
      },
      { label: 'Streaming & views', items: ['Kafka', 'Temp views'] },
    ],
    cta: 'See every connector',
  },
  runs: {
    eyebrow: 'Where it runs',
    title: 'Your Spark, your rules.',
    body: 'Sparquet is a library, not a platform. There is no control plane, no hosted runtime, and nothing phones home.',
    targets: [
      { name: 'Local', body: 'A laptop with PySpark installed, for development and tests.' },
      { name: 'Databricks', body: 'Reuses the active session; jobs and notebooks work unchanged.' },
      { name: 'EMR & Dataproc', body: 'Submit it like any other PySpark application.' },
      { name: 'Synapse', body: 'Detected automatically, like every other supported environment.' },
    ],
  },
  cta: {
    title: 'Standardize your first pipeline in five minutes.',
    body: 'Install the framework, open the studio, and run a real job before your coffee gets cold.',
    primary: 'Start the tutorial',
    secondary: 'Star on GitHub',
    install: 'pip install sparquet',
  },
}

const PT: LandingCopy = {
  meta: {
    title: 'Sparquet, o padrão para engenharia de dados',
    description:
      'Um framework que padroniza engenharia de dados: cada pipeline é um contrato JSON declarativo. Escreva, gere com IA ou desenhe no Sparquet Studio. Open source, Apache 2.0.',
  },
  hero: {
    eyebrow: 'O melhor framework open source de engenharia de dados',
    title: 'Todo pipeline do seu time,',
    titleAccent: 'escrito do mesmo jeito.',
    subtitle:
      'O Sparquet padroniza a engenharia de dados: ingestão, transformação, qualidade e entrega vivem em um contrato JSON declarativo, não em mil scripts sob medida. E como o pipeline é só JSON, uma LLM escreve tão bem quanto você: peça ao assistente dentro do Sparquet Studio, ou ao ChatGPT, Claude e Copilot em qualquer outro lugar, revise o diff e rode em qualquer Spark.',
    primaryCta: 'Começar agora',
    secondaryCta: 'Ver a documentação',
    note: 'Apache 2.0 · Sem cadastro · Roda na sua infraestrutura',
    stats: [
      { value: '27', label: 'conectores' },
      { value: '20', label: 'transformações' },
      { value: '1', label: 'artefato' },
    ],
    aiNote: 'Pipeline é JSON, então qualquer modelo escreve um, e o linter prova antes de o Spark começar.',
    fileName: 'pedidos_curados.json',
    canvasCaption: 'O mesmo contrato, como Job no canvas do Studio',
    canvasStatus: '{count} nós · válido · compila para {file}',
  },
  problem: {
    eyebrow: 'Por que existe',
    title: 'Dez engenheiros, dez jeitos de ler um CSV.',
    body: 'O que falta a times de dados raramente é ferramenta, é forma comum. Cada job inventa a própria estrutura, a própria nomenclatura e a própria ideia do que significa "validado", até que ninguém consegue revisar com segurança um pipeline que não escreveu. O Sparquet troca isso por um contrato que todo job obedece.',
    points: [
      {
        title: 'Um contrato só',
        body: 'Fonte, transformações, regras de qualidade e destinos são campos do mesmo documento. Quem entra no time lê qualquer pipeline no primeiro dia.',
      },
      {
        title: 'Revisável por construção',
        body: 'Mudança vira diff que o revisor consegue avaliar, não escavação entre células de notebook e estado de cluster.',
      },
      {
        title: 'Reaproveitado, não copiado',
        body: 'Parâmetros transformam um contrato em todas as regiões, datas e clientes. Os valores mudam; a lógica fica em um lugar só.',
      },
    ],
  },
  ways: {
    eyebrow: 'Como você constrói',
    title: 'Três formas de escrever. Um artefato.',
    subtitle:
      'O padrão é o arquivo. Como ele nasce é escolha sua, e os três caminhos são intercambiáveis, nos dois sentidos.',
    items: [
      {
        step: '01',
        title: 'Escreva',
        body: 'Escreva o JSON direto. O schema é pequeno o suficiente para caber na cabeça e rígido o suficiente para ser revisado em um pull request.',
        caption: 'pedidos_curados.json',
      },
      {
        step: '02',
        title: 'Gere',
        body: 'Descreva em linguagem natural, para o assistente dentro do Studio ou para qualquer modelo que você já paga. A linguagem inteira cabe em um prompt, então o que volta é JSON válido, não código que você precisa rodar para confiar.',
        caption: 'Qualquer LLM',
      },
      {
        step: '03',
        title: 'Desenhe',
        body: 'Abra o Sparquet Studio e monte no canvas. Os nós são as entradas do arquivo, as conexões são a ordem, e o arquivo é o que realmente roda.',
        caption: 'Sparquet Studio',
      },
    ],
    footnote:
      'Importe para o canvas um arquivo escrito na mão, ou exporte para o git um que você desenhou. Nada se perde no caminho.',
  },
  features: {
    eyebrow: 'O que o padrão cobre',
    title: 'Uma linguagem de pipeline completa.',
    subtitle:
      'Vinte transformações, vinte e sete conectores e um motor de qualidade de dados, todos documentados, tipados no editor e compreendidos pelo assistente.',
    items: [
      {
        title: 'Transformações',
        body: 'filter, select, cast, with_column, struct, group_by, join, union, sort, distinct e mais, aplicadas na ordem em que você escreve.',
      },
      {
        title: 'Qualidade de dados',
        body: 'Nulos, unicidade, faixa, regex, contagem de linhas, invariantes SQL e checagem de schema, com políticas fail / warn / skip e relatório por regra.',
      },
      {
        title: 'Conectores',
        body: 'Tabelas de lakehouse, arquivos, bancos relacionais, data warehouses, NoSQL, busca e streams, o mesmo modelo de nós para todos.',
      },
      {
        title: 'Quarentena',
        body: 'Roteie as linhas que violam uma regra para um destino próprio: o conjunto limpo segue, o problemático fica disponível para análise.',
      },
      {
        title: 'Parâmetros',
        body: 'Um arquivo, muitas execuções: placeholders {param} formatados para SQL e skip_if_false para ligar e desligar etapas inteiras.',
      },
      {
        title: 'Pushdown em runtime',
        body: 'collect leva um conjunto de chaves para uma {{variável}} e empurra o filtro literal IN (...) na leitura seguinte, data skipping sem gambiarra.',
      },
      {
        title: 'Múltiplos destinos',
        body: 'Um DataFrame, várias formas: transformações e projeções por destino em uma única passada sobre os dados.',
      },
      {
        title: 'Extensível',
        body: 'Registre seus próprios readers, writers, transformações e validadores. O Studio preserva tipos desconhecidos ao abrir o arquivo.',
      },
    ],
    cta: 'Ver a referência completa',
  },
  studio: {
    eyebrow: 'Sparquet Studio',
    title: 'A interface visual dos seus workflows.',
    body: 'O Studio é onde o contrato vira algo que o time enxerga. Arraste uma fonte, conecte transformações, ligue um destino, e aperte ⌘J para ler o JSON exato que vai rodar. Sem formato proprietário, sem geração escondida.',
    vocabulary: [
      {
        term: 'Workflow',
        body: 'o container, normalmente um por domínio: Vendas, Faturamento, CRM.',
      },
      { term: 'Job', body: 'um JSON de pipeline, desenhado no canvas.' },
      {
        term: 'Pipeline',
        body: 'um conjunto ordenado de Jobs, executados em sequência na mesma sessão.',
      },
    ],
    bullets: [
      'Cada campo documentado no lugar, com os comportamentos que normalmente só existem no código-fonte',
      'Análise ao vivo que pega os erros que o Spark só reporta depois de uma hora de processamento',
      'Rode um Job ou um Pipeline inteiro pelo canvas e leia contadores, qualidade e uma prévia dos dados',
      'Tudo guardado no seu navegador, sem cadastro, sem servidor, sem telemetria',
    ],
    cta: 'Conhecer o Studio',
    imageAlt: 'Sparquet Studio com um Job no canvas e o inspetor aberto',
  },
  ai: {
    eyebrow: 'IA nativa por design',
    title: 'O formato é o que torna a IA confiável.',
    body: 'Peça um job PySpark a um modelo e você recebe um script que ninguém consegue verificar sem rodar em dados reais. Peça um pipeline Sparquet e você recebe um JSON curto: todo campo existe no catálogo, o linter confere ali mesmo e o diff é revisável linha a linha. O Studio traz um assistente que usa a sua própria chave, e qualquer modelo fora dele funciona igual, porque a linguagem inteira cabe em um prompt.',
    prompt:
      'Leia pedidos da tabela Delta vendas.pedidos, mantenha os confirmados, remova duplicatas por id, falhe se id tiver nulo e faça upsert da receita por cliente em analytics.receita_cliente',
    answer: 'Job proposto com 6 nós · 1 destino · 2 regras de qualidade',
    bullets: [
      'Gere um Job completo ou modifique o que já está no canvas',
      'Funciona fora do Studio também: cole a referência no ChatGPT, Claude, Copilot ou no seu agente',
      'Peça para explicar, otimizar ou corrigir exatamente os problemas que o linter encontrou',
      'Toda proposta é revisada antes de tocar seu trabalho, e desfazer é uma tecla',
    ],
    cta: 'Como o assistente funciona',
  },
  connectors: {
    eyebrow: 'Conectores',
    title: 'Vinte e sete fontes e destinos.',
    body: 'Trocar de onde vêm os dados é mudar um campo, não reescrever o job. O mesmo modelo de nós cobre lakehouse, arquivos, bancos operacionais, data warehouses, NoSQL e streams.',
    groups: [
      { label: 'Lakehouse', items: ['Delta Lake', 'Iceberg', 'Hudi', 'Parquet', 'ORC', 'Avro'] },
      { label: 'Arquivos', items: ['CSV', 'JSON', 'XML', 'Texto', 'Binário'] },
      { label: 'Bancos', items: ['PostgreSQL', 'MySQL', 'MariaDB', 'SQL Server', 'Oracle'] },
      { label: 'Data warehouses', items: ['BigQuery', 'Snowflake', 'Redshift'] },
      {
        label: 'NoSQL e busca',
        items: ['MongoDB', 'DocumentDB', 'DynamoDB', 'Cassandra', 'Elasticsearch', 'OpenSearch'],
      },
      { label: 'Streaming e views', items: ['Kafka', 'Views temporárias'] },
    ],
    cta: 'Ver todos os conectores',
  },
  runs: {
    eyebrow: 'Onde roda',
    title: 'Seu Spark, suas regras.',
    body: 'O Sparquet é uma biblioteca, não uma plataforma. Não há control plane, não há runtime hospedado e nada liga para casa.',
    targets: [
      { name: 'Local', body: 'Um notebook com PySpark instalado, para desenvolver e testar.' },
      {
        name: 'Databricks',
        body: 'Reaproveita a sessão ativa; jobs e notebooks funcionam sem mudança.',
      },
      { name: 'EMR e Dataproc', body: 'Submeta como qualquer outra aplicação PySpark.' },
      { name: 'Synapse', body: 'Detectado automaticamente, como todo ambiente suportado.' },
    ],
  },
  cta: {
    title: 'Padronize seu primeiro pipeline em cinco minutos.',
    body: 'Instale o framework, abra o studio e rode um job de verdade antes do café esfriar.',
    primary: 'Começar o tutorial',
    secondary: 'Dar uma estrela no GitHub',
    install: 'pip install sparquet',
  },
}

const ES: LandingCopy = {
  meta: {
    title: 'Sparquet, el estándar para ingeniería de datos',
    description:
      'Un framework que estandariza la ingeniería de datos: cada pipeline es un contrato JSON declarativo. Escríbelo, genéralo con IA o diséñalo en Sparquet Studio. Open source, Apache 2.0.',
  },
  hero: {
    eyebrow: 'El mejor framework open source de ingeniería de datos',
    title: 'Cada pipeline de tu equipo,',
    titleAccent: 'escrito de la misma forma.',
    subtitle:
      'Sparquet estandariza la ingeniería de datos: ingesta, transformación, calidad y entrega viven en un contrato JSON declarativo, no en mil scripts a medida. Y como el pipeline es solo JSON, un LLM lo escribe tan bien como tú: pídeselo al asistente dentro de Sparquet Studio, o a ChatGPT, Claude y Copilot en cualquier otro lugar, revisa el diff y córrelo en cualquier Spark.',
    primaryCta: 'Empezar',
    secondaryCta: 'Ver la documentación',
    note: 'Apache 2.0 · Sin cuenta · Corre en tu infraestructura',
    stats: [
      { value: '27', label: 'conectores' },
      { value: '20', label: 'transformaciones' },
      { value: '1', label: 'artefacto' },
    ],
    aiNote: 'Un pipeline es JSON, así que cualquier modelo escribe uno, y el linter lo prueba antes de que Spark arranque.',
    fileName: 'pedidos_curados.json',
    canvasCaption: 'El mismo contrato, como Job en el lienzo del Studio',
    canvasStatus: '{count} nodos · válido · compila a {file}',
  },
  problem: {
    eyebrow: 'Por qué existe',
    title: 'Diez ingenieros, diez formas de leer un CSV.',
    body: 'A los equipos de datos rara vez les faltan herramientas, les falta una forma común. Cada trabajo inventa su estructura, su nomenclatura y su idea de qué significa "validado", hasta que nadie puede revisar con seguridad un pipeline que no escribió. Sparquet lo reemplaza por un contrato que todos cumplen.',
    points: [
      {
        title: 'Un solo contrato',
        body: 'Fuente, transformaciones, reglas de calidad y destinos son campos del mismo documento. Quien entra al equipo lee cualquier pipeline el primer día.',
      },
      {
        title: 'Revisable por diseño',
        body: 'Un cambio aparece como un diff que se puede razonar, no como arqueología entre celdas de cuaderno y estado del clúster.',
      },
      {
        title: 'Reutilizado, no copiado',
        body: 'Los parámetros convierten un contrato en cada región, fecha y cliente. Cambian los valores; la lógica vive en un solo sitio.',
      },
    ],
  },
  ways: {
    eyebrow: 'Cómo lo construyes',
    title: 'Tres formas de escribirlo. Un artefacto.',
    subtitle:
      'El estándar es el archivo. Cómo se escribe lo decides tú, y los tres caminos son intercambiables, en ambos sentidos.',
    items: [
      {
        step: '01',
        title: 'Escríbelo',
        body: 'Escribe el JSON directamente. El esquema es lo bastante pequeño para tenerlo en la cabeza y lo bastante estricto para revisarlo en un pull request.',
        caption: 'pedidos_curados.json',
      },
      {
        step: '02',
        title: 'Genéralo',
        body: 'Descríbelo en lenguaje natural, al asistente dentro del Studio o a cualquier modelo que ya pagas. El lenguaje entero cabe en un prompt, así que lo que vuelve es JSON válido, no código que tengas que ejecutar para confiar.',
        caption: 'Cualquier LLM',
      },
      {
        step: '03',
        title: 'Dibújalo',
        body: 'Abre Sparquet Studio y móntalo en el lienzo. Los nodos son las entradas del archivo, las conexiones su orden, y el archivo es lo que realmente corre.',
        caption: 'Sparquet Studio',
      },
    ],
    footnote:
      'Importa al lienzo un archivo escrito a mano, o exporta a git uno que dibujaste. No se pierde nada en el camino.',
  },
  features: {
    eyebrow: 'Qué cubre el estándar',
    title: 'Un lenguaje de pipelines completo.',
    subtitle:
      'Veinte transformaciones, veintisiete conectores y un motor de calidad de datos, todos documentados, tipados en el editor y comprendidos por el asistente.',
    items: [
      {
        title: 'Transformaciones',
        body: 'filter, select, cast, with_column, struct, group_by, join, union, sort, distinct y más, aplicadas en el orden en que las escribes.',
      },
      {
        title: 'Calidad de datos',
        body: 'Nulos, unicidad, rango, regex, conteo de filas, invariantes SQL y comprobación de esquema, con políticas fail / warn / skip e informe por regla.',
      },
      {
        title: 'Conectores',
        body: 'Tablas de lakehouse, archivos, bases relacionales, data warehouses, NoSQL, búsqueda y streams, el mismo modelo de nodos para todos.',
      },
      {
        title: 'Cuarentena',
        body: 'Envía las filas que incumplen una regla a su propio destino: lo limpio se publica y lo problemático queda para revisar.',
      },
      {
        title: 'Parámetros',
        body: 'Un archivo, muchas ejecuciones: marcadores {param} formateados para SQL y skip_if_false para activar o desactivar pasos enteros.',
      },
      {
        title: 'Pushdown en runtime',
        body: 'collect lleva un conjunto de claves a una {{variable}} y empuja el filtro literal IN (...) en la siguiente lectura, data skipping sin pegamento manual.',
      },
      {
        title: 'Múltiples destinos',
        body: 'Un DataFrame, varias formas: transformaciones y proyecciones por destino en una sola pasada sobre los datos.',
      },
      {
        title: 'Extensible',
        body: 'Registra tus propios readers, writers, transformaciones y validadores. El Studio conserva intactos los tipos que no conoce.',
      },
    ],
    cta: 'Ver la referencia completa',
  },
  studio: {
    eyebrow: 'Sparquet Studio',
    title: 'La interfaz visual de tus workflows.',
    body: 'El Studio es donde el contrato se vuelve algo que el equipo ve. Arrastra una fuente, conecta transformaciones, enlaza un destino, y pulsa ⌘J para leer el JSON exacto que se ejecutará. Sin formato propietario, sin generación oculta.',
    vocabulary: [
      {
        term: 'Workflow',
        body: 'el contenedor, normalmente uno por dominio: Ventas, Facturación, CRM.',
      },
      { term: 'Job', body: 'un JSON de pipeline, dibujado en el lienzo.' },
      {
        term: 'Pipeline',
        body: 'un conjunto ordenado de Jobs, ejecutados en secuencia en una sesión.',
      },
    ],
    bullets: [
      'Cada campo documentado en su sitio, con los comportamientos que suelen vivir solo en el código fuente',
      'Análisis en vivo que atrapa los errores que Spark solo reporta tras una hora de cómputo',
      'Ejecuta un Job o un Pipeline completo desde el lienzo y lee contadores, calidad y una vista previa',
      'Todo guardado en tu navegador, sin cuenta, sin servidor, sin telemetría',
    ],
    cta: 'Explorar el Studio',
    imageAlt: 'Sparquet Studio mostrando un Job en el lienzo con el inspector abierto',
  },
  ai: {
    eyebrow: 'IA nativa por diseño',
    title: 'El formato es lo que hace confiable a la IA.',
    body: 'Pídele a un modelo un job de PySpark y recibes un script que nadie puede verificar sin correrlo sobre datos reales. Pídele un pipeline de Sparquet y recibes un JSON corto: cada campo existe en el catálogo, el linter lo revisa ahí mismo y el diff se lee línea por línea. El Studio trae un asistente que usa tu propia clave, y cualquier modelo fuera de él funciona igual, porque el lenguaje entero cabe en un prompt.',
    prompt:
      'Lee pedidos de la tabla Delta ventas.pedidos, quédate con los confirmados, elimina duplicados por id, falla si id tiene nulos y haz upsert de los ingresos por cliente en analytics.ingresos_cliente',
    answer: 'Job propuesto con 6 nodos · 1 destino · 2 reglas de calidad',
    bullets: [
      'Genera un Job completo o modifica el que ya está en el lienzo',
      'Funciona fuera del Studio también: pega la referencia en ChatGPT, Claude, Copilot o tu propio agente',
      'Pídele que explique, optimice o corrija justo los problemas que encontró el linter',
      'Cada propuesta se revisa antes de tocar tu trabajo, y deshacer es una tecla',
    ],
    cta: 'Cómo funciona el asistente',
  },
  connectors: {
    eyebrow: 'Conectores',
    title: 'Veintisiete fuentes y destinos.',
    body: 'Cambiar de dónde vienen los datos es cambiar un campo, no reescribir el trabajo. El mismo modelo de nodos cubre lakehouse, archivos, bases operativas, data warehouses, NoSQL y streams.',
    groups: [
      { label: 'Lakehouse', items: ['Delta Lake', 'Iceberg', 'Hudi', 'Parquet', 'ORC', 'Avro'] },
      { label: 'Archivos', items: ['CSV', 'JSON', 'XML', 'Texto', 'Binario'] },
      { label: 'Bases de datos', items: ['PostgreSQL', 'MySQL', 'MariaDB', 'SQL Server', 'Oracle'] },
      { label: 'Data warehouses', items: ['BigQuery', 'Snowflake', 'Redshift'] },
      {
        label: 'NoSQL y búsqueda',
        items: ['MongoDB', 'DocumentDB', 'DynamoDB', 'Cassandra', 'Elasticsearch', 'OpenSearch'],
      },
      { label: 'Streaming y vistas', items: ['Kafka', 'Vistas temporales'] },
    ],
    cta: 'Ver todos los conectores',
  },
  runs: {
    eyebrow: 'Dónde corre',
    title: 'Tu Spark, tus reglas.',
    body: 'Sparquet es una librería, no una plataforma. No hay plano de control, no hay runtime alojado y nada llama a casa.',
    targets: [
      { name: 'Local', body: 'Un portátil con PySpark instalado, para desarrollo y pruebas.' },
      { name: 'Databricks', body: 'Reutiliza la sesión activa; los jobs y cuadernos siguen igual.' },
      { name: 'EMR y Dataproc', body: 'Envíalo como cualquier otra aplicación PySpark.' },
      { name: 'Synapse', body: 'Detectado automáticamente, como cualquier entorno soportado.' },
    ],
  },
  cta: {
    title: 'Estandariza tu primer pipeline en cinco minutos.',
    body: 'Instala el framework, abre el studio y ejecuta un trabajo real antes de que se enfríe el café.',
    primary: 'Empezar el tutorial',
    secondary: 'Estrella en GitHub',
    install: 'pip install sparquet',
  },
}

export const LANDING: Record<Locale, LandingCopy> = { en: EN, pt: PT, es: ES }
