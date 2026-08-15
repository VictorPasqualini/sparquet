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
    fileName: string
    canvasCaption: string
  }
  problem: {
    eyebrow: string
    title: string
    body: string
    points: { title: string; body: string }[]
  }
  how: {
    eyebrow: string
    title: string
    subtitle: string
    steps: { step: string; title: string; body: string }[]
  }
  features: {
    eyebrow: string
    title: string
    subtitle: string
    items: { title: string; body: string }[]
  }
  studio: {
    eyebrow: string
    title: string
    body: string
    bullets: string[]
    cta: string
    imageAlt: string
  }
  connectors: {
    eyebrow: string
    title: string
    body: string
    readWrite: string
    writeOnly: string
    databases: string
    cta: string
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
    title: 'Sparquet — data engineering as JSON',
    description:
      'Describe a Spark pipeline in one JSON file, design it on a canvas, and run it anywhere Spark runs. Open source, MIT licensed.',
  },
  hero: {
    eyebrow: 'Open source data engineering',
    title: 'Your pipeline is a file.',
    titleAccent: 'Now it is also a canvas.',
    subtitle:
      'Sparquet runs Spark pipelines described in JSON — readable, diffable, parameterized. Sparquet Studio turns that same file into a node canvas you can draw, lint and execute, without ever hiding the file underneath.',
    primaryCta: 'Get started',
    secondaryCta: 'Read the docs',
    note: 'MIT licensed · No account · Runs on your infrastructure',
    fileName: 'orders_curated.json',
    canvasCaption: 'The same file, on the Studio canvas',
  },
  problem: {
    eyebrow: 'Why it exists',
    title: 'Pipelines are code nobody wants to write twice.',
    body: 'Every team rewrites the same read, filter, join, validate and write in a new notebook, with new variable names and a new set of mistakes. Sparquet moves the repetition into a schema, so what changes between jobs is data — not code.',
    points: [
      {
        title: 'Declarative, not disposable',
        body: 'A pipeline is a document you can review in a pull request, diff across environments and reuse with parameters. There is no notebook to archaeologize.',
      },
      {
        title: 'Visual without lock-in',
        body: 'The canvas compiles to the same JSON the CLI runs. Nothing is generated behind your back, and deleting Studio does not break a single job.',
      },
      {
        title: 'Guardrails built in',
        body: 'Data quality rules, run conditions and merge semantics are part of the language, not an afterthought bolted on after the first incident.',
      },
    ],
  },
  how: {
    eyebrow: 'How it works',
    title: 'Three steps, one artifact.',
    subtitle:
      'Everything below produces or consumes the same JSON document. Pick the entry point that fits the moment.',
    steps: [
      {
        step: '01',
        title: 'Describe',
        body: 'Write the pipeline as JSON, or draw it in Studio and let the compiler write it. Sources, transformations, validations and destinations are all fields in one file.',
      },
      {
        step: '02',
        title: 'Validate',
        body: 'Studio lints the graph as you type — a merge without keys, a runtime variable nothing publishes, a parameter you never declared. Data quality rules run inside the pipeline itself.',
      },
      {
        step: '03',
        title: 'Run',
        body: 'Execute from Python, from the CLI, or from the canvas through a local runner. The same file runs on your laptop, on Databricks, on EMR.',
      },
    ],
  },
  features: {
    eyebrow: 'What you get',
    title: 'A complete pipeline language.',
    subtitle:
      'Twenty transformations, twelve IO formats and six validators — every one of them documented, typed in the editor, and understood by the assistant.',
    items: [
      {
        title: 'Transformations',
        body: 'filter, select, cast, with_column, struct, group_by, join, union, sort, distinct and more — applied in the order you write them.',
      },
      {
        title: 'Data quality',
        body: 'not_null, unique, range, regex, row_count and custom SQL, with fail / warn / skip policies and a per-rule report written to any destination.',
      },
      {
        title: 'Databases',
        body: 'PostgreSQL, MySQL, SQL Server, Oracle and generic JDBC as sources and destinations, with parallel reads and native upserts.',
      },
      {
        title: 'Lakehouse formats',
        body: 'Parquet, Delta and Iceberg with MERGE upserts and time travel, plus CSV, text, temp views and Kafka publication.',
      },
      {
        title: 'Parameters',
        body: 'One file, many runs: {param} placeholders formatted for SQL, and skip_if_false to switch whole steps on and off per execution.',
      },
      {
        title: 'Runtime pushdown',
        body: 'collect a key set into a {{variable}} and push it into a later read as a literal IN (...) — data skipping without hand-written glue.',
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
  },
  studio: {
    eyebrow: 'Sparquet Studio',
    title: 'The canvas that writes the file.',
    body: 'A browser app that reads and writes exactly the JSON the framework runs. Drag a source, connect transformations, wire a destination — then press ⌘J and read the file you just drew.',
    bullets: [
      'Every field documented in place, with the gotchas that usually live in the source code',
      'Live linting that catches the mistakes Spark only reports after an hour of compute',
      'Run the pipeline from the canvas through a local runner and read counters, validations and a data preview',
      'Projects, templates and lessons stored in your browser — no account, no server, no telemetry',
    ],
    cta: 'Explore Studio',
    imageAlt: 'Sparquet Studio showing a pipeline on the canvas with the inspector open',
  },
  connectors: {
    eyebrow: 'Connectors',
    title: 'Read from anywhere. Write to anywhere.',
    body: 'The same node model covers files, lakehouse tables, streams and operational databases. Swapping a source is a field, not a rewrite.',
    readWrite: 'Read and write',
    writeOnly: 'Write only',
    databases: 'Databases over JDBC',
    cta: 'See all connectors',
  },
  ai: {
    eyebrow: 'AI assistant',
    title: 'Describe the pipeline. Review the diff.',
    body: 'The assistant knows the language because its prompt is generated from the same catalog that drives the editor — it cannot invent a transformation the framework does not have. Bring your own key: requests go straight from your browser to your provider.',
    prompt: 'Read orders from Postgres, keep the confirmed ones, aggregate revenue per customer and upsert it into analytics.customer_revenue',
    answer: 'Proposed a pipeline with 6 nodes · 1 destination',
    bullets: [
      'Generate a complete pipeline, or modify the one on the canvas',
      'Ask it to explain, optimize or fix the issues the linter found',
      'Every proposal is reviewed before it touches your work, and undo is one keystroke',
    ],
    cta: 'How the assistant works',
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
    title: 'Write your first pipeline in five minutes.',
    body: 'Install the framework, open the studio, and run a real job before your coffee gets cold.',
    primary: 'Start the tutorial',
    secondary: 'Star on GitHub',
    install: 'pip install spark-framework',
  },
}

const PT: LandingCopy = {
  meta: {
    title: 'Sparquet — engenharia de dados como JSON',
    description:
      'Descreva um pipeline Spark em um arquivo JSON, desenhe no canvas e execute onde o Spark rodar. Open source, licença MIT.',
  },
  hero: {
    eyebrow: 'Engenharia de dados open source',
    title: 'Seu pipeline é um arquivo.',
    titleAccent: 'Agora também é um canvas.',
    subtitle:
      'O Sparquet executa pipelines Spark descritos em JSON — legíveis, versionáveis, parametrizáveis. O Sparquet Studio transforma esse mesmo arquivo em um canvas de nós para desenhar, validar e executar, sem nunca esconder o arquivo por baixo.',
    primaryCta: 'Começar agora',
    secondaryCta: 'Ver a documentação',
    note: 'Licença MIT · Sem cadastro · Roda na sua infraestrutura',
    fileName: 'pedidos_curados.json',
    canvasCaption: 'O mesmo arquivo, no canvas do Studio',
  },
  problem: {
    eyebrow: 'Por que existe',
    title: 'Pipeline é o código que ninguém quer escrever duas vezes.',
    body: 'Todo time reescreve o mesmo ler, filtrar, juntar, validar e gravar em um notebook novo, com nomes novos e um conjunto novo de erros. O Sparquet move a repetição para um schema: o que muda entre jobs vira dado, não código.',
    points: [
      {
        title: 'Declarativo, não descartável',
        body: 'O pipeline é um documento que você revisa em pull request, compara entre ambientes e reaproveita com parâmetros. Não existe notebook para escavar depois.',
      },
      {
        title: 'Visual sem aprisionamento',
        body: 'O canvas compila para o mesmo JSON que a CLI executa. Nada é gerado escondido, e remover o Studio não quebra um job sequer.',
      },
      {
        title: 'Proteções embutidas',
        body: 'Regras de qualidade, condições de execução e semântica de merge fazem parte da linguagem — não são um remendo depois do primeiro incidente.',
      },
    ],
  },
  how: {
    eyebrow: 'Como funciona',
    title: 'Três passos, um artefato.',
    subtitle:
      'Tudo aqui produz ou consome o mesmo documento JSON. Escolha o ponto de entrada que fizer sentido no momento.',
    steps: [
      {
        step: '01',
        title: 'Descreva',
        body: 'Escreva o pipeline em JSON, ou desenhe no Studio e deixe o compilador escrever. Fontes, transformações, validações e destinos são campos de um único arquivo.',
      },
      {
        step: '02',
        title: 'Valide',
        body: 'O Studio analisa o grafo enquanto você edita — merge sem chaves, variável de runtime que ninguém publica, parâmetro não declarado. As regras de qualidade rodam dentro do próprio pipeline.',
      },
      {
        step: '03',
        title: 'Execute',
        body: 'Rode pelo Python, pela CLI ou direto do canvas com o runner local. O mesmo arquivo roda no seu notebook, no Databricks e no EMR.',
      },
    ],
  },
  features: {
    eyebrow: 'O que você ganha',
    title: 'Uma linguagem de pipeline completa.',
    subtitle:
      'Vinte transformações, doze formatos de IO e seis validadores — todos documentados, tipados no editor e compreendidos pelo assistente.',
    items: [
      {
        title: 'Transformações',
        body: 'filter, select, cast, with_column, struct, group_by, join, union, sort, distinct e mais — aplicadas na ordem em que você escreve.',
      },
      {
        title: 'Qualidade de dados',
        body: 'not_null, unique, range, regex, row_count e SQL customizado, com políticas fail / warn / skip e relatório por regra gravado em qualquer destino.',
      },
      {
        title: 'Bancos de dados',
        body: 'PostgreSQL, MySQL, SQL Server, Oracle e JDBC genérico como fonte e destino, com leitura paralela e upsert nativo.',
      },
      {
        title: 'Formatos de lakehouse',
        body: 'Parquet, Delta e Iceberg com MERGE e time travel, além de CSV, texto, views temporárias e publicação no Kafka.',
      },
      {
        title: 'Parâmetros',
        body: 'Um arquivo, muitas execuções: placeholders {param} formatados para SQL e skip_if_false para ligar e desligar etapas inteiras.',
      },
      {
        title: 'Pushdown em runtime',
        body: 'collect leva um conjunto de chaves para uma {{variável}} e empurra o filtro literal IN (...) na leitura seguinte — data skipping sem gambiarra.',
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
  },
  studio: {
    eyebrow: 'Sparquet Studio',
    title: 'O canvas que escreve o arquivo.',
    body: 'Um app de navegador que lê e escreve exatamente o JSON que o framework executa. Arraste uma fonte, conecte transformações, ligue um destino — e aperte ⌘J para ler o arquivo que você acabou de desenhar.',
    bullets: [
      'Cada campo documentado no lugar, com as pegadinhas que normalmente só existem no código-fonte',
      'Análise ao vivo que pega os erros que o Spark só reporta depois de uma hora de processamento',
      'Execute do canvas pelo runner local e leia contadores, validações e uma prévia dos dados',
      'Projetos, templates e lições guardados no seu navegador — sem cadastro, sem servidor, sem telemetria',
    ],
    cta: 'Conhecer o Studio',
    imageAlt: 'Sparquet Studio com um pipeline no canvas e o inspetor aberto',
  },
  connectors: {
    eyebrow: 'Conectores',
    title: 'Leia de qualquer lugar. Grave em qualquer lugar.',
    body: 'O mesmo modelo de nós cobre arquivos, tabelas de lakehouse, streams e bancos operacionais. Trocar de fonte é mudar um campo, não reescrever o job.',
    readWrite: 'Leitura e escrita',
    writeOnly: 'Somente escrita',
    databases: 'Bancos via JDBC',
    cta: 'Ver todos os conectores',
  },
  ai: {
    eyebrow: 'Assistente de IA',
    title: 'Descreva o pipeline. Revise a proposta.',
    body: 'O assistente conhece a linguagem porque o prompt é gerado do mesmo catálogo que alimenta o editor — ele não inventa transformação que o framework não tem. Use sua própria chave: as requisições vão direto do seu navegador para o seu provedor.',
    prompt: 'Leia pedidos do Postgres, mantenha os confirmados, agregue a receita por cliente e faça upsert em analytics.receita_cliente',
    answer: 'Pipeline proposto com 6 nós · 1 destino',
    bullets: [
      'Gere um pipeline completo ou modifique o que está no canvas',
      'Peça para explicar, otimizar ou corrigir os problemas que o linter encontrou',
      'Toda proposta é revisada antes de tocar seu trabalho, e desfazer é uma tecla',
    ],
    cta: 'Como o assistente funciona',
  },
  runs: {
    eyebrow: 'Onde roda',
    title: 'Seu Spark, suas regras.',
    body: 'O Sparquet é uma biblioteca, não uma plataforma. Não há control plane, não há runtime hospedado e nada liga para casa.',
    targets: [
      { name: 'Local', body: 'Um notebook com PySpark instalado, para desenvolver e testar.' },
      { name: 'Databricks', body: 'Reaproveita a sessão ativa; jobs e notebooks funcionam sem mudança.' },
      { name: 'EMR e Dataproc', body: 'Submeta como qualquer outra aplicação PySpark.' },
      { name: 'Synapse', body: 'Detectado automaticamente, como todo ambiente suportado.' },
    ],
  },
  cta: {
    title: 'Escreva seu primeiro pipeline em cinco minutos.',
    body: 'Instale o framework, abra o studio e rode um job de verdade antes do café esfriar.',
    primary: 'Começar o tutorial',
    secondary: 'Dar uma estrela no GitHub',
    install: 'pip install spark-framework',
  },
}

const ES: LandingCopy = {
  meta: {
    title: 'Sparquet — ingeniería de datos como JSON',
    description:
      'Describe un pipeline de Spark en un archivo JSON, diséñalo en un lienzo y ejecútalo donde corra Spark. Open source, licencia MIT.',
  },
  hero: {
    eyebrow: 'Ingeniería de datos open source',
    title: 'Tu pipeline es un archivo.',
    titleAccent: 'Ahora también es un lienzo.',
    subtitle:
      'Sparquet ejecuta pipelines de Spark descritos en JSON — legibles, versionables, parametrizables. Sparquet Studio convierte ese mismo archivo en un lienzo de nodos para dibujar, validar y ejecutar, sin ocultar nunca el archivo que hay debajo.',
    primaryCta: 'Empezar',
    secondaryCta: 'Ver la documentación',
    note: 'Licencia MIT · Sin cuenta · Corre en tu infraestructura',
    fileName: 'pedidos_curados.json',
    canvasCaption: 'El mismo archivo, en el lienzo del Studio',
  },
  problem: {
    eyebrow: 'Por qué existe',
    title: 'Un pipeline es código que nadie quiere escribir dos veces.',
    body: 'Cada equipo reescribe el mismo leer, filtrar, unir, validar y escribir en un cuaderno nuevo, con nombres nuevos y errores nuevos. Sparquet mueve la repetición a un esquema: lo que cambia entre trabajos son datos, no código.',
    points: [
      {
        title: 'Declarativo, no desechable',
        body: 'El pipeline es un documento que revisas en un pull request, comparas entre entornos y reutilizas con parámetros. No hay cuaderno que excavar después.',
      },
      {
        title: 'Visual sin ataduras',
        body: 'El lienzo compila al mismo JSON que ejecuta la CLI. Nada se genera a tus espaldas, y quitar el Studio no rompe ningún trabajo.',
      },
      {
        title: 'Salvaguardas incluidas',
        body: 'Las reglas de calidad, las condiciones de ejecución y la semántica de merge son parte del lenguaje, no un parche tras el primer incidente.',
      },
    ],
  },
  how: {
    eyebrow: 'Cómo funciona',
    title: 'Tres pasos, un artefacto.',
    subtitle:
      'Todo lo de abajo produce o consume el mismo documento JSON. Elige el punto de entrada que encaje con el momento.',
    steps: [
      {
        step: '01',
        title: 'Describe',
        body: 'Escribe el pipeline en JSON, o dibújalo en el Studio y deja que el compilador lo escriba. Fuentes, transformaciones, validaciones y destinos son campos de un solo archivo.',
      },
      {
        step: '02',
        title: 'Valida',
        body: 'El Studio analiza el grafo mientras escribes — un merge sin claves, una variable de runtime que nadie publica, un parámetro sin declarar. Las reglas de calidad corren dentro del propio pipeline.',
      },
      {
        step: '03',
        title: 'Ejecuta',
        body: 'Lánzalo desde Python, desde la CLI o desde el lienzo con el runner local. El mismo archivo corre en tu portátil, en Databricks y en EMR.',
      },
    ],
  },
  features: {
    eyebrow: 'Lo que obtienes',
    title: 'Un lenguaje de pipelines completo.',
    subtitle:
      'Veinte transformaciones, doce formatos de IO y seis validadores — todos documentados, tipados en el editor y comprendidos por el asistente.',
    items: [
      {
        title: 'Transformaciones',
        body: 'filter, select, cast, with_column, struct, group_by, join, union, sort, distinct y más — aplicadas en el orden en que las escribes.',
      },
      {
        title: 'Calidad de datos',
        body: 'not_null, unique, range, regex, row_count y SQL propio, con políticas fail / warn / skip y un informe por regla escrito en cualquier destino.',
      },
      {
        title: 'Bases de datos',
        body: 'PostgreSQL, MySQL, SQL Server, Oracle y JDBC genérico como fuente y destino, con lecturas paralelas y upserts nativos.',
      },
      {
        title: 'Formatos de lakehouse',
        body: 'Parquet, Delta e Iceberg con MERGE y time travel, además de CSV, texto, vistas temporales y publicación en Kafka.',
      },
      {
        title: 'Parámetros',
        body: 'Un archivo, muchas ejecuciones: marcadores {param} formateados para SQL y skip_if_false para activar o desactivar pasos enteros.',
      },
      {
        title: 'Pushdown en runtime',
        body: 'collect lleva un conjunto de claves a una {{variable}} y empuja el filtro literal IN (...) en la siguiente lectura — data skipping sin pegamento manual.',
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
  },
  studio: {
    eyebrow: 'Sparquet Studio',
    title: 'El lienzo que escribe el archivo.',
    body: 'Una aplicación de navegador que lee y escribe exactamente el JSON que ejecuta el framework. Arrastra una fuente, conecta transformaciones, enlaza un destino — y pulsa ⌘J para leer el archivo que acabas de dibujar.',
    bullets: [
      'Cada campo documentado en su sitio, con los detalles que suelen vivir solo en el código fuente',
      'Análisis en vivo que atrapa los errores que Spark solo reporta tras una hora de cómputo',
      'Ejecuta desde el lienzo con el runner local y lee contadores, validaciones y una vista previa de los datos',
      'Proyectos, plantillas y lecciones guardados en tu navegador — sin cuenta, sin servidor, sin telemetría',
    ],
    cta: 'Explorar el Studio',
    imageAlt: 'Sparquet Studio mostrando un pipeline en el lienzo con el inspector abierto',
  },
  connectors: {
    eyebrow: 'Conectores',
    title: 'Lee desde donde sea. Escribe donde sea.',
    body: 'El mismo modelo de nodos cubre archivos, tablas de lakehouse, streams y bases operativas. Cambiar de fuente es cambiar un campo, no reescribir el trabajo.',
    readWrite: 'Lectura y escritura',
    writeOnly: 'Solo escritura',
    databases: 'Bases de datos por JDBC',
    cta: 'Ver todos los conectores',
  },
  ai: {
    eyebrow: 'Asistente de IA',
    title: 'Describe el pipeline. Revisa la propuesta.',
    body: 'El asistente conoce el lenguaje porque su prompt se genera del mismo catálogo que alimenta el editor — no puede inventar una transformación que el framework no tiene. Usa tu propia clave: las peticiones van directas de tu navegador a tu proveedor.',
    prompt: 'Lee pedidos de Postgres, quédate con los confirmados, agrega los ingresos por cliente y haz upsert en analytics.ingresos_cliente',
    answer: 'Pipeline propuesto con 6 nodos · 1 destino',
    bullets: [
      'Genera un pipeline completo o modifica el que está en el lienzo',
      'Pídele que explique, optimice o corrija los problemas que encontró el linter',
      'Cada propuesta se revisa antes de tocar tu trabajo, y deshacer es una tecla',
    ],
    cta: 'Cómo funciona el asistente',
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
    title: 'Escribe tu primer pipeline en cinco minutos.',
    body: 'Instala el framework, abre el studio y ejecuta un trabajo real antes de que se enfríe el café.',
    primary: 'Empezar el tutorial',
    secondary: 'Estrella en GitHub',
    install: 'pip install spark-framework',
  },
}

export const LANDING: Record<Locale, LandingCopy> = { en: EN, pt: PT, es: ES }
