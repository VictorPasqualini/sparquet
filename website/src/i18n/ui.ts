/**
 * Shared chrome strings — header, footer, language switcher.
 *
 * English is the source language; every other locale must define the same keys,
 * which `Ui` enforces at build time.
 */

export const LOCALES = ['en', 'pt', 'es'] as const
export type Locale = (typeof LOCALES)[number]

export const LOCALE_META: Record<Locale, { label: string; htmlLang: string; hreflang: string }> = {
  en: { label: 'English', htmlLang: 'en', hreflang: 'en' },
  pt: { label: 'Português', htmlLang: 'pt-BR', hreflang: 'pt-BR' },
  es: { label: 'Español', htmlLang: 'es', hreflang: 'es' },
}

export const GITHUB_URL = 'https://github.com/VictorPasqualini/sparquet'

/** Landing path for a locale. English is served from the site root. */
export const homeOf = (locale: Locale): string => (locale === 'en' ? '/' : `/${locale}/`)

/** Docs path for a locale, optionally pointing at a page. */
export const docsOf = (locale: Locale, page = ''): string =>
  locale === 'en' ? `/docs/${page}` : `/${locale}/docs/${page}`

interface Ui {
  nav: {
    docs: string
    studio: string
    connectors: string
    reference: string
    github: string
    getStarted: string
    skipToContent: string
    openMenu: string
    language: string
  }
  footer: {
    tagline: string
    product: string
    docs: string
    community: string
    license: string
    builtWith: string
    rights: string
  }
}

export const UI: Record<Locale, Ui> = {
  en: {
    nav: {
      docs: 'Docs',
      studio: 'Studio',
      connectors: 'Connectors',
      reference: 'Reference',
      github: 'GitHub',
      getStarted: 'Get started',
      skipToContent: 'Skip to content',
      openMenu: 'Open menu',
      language: 'Language',
    },
    footer: {
      tagline: 'Data engineering as JSON — designed on a canvas, executed on Spark.',
      product: 'Product',
      docs: 'Documentation',
      community: 'Community',
      license: 'MIT licensed',
      builtWith: 'Built with Astro and Starlight.',
      rights: 'Open source, free forever.',
    },
  },
  pt: {
    nav: {
      docs: 'Documentação',
      studio: 'Studio',
      connectors: 'Conectores',
      reference: 'Referência',
      github: 'GitHub',
      getStarted: 'Começar',
      skipToContent: 'Pular para o conteúdo',
      openMenu: 'Abrir menu',
      language: 'Idioma',
    },
    footer: {
      tagline: 'Engenharia de dados como JSON — desenhada no canvas, executada no Spark.',
      product: 'Produto',
      docs: 'Documentação',
      community: 'Comunidade',
      license: 'Licença MIT',
      builtWith: 'Feito com Astro e Starlight.',
      rights: 'Open source, para sempre.',
    },
  },
  es: {
    nav: {
      docs: 'Documentación',
      studio: 'Studio',
      connectors: 'Conectores',
      reference: 'Referencia',
      github: 'GitHub',
      getStarted: 'Empezar',
      skipToContent: 'Saltar al contenido',
      openMenu: 'Abrir menú',
      language: 'Idioma',
    },
    footer: {
      tagline: 'Ingeniería de datos como JSON — diseñada en un lienzo, ejecutada en Spark.',
      product: 'Producto',
      docs: 'Documentación',
      community: 'Comunidad',
      license: 'Licencia MIT',
      builtWith: 'Hecho con Astro y Starlight.',
      rights: 'Open source, para siempre.',
    },
  },
}
