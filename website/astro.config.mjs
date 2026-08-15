// @ts-check
import sitemap from '@astrojs/sitemap'
import starlight from '@astrojs/starlight'
import tailwind from '@tailwindcss/vite'
import { defineConfig } from 'astro/config'

/**
 * Docs live under `src/content/docs/docs/**`, which Starlight serves at `/docs/*`
 * and `/{locale}/docs/*`. The extra level keeps the marketing pages in
 * `src/pages` from competing with documentation slugs at the site root.
 */
export default defineConfig({
  site: 'https://sparquet.dev',
  integrations: [
    starlight({
      title: 'Sparquet',
      description:
        'Data engineering as JSON. Describe a Spark pipeline in a file, design it on a canvas, run it anywhere Spark runs.',
      logo: {
        src: './src/assets/logo.svg',
        alt: 'Sparquet',
        replacesTitle: false,
      },
      favicon: '/favicon.svg',
      customCss: ['./src/styles/theme.css'],
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/VictorPasqualini/sparquet',
        },
      ],
      editLink: {
        baseUrl:
          'https://github.com/VictorPasqualini/sparquet/edit/main/website/src/content/docs/',
      },
      defaultLocale: 'root',
      locales: {
        root: { label: 'English', lang: 'en' },
        pt: { label: 'Português', lang: 'pt-BR' },
        es: { label: 'Español', lang: 'es' },
      },
      components: {
        SiteTitle: './src/components/docs/SiteTitle.astro',
      },
      sidebar: [
        {
          label: 'Start here',
          translations: { 'pt-BR': 'Comece aqui', es: 'Empieza aquí' },
          items: [{ autogenerate: { directory: 'docs/start' } }],
        },
        {
          label: 'Sparquet Studio',
          items: [{ autogenerate: { directory: 'docs/studio' } }],
        },
        {
          label: 'Pipeline reference',
          translations: {
            'pt-BR': 'Referência do pipeline',
            es: 'Referencia del pipeline',
          },
          items: [{ autogenerate: { directory: 'docs/reference' } }],
        },
        {
          label: 'Guides',
          translations: { 'pt-BR': 'Guias', es: 'Guías' },
          items: [{ autogenerate: { directory: 'docs/guides' } }],
        },
        {
          label: 'Operating',
          translations: { 'pt-BR': 'Operação', es: 'Operación' },
          items: [{ autogenerate: { directory: 'docs/operating' } }],
        },
      ],
      lastUpdated: true,
      pagination: true,
      credits: false,
    }),
    sitemap({
      i18n: {
        defaultLocale: 'en',
        locales: { en: 'en', pt: 'pt-BR', es: 'es' },
      },
    }),
  ],
  vite: {
    plugins: [tailwind()],
  },
})
