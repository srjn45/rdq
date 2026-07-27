import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightBlog from 'starlight-blog';

// GitHub Pages project site lives at https://srjn45.github.io/rdq
export default defineConfig({
  site: 'https://srjn45.github.io',
  base: '/rdq/',
  integrations: [
    starlight({
      title: 'rdq',
      description: 'Retry & Dead-letter Queues for any broker, any storage, any language.',
      plugins: [
        starlightBlog({
          title: 'Blog',
          // "Blog" link sits in the header, before the theme switcher.
          navigation: 'header-end',
          // Global authors — reference by key in a post's `authors` frontmatter.
          authors: {
            srjn45: {
              name: 'Srajan Pathak',
              title: 'rdq author',
              url: 'https://github.com/srjn45',
            },
          },
          metrics: { readingTime: true, words: false },
        }),
      ],
      logo: {
        light: './src/assets/rdq-wordmark-light.svg',
        dark: './src/assets/rdq-wordmark-dark.svg',
        replacesTitle: true,
      },
      favicon: '/favicon.svg',
      customCss: ['./src/styles/docs.css'],
      head: [
        { tag: 'meta', attrs: { property: 'og:image', content: 'https://srjn45.github.io/rdq/og-image.svg' } },
        { tag: 'meta', attrs: { name: 'twitter:card', content: 'summary_large_image' } },
      ],
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/srjn45/rdq' },
      ],
      sidebar: [
        { label: 'Start here', items: [
          { label: 'What is rdq?', slug: 'start/what-is-rdq' },
          { label: 'Install', slug: 'start/install' },
          { label: 'Quickstart', slug: 'start/quickstart' },
        ]},
        { label: 'Concepts', items: [
          { label: 'Architecture — one core, two hosts', slug: 'concepts/architecture' },
          { label: 'Tasks, attempts & the lifecycle', slug: 'concepts/task-lifecycle' },
          { label: 'The outcome contract', slug: 'concepts/outcome-contract' },
          { label: 'The wire envelope', slug: 'concepts/wire-envelope' },
          { label: 'Storage SPI & compliance kit', slug: 'concepts/storage-spi' },
        ]},
        { label: 'Guides', items: [
          { label: 'Go SDK', slug: 'guides/go-sdk' },
          { label: 'Java SDK', slug: 'guides/java-sdk' },
          { label: 'Running rdq-server', slug: 'guides/rdq-server' },
          { label: 'Queue configuration & retry policies', slug: 'guides/queue-configuration' },
          { label: 'DLQ analysis & redrive', slug: 'guides/dlq-and-redrive' },
          { label: 'The rdq CLI', slug: 'guides/cli' },
          { label: 'Observability & metrics', slug: 'guides/observability' },
        ]},
        { label: 'Reference', items: [
          { label: 'Server API (REST & gRPC)', slug: 'reference/server-api' },
          { label: 'Configuration', slug: 'reference/configuration' },
          { label: 'Storage backends & sizing', slug: 'reference/storage-backends' },
          { label: 'Roadmap', slug: 'reference/roadmap' },
        ]},
      ],
    }),
  ],
});
