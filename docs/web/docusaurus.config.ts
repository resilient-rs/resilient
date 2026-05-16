import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'resilient',
  tagline: 'Composable async resilience for Rust',
  favicon: 'img/icon.png',

  future: {
    v4: true,
  },

  url: 'https://your-docusaurus-site.example.com',
  baseUrl: '/',

  organizationName: 'resilient-rs',
  projectName: 'resilient',

  onBrokenLinks: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl:
            'https://github.com/resilient-rs/resilient/tree/main/docs/web',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/icon.png',
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'resilient',
      logo: {
        alt: 'resilient logo',
        src: 'img/icon.png',
        href: '/docs/',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          href: 'https://github.com/resilient-rs/resilient',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Quickstart',
              to: '/docs/getting-started/quickstart',
            },
            {
              label: 'Retry',
              to: '/docs/policies/retry',
            },
            {
              label: 'Circuit Breaker',
              to: '/docs/policies/circuit-breaker',
            },
            {
              label: 'Rate Limiter',
              to: '/docs/policies/rate-limiter',
            },
            {
              label: 'Timeout',
              to: '/docs/policies/timeout',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/resilient-rs/resilient',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} resilient-rs. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['rust'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
