import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'index',
    {
      type: 'category',
      label: 'Getting Started',
      items: [
        'getting-started/installation',
        'getting-started/quickstart',
      ],
    },
    {
      type: 'category',
      label: 'Core Concepts',
      items: [
        'core-concepts/policy-trait',
        'core-concepts/pipeline',
        'core-concepts/error-handling',
      ],
    },
    {
      type: 'category',
      label: 'Policies',
      items: [
        'policies/retry',
        'policies/timeout',
        'policies/circuit-breaker',
        'policies/bulkhead',
        'policies/rate-limiter',
      ],
    },
    {
      type: 'category',
      label: 'Advanced',
      items: [
        'advanced/fallback',
        'advanced/custom-policies',
        'advanced/best-practices',
      ],
    },
  ],
};

export default sidebars;
