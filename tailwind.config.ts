import type { Config } from 'tailwindcss'

const config: Config = {
  content: [
    './pages/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
    './app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['var(--font-inter)', 'system-ui', 'sans-serif'],
        mono: ['var(--font-mono)', 'Menlo', 'monospace'],
      },
      colors: {
        // All chef-* colors use CSS variables so dark/light themes work
        // automatically without changing any component code.
        chef: {
          bg:             'rgb(var(--chef-bg) / <alpha-value>)',
          surface:        'rgb(var(--chef-surface) / <alpha-value>)',
          card:           'rgb(var(--chef-card) / <alpha-value>)',
          'card-hover':   'rgb(var(--chef-card-hover) / <alpha-value>)',
          border:         'rgb(var(--chef-border) / <alpha-value>)',
          'border-dim':   'rgb(var(--chef-border-dim) / <alpha-value>)',
          text:           'rgb(var(--chef-text) / <alpha-value>)',
          'text-dim':     'rgb(var(--chef-text-dim) / <alpha-value>)',
          muted:          'rgb(var(--chef-muted) / <alpha-value>)',
          'muted-bright': 'rgb(var(--chef-muted-bright) / <alpha-value>)',
        },
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'fade-in':    'fadeIn 0.2s ease-out',
        'slide-in':   'slideIn 0.25s ease-out',
      },
      keyframes: {
        fadeIn: {
          '0%':   { opacity: '0' },
          '100%': { opacity: '1' },
        },
        slideIn: {
          '0%':   { opacity: '0', transform: 'translateX(8px)' },
          '100%': { opacity: '1', transform: 'translateX(0)' },
        },
      },
    },
  },
  plugins: [],
}

export default config
