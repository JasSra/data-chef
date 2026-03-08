'use client'

import { Bell, ChevronDown, Search, Sun, Moon } from 'lucide-react'
import { useTheme } from '@/components/ThemeProvider'

const pageMeta: Record<string, { title: string; subtitle: string }> = {
  '/datasets':    { title: 'Datasets',     subtitle: '7 datasets · ~20.2M total records' },
  '/query':       { title: 'Query Editor', subtitle: 'SQL · JSONPath · JMESPath · KQL' },
  '/pipelines':   { title: 'Pipelines',    subtitle: '4 pipelines · 3 active' },
  '/connections': { title: 'Connections',  subtitle: '6 connectors · 5 connected' },
  '/settings':    { title: 'Settings',     subtitle: 'Workspace configuration' },
}

export default function TopBar({ pathname }: { pathname: string }) {
  const { theme, toggle } = useTheme()
  const key  = Object.keys(pageMeta).find(k => pathname.startsWith(k))
  const meta = key ? pageMeta[key] : { title: 'dataChef', subtitle: '' }

  return (
    <header className="h-12 border-b border-chef-border bg-chef-surface flex items-center px-5 gap-3 shrink-0">
      <div className="flex-1 min-w-0">
        <div className="flex items-baseline gap-2">
          <h1 className="text-sm font-semibold text-chef-text">{meta.title}</h1>
          {meta.subtitle && (
            <span className="text-[11px] text-chef-muted hidden sm:block">{meta.subtitle}</span>
          )}
        </div>
      </div>

      {/* Search */}
      <button className="hidden md:flex items-center gap-2 text-xs text-chef-muted hover:text-chef-text border border-chef-border rounded-md px-3 py-1.5 transition-colors hover:border-chef-border bg-chef-card">
        <Search size={12} />
        <span>Search...</span>
        <kbd className="ml-2 text-[10px] text-chef-muted bg-chef-border rounded px-1">⌘K</kbd>
      </button>

      {/* Workspace switcher */}
      <button className="flex items-center gap-1.5 text-[11px] text-chef-muted hover:text-chef-text transition-colors border border-chef-border rounded-md px-2.5 py-1.5 bg-chef-card">
        <div className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
        <span>acme-labs</span>
        <ChevronDown size={11} />
      </button>

      {/* Theme toggle */}
      <button
        onClick={toggle}
        title={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
        className="p-1.5 text-chef-muted hover:text-chef-text transition-colors rounded-lg hover:bg-chef-card"
        aria-label="Toggle theme"
      >
        {theme === 'dark'
          ? <Sun  size={15} className="transition-transform hover:rotate-12" />
          : <Moon size={15} className="transition-transform hover:-rotate-12" />}
      </button>

      {/* Notifications */}
      <button className="relative text-chef-muted hover:text-chef-text transition-colors p-1">
        <Bell size={15} />
        <span className="absolute top-0.5 right-0.5 w-1.5 h-1.5 rounded-full bg-indigo-500" />
      </button>

      {/* Avatar */}
      <div className="w-7 h-7 rounded-full bg-gradient-to-br from-violet-500 to-indigo-600 flex items-center justify-center text-white text-[11px] font-bold cursor-pointer shrink-0">
        JD
      </div>
    </header>
  )
}
