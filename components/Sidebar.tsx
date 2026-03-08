'use client'

import Link from 'next/link'
import { useEffect, useState } from 'react'
import {
  Database,
  Code2,
  GitBranch,
  Plug2,
  Settings,
  ChefHat,
  Zap,
} from 'lucide-react'

const navItems = [
  { href: '/datasets',    icon: Database,  label: 'Datasets',    badge: '7' },
  { href: '/query',       icon: Code2,     label: 'Query' },
  { href: '/pipelines',   icon: GitBranch, label: 'Pipelines',   badge: '4' },
  { href: '/connections', icon: Plug2,     label: 'Connections', badge: '6' },
  { href: '/settings',    icon: Settings,  label: 'Settings' },
]

interface WorkerState { active: number; total: number; pct: number }

export default function Sidebar({ pathname }: { pathname: string }) {
  const [workers, setWorkers] = useState<WorkerState>({ active: 0, total: 5, pct: 0 })

  useEffect(() => {
    let cancelled = false

    async function poll() {
      try {
        const res = await fetch('/api/workers')
        if (!cancelled && res.ok) setWorkers(await res.json())
      } catch { /* ignore — keep showing last known state */ }
    }

    poll()
    const id = setInterval(poll, 3_000)
    return () => { cancelled = true; clearInterval(id) }
  }, [])

  return (
    <aside className="w-[200px] shrink-0 flex flex-col bg-chef-surface border-r border-chef-border h-full">
      {/* Logo */}
      <div className="flex items-center gap-2.5 px-4 h-12 border-b border-chef-border shrink-0">
        <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-indigo-500 to-violet-600 flex items-center justify-center shrink-0 shadow-lg shadow-indigo-900/40">
          <ChefHat size={14} className="text-white" />
        </div>
        <div className="min-w-0">
          <div className="font-bold text-chef-text text-sm tracking-tight leading-none">dataChef</div>
          <div className="text-[10px] text-chef-muted font-mono leading-none mt-0.5">v0.1.0-alpha</div>
        </div>
      </div>

      {/* Workspace indicator */}
      <div className="mx-3 mt-3 mb-1 px-2.5 py-2 rounded-lg bg-chef-card border border-chef-border flex items-center gap-2">
        <div className="w-2 h-2 rounded-full bg-emerald-500 shrink-0" />
        <div className="min-w-0">
          <div className="text-xs font-medium text-chef-text truncate">acme-labs</div>
          <div className="text-[10px] text-chef-muted">ap-southeast-2</div>
        </div>
      </div>

      {/* Nav */}
      <nav className="flex-1 py-2 px-2 overflow-y-auto">
        <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest px-2 mb-1.5 mt-1">
          Main
        </div>
        {navItems.map(({ href, icon: Icon, label, badge }) => {
          const isActive = pathname.startsWith(href)
          return (
            <Link
              key={href}
              href={href}
              className={`group flex items-center gap-2.5 px-2.5 py-2 rounded-lg mb-0.5 text-sm transition-all duration-150 ${
                isActive
                  ? 'bg-indigo-500/10 text-indigo-400'
                  : 'text-chef-muted hover:text-chef-text hover:bg-white/[0.04]'
              }`}
            >
              <Icon
                size={15}
                className={`shrink-0 transition-colors ${isActive ? 'text-indigo-400' : 'text-chef-muted group-hover:text-chef-muted-bright'}`}
              />
              <span className="flex-1 font-medium">{label}</span>
              {badge && (
                <span className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${
                  isActive ? 'bg-indigo-500/20 text-indigo-400' : 'bg-chef-border text-chef-muted'
                }`}>
                  {badge}
                </span>
              )}
            </Link>
          )
        })}

        {/* Worker capacity widget */}
        <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest px-2 mb-1.5 mt-4">
          Workspace
        </div>
        <div className="px-2.5 py-2 rounded-lg bg-indigo-500/5 border border-indigo-500/10">
          <div className="flex items-center gap-2 text-xs text-chef-muted">
            <Zap size={11} className={`shrink-0 ${workers.active > 0 ? 'text-indigo-400' : 'text-chef-muted'}`} />
            <span>
              {workers.active === 0
                ? 'No active jobs'
                : `${workers.active} job${workers.active !== 1 ? 's' : ''} running`}
            </span>
          </div>
          <div className="mt-1.5 h-1 rounded-full bg-chef-border overflow-hidden">
            <div
              className="h-full rounded-full bg-gradient-to-r from-indigo-600 to-violet-500 transition-all duration-700"
              style={{ width: `${workers.pct}%` }}
            />
          </div>
          <div className="mt-1 text-[10px] text-chef-muted">
            {workers.pct}% worker capacity · {workers.active}/{workers.total}
          </div>
        </div>
      </nav>

      {/* User */}
      <div className="px-3 py-3 border-t border-chef-border shrink-0">
        <div className="flex items-center gap-2.5">
          <div className="w-7 h-7 rounded-full bg-gradient-to-br from-violet-500 to-indigo-600 flex items-center justify-center text-white text-[11px] font-bold shrink-0">
            JD
          </div>
          <div className="min-w-0 flex-1">
            <div className="text-xs font-semibold text-chef-text truncate">Jane Doe</div>
            <div className="text-[10px] text-chef-muted">Data Builder</div>
          </div>
        </div>
      </div>
    </aside>
  )
}
