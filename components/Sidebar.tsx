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
  ShieldCheck,
  Info,
  ExternalLink,
  Compass,
  type LucideIcon,
} from 'lucide-react'
import { useAppSettings } from '@/components/SettingsProvider'

interface NavItem {
  href: string; icon: LucideIcon; label: string
  brandClass?: string; countKey?: 'datasets' | 'connections' | 'apiServices'
}

interface NavGroup { heading: string; items: NavItem[] }

const navGroups: NavGroup[] = [
  {
    heading: 'Data',
    items: [
      { href: '/connections', icon: Plug2,     label: 'Connections', countKey: 'connections' },
      { href: '/datasets',    icon: Database,  label: 'Datasets',    countKey: 'datasets' },
      { href: '/query',       icon: Code2,     label: 'Query' },
    ],
  },
  {
    heading: 'Integrate',
    items: [
      { href: '/api-explorer', icon: Compass,  label: 'API Explorer', brandClass: 'fa-solid fa-plug-circle-bolt', countKey: 'apiServices' },
      { href: '/pipelines',    icon: GitBranch, label: 'Pipelines' },
    ],
  },
  {
    heading: 'System',
    items: [
      { href: '/settings', icon: Settings, label: 'Settings' },
      { href: '/about',    icon: Info,     label: 'About' },
    ],
  },
]

interface WorkerState { active: number; total: number; pct: number }
interface AppInfoState {
  name: string
  version: string
  builtAt: string
  attribution: { parentCompany: string; url: string }
}

export default function Sidebar({ pathname }: { pathname: string }) {
  const [workers, setWorkers] = useState<WorkerState>({ active: 0, total: 5, pct: 0 })
  const [counts, setCounts] = useState({ datasets: 0, connections: 0, apiServices: 0 })
  const [appInfo, setAppInfo] = useState<AppInfoState | null>(null)
  const { settings } = useAppSettings()
  const productName = settings?.branding.productName ?? appInfo?.name ?? 'dataChef'
  const companyLabel = settings?.branding.parentCompanyLabel ?? appInfo?.attribution.parentCompany ?? 'ThreatCo'
  const websiteUrl = settings?.branding.websiteUrl ?? appInfo?.attribution.url ?? 'https://www.threatco.io'
  const logoUrl = settings?.branding.logoUrl ?? ''
  const logoMode = settings?.branding.logoMode ?? 'both'
  const initials = settings?.owner.name
    .split(/\s+/)
    .map(part => part[0]?.toUpperCase())
    .slice(0, 2)
    .join('') || 'DC'

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

  useEffect(() => {
    let cancelled = false

    async function loadCounts() {
      try {
        const [datasetsRes, connectionsRes, apiServicesRes] = await Promise.all([
          fetch('/api/datasets'),
          fetch('/api/connectors'),
          fetch('/api/api-services'),
        ])
        const [datasets, connections, apiServices] = await Promise.all([
          datasetsRes.ok ? datasetsRes.json() : [],
          connectionsRes.ok ? connectionsRes.json() : [],
          apiServicesRes.ok ? apiServicesRes.json() : [],
        ])
        if (!cancelled) {
          setCounts({
            datasets: Array.isArray(datasets) ? datasets.length : 0,
            connections: Array.isArray(connections) ? connections.length : 0,
            apiServices: Array.isArray(apiServices) ? apiServices.length : 0,
          })
        }
      } catch { /* ignore */ }
    }

    void loadCounts()
    const id = setInterval(loadCounts, 5_000)
    return () => { cancelled = true; clearInterval(id) }
  }, [])

  useEffect(() => {
    let cancelled = false

    async function loadAppInfo() {
      try {
        const res = await fetch('/api/app-info')
        const data = res.ok ? await res.json() : null
        if (!cancelled) setAppInfo(data)
      } catch {
        if (!cancelled) setAppInfo(null)
      }
    }

    void loadAppInfo()
  }, [])

  const builtLabel = appInfo
    ? new Date(appInfo.builtAt).toLocaleString('en-AU', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      })
    : null

  return (
    <aside className="w-[220px] shrink-0 flex flex-col bg-chef-surface border-r border-chef-border h-full relative overflow-hidden">
      <div className="absolute inset-x-0 top-0 h-40 bg-[radial-gradient(circle_at_top,rgba(34,211,238,0.18),transparent_68%)] pointer-events-none" />
      {/* Logo */}
      <div className="flex items-center gap-2.5 px-4 h-14 border-b border-chef-border shrink-0 relative">
        {logoMode !== 'wordmark' ? (
          <div className="w-8 h-8 rounded-xl bg-gradient-to-br from-cyan-400 via-sky-500 to-blue-700 flex items-center justify-center shrink-0 shadow-lg shadow-cyan-950/30">
            {logoUrl ? (
              <img src={logoUrl} alt="" className="h-6 w-6 rounded-lg object-cover" />
            ) : (
              <ChefHat size={14} className="text-white" />
            )}
          </div>
        ) : null}
        {logoMode !== 'icon' ? (
          <div className="min-w-0">
            <div className="font-display font-bold text-chef-text text-sm tracking-tight leading-none truncate">{productName}</div>
            <div className="text-[10px] text-cyan-300 font-medium leading-none mt-0.5 truncate">{companyLabel}</div>
          </div>
        ) : null}
      </div>

      {/* Workspace indicator */}
      <div className="mx-3 mt-3 mb-1 px-2.5 py-2 rounded-xl bg-chef-card border border-chef-border flex items-center gap-2">
          <div className="w-2 h-2 rounded-full bg-cyan-400 shrink-0" />
          <div className="min-w-0">
          <div className="text-xs font-medium text-chef-text truncate">{settings?.workspace.workspaceName ?? 'workspace'}</div>
          <div className="text-[10px] text-chef-muted">{settings?.tenant.region ?? settings?.workspace.region ?? 'region'} data residency</div>
        </div>
      </div>

      {/* Nav */}
      <nav className="flex-1 py-2 px-2 overflow-y-auto">
        {navGroups.map((group, gi) => (
          <div key={group.heading}>
            <div className={`text-[10px] font-semibold text-chef-muted uppercase tracking-widest px-2 mb-1.5 ${gi === 0 ? 'mt-1' : 'mt-3'}`}>
              {group.heading}
            </div>
            {group.items.map(({ href, icon: Icon, brandClass, label, countKey }) => {
              const isActive = pathname.startsWith(href)
              const badge = countKey ? counts[countKey] : null
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
                  {brandClass ? (
                    <i className={`${brandClass} shrink-0 transition-colors ${isActive ? 'text-indigo-400' : 'text-chef-muted group-hover:text-chef-muted-bright'}`} style={{ fontSize: '14px', width: '15px', textAlign: 'center' }} />
                  ) : (
                    <Icon
                      size={15}
                      className={`shrink-0 transition-colors ${isActive ? 'text-indigo-400' : 'text-chef-muted group-hover:text-chef-muted-bright'}`}
                    />
                  )}
                  <span className="flex-1 font-medium">{label}</span>
                  {badge != null && (
                    <span className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${
                      isActive ? 'bg-indigo-500/20 text-indigo-400' : 'bg-chef-border text-chef-muted'
                    }`}>
                      {badge}
                    </span>
                  )}
                </Link>
              )
            })}
          </div>
        ))}

        {/* Worker capacity widget */}
        <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest px-2 mb-1.5 mt-4">
          Workspace
        </div>
        <div className="px-2.5 py-2 rounded-xl bg-cyan-500/5 border border-cyan-500/10">
          <div className="flex items-center gap-2 text-xs text-chef-muted">
            <Zap size={11} className={`shrink-0 ${workers.active > 0 ? 'text-cyan-400' : 'text-chef-muted'}`} />
            <span>
              {workers.active === 0
                ? 'No active jobs'
                : `${workers.active} job${workers.active !== 1 ? 's' : ''} running`}
            </span>
          </div>
          <div className="mt-1.5 h-1 rounded-full bg-chef-border overflow-hidden">
            <div
              className="h-full rounded-full bg-gradient-to-r from-cyan-500 to-blue-600 transition-all duration-700"
              style={{ width: `${workers.pct}%` }}
            />
          </div>
          <div className="mt-1 text-[10px] text-chef-muted">
            {workers.pct}% worker capacity · {workers.active}/{workers.total}
          </div>
        </div>
      </nav>

      {/* User */}
      <div className="px-3 py-3 border-t border-chef-border shrink-0 space-y-3">
        <div className="flex items-center gap-2.5">
          <div className="w-7 h-7 rounded-full bg-gradient-to-br from-cyan-500 to-blue-700 flex items-center justify-center text-white text-[11px] font-bold shrink-0">
            {initials}
          </div>
          <div className="min-w-0 flex-1">
            <div className="text-xs font-semibold text-chef-text truncate">{settings?.owner.name ?? 'Workspace owner'}</div>
            <div className="text-[10px] text-chef-muted">{settings?.owner.role ?? 'Owner'}</div>
          </div>
        </div>
        <div className="rounded-xl border border-chef-border bg-chef-card/60 p-2.5">
          <div className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-widest text-cyan-300">
            <ShieldCheck size={11} />
            <span>{companyLabel}</span>
          </div>
          <a
            href={websiteUrl}
            target="_blank"
            rel="noreferrer"
            className="mt-2 flex items-center gap-1 text-[11px] text-chef-text hover:text-cyan-300 transition-colors"
          >
            <span>{websiteUrl.replace(/^https?:\/\//, '')}</span>
            <ExternalLink size={11} />
          </a>
          {appInfo && (
            <div className="mt-2 space-y-0.5 text-[10px] text-chef-muted font-mono">
              <div>v{appInfo.version}</div>
              {builtLabel && <div>built {builtLabel}</div>}
            </div>
          )}
        </div>
      </div>
    </aside>
  )
}
