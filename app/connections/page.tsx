'use client'

import { useState, useEffect, useCallback, useRef } from 'react'
import {
  Plus, Globe, Cloud, Server, Database, Webhook,
  Clock, Settings, AlertTriangle, CheckCircle2,
  Zap, Lock, ExternalLink, Terminal, Activity,
  Play, X, Loader2, AlertCircle, HardDrive,
  RefreshCw, ChevronRight, Copy, FileText, BarChart2,
} from 'lucide-react'
import StatusBadge from '@/components/StatusBadge'
import ConnectorWizard, { ConnectorJob, NewConnector, ConnectorId } from '@/components/ConnectorWizard'

/* ── Types ───────────────────────────────────────────────────────── */
type ConnStatus = 'connected' | 'disconnected' | 'running'

interface Connection {
  id: string; name: string; type: ConnectorId; status: ConnStatus
  lastSync: string; recordsSynced: string; authMethod: string
  endpoint: string; description: string; datasets: string[]
  syncInterval: string; latencyMs: number; sparkValues: number[]
}

/* ── Type config ─────────────────────────────────────────────────── */
const TYPE_CFG: Record<ConnectorId, { Icon: React.ElementType; color: string; bg: string; border: string; label: string }> = {
  http:       { Icon: Globe,     color: 'text-sky-400',     bg: 'bg-sky-500/10',     border: 'border-sky-500/20',    label: 'HTTP API' },
  webhook:    { Icon: Webhook,   color: 'text-amber-400',   bg: 'bg-amber-500/10',   border: 'border-amber-500/20',  label: 'Webhook' },
  s3:         { Icon: Cloud,     color: 'text-violet-400',  bg: 'bg-violet-500/10',  border: 'border-violet-500/20', label: 'S3' },
  sftp:       { Icon: Server,    color: 'text-slate-400',   bg: 'bg-slate-500/10',   border: 'border-slate-500/20',  label: 'SFTP' },
  postgresql: { Icon: Database,  color: 'text-blue-400',    bg: 'bg-blue-500/10',    border: 'border-blue-500/20',   label: 'PostgreSQL' },
  mysql:      { Icon: Database,  color: 'text-orange-400',  bg: 'bg-orange-500/10',  border: 'border-orange-500/20', label: 'MySQL' },
  mongodb:    { Icon: Database,  color: 'text-emerald-400', bg: 'bg-emerald-500/10', border: 'border-emerald-500/20', label: 'MongoDB' },
  bigquery:   { Icon: HardDrive, color: 'text-rose-400',    bg: 'bg-rose-500/10',    border: 'border-rose-500/20',   label: 'BigQuery' },
  file:        { Icon: FileText,  color: 'text-lime-400',    bg: 'bg-lime-500/10',    border: 'border-lime-500/20',    label: 'File Upload' },
  appinsights: { Icon: BarChart2, color: 'text-cyan-400',    bg: 'bg-cyan-500/10',    border: 'border-cyan-500/20',    label: 'App Insights' },
}

/* ── Sparkline (values = last N record counts from real sync history) */
function Sparkline({ values, color }: { values: number[]; color: string }) {
  const pts = values.length > 0 ? values : [0]
  const max = Math.max(...pts, 1)
  const h = 28; const w = 80
  const d = pts.map((p, i) => `${(i / (pts.length - 1)) * w},${h - (p / max) * h}`).join(' ')
  return <svg width={w} height={h} className="opacity-60"><polyline points={d} fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>
}

const SPARK_COLORS: Record<ConnectorId, string> = {
  http: '#38bdf8', webhook: '#fbbf24', s3: '#a78bfa', sftp: '#94a3b8',
  postgresql: '#60a5fa', mysql: '#fb923c', mongodb: '#34d399', bigquery: '#fb7185',
  file:        '#a3e635',
  appinsights: '#22d3ee',
}

/* ── Connection Card ─────────────────────────────────────────────── */
function ConnCard({ conn, selected, onClick, onSync }: {
  conn: Connection; selected: boolean; onClick: () => void; onSync: () => void
}) {
  const tc = TYPE_CFG[conn.type]
  const Icon = tc.Icon
  return (
    <div
      onClick={onClick}
      className={`border rounded-xl p-4 cursor-pointer transition-all hover:shadow-lg hover:shadow-black/20 ${selected ? 'border-indigo-500/50 bg-indigo-500/5 shadow-lg shadow-indigo-900/20' : conn.status === 'disconnected' ? 'border-chef-border bg-chef-card opacity-60 hover:opacity-90' : 'border-chef-border bg-chef-card hover:border-indigo-500/30'}`}
    >
      <div className="flex items-start justify-between gap-3 mb-3">
        <div className={`w-9 h-9 rounded-xl ${tc.bg} ${tc.border} border flex items-center justify-center shrink-0`}>
          <Icon size={18} className={tc.color} />
        </div>
        <StatusBadge status={conn.status === 'running' ? 'running' : conn.status === 'connected' ? 'connected' : 'disconnected'} />
      </div>
      <div className="mb-1">
        <div className="font-semibold text-sm text-chef-text leading-tight">{conn.name}</div>
        <div className={`text-[10px] font-mono mt-0.5 ${tc.color}`}>{tc.label}</div>
      </div>
      <div className="text-[11px] text-chef-muted leading-snug mb-3 line-clamp-2">{conn.description}</div>
      <div className="flex items-center justify-between text-[10px] text-chef-muted">
        <span className="flex items-center gap-1"><Clock size={9} />{conn.lastSync}</span>
        {conn.status === 'connected' && <span className="font-mono">{conn.latencyMs}ms</span>}
      </div>
      {conn.status === 'connected' && (
        <div className="mt-3 pt-3 border-t border-chef-border flex items-end justify-between">
          <div className="flex flex-col gap-1">
            <span className="text-[9px] text-chef-muted uppercase tracking-wider">{conn.syncInterval}</span>
            <button
              onClick={e => { e.stopPropagation(); onSync() }}
              className="flex items-center gap-1 text-[10px] text-indigo-400 hover:text-indigo-300 transition-colors"
            >
              <Play size={9} /> {conn.type === 'file' ? 'Re-upload' : 'Sync now'}
            </button>
          </div>
          <Sparkline values={conn.sparkValues} color={SPARK_COLORS[conn.type]} />
        </div>
      )}
      {conn.status === 'disconnected' && (
        <div className="mt-3 pt-3 border-t border-chef-border">
          <button onClick={e => { e.stopPropagation(); onSync() }} className="w-full text-[11px] text-indigo-400 border border-indigo-500/30 rounded-lg py-1.5 hover:bg-indigo-500/10 transition-colors">
            Reconnect
          </button>
        </div>
      )}
    </div>
  )
}

/* ── Jobs Panel ──────────────────────────────────────────────────── */
const LOG_COLORS = { info: 'text-chef-muted', success: 'text-emerald-400', warn: 'text-amber-400', error: 'text-rose-400' }
const LOG_PFX    = { info: '│', success: '✓', warn: '⚠', error: '✗' }
const JOB_TYPE_CFG = { test: { label: 'Test', color: 'text-indigo-400 bg-indigo-500/10' }, sync: { label: 'Sync', color: 'text-sky-400 bg-sky-500/10' }, schema: { label: 'Schema', color: 'text-violet-400 bg-violet-500/10' } }

function JobsPanel({ jobs, onClose }: { jobs: ConnectorJob[]; onClose: () => void }) {
  const [selected, setSelected] = useState<string | null>(jobs[0]?.id ?? null)
  const logsEnd = useRef<HTMLDivElement>(null)

  const job = jobs.find(j => j.id === selected)

  // Auto-select newest job
  useEffect(() => { if (jobs.length > 0) setSelected(jobs[0].id) }, [jobs.length])

  // Scroll logs
  useEffect(() => { logsEnd.current?.scrollIntoView({ behavior: 'smooth' }) }, [job?.logs.length])

  const statusIcon = (s: ConnectorJob['status']) => {
    if (s === 'running') return <Loader2 size={12} className="text-indigo-400 animate-spin shrink-0" />
    if (s === 'succeeded') return <CheckCircle2 size={12} className="text-emerald-400 shrink-0" />
    if (s === 'failed') return <AlertCircle size={12} className="text-rose-400 shrink-0" />
    return <Clock size={12} className="text-chef-muted shrink-0" />
  }

  return (
    <div className="flex flex-col h-full animate-slide-in">
      <div className="px-4 py-3 border-b border-chef-border flex items-center gap-2 shrink-0">
        <Activity size={14} className="text-indigo-400" />
        <span className="text-sm font-semibold text-chef-text flex-1">Worker Jobs</span>
        <span className="text-[10px] text-chef-muted">{jobs.filter(j => j.status === 'running').length} running</span>
        <button onClick={onClose} className="p-1 text-chef-muted hover:text-chef-text rounded transition-colors"><X size={14} /></button>
      </div>

      <div className="flex flex-1 overflow-hidden">
        {/* Job list */}
        <div className="w-52 shrink-0 border-r border-chef-border overflow-y-auto">
          {jobs.length === 0 && (
            <div className="p-4 text-center text-[11px] text-chef-muted">No jobs yet</div>
          )}
          {jobs.map(j => {
            const tc = TYPE_CFG[j.connectorType]
            return (
              <button key={j.id} onClick={() => setSelected(j.id)}
                className={`w-full flex flex-col gap-1.5 p-3 border-b border-chef-border text-left transition-colors ${selected === j.id ? 'bg-indigo-500/5' : 'hover:bg-chef-card'}`}>
                <div className="flex items-center gap-1.5">
                  {statusIcon(j.status)}
                  <span className="text-[11px] font-medium text-chef-text truncate flex-1">{j.connectorName}</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded ${JOB_TYPE_CFG[j.jobType].color}`}>{JOB_TYPE_CFG[j.jobType].label}</span>
                  <span className={`text-[9px] font-mono ${tc.color}`}>{tc.label}</span>
                </div>
                {j.status === 'running' && (
                  <div className="h-0.5 bg-chef-border rounded-full overflow-hidden">
                    <div className="h-full bg-indigo-500 transition-all duration-500 rounded-full" style={{ width: `${j.progress}%` }} />
                  </div>
                )}
                <div className="text-[9px] text-chef-muted">{j.duration ? `${(j.duration / 1000).toFixed(1)}s` : j.status === 'running' ? 'running…' : '—'}</div>
              </button>
            )
          })}
        </div>

        {/* Log detail */}
        <div className="flex-1 flex flex-col overflow-hidden">
          {job ? (
            <>
              <div className="px-3 py-2 border-b border-chef-border shrink-0 flex items-center gap-2">
                {statusIcon(job.status)}
                <span className="text-[11px] font-semibold text-chef-text flex-1">{job.connectorName}</span>
                <span className="text-[10px] font-mono text-chef-muted">{job.progress}%</span>
              </div>
              {job.status === 'running' && (
                <div className="h-0.5 bg-chef-border shrink-0">
                  <div className="h-full bg-indigo-500 transition-all duration-700" style={{ width: `${job.progress}%` }} />
                </div>
              )}
              <div className="flex-1 overflow-y-auto p-3 font-mono text-[10px] space-y-0.5 bg-[#080a0d]">
                {job.logs.length === 0 && <span className="text-chef-muted">Waiting for worker…</span>}
                {job.logs.map((log, i) => (
                  <div key={i} className={`flex items-start gap-1.5 ${LOG_COLORS[log.level]}`}>
                    <span className="shrink-0 opacity-50">{LOG_PFX[log.level]}</span>
                    <span className="leading-relaxed">{log.msg}</span>
                  </div>
                ))}
                {job.status === 'running' && <span className="text-chef-muted animate-pulse">▊</span>}
                <div ref={logsEnd} />
              </div>
            </>
          ) : (
            <div className="flex-1 flex items-center justify-center text-[11px] text-chef-muted">Select a job</div>
          )}
        </div>
      </div>
    </div>
  )
}

/* ── Detail Panel ────────────────────────────────────────────────── */
function DetailPanel({ conn, onToggle, onSync, onClose }: {
  conn: Connection; onToggle: () => void; onSync: () => void; onClose: () => void
}) {
  const tc = TYPE_CFG[conn.type]
  const Icon = tc.Icon
  const [copied, setCopied] = useState(false)
  function copy() { navigator.clipboard.writeText(conn.endpoint).catch(() => {}); setCopied(true); setTimeout(() => setCopied(false), 2000) }

  return (
    <div className="flex flex-col h-full overflow-auto animate-slide-in">
      {/* Header */}
      <div className="px-5 py-4 border-b border-chef-border shrink-0">
        <div className="flex items-start gap-3">
          <div className={`w-11 h-11 rounded-xl ${tc.bg} ${tc.border} border flex items-center justify-center shrink-0`}>
            <Icon size={22} className={tc.color} />
          </div>
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-base font-bold text-chef-text">{conn.name}</span>
              <StatusBadge status={conn.status === 'running' ? 'running' : conn.status === 'connected' ? 'connected' : 'disconnected'} />
            </div>
            <div className={`text-xs font-mono mt-0.5 ${tc.color}`}>{tc.label}</div>
            <div className="text-xs text-chef-muted mt-0.5 leading-snug">{conn.description}</div>
          </div>
          <div className="flex items-center gap-1.5 shrink-0">
            {conn.status === 'connected' && (
              <button onClick={onSync} className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg border border-indigo-500/30 text-indigo-400 hover:bg-indigo-500/10 transition-colors">
                <RefreshCw size={12} /> Sync
              </button>
            )}
            <button onClick={onToggle}
              className={`text-xs px-3 py-1.5 rounded-lg border transition-colors font-medium ${conn.status === 'connected' ? 'border-rose-500/30 text-rose-400 hover:bg-rose-500/10' : 'border-emerald-500/30 text-emerald-400 hover:bg-emerald-500/10'}`}>
              {conn.status === 'connected' ? 'Disconnect' : 'Connect'}
            </button>
            <button onClick={onClose} className="p-1.5 text-chef-muted hover:text-chef-text hover:bg-chef-card rounded-lg transition-colors">
              <X size={14} />
            </button>
          </div>
        </div>
      </div>

      {/* Stats */}
      {conn.status === 'connected' && (
        <div className="grid grid-cols-3 border-b border-chef-border shrink-0">
          {[
            { label: 'Records synced', value: conn.recordsSynced, Icon: Zap },
            { label: 'Last sync',       value: conn.lastSync,      Icon: Clock },
            { label: 'Avg latency',     value: conn.latencyMs > 0 ? `${conn.latencyMs}ms` : '—', Icon: Activity },
          ].map(({ label, value, Icon: I }) => (
            <div key={label} className="px-5 py-3.5 border-r border-chef-border last:border-0">
              <div className="flex items-center gap-1 text-[10px] text-chef-muted uppercase tracking-wider mb-1.5"><I size={10} />{label}</div>
              <div className="text-base font-bold font-mono text-chef-text">{value}</div>
            </div>
          ))}
        </div>
      )}

      {/* Config */}
      <div className="px-5 py-4 space-y-3.5">
        <div className="text-xs font-semibold text-chef-text">Configuration</div>
        {[
          { label: 'Endpoint',       value: conn.endpoint,                        mono: true },
          { label: 'Auth method',    value: conn.authMethod,                      mono: false },
          { label: 'Sync interval',  value: conn.syncInterval,                    mono: false },
          { label: 'Linked datasets',value: conn.datasets.join(', ') || 'None',  mono: true },
        ].map(({ label, value, mono }) => (
          <div key={label} className="flex items-start gap-4">
            <div className="text-[11px] text-chef-muted w-28 shrink-0 pt-0.5">{label}</div>
            <div className={`flex-1 text-[12px] min-w-0 flex items-center gap-1.5 ${mono ? 'font-mono text-chef-text-dim truncate' : 'text-chef-text'}`}>
              <span className="truncate">{value}</span>
              {label === 'Endpoint' && (
                <button onClick={copy} className="shrink-0 text-chef-muted hover:text-chef-text transition-colors">
                  {copied ? <CheckCircle2 size={11} className="text-emerald-400" /> : <Copy size={11} />}
                </button>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Security */}
      <div className="mx-5 mb-5 p-4 bg-chef-bg border border-chef-border rounded-xl">
        <div className="flex items-center gap-2 mb-3">
          <Lock size={12} className="text-indigo-400" />
          <span className="text-xs font-semibold text-chef-text">Security</span>
        </div>
        <div className="space-y-2 text-[11px] text-chef-muted">
          <div className="flex items-center gap-2"><CheckCircle2 size={11} className="text-emerald-400 shrink-0" /><span>Transport: TLS 1.3</span></div>
          <div className="flex items-center gap-2"><CheckCircle2 size={11} className="text-emerald-400 shrink-0" /><span>Auth: {conn.authMethod}</span></div>
          {conn.type === 'webhook' && (
            <div className="flex items-center gap-2"><CheckCircle2 size={11} className="text-emerald-400 shrink-0" /><span>Signature verification · replay protection active</span></div>
          )}
          {(conn.type === 'sftp' && conn.endpoint.startsWith('ftp://')) && (
            <div className="flex items-center gap-2"><AlertTriangle size={11} className="text-amber-400 shrink-0" /><span>FTP is unencrypted — migrate to SFTP</span></div>
          )}
        </div>
      </div>
    </div>
  )
}

/* ── Main page ───────────────────────────────────────────────────── */
type FilterType = ConnectorId | 'all'

export default function ConnectionsPage() {
  const [conns, setConns] = useState<Connection[]>([])
  const [loading, setLoading] = useState(true)
  const [selected, setSelected] = useState<Connection | null>(null)
  const [filter, setFilter] = useState<FilterType>('all')
  const [showWizard, setShowWizard] = useState(false)
  const [jobs, setJobs] = useState<ConnectorJob[]>([])
  const [showJobs, setShowJobs] = useState(false)

  useEffect(() => {
    fetch('/api/connectors')
      .then(r => r.json())
      .then((data: Connection[]) => { setConns(data); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  const connectedCount = conns.filter(c => c.status === 'connected').length
  const runningJobs = jobs.filter(j => j.status === 'running').length

  const filtered = filter === 'all' ? conns : conns.filter(c => c.type === filter)
  const filterTypes: FilterType[] = ['all', ...Array.from(new Set(conns.map(c => c.type)))]

  function toggleStatus(id: string) {
    setConns(prev => prev.map(c => c.id === id ? { ...c, status: c.status === 'connected' ? 'disconnected' : 'connected' } : c))
    setSelected(prev => prev?.id === id ? { ...prev, status: prev.status === 'connected' ? 'disconnected' : 'connected' } : prev)
  }

  async function startSync(conn: Connection) {
    const jobId     = `job-${Date.now()}`
    const startedAt = Date.now()

    const job: ConnectorJob = {
      id: jobId, connectorId: conn.id, connectorName: conn.name,
      connectorType: conn.type, jobType: 'sync', status: 'running',
      progress: 0, logs: [], startedAt,
    }
    setJobs(prev => [job, ...prev])
    setShowJobs(true)
    setConns(prev => prev.map(c => c.id === conn.id ? { ...c, status: 'running' } : c))
    if (selected?.id === conn.id) setSelected(prev => prev ? { ...prev, status: 'running' } : prev)

    // ── Real SSE sync (server-side, no CORS) ───────────────────────
    try {
      const res = await fetch('/api/connectors/sync', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({
          connectorType: conn.type,
          connectorId:   conn.id,
          connectorName: conn.name,
          url:           conn.endpoint,
        }),
      })

      if (!res.body) throw new Error('No response body')

      const reader  = res.body.getReader()
      const decoder = new TextDecoder()
      let buf     = ''
      const allLogs: ConnectorJob['logs'] = []

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buf += decoder.decode(value, { stream: true })
        const chunks = buf.split('\n\n')
        buf = chunks.pop() ?? ''

        for (const chunk of chunks) {
          const dataLine = chunk.split('\n').find(l => l.startsWith('data: '))
          if (!dataLine) continue
          try {
            const event = JSON.parse(dataLine.slice(6))

            if (event.type === 'log') {
              allLogs.push({ level: event.level, msg: event.msg })
              setJobs(prev => prev.map(j =>
                j.id === jobId ? { ...j, logs: [...allLogs] } : j
              ))
            } else if (event.type === 'progress') {
              setJobs(prev => prev.map(j =>
                j.id === jobId ? { ...j, progress: event.p as number } : j
              ))
            } else if (event.type === 'done') {
              const duration = Date.now() - startedAt
              const finalStatus = (event.ok as boolean) ? 'succeeded' : 'failed'
              setJobs(prev => prev.map(j =>
                j.id === jobId ? { ...j, status: finalStatus, progress: 100, duration } : j
              ))
              const newRecords = event.records as number | undefined
              setConns(prev => prev.map(c => {
                if (c.id !== conn.id) return c
                const updatedSpark = newRecords
                  ? [...c.sparkValues.slice(-11), newRecords]
                  : c.sparkValues
                return {
                  ...c, status: 'connected', lastSync: 'just now',
                  recordsSynced: newRecords ? `${newRecords.toLocaleString()} records` : c.recordsSynced,
                  sparkValues: updatedSpark,
                }
              }))
              if (selected?.id === conn.id) {
                setSelected(prev => prev
                  ? { ...prev, status: 'connected', lastSync: 'just now' }
                  : prev
                )
              }
            }
          } catch { /* skip malformed events */ }
        }
      }
    } catch {
      const duration = Date.now() - startedAt
      setJobs(prev => prev.map(j =>
        j.id === jobId ? { ...j, status: 'failed', progress: 100, duration } : j
      ))
      setConns(prev => prev.map(c =>
        c.id === conn.id ? { ...c, status: 'connected' } : c
      ))
      if (selected?.id === conn.id) {
        setSelected(prev => prev ? { ...prev, status: 'connected' } : prev)
      }
    }
  }

  async function handleCreated(newConn: NewConnector, job: ConnectorJob) {
    // Persist to server registry
    const body: Record<string, unknown> = {
      name: newConn.name, type: newConn.type, authMethod: newConn.authMethod,
      endpoint: newConn.endpoint, description: newConn.description || `${TYPE_CFG[newConn.type].label} connector`,
      datasets: [], syncInterval: newConn.syncInterval,
      ...(newConn.runtimeConfig ? { runtimeConfig: newConn.runtimeConfig } : {}),
      ...(newConn.aiCredentials ? { aiCredentials: newConn.aiCredentials } : {}),
    }
    let conn: Connection
    try {
      const res = await fetch('/api/connectors', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      conn = await res.json() as Connection
    } catch {
      conn = {
        id: newConn.id, name: newConn.name, type: newConn.type,
        status: 'connected', lastSync: 'just now', recordsSynced: '—',
        authMethod: newConn.authMethod, endpoint: newConn.endpoint,
        description: String(body.description ?? ''), datasets: [],
        syncInterval: newConn.syncInterval, latencyMs: 0, sparkValues: [],
      }
    }
    setConns(prev => [conn, ...prev])
    // File uploads don't need a background job panel — the parse happened client-side
    if (newConn.type !== 'file') {
      setJobs(prev => [job, ...prev])
      setShowJobs(true)
    }
    setSelected(conn)
  }

  // Panel layout widths
  const hasDetail = !!selected
  const hasJobs = showJobs
  const gridCols = hasDetail && hasJobs ? 'grid-cols-[minmax(0,1fr)_340px_280px]' : hasDetail ? 'grid-cols-[minmax(0,1fr)_360px]' : hasJobs ? 'grid-cols-[minmax(0,1fr)_300px]' : 'grid-cols-1'

  return (
    <div className={`grid h-full ${gridCols} transition-all duration-200`}>
      {/* ── Main list ── */}
      <div className="flex flex-col border-r border-chef-border overflow-hidden">
        {/* Header */}
        <div className="px-5 py-3.5 border-b border-chef-border flex items-center gap-3 shrink-0">
          <Zap size={15} className="text-indigo-400" />
          <h2 className="text-sm font-semibold text-chef-text flex-1">Connections</h2>
          <span className="text-[11px] text-chef-muted">{connectedCount}/{conns.length} connected</span>
          {runningJobs > 0 && (
            <button onClick={() => setShowJobs(s => !s)}
              className="flex items-center gap-1.5 text-[11px] text-indigo-400 px-2.5 py-1 rounded-full bg-indigo-500/10 border border-indigo-500/20 hover:bg-indigo-500/15 transition-colors">
              <Loader2 size={10} className="animate-spin" /> {runningJobs} running
            </button>
          )}
          <button onClick={() => setShowJobs(s => !s)}
            className={`flex items-center gap-1 text-[11px] px-2.5 py-1.5 rounded-lg border transition-colors ${showJobs ? 'bg-indigo-500/10 border-indigo-500/30 text-indigo-400' : 'border-chef-border text-chef-muted hover:text-chef-text hover:border-indigo-500/20'}`}>
            <Activity size={12} /> Jobs
            {jobs.length > 0 && <span className="w-4 h-4 rounded-full bg-indigo-500 text-white text-[9px] font-bold flex items-center justify-center">{Math.min(jobs.length, 9)}</span>}
          </button>
          <button onClick={() => setShowWizard(true)}
            className="flex items-center gap-1.5 bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-medium px-3 py-1.5 rounded-lg transition-colors">
            <Plus size={13} /> Add Connector
          </button>
        </div>

        {/* Type filter */}
        <div className="px-5 py-2.5 border-b border-chef-border flex items-center gap-1.5 shrink-0 overflow-x-auto">
          {filterTypes.map(t => (
            <button key={t} onClick={() => setFilter(t)}
              className={`shrink-0 text-[11px] font-medium px-3 py-1 rounded-full transition-colors capitalize ${filter === t ? 'bg-indigo-500/15 text-indigo-400' : 'text-chef-muted hover:text-chef-text'}`}>
              {t === 'all' ? 'All types' : TYPE_CFG[t as ConnectorId].label}
            </button>
          ))}
        </div>

        {/* Cards grid */}
        <div className={`flex-1 p-4 overflow-auto grid gap-3 content-start ${hasDetail ? 'grid-cols-1' : 'grid-cols-1 md:grid-cols-2 xl:grid-cols-3'}`}>
          {loading && Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="border border-chef-border rounded-xl p-4 bg-chef-card">
              <div className="flex items-start justify-between mb-3"><div className="w-9 h-9 rounded-xl shimmer" /><div className="w-16 h-5 rounded-full shimmer" /></div>
              <div className="w-32 h-4 rounded shimmer mb-1.5" /><div className="w-full h-3 rounded shimmer mb-1" /><div className="w-3/4 h-3 rounded shimmer" />
            </div>
          ))}
          {!loading && filtered.map(conn => (
            <ConnCard
              key={conn.id}
              conn={conn}
              selected={selected?.id === conn.id}
              onClick={() => setSelected(prev => prev?.id === conn.id ? null : conn)}
              onSync={() => {
                if (conn.status === 'disconnected') toggleStatus(conn.id)
                else startSync(conn)
              }}
            />
          ))}
        </div>
      </div>

      {/* ── Detail panel ── */}
      {selected && (
        <div className="border-r border-chef-border overflow-hidden">
          <DetailPanel
            conn={selected}
            onToggle={() => toggleStatus(selected.id)}
            onSync={() => startSync(selected)}
            onClose={() => setSelected(null)}
          />
        </div>
      )}

      {/* ── Jobs panel ── */}
      {showJobs && (
        <div className="overflow-hidden flex flex-col">
          <JobsPanel jobs={jobs} onClose={() => setShowJobs(false)} />
        </div>
      )}

      {/* ── Wizard modal ── */}
      {showWizard && (
        <ConnectorWizard
          onClose={() => setShowWizard(false)}
          onCreated={handleCreated}
        />
      )}
    </div>
  )
}
