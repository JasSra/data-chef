'use client'

import { useState, useEffect, useCallback, useRef } from 'react'
import {
  Plus, Globe, Cloud, Server, Database, Webhook,
  Clock, Settings, AlertTriangle, CheckCircle2,
  Zap, Lock, ExternalLink, Terminal, Activity,
  Play, X, Loader2, AlertCircle, HardDrive,
  RefreshCw, ChevronRight, Copy, FileText, BarChart2, Users, Radar, EyeOff, Undo2,
  Download, Upload,
} from 'lucide-react'
import StatusBadge from '@/components/StatusBadge'
import ConnectorWizard, { ConnectorJob, NewConnector, ConnectorId, DiscoveryConnectorDraft } from '@/components/ConnectorWizard'
import BrandIcon from '@/components/BrandIcon'
import ConfirmDialog from '@/components/ConfirmDialog'

/* ── Types ───────────────────────────────────────────────────────── */
type ConnStatus = 'connected' | 'disconnected' | 'running'

interface Connection {
  id: string; name: string; type: ConnectorId; status: ConnStatus
  lastSync: string; recordsSynced: string; authMethod: string
  endpoint: string; description: string; datasets: string[]
  syncInterval: string; latencyMs: number; sparkValues: number[]
}

interface DiscoveryCandidate {
  id: string
  type: Extract<ConnectorId, 'postgresql' | 'mysql' | 'mongodb' | 'redis' | 's3' | 'sftp' | 'elasticsearch'>
  host: string
  port: number
  displayName: string
  confidence: number
  matchReason: string
  status: 'new' | 'dismissed' | 'added'
  lastSeenAt: number
  lastSeen: string
  connectorId?: string | null
}

interface DiscoveryOverview {
  enabled: boolean
  running: boolean
  lastScanAt: number | null
  lastScan: string
  lastScanDurationMs: number | null
  candidates: DiscoveryCandidate[]
}

function isKnownConnectorType(value: string): value is ConnectorId {
  return value in TYPE_CFG
}

/* ── Type config ─────────────────────────────────────────────────── */
const TYPE_CFG: Record<ConnectorId, { Icon?: React.ElementType; brandClass?: string; color: string; bg: string; border: string; label: string }> = {
  http:       { Icon: Globe,     color: 'text-sky-400',     bg: 'bg-sky-500/10',     border: 'border-sky-500/20',    label: 'HTTP API' },
  github:     { brandClass: 'fa-brands fa-github', color: 'text-zinc-200', bg: 'bg-zinc-500/10', border: 'border-zinc-500/20', label: 'GitHub' },
  azuredevops:{ brandClass: 'fa-brands fa-microsoft', color: 'text-cyan-200', bg: 'bg-cyan-500/10', border: 'border-cyan-500/20', label: 'Azure DevOps' },
  webhook:    { Icon: Webhook,   color: 'text-amber-400',   bg: 'bg-amber-500/10',   border: 'border-amber-500/20',  label: 'Webhook' },
  s3:         { Icon: Cloud,     color: 'text-violet-400',  bg: 'bg-violet-500/10',  border: 'border-violet-500/20', label: 'S3' },
  sftp:       { Icon: Server,    color: 'text-slate-400',   bg: 'bg-slate-500/10',   border: 'border-slate-500/20',  label: 'SFTP' },
  postgresql: { Icon: Database,  color: 'text-blue-400',    bg: 'bg-blue-500/10',    border: 'border-blue-500/20',   label: 'PostgreSQL' },
  mysql:      { Icon: Database,  color: 'text-orange-400',  bg: 'bg-orange-500/10',  border: 'border-orange-500/20', label: 'MySQL' },
  mongodb:    { Icon: Database,  color: 'text-emerald-400', bg: 'bg-emerald-500/10', border: 'border-emerald-500/20', label: 'MongoDB' },
  redis:      { Icon: Database,  color: 'text-red-400',    bg: 'bg-red-500/10',     border: 'border-red-500/20',    label: 'Redis' },
  mssql:      { Icon: Database,  color: 'text-sky-300',    bg: 'bg-sky-500/10',     border: 'border-sky-500/20',    label: 'SQL Server' },
  rabbitmq:   { Icon: Zap,       color: 'text-orange-300', bg: 'bg-orange-500/10',  border: 'border-orange-500/20', label: 'RabbitMQ' },
  mqtt:       { Icon: Zap,       color: 'text-emerald-300', bg: 'bg-emerald-500/10', border: 'border-emerald-500/20', label: 'MQTT' },
  bigquery:   { brandClass: 'fa-brands fa-google', color: 'text-rose-400', bg: 'bg-rose-500/10', border: 'border-rose-500/20', label: 'BigQuery' },
  file:       { Icon: FileText,  color: 'text-lime-400',    bg: 'bg-lime-500/10',    border: 'border-lime-500/20',    label: 'File Upload' },
  appinsights:{ brandClass: 'fa-brands fa-microsoft', color: 'text-cyan-400', bg: 'bg-cyan-500/10', border: 'border-cyan-500/20', label: 'App Insights' },
  azuremonitor:{ brandClass: 'fa-brands fa-microsoft', color: 'text-cyan-300', bg: 'bg-cyan-500/10', border: 'border-cyan-500/20', label: 'Azure Monitor' },
  elasticsearch:{ Icon: Database, color: 'text-amber-300', bg: 'bg-amber-500/10', border: 'border-amber-500/20', label: 'Elasticsearch / OpenSearch' },
  datadog:    { Icon: Activity,  color: 'text-orange-300', bg: 'bg-orange-500/10', border: 'border-orange-500/20', label: 'Datadog' },
  azureb2c:   { brandClass: 'fa-brands fa-microsoft', color: 'text-teal-400', bg: 'bg-teal-500/10', border: 'border-teal-500/20', label: 'Azure AD B2C' },
  azureentraid:{ brandClass: 'fa-brands fa-microsoft', color: 'text-sky-300', bg: 'bg-sky-500/10', border: 'border-sky-500/20', label: 'Azure Entra ID' },
}

function getTypeConfig(type: string) {
  return TYPE_CFG[type as ConnectorId] ?? {
    Icon: Globe,
    color: 'text-chef-muted',
    bg: 'bg-chef-bg',
    border: 'border-chef-border',
    label: type,
  }
}

function normalizeConnection(raw: Partial<Connection> & { id: string; name: string; type: string }): Connection {
  const status: ConnStatus = raw.status === 'running' || raw.status === 'disconnected' ? raw.status : 'connected'
  const type = raw.type as ConnectorId

  return {
    id: raw.id,
    name: raw.name,
    type,
    status,
    lastSync: raw.lastSync ?? 'Never',
    recordsSynced: raw.recordsSynced ?? '—',
    authMethod: raw.authMethod ?? 'Unknown',
    endpoint: raw.endpoint ?? '',
    description: raw.description ?? `${getTypeConfig(raw.type).label} connector`,
    datasets: Array.isArray(raw.datasets) ? raw.datasets : [],
    syncInterval: raw.syncInterval ?? 'Manual',
    latencyMs: typeof raw.latencyMs === 'number' ? raw.latencyMs : 0,
    sparkValues: Array.isArray(raw.sparkValues) ? raw.sparkValues : [],
  }
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
  http: '#38bdf8', github: '#d4d4d8', azuredevops: '#67e8f9', webhook: '#fbbf24', s3: '#a78bfa', sftp: '#94a3b8',
  postgresql: '#60a5fa', mysql: '#fb923c', mongodb: '#34d399', redis: '#f87171', mssql: '#7dd3fc', rabbitmq: '#fdba74', mqtt: '#6ee7b7', bigquery: '#fb7185',
  file:        '#a3e635',
  appinsights: '#22d3ee',
  azuremonitor:'#67e8f9',
  elasticsearch:'#fbbf24',
  datadog:     '#fdba74',
  azureb2c:    '#2dd4bf',
  azureentraid:'#7dd3fc',
}

/* ── Connection Card ─────────────────────────────────────────────── */
function ConnCard({ conn, selected, onClick, onSync }: {
  conn: Connection; selected: boolean; onClick: () => void; onSync: () => void
}) {
  const tc = getTypeConfig(conn.type)
  return (
    <div
      onClick={onClick}
      className={`border rounded-xl p-4 cursor-pointer transition-all hover:shadow-lg hover:shadow-black/20 ${selected ? 'border-indigo-500/50 bg-indigo-500/5 shadow-lg shadow-indigo-900/20' : conn.status === 'disconnected' ? 'border-chef-border bg-chef-card opacity-60 hover:opacity-90' : 'border-chef-border bg-chef-card hover:border-indigo-500/30'}`}
    >
      <div className="flex items-start justify-between gap-3 mb-3">
        <div className={`w-9 h-9 rounded-xl ${tc.bg} ${tc.border} border flex items-center justify-center shrink-0`}>
          <BrandIcon icon={tc.Icon} brandClass={tc.brandClass} size={18} className={tc.color} />
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
          <Sparkline values={conn.sparkValues} color={SPARK_COLORS[conn.type] ?? '#94a3b8'} />
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

function DiscoveryCard({
  candidate,
  onAdd,
  onDismiss,
  onRestore,
}: {
  candidate: DiscoveryCandidate
  onAdd: () => void
  onDismiss: () => void
  onRestore: () => void
}) {
  const tc = getTypeConfig(candidate.type)
  const Icon = tc.Icon

  return (
    <div className={`rounded-xl border p-4 ${candidate.status === 'dismissed' ? 'border-chef-border/70 bg-chef-bg/60 opacity-80' : 'border-chef-border bg-chef-card'}`}>
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-start gap-3 min-w-0">
          <div className={`w-10 h-10 rounded-xl ${tc.bg} ${tc.border} border flex items-center justify-center shrink-0`}>
            <BrandIcon icon={Icon} brandClass={tc.brandClass} size={18} className={tc.color} />
          </div>
          <div className="min-w-0">
            <div className="text-sm font-semibold text-chef-text truncate">{candidate.displayName}</div>
            <div className={`text-[10px] font-mono mt-0.5 ${tc.color}`}>{tc.label}</div>
          </div>
        </div>
        <span className={`rounded-full px-2 py-1 text-[10px] font-semibold ${candidate.confidence >= 0.9 ? 'bg-emerald-500/10 text-emerald-300' : candidate.confidence >= 0.75 ? 'bg-sky-500/10 text-sky-300' : 'bg-amber-500/10 text-amber-300'}`}>
          {Math.round(candidate.confidence * 100)}%
        </span>
      </div>

      <div className="mt-3 text-[11px] leading-relaxed text-chef-muted">
        {candidate.matchReason}
      </div>

      <div className="mt-3 flex items-center gap-2 text-[10px] text-chef-muted">
        <Clock size={10} />
        <span>{candidate.lastSeen}</span>
        <span className="font-mono">{candidate.host}:{candidate.port}</span>
      </div>

      <div className="mt-4 flex flex-wrap items-center gap-2">
        {candidate.status === 'dismissed' ? (
          <button
            onClick={onRestore}
            className="inline-flex items-center gap-1.5 rounded-lg border border-chef-border px-3 py-1.5 text-[11px] text-chef-text hover:border-indigo-500/20 hover:bg-chef-bg transition-colors"
          >
            <Undo2 size={11} /> Restore
          </button>
        ) : (
          <>
            <button
              onClick={onAdd}
              className="inline-flex items-center gap-1.5 rounded-lg bg-indigo-600 px-3 py-1.5 text-[11px] font-medium text-white hover:bg-indigo-500 transition-colors"
            >
              <Plus size={11} /> Add connector
            </button>
            <button
              onClick={onDismiss}
              className="inline-flex items-center gap-1.5 rounded-lg border border-chef-border px-3 py-1.5 text-[11px] text-chef-muted hover:text-chef-text hover:border-indigo-500/20 transition-colors"
            >
              <EyeOff size={11} /> Dismiss
            </button>
          </>
        )}
      </div>
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
            const tc = getTypeConfig(j.connectorType)
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
function DetailPanel({ conn, onToggle, onSync, onExport, onCopy, onClone, onEdit, onDelete, onClose, deleteBusy }: {
  conn: Connection
  onToggle: () => void
  onSync: () => void
  onExport: () => void
  onCopy: () => void
  onClone: () => void
  onEdit: () => void
  onDelete: () => void
  onClose: () => void
  deleteBusy: boolean
}) {
  const tc = getTypeConfig(conn.type)
  const [copied, setCopied] = useState(false)
  const endpoint = conn.endpoint ?? ''
  const datasets = Array.isArray(conn.datasets) ? conn.datasets : []

  function copy() { navigator.clipboard.writeText(endpoint).catch(() => {}); setCopied(true); setTimeout(() => setCopied(false), 2000) }

  return (
    <div className="flex flex-col h-full min-h-0 overflow-auto animate-slide-in">
      {/* Header */}
      <div className="px-5 py-4 border-b border-chef-border shrink-0">
        <div className="flex items-start gap-3">
          <div className={`w-11 h-11 rounded-xl ${tc.bg} ${tc.border} border flex items-center justify-center shrink-0`}>
            <BrandIcon icon={tc.Icon} brandClass={tc.brandClass} size={22} className={tc.color} />
          </div>
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-base font-bold text-chef-text">{conn.name}</span>
              <StatusBadge status={conn.status === 'running' ? 'running' : conn.status === 'connected' ? 'connected' : 'disconnected'} />
            </div>
            <div className={`text-xs font-mono mt-0.5 ${tc.color}`}>{tc.label}</div>
            <div className="text-xs text-chef-muted mt-0.5 leading-snug">{conn.description}</div>
          </div>
          <div className="flex items-center justify-end gap-1.5 shrink-0 flex-wrap max-w-[220px]">
            <button onClick={onExport} className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg border border-chef-border text-chef-muted hover:text-chef-text hover:border-indigo-500/20 hover:bg-chef-card transition-colors">
              <Download size={12} /> Export
            </button>
            <button onClick={onCopy} className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg border border-chef-border text-chef-muted hover:text-chef-text hover:border-indigo-500/20 hover:bg-chef-card transition-colors">
              <Copy size={12} /> Copy JSON
            </button>
            <button onClick={onClone} className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg border border-chef-border text-chef-muted hover:text-chef-text hover:border-indigo-500/20 hover:bg-chef-card transition-colors">
              <Plus size={12} /> Clone
            </button>
            <button onClick={onEdit} className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg border border-chef-border text-chef-muted hover:text-chef-text hover:border-indigo-500/20 hover:bg-chef-card transition-colors">
              <Settings size={12} /> Edit
            </button>
            <button
              onClick={onDelete}
              disabled={deleteBusy}
              className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg border border-rose-500/30 text-rose-400 hover:bg-rose-500/10 transition-colors disabled:cursor-not-allowed disabled:opacity-50"
            >
              <X size={12} /> Delete
            </button>
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
          { label: 'Endpoint',       value: endpoint || '—',                      mono: true },
          { label: 'Auth method',    value: conn.authMethod || 'Unknown',         mono: false },
          { label: 'Sync interval',  value: conn.syncInterval || 'Manual',        mono: false },
          { label: 'Linked datasets',value: datasets.join(', ') || 'None',        mono: true },
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
          {(conn.type === 'sftp' && endpoint.startsWith('ftp://')) && (
            <div className="flex items-center gap-2"><AlertTriangle size={11} className="text-amber-400 shrink-0" /><span>FTP is unencrypted — migrate to SFTP</span></div>
          )}
          {conn.type === 'azureb2c' && (
            <>
              <div className="flex items-start gap-2"><AlertTriangle size={11} className="text-amber-400 shrink-0 mt-0.5" /><span>Azure AD B2C is unavailable for new customers after May 1, 2025. This connector is intended for existing tenants.</span></div>
              <div className="flex items-start gap-2"><AlertTriangle size={11} className="text-sky-400 shrink-0 mt-0.5" /><span><span className="font-medium text-sky-300">Beta endpoints:</span> <code className="bg-chef-card px-1 rounded">userFlows</code> and <code className="bg-chef-card px-1 rounded">customPolicies</code> use Microsoft Graph beta APIs; <code className="bg-chef-card px-1 rounded">users</code> stays on Graph v1.0.</span></div>
            </>
          )}
          {conn.type === 'azureentraid' && (
            <div className="flex items-start gap-2"><CheckCircle2 size={11} className="text-sky-400 shrink-0 mt-0.5" /><span>Azure Entra ID resources use Microsoft Graph v1.0 for <code className="bg-chef-card px-1 rounded">users</code>, <code className="bg-chef-card px-1 rounded">groups</code>, and <code className="bg-chef-card px-1 rounded">applications</code>.</span></div>
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
  const [discovery, setDiscovery] = useState<DiscoveryOverview | null>(null)
  const [discoveryLoading, setDiscoveryLoading] = useState(true)
  const [showDismissedDiscovery, setShowDismissedDiscovery] = useState(false)
  const [wizardDraft, setWizardDraft] = useState<DiscoveryConnectorDraft | null>(null)
  const [selected, setSelected] = useState<Connection | null>(null)
  const [filter, setFilter] = useState<FilterType>('all')
  const [showWizard, setShowWizard] = useState(false)
  const [showDeleteDialog, setShowDeleteDialog] = useState(false)
  const [deleteBusy, setDeleteBusy] = useState(false)
  const [jobs, setJobs] = useState<ConnectorJob[]>([])
  const [showJobs, setShowJobs] = useState(false)
  const [transferNotice, setTransferNotice] = useState<{ tone: 'success' | 'error'; msg: string } | null>(null)
  const [scanLogs, setScanLogs] = useState<string[]>([])
  const [showScanLogs, setShowScanLogs] = useState(false)
  const importRef = useRef<HTMLInputElement>(null)

  const showNotice = useCallback((tone: 'success' | 'error', msg: string) => {
    setTransferNotice({ tone, msg })
  }, [])

  const loadConnectors = useCallback(async () => {
    try {
      const res = await fetch('/api/connectors')
      const data = await res.json() as Array<Partial<Connection> & { id: string; name: string; type: string }>
      const normalized = Array.isArray(data) ? data.map(normalizeConnection) : []
      setConns(normalized)
      setSelected(prev => prev ? normalized.find(conn => conn.id === prev.id) ?? null : null)
    } finally {
      setLoading(false)
    }
  }, [])

  const loadDiscovery = useCallback(async (includeDismissed = true) => {
    try {
      const res = await fetch(`/api/discovery?includeDismissed=${includeDismissed ? 'true' : 'false'}`, { cache: 'no-store' })
      const data = await res.json() as DiscoveryOverview
      setDiscovery(data)
    } finally {
      setDiscoveryLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadConnectors()
    void loadDiscovery()
  }, [loadConnectors, loadDiscovery])

  useEffect(() => {
    if (!transferNotice) return
    const timer = window.setTimeout(() => setTransferNotice(null), 4000)
    return () => window.clearTimeout(timer)
  }, [transferNotice])

  const connectedCount = conns.filter(c => c.status === 'connected').length
  const runningJobs = jobs.filter(j => j.status === 'running').length
  const activeDiscovery = (discovery?.candidates ?? []).filter(candidate => candidate.status !== 'dismissed')
  const dismissedDiscovery = (discovery?.candidates ?? []).filter(candidate => candidate.status === 'dismissed')

  const filtered = filter === 'all' ? conns : conns.filter(c => c.type === filter)
  const filterTypes: FilterType[] = ['all', ...Array.from(new Set(conns.map(c => c.type).filter(isKnownConnectorType)))]

  function downloadJsonFile(filename: string, payload: unknown) {
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const anchor = document.createElement('a')
    anchor.href = url
    anchor.download = filename
    anchor.click()
    URL.revokeObjectURL(url)
  }

  async function fetchTransferPayload(ids?: string[]) {
    const query = ids && ids.length > 0
      ? `?${ids.map(id => `id=${encodeURIComponent(id)}`).join('&')}`
      : ''
    const res = await fetch(`/api/connectors/transfer${query}`)
    if (!res.ok) throw new Error('Export request failed')
    return res.json()
  }

  async function importConfigPayload(payload: unknown, successMessage?: string) {
    try {
      const res = await fetch('/api/connectors/transfer', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.error ?? 'Import failed')
      await loadConnectors()
      if (Array.isArray(data.connectors) && data.connectors.length > 0) {
        setSelected(normalizeConnection(data.connectors[0] as Partial<Connection> & { id: string; name: string; type: string }))
      }
      showNotice('success', successMessage ?? `Imported ${data.imported} connector${data.imported === 1 ? '' : 's'}`)
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Import failed'
      showNotice('error', msg)
    }
  }

  async function exportConfig(ids?: string[]) {
    try {
      const data = await fetchTransferPayload(ids)
      const suffix = ids && ids.length === 1 && selected
        ? selected.name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || 'connector'
        : 'all-connectors'
      downloadJsonFile(`datachef-connectors-${suffix}.json`, data)
      showNotice('success', ids?.length === 1 ? 'Connector exported as JSON' : 'Connector bundle exported as JSON')
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Export failed'
      showNotice('error', msg)
    }
  }

  async function copyConfigToClipboard(ids?: string[]) {
    try {
      const data = await fetchTransferPayload(ids)
      await navigator.clipboard.writeText(JSON.stringify(data, null, 2))
      showNotice('success', ids?.length === 1 ? 'Connector JSON copied to clipboard' : 'Connector bundle copied to clipboard')
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Clipboard copy failed'
      showNotice('error', msg)
    }
  }

  async function pasteConfigFromClipboard() {
    try {
      const raw = await navigator.clipboard.readText()
      if (!raw.trim()) throw new Error('Clipboard is empty')
      await importConfigPayload(JSON.parse(raw))
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Clipboard import failed'
      showNotice('error', msg)
    }
  }

  async function importConfig(file: File) {
    try {
      const raw = await file.text()
      await importConfigPayload(JSON.parse(raw))
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Import failed'
      showNotice('error', msg)
    }
  }

async function cloneConnector(conn: Connection) {
    const nextName = window.prompt('Clone connector as', `${conn.name} Copy`)
    if (!nextName?.trim()) return

    try {
      const payload = await fetchTransferPayload([conn.id]) as { version?: number; connectors?: Array<Record<string, unknown>> }
      const source = payload.connectors?.[0]
      if (!source) throw new Error('Connector export payload was empty')

      const clone = {
        ...source,
        id: undefined,
        name: nextName.trim(),
        datasets: [],
        syncHistory: [],
        recordsRaw: 0,
        latencyMs: 0,
        lastSyncAt: null,
        lastRunAt: null,
        lastError: null,
      }

      await importConfigPayload({
        version: payload.version ?? 1,
        connectors: [clone],
      }, `Cloned connector as "${nextName.trim()}"`)
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Clone failed'
      showNotice('error', msg)
  }
}

  async function editConnector(conn: Connection) {
    try {
      const payload = await fetchTransferPayload([conn.id]) as {
        connectors?: Array<{
          id?: string
          type: ConnectorId
          name: string
          description?: string
          endpoint?: string
          runtimeConfig?: Record<string, unknown>
          aiCredentials?: NewConnector['aiCredentials']
          observabilityCredentials?: NewConnector['observabilityCredentials']
          azureB2cCredentials?: NewConnector['azureB2cCredentials']
          azureEntraIdCredentials?: NewConnector['azureEntraIdCredentials']
        }>
      }
      const source = payload.connectors?.[0]
      if (!source) throw new Error('Connector export payload was empty')

      setWizardDraft({
        existingConnectorId: conn.id,
        type: source.type,
        name: source.name,
        description: source.description ?? '',
        endpoint: source.endpoint ?? '',
        runtimeConfig: source.runtimeConfig ?? {},
        aiCredentials: source.aiCredentials,
        observabilityCredentials: source.observabilityCredentials,
        azureB2cCredentials: source.azureB2cCredentials,
        azureEntraIdCredentials: source.azureEntraIdCredentials,
      })
      setShowWizard(true)
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to load connector for editing'
      showNotice('error', msg)
    }
  }

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
                  ? [...(Array.isArray(c.sparkValues) ? c.sparkValues : []).slice(-11), newRecords]
                  : (Array.isArray(c.sparkValues) ? c.sparkValues : [])
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
      ...(newConn.existingConnectorId ? { id: newConn.existingConnectorId } : {}),
      name: newConn.name, type: newConn.type, authMethod: newConn.authMethod,
      endpoint: newConn.endpoint, description: newConn.description || `${TYPE_CFG[newConn.type].label} connector`,
      datasets: [], syncInterval: newConn.syncInterval,
      ...(newConn.sourceDiscoveryId ? { sourceDiscoveryId: newConn.sourceDiscoveryId } : {}),
      ...(newConn.runtimeConfig ? { runtimeConfig: newConn.runtimeConfig } : {}),
      ...(newConn.aiCredentials ? { aiCredentials: newConn.aiCredentials } : {}),
      ...(newConn.observabilityCredentials ? { observabilityCredentials: newConn.observabilityCredentials } : {}),
      ...(newConn.azureB2cCredentials ? { azureB2cCredentials: newConn.azureB2cCredentials } : {}),
      ...(newConn.azureEntraIdCredentials ? { azureEntraIdCredentials: newConn.azureEntraIdCredentials } : {}),
      ...(newConn.githubCredentials ? { githubCredentials: newConn.githubCredentials } : {}),
      ...(newConn.githubAuthTransactionId ? { githubAuthTransactionId: newConn.githubAuthTransactionId } : {}),
      ...(newConn.azureDevOpsCredentials ? { azureDevOpsCredentials: newConn.azureDevOpsCredentials } : {}),
      ...(newConn.azureDevOpsAuthTransactionId ? { azureDevOpsAuthTransactionId: newConn.azureDevOpsAuthTransactionId } : {}),
    }
    let conn: Connection
    try {
      const res = await fetch('/api/connectors', {
        method: newConn.existingConnectorId ? 'PATCH' : 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const payload = await res.json() as Connection & { error?: string }
      if (!res.ok) throw new Error(payload.error ?? 'Connector create failed')
      conn = payload as Connection
      conn = normalizeConnection(conn)
    } catch (error) {
      showNotice('error', error instanceof Error ? error.message : 'Connector create failed')
      conn = normalizeConnection({
        id: newConn.id, name: newConn.name, type: newConn.type,
        status: 'connected', lastSync: 'just now', recordsSynced: '—',
        authMethod: newConn.authMethod, endpoint: newConn.endpoint,
        description: String(body.description ?? ''), datasets: [],
        syncInterval: newConn.syncInterval, latencyMs: 0, sparkValues: [],
      })
    }
    setConns(prev => newConn.existingConnectorId
      ? prev.map(existing => existing.id === conn.id ? conn : existing)
      : [conn, ...prev])
    setWizardDraft(null)
    // File uploads don't need a background job panel — the parse happened client-side
    if (newConn.type !== 'file' && !newConn.existingConnectorId) {
      setJobs(prev => [job, ...prev])
      setShowJobs(true)
    }
    setSelected(conn)
    void loadDiscovery()
  }

  async function runDiscoveryScan() {
    setDiscoveryLoading(true)
    setScanLogs([])
    setShowScanLogs(true)
    try {
      const res = await fetch('/api/discovery', { method: 'POST' })
      const data = await res.json() as DiscoveryOverview & { scan?: { logs?: string[] } }
      setDiscovery(data)
      if (data.scan?.logs) {
        setScanLogs(data.scan.logs)
      }
    } catch {
      showNotice('error', 'Discovery scan failed')
    } finally {
      setDiscoveryLoading(false)
    }
  }

  async function updateDiscoveryStatus(id: string, status: 'new' | 'dismissed') {
    try {
      const res = await fetch(`/api/discovery/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status }),
      })
      if (!res.ok) throw new Error('Discovery update failed')
      await loadDiscovery()
    } catch {
      showNotice('error', 'Failed to update discovery candidate')
    }
  }

  async function startDiscoveryConnector(candidateId: string) {
    try {
      const res = await fetch(`/api/discovery/${candidateId}/convert`, { method: 'POST' })
      if (!res.ok) throw new Error('Draft conversion failed')
      const draft = await res.json() as DiscoveryConnectorDraft
      setWizardDraft(draft)
      setShowWizard(true)
    } catch {
      showNotice('error', 'Failed to prepare connector draft')
    }
  }

  async function deleteSelectedConnector() {
    if (!selected || deleteBusy) return

    try {
      setDeleteBusy(true)
      const conn = selected
      const res = await fetch(`/api/connectors/${conn.id}`, { method: 'DELETE' })
      const data = await res.json() as {
        error?: string
        deletedDatasets?: number
        deletedPipelines?: number
        deletedSavedQueries?: number
      }
      if (!res.ok) throw new Error(data.error ?? 'Delete failed')

      setConns(prev => prev.filter(item => item.id !== conn.id))
      setSelected(null)
      setShowDeleteDialog(false)
      await loadDiscovery()

      const summary = [
        `Deleted connector "${conn.name}"`,
        data.deletedDatasets ? `, ${data.deletedDatasets} dataset${data.deletedDatasets === 1 ? '' : 's'}` : '',
        data.deletedPipelines ? `, ${data.deletedPipelines} pipeline${data.deletedPipelines === 1 ? '' : 's'}` : '',
        data.deletedSavedQueries ? `, ${data.deletedSavedQueries} saved quer${data.deletedSavedQueries === 1 ? 'y' : 'ies'}` : '',
      ].join('')
      showNotice('success', summary)
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Delete failed'
      showNotice('error', msg)
    } finally {
      setDeleteBusy(false)
    }
  }

  // Panel layout widths
  const hasDetail = !!selected
  const hasJobs = showJobs
  const gridCols = hasDetail && hasJobs
    ? 'grid-cols-1 xl:grid-cols-[minmax(0,1.45fr)_minmax(360px,0.95fr)] 2xl:grid-cols-[minmax(0,1.6fr)_minmax(380px,0.95fr)_minmax(300px,0.8fr)]'
    : hasDetail
      ? 'grid-cols-1 xl:grid-cols-[minmax(0,1.5fr)_minmax(380px,0.95fr)]'
      : hasJobs
        ? 'grid-cols-1 xl:grid-cols-[minmax(0,1.55fr)_minmax(320px,0.85fr)]'
        : 'grid-cols-1'

  return (
    <div className={`grid h-full min-h-0 ${gridCols} transition-all duration-200`}>
      {/* ── Main list ── */}
      <div className={`flex flex-col min-h-0 ${hasDetail || hasJobs ? 'xl:border-r xl:border-chef-border' : ''}`}>
        {/* Header */}
        <div className="px-5 py-3.5 border-b border-chef-border shrink-0">
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex items-center gap-3 min-w-0 flex-1">
              <Zap size={15} className="text-indigo-400 shrink-0" />
              <h2 className="text-sm font-semibold text-chef-text">Connections</h2>
              <span className="text-[11px] text-chef-muted">{connectedCount}/{conns.length} connected</span>
            </div>
            {runningJobs > 0 && (
              <button onClick={() => setShowJobs(s => !s)}
                className="flex items-center gap-1.5 text-[11px] text-indigo-400 px-2.5 py-1 rounded-full bg-indigo-500/10 border border-indigo-500/20 hover:bg-indigo-500/15 transition-colors">
                <Loader2 size={10} className="animate-spin" /> {runningJobs} running
              </button>
            )}
            <div className="flex flex-wrap items-center gap-2">
              <input
                ref={importRef}
                type="file"
                accept="application/json,.json"
                className="hidden"
                onChange={async e => {
                  const file = e.target.files?.[0]
                  e.currentTarget.value = ''
                  if (!file) return
                  await importConfig(file)
                }}
              />
              <button
                onClick={() => importRef.current?.click()}
                className="flex items-center gap-1.5 text-[11px] px-2.5 py-1.5 rounded-lg border border-chef-border text-chef-muted hover:text-chef-text hover:border-indigo-500/20 transition-colors"
              >
                <Upload size={12} /> Import JSON
              </button>
              <button
                onClick={() => void pasteConfigFromClipboard()}
                className="flex items-center gap-1.5 text-[11px] px-2.5 py-1.5 rounded-lg border border-chef-border text-chef-muted hover:text-chef-text hover:border-indigo-500/20 transition-colors"
              >
                <Copy size={12} /> Paste JSON
              </button>
              <button
                onClick={() => void exportConfig()}
                disabled={conns.length === 0}
                className="flex items-center gap-1.5 text-[11px] px-2.5 py-1.5 rounded-lg border border-chef-border text-chef-muted hover:text-chef-text hover:border-indigo-500/20 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <Download size={12} /> Export JSON
              </button>
              <button
                onClick={() => void copyConfigToClipboard()}
                disabled={conns.length === 0}
                className="flex items-center gap-1.5 text-[11px] px-2.5 py-1.5 rounded-lg border border-chef-border text-chef-muted hover:text-chef-text hover:border-indigo-500/20 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <Copy size={12} /> Copy JSON
              </button>
              <button onClick={() => setShowJobs(s => !s)}
                className={`flex items-center gap-1 text-[11px] px-2.5 py-1.5 rounded-lg border transition-colors ${showJobs ? 'bg-indigo-500/10 border-indigo-500/30 text-indigo-400' : 'border-chef-border text-chef-muted hover:text-chef-text hover:border-indigo-500/20'}`}>
                <Activity size={12} /> Jobs
                {jobs.length > 0 && <span className="w-4 h-4 rounded-full bg-indigo-500 text-white text-[9px] font-bold flex items-center justify-center">{Math.min(jobs.length, 9)}</span>}
              </button>
              <button onClick={() => { setWizardDraft(null); setShowWizard(true) }}
                className="flex items-center gap-1.5 bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-medium px-3 py-1.5 rounded-lg transition-colors">
                <Plus size={13} /> Add Connector
              </button>
            </div>
          </div>
          {transferNotice && (
            <div className={`mt-3 inline-flex max-w-full items-center gap-2 rounded-lg border px-3 py-1.5 text-[11px] ${
              transferNotice.tone === 'success'
                ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300'
                : 'border-rose-500/20 bg-rose-500/10 text-rose-300'
            }`}>
              {transferNotice.tone === 'success'
                ? <CheckCircle2 size={12} className="shrink-0" />
                : <AlertCircle size={12} className="shrink-0" />}
              <span className="truncate">{transferNotice.msg}</span>
            </div>
          )}
        </div>

        {(discoveryLoading || discovery?.enabled || activeDiscovery.length > 0 || dismissedDiscovery.length > 0) && (
          <div className="px-5 py-4 border-b border-chef-border bg-chef-bg/40">
            <div className="flex flex-wrap items-center gap-3">
              <div className="flex items-center gap-2 min-w-0 flex-1">
                <Radar size={14} className="text-indigo-300 shrink-0" />
                <div className="min-w-0">
                  <div className="text-sm font-semibold text-chef-text">Discovered instances</div>
                  <div className="text-[11px] text-chef-muted">
                    {discoveryLoading
                      ? 'Loading discovery status…'
                      : discovery?.enabled
                        ? `${activeDiscovery.length} candidate${activeDiscovery.length === 1 ? '' : 's'} ready · last scan ${discovery?.lastScan ?? 'never'}`
                        : 'Discovery is disabled in setup settings'}
                  </div>
                </div>
              </div>
              <button
                onClick={() => void runDiscoveryScan()}
                disabled={discoveryLoading || discovery?.running || !discovery?.enabled}
                className="inline-flex items-center gap-1.5 rounded-lg border border-indigo-500/20 px-3 py-1.5 text-[11px] text-indigo-300 hover:bg-indigo-500/10 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <RefreshCw size={11} className={discoveryLoading || discovery?.running ? 'animate-spin' : ''} />
                Rescan
              </button>
              {scanLogs.length > 0 && (
                <button
                  onClick={() => setShowScanLogs(v => !v)}
                  className="inline-flex items-center gap-1.5 rounded-lg border border-chef-border px-3 py-1.5 text-[11px] text-chef-muted hover:text-chef-text hover:bg-chef-card transition-colors"
                >
                  <Terminal size={11} />
                  {showScanLogs ? 'Hide' : 'Show'} Scan Log
                </button>
              )}
            </div>

            {showScanLogs && scanLogs.length > 0 && (
              <div className="mt-4 rounded-lg border border-chef-border bg-[#0a0c10] p-3 max-h-[300px] overflow-y-auto">
                <div className="font-mono text-[10px] space-y-0.5">
                  {scanLogs.map((log, i) => (
                    <div key={i} className={log.startsWith('═══') ? 'text-indigo-400 font-semibold mt-2' : log.startsWith('✓') ? 'text-emerald-400' : log.includes('error') || log.includes('failed') ? 'text-red-400' : 'text-chef-muted'}>
                      {log}
                    </div>
                  ))}
                </div>
              </div>
            )}

            {discovery?.enabled && (
              <div className="mt-4 grid gap-3 md:grid-cols-2">
                {activeDiscovery.length === 0 && !discoveryLoading && (
                  <div className="rounded-xl border border-dashed border-chef-border px-4 py-5 text-[11px] text-chef-muted md:col-span-2">
                    No addable services are currently discoverable on the local network.
                  </div>
                )}
                {activeDiscovery.map(candidate => (
                  <DiscoveryCard
                    key={candidate.id}
                    candidate={candidate}
                    onAdd={() => void startDiscoveryConnector(candidate.id)}
                    onDismiss={() => void updateDiscoveryStatus(candidate.id, 'dismissed')}
                    onRestore={() => void updateDiscoveryStatus(candidate.id, 'new')}
                  />
                ))}
              </div>
            )}

            {dismissedDiscovery.length > 0 && (
              <div className="mt-4">
                <button
                  onClick={() => setShowDismissedDiscovery(value => !value)}
                  className="inline-flex items-center gap-1.5 text-[11px] text-chef-muted hover:text-chef-text transition-colors"
                >
                  <ChevronRight size={11} className={`transition-transform ${showDismissedDiscovery ? 'rotate-90' : ''}`} />
                  {dismissedDiscovery.length} dismissed candidate{dismissedDiscovery.length === 1 ? '' : 's'}
                </button>
                {showDismissedDiscovery && (
                  <div className="mt-3 grid gap-3 md:grid-cols-2">
                    {dismissedDiscovery.map(candidate => (
                      <DiscoveryCard
                        key={candidate.id}
                        candidate={candidate}
                        onAdd={() => void startDiscoveryConnector(candidate.id)}
                        onDismiss={() => void updateDiscoveryStatus(candidate.id, 'dismissed')}
                        onRestore={() => void updateDiscoveryStatus(candidate.id, 'new')}
                      />
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Type filter */}
        <div className="px-5 py-2.5 border-b border-chef-border flex items-center gap-1.5 shrink-0 overflow-x-auto">
          {filterTypes.map(t => (
            <button key={t} onClick={() => setFilter(t)}
              className={`shrink-0 text-[11px] font-medium px-3 py-1 rounded-full transition-colors capitalize ${filter === t ? 'bg-indigo-500/15 text-indigo-400' : 'text-chef-muted hover:text-chef-text'}`}>
              {t === 'all' ? 'All types' : (TYPE_CFG[t as ConnectorId]?.label ?? t)}
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
        <div className={`overflow-hidden min-h-[320px] ${hasJobs ? 'border-t border-chef-border xl:border-t-0 2xl:border-r 2xl:border-chef-border' : 'border-t border-chef-border xl:border-t-0'}`}>
          <DetailPanel
            conn={selected}
            onToggle={() => toggleStatus(selected.id)}
            onSync={() => startSync(selected)}
            onExport={() => void exportConfig([selected.id])}
            onCopy={() => void copyConfigToClipboard([selected.id])}
            onClone={() => void cloneConnector(selected)}
            onEdit={() => void editConnector(selected)}
            onDelete={() => setShowDeleteDialog(true)}
            onClose={() => setSelected(null)}
            deleteBusy={deleteBusy}
          />
        </div>
      )}

      {/* ── Jobs panel ── */}
      {showJobs && (
        <div className={`overflow-hidden flex flex-col min-h-[280px] border-t border-chef-border ${hasDetail ? 'xl:col-span-2 2xl:col-span-1 2xl:border-t-0' : 'xl:border-t-0'} ${hasDetail ? 'max-h-[42vh] 2xl:max-h-none' : ''}`}>
          <JobsPanel jobs={jobs} onClose={() => setShowJobs(false)} />
        </div>
      )}

      {/* ── Wizard modal ── */}
      {showWizard && (
        <ConnectorWizard
          onClose={() => {
            setShowWizard(false)
            setWizardDraft(null)
          }}
          onCreated={handleCreated}
          initialDraft={wizardDraft}
        />
      )}

      <ConfirmDialog
        open={showDeleteDialog && !!selected}
        title={selected ? `Delete ${selected.name}?` : 'Delete connector?'}
        description="This will remove the connector and cascade delete data that depends on it."
        details={selected ? [
          `${selected.datasets.length} linked dataset${selected.datasets.length === 1 ? '' : 's'} will be deleted.`,
          'Pipelines sourced from this connector or its datasets will also be deleted.',
          'Saved observability queries tied to this connector will be removed.',
        ] : []}
        confirmLabel="Delete connector"
        tone="danger"
        busy={deleteBusy}
        onCancel={() => {
          if (deleteBusy) return
          setShowDeleteDialog(false)
        }}
        onConfirm={() => void deleteSelectedConnector()}
      />
    </div>
  )
}
