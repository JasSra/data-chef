'use client'

import { useState, useEffect, useRef, useCallback } from 'react'
import { useRouter } from 'next/navigation'
import {
  Plus, Search, X, Database,
  Clock, Table, GitBranch, CheckCircle2,
  Download, RefreshCw, Loader2,
  ArrowRight, Cpu, Upload, AlertCircle,
} from 'lucide-react'
import StatusBadge from '@/components/StatusBadge'
import type { RMCharacter } from '@/lib/rm-api'
import NewDatasetWizard from '@/components/NewDatasetWizard'
import type { DatasetRecord, SchemaField } from '@/lib/datasets'
import ConfirmDialog from '@/components/ConfirmDialog'

/* ── Types ───────────────────────────────────────────────────────────────── */
type Tab = 'preview' | 'schema' | 'runs'

interface ServerPreview {
  columns: string[]
  rows:    string[][]
  rowCount:   number
  durationMs: number
}

const DATASET_PREVIEW_PAGE_SIZE = 25
const LIVE_PREVIEW_PAGE_SIZE = 50

/* ── Flat schema table (for real inferred schemas) ───────────────────────── */
const TYPE_COLORS: Record<string, string> = {
  integer:   'text-orange-400',
  float:     'text-amber-400',
  string:    'text-emerald-400',
  timestamp: 'text-sky-400',
  date:      'text-sky-300',
  boolean:   'text-rose-400',
  object:    'text-violet-400',
  array:     'text-violet-300',
  null:      'text-chef-muted',
}

function FlatSchemaTable({ schema }: { schema: SchemaField[] }) {
  return (
    <div className="rounded-xl border border-chef-border overflow-hidden">
      <table className="w-full text-[12px]">
        <thead>
          <tr className="border-b border-chef-border bg-chef-card">
            <th className="px-4 py-2.5 text-left text-[10px] font-semibold text-chef-muted uppercase tracking-wider">Field</th>
            <th className="px-4 py-2.5 text-left text-[10px] font-semibold text-chef-muted uppercase tracking-wider">Type</th>
            <th className="px-4 py-2.5 text-left text-[10px] font-semibold text-chef-muted uppercase tracking-wider">Nullable</th>
            <th className="px-4 py-2.5 text-left text-[10px] font-semibold text-chef-muted uppercase tracking-wider">Example</th>
          </tr>
        </thead>
        <tbody>
          {schema.map((row, i) => (
            <tr key={i} className="border-b last:border-0 border-chef-border hover:bg-chef-card/40 transition-colors">
              <td className="px-4 py-2.5 font-mono font-semibold text-chef-text">{row.field}</td>
              <td className="px-4 py-2.5 font-mono">
                <span className={TYPE_COLORS[row.type] ?? 'text-chef-muted'}>{row.type}</span>
              </td>
              <td className="px-4 py-2.5 text-chef-muted">{row.nullable ? 'yes' : 'no'}</td>
              <td className="px-4 py-2.5 font-mono text-chef-muted truncate max-w-[160px]">{row.example}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/* ── Flat sample rows table ──────────────────────────────────────────────── */
function FlatSampleTable({ schema, rows }: { schema: SchemaField[]; rows: Record<string, unknown>[] }) {
  const cols = schema.slice(0, 7).map(f => f.field)
  function cell(v: unknown) {
    if (v === null || v === undefined) return 'null'
    if (typeof v === 'object') return JSON.stringify(v).slice(0, 40)
    return String(v).slice(0, 50)
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-[11px]">
        <thead className="sticky top-0 bg-chef-surface">
          <tr className="border-b border-chef-border">
            {cols.map(c => (
              <th key={c} className="text-left px-4 py-2.5 text-[10px] font-semibold uppercase tracking-wider text-chef-muted whitespace-nowrap font-mono">{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className="border-b border-chef-border hover:bg-chef-card/50 transition-colors">
              {cols.map(c => (
                <td key={c} className="px-4 py-2 font-mono text-chef-text-dim whitespace-nowrap max-w-[140px] truncate">{cell(row[c])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/* ── Seeded run history (realistic, for existing datasets) ───────────────── */
const SEED_RUNS: Record<string, { id: string; status: 'succeeded' | 'failed'; started: string; duration: string; records: string; bytes: string }[]> = {}

/* ── event_type colour ───────────────────────────────────────────────────── */
function eventTypeBadge(t: string) {
  const m: Record<string, string> = {
    purchase: 'bg-emerald-500/10 text-emerald-400',
    refund:   'bg-rose-500/10 text-rose-400',
    error:    'bg-rose-500/10 text-rose-400',
    signup:   'bg-sky-500/10 text-sky-400',
    click:    'bg-amber-500/10 text-amber-400',
    view:     'bg-slate-500/10 text-slate-400',
  }
  return m[t] || 'bg-slate-500/10 text-slate-400'
}

/* ── Main page ───────────────────────────────────────────────────────────── */
export default function DatasetsPage() {
  const router = useRouter()
  const [showWizard, setShowWizard] = useState(false)
  const [showDeleteDialog, setShowDeleteDialog] = useState(false)
  const [deleteBusy, setDeleteBusy] = useState(false)
  const [datasets,   setDatasets]   = useState<DatasetRecord[]>([])
  const [selected,   setSelected]   = useState<DatasetRecord | null>(null)
  const [tab,        setTab]        = useState<Tab>('preview')
  const [search,     setSearch]     = useState('')
  const [loading,    setLoading]    = useState(true)
  const [notice,     setNotice]     = useState<{ ok: boolean; msg: string } | null>(null)
  const [visibleStoredRows, setVisibleStoredRows] = useState(DATASET_PREVIEW_PAGE_SIZE)
  const [visibleLiveRows, setVisibleLiveRows] = useState(LIVE_PREVIEW_PAGE_SIZE)
  const [visibleServerRows, setVisibleServerRows] = useState(LIVE_PREVIEW_PAGE_SIZE)

  /* Rick & Morty live data */
  const [liveChars,   setLiveChars]   = useState<RMCharacter[]>([])
  const [liveLoading, setLiveLoading] = useState(false)
  const [liveFetched, setLiveFetched] = useState(false)

  /* Server-API preview (synthetic events) */
  const [serverPreview,        setServerPreview]        = useState<ServerPreview | null>(null)
  const [serverPreviewLoading, setServerPreviewLoading] = useState(false)
  const serverPreviewFetchedFor = useRef<string | null>(null)

  /* Schema refresh state */
  const [refreshing,     setRefreshing]     = useState(false)
  const [refreshResult,  setRefreshResult]  = useState<{ ok: boolean; msg: string } | null>(null)

  /* Load datasets from server */
  const loadDatasets = useCallback(async () => {
    try {
      const res = await fetch('/api/datasets')
      if (res.ok) setDatasets(await res.json())
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { loadDatasets() }, [loadDatasets])

  useEffect(() => {
    if (!notice) return
    const timer = window.setTimeout(() => setNotice(null), 4000)
    return () => window.clearTimeout(timer)
  }, [notice])

  /* Keep selected in sync after reload */
  useEffect(() => {
    if (selected) {
      const fresh = datasets.find(d => d.id === selected.id)
      if (fresh) setSelected(fresh)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [datasets])

  /* Rick & Morty live fetch */
  useEffect(() => {
    if (selected?.liveType === 'rm-api' && tab === 'preview' && !liveFetched) {
      setLiveLoading(true)
      fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT * FROM "rick-morty-characters" LIMIT 826', lang: 'sql', dataset: 'rick-morty-characters' }),
      })
        .then(r => r.json())
        .then((d: { rows: string[][] }) => {
          // Convert rows back to RMCharacter-like objects for the existing renderer
          const keys = ['id','name','status','species','type','gender','origin','location','episodes','created']
          const chars = (d.rows ?? []).map(row => {
            const obj: Record<string, unknown> = {}
            keys.forEach((k, i) => { obj[k] = row[i] })
            return obj as unknown as RMCharacter
          })
          setLiveChars(chars)
          setLiveFetched(true)
        })
        .catch(() => setLiveFetched(true))
        .finally(() => setLiveLoading(false))
    }
  }, [selected, tab, liveFetched])

  /* Synthetic events preview */
  useEffect(() => {
    if (
      selected?.liveType === 'server-api' &&
      tab === 'preview' &&
      serverPreviewFetchedFor.current !== selected.id
    ) {
      serverPreviewFetchedFor.current = selected.id
      setServerPreviewLoading(true)
      setServerPreview(null)
      fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT * FROM events LIMIT 50', lang: 'sql', dataset: 'events' }),
      })
        .then(r => r.json())
        .then((d: ServerPreview) => setServerPreview(d))
        .finally(() => setServerPreviewLoading(false))
    }
  }, [selected, tab])

  /* Schema refresh */
  async function handleRefresh() {
    if (!selected || refreshing) return
    setRefreshing(true)
    setRefreshResult(null)
    try {
      const res = await fetch(`/api/datasets/${selected.id}/refresh`, { method: 'POST' })
      const data = await res.json()
      if (!res.ok) {
        setRefreshResult({ ok: false, msg: data.error ?? 'Refresh failed' })
      } else {
        setRefreshResult({ ok: true, msg: `Schema refreshed — ${data.schema?.length ?? 0} fields, ${data.totalRows?.toLocaleString() ?? 0} rows` })
        await loadDatasets()
      }
    } catch (e) {
      setRefreshResult({ ok: false, msg: e instanceof Error ? e.message : 'Network error' })
    } finally {
      setRefreshing(false)
    }
  }

  /* Reset refresh state when selected changes */
  useEffect(() => { setRefreshResult(null) }, [selected?.id])

  useEffect(() => {
    setVisibleStoredRows(DATASET_PREVIEW_PAGE_SIZE)
    setVisibleLiveRows(LIVE_PREVIEW_PAGE_SIZE)
    setVisibleServerRows(LIVE_PREVIEW_PAGE_SIZE)
  }, [selected?.id, tab])

  const filtered = datasets.filter(d =>
    d.name.toLowerCase().includes(search.toLowerCase()) ||
    d.format.toLowerCase().includes(search.toLowerCase())
  )
  const canRefreshFromSource = Boolean(selected?.url || selected?.connectorId)
  const selectedSampleRows = selected?.sampleRows ?? []

  const formatBadge = (fmt: string) => {
    const colors: Record<string, string> = {
      JSONL:    'bg-indigo-500/10 text-indigo-400',
      JSON:     'bg-sky-500/10 text-sky-400',
      'JSON-LD':'bg-violet-500/10 text-violet-400',
    }
    return colors[fmt] || 'bg-slate-500/10 text-slate-400'
  }

  const handleQueryLink = (ds: DatasetRecord) => {
    if (!ds.queryDataset) return
    localStorage.setItem('datachef:jumpToDataset', ds.queryDataset)
    router.push('/query')
  }

  const totalRecords = datasets.reduce((s, d) => s + (d.recordsRaw ?? 0), 0)
  const totalRecordsDisplay = totalRecords >= 1_000_000
    ? `~${(totalRecords / 1_000_000).toFixed(1)}M`
    : `~${(totalRecords / 1_000).toFixed(0)}K`

  async function handleDeleteDataset() {
    if (!selected || deleteBusy) return

    try {
      setDeleteBusy(true)
      const dataset = selected
      const res = await fetch(`/api/datasets/${dataset.id}`, { method: 'DELETE' })
      const data = await res.json() as { error?: string; deletedPipelines?: number }
      if (!res.ok) throw new Error(data.error ?? 'Delete failed')

      setDatasets(prev => prev.filter(item => item.id !== dataset.id))
      setSelected(null)
      setShowDeleteDialog(false)
      setNotice({
        ok: true,
        msg: data.deletedPipelines
          ? `Deleted dataset and ${data.deletedPipelines} related pipeline${data.deletedPipelines === 1 ? '' : 's'}`
          : 'Deleted dataset',
      })
    } catch (e) {
      setNotice({ ok: false, msg: e instanceof Error ? e.message : 'Delete failed' })
    } finally {
      setDeleteBusy(false)
    }
  }

  return (
    <div className="flex h-full">
      {/* ── Left: dataset list ── */}
      <div className={`flex flex-col transition-all duration-200 ${selected ? 'w-[420px] shrink-0' : 'flex-1'} border-r border-chef-border`}>
        {/* Header */}
        <div className="px-5 py-4 border-b border-chef-border flex items-center gap-3">
          <Database size={16} className="text-indigo-400 shrink-0" />
          <h2 className="text-sm font-semibold text-chef-text flex-1">All Datasets</h2>
          <button
            onClick={() => setShowWizard(true)}
            className="flex items-center gap-1.5 bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-medium px-3 py-1.5 rounded-lg transition-colors"
          >
            <Plus size={13} /> New Dataset
          </button>
        </div>
        {notice && (
          <div className={`mx-4 mt-3 inline-flex items-center gap-2 rounded-lg border px-3 py-1.5 text-[11px] ${
            notice.ok
              ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300'
              : 'border-rose-500/20 bg-rose-500/10 text-rose-300'
          }`}>
            {notice.ok ? <CheckCircle2 size={12} /> : <AlertCircle size={12} />}
            <span>{notice.msg}</span>
          </div>
        )}

        {/* Search */}
        <div className="px-4 py-3 border-b border-chef-border">
          <div className="flex items-center gap-2 bg-chef-bg border border-chef-border rounded-lg px-3 py-1.5">
            <Search size={13} className="text-chef-muted shrink-0" />
            <input
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Filter datasets…"
              className="flex-1 bg-transparent text-xs text-chef-text placeholder:text-chef-muted outline-none"
            />
          </div>
        </div>

        {/* Table */}
        <div className="flex-1 overflow-auto">
          {loading ? (
            <div className="flex items-center justify-center py-16 gap-2 text-chef-muted">
              <Loader2 size={16} className="animate-spin text-indigo-400" />
              <span className="text-xs">Loading datasets…</span>
            </div>
          ) : (
            <table className="w-full">
              <thead className="sticky top-0 bg-chef-surface">
                <tr className="border-b border-chef-border">
                  <th className="text-left px-4 py-2.5 text-[10px] font-semibold uppercase tracking-widest text-chef-muted">Name</th>
                  <th className="text-left px-4 py-2.5 text-[10px] font-semibold uppercase tracking-widest text-chef-muted">Format</th>
                  {!selected && (
                    <>
                      <th className="text-right px-4 py-2.5 text-[10px] font-semibold uppercase tracking-widest text-chef-muted">Records</th>
                      <th className="text-left px-4 py-2.5 text-[10px] font-semibold uppercase tracking-widest text-chef-muted">Schema</th>
                      <th className="text-left px-4 py-2.5 text-[10px] font-semibold uppercase tracking-widest text-chef-muted">Ingested</th>
                    </>
                  )}
                </tr>
              </thead>
              <tbody>
                {filtered.map(ds => (
                  <tr
                    key={ds.id}
                    onClick={() => { setSelected(ds); setTab('preview') }}
                    className={`border-b border-chef-border cursor-pointer transition-colors ${
                      selected?.id === ds.id
                        ? 'bg-indigo-500/5 border-l-2 border-l-indigo-500'
                        : 'hover:bg-chef-card border-l-2 border-l-transparent'
                    }`}
                  >
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-1.5">
                        <span className="text-xs font-mono font-medium text-chef-text">{ds.name}</span>
                        {ds.liveType === 'rm-api' && (
                          <span className="text-[9px] font-mono text-emerald-400 bg-emerald-500/10 px-1 py-0.5 rounded">LIVE</span>
                        )}
                        {ds.liveType === 'server-api' && (
                          <span className="text-[9px] font-mono text-indigo-400 bg-indigo-500/10 px-1 py-0.5 rounded">MEM</span>
                        )}
                      </div>
                      {selected && (
                        <div className="text-[10px] text-chef-muted mt-0.5">{ds.records} records</div>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      <span className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${formatBadge(ds.format)}`}>
                        {ds.format}
                      </span>
                    </td>
                    {!selected && (
                      <>
                        <td className="px-4 py-3 text-right text-xs font-mono text-chef-text">{ds.records}</td>
                        <td className="px-4 py-3">
                          <span className="text-[10px] font-mono text-emerald-400">{ds.schemaVersion}</span>
                        </td>
                        <td className="px-4 py-3">
                          <div className="flex items-center gap-1.5 text-[11px] text-chef-muted">
                            <Clock size={11} /> {ds.lastIngested}
                          </div>
                        </td>
                      </>
                    )}
                  </tr>
                ))}
                {filtered.length === 0 && !loading && (
                  <tr>
                    <td colSpan={5} className="px-4 py-10 text-center text-xs text-chef-muted">No datasets found</td>
                  </tr>
                )}
              </tbody>
            </table>
          )}
        </div>

        {/* Footer stats */}
        <div className="px-4 py-2.5 border-t border-chef-border flex items-center gap-4 text-[10px] text-chef-muted">
          <span>{datasets.length} datasets</span>
          <span>·</span>
          <span>{totalRecordsDisplay} total records</span>
        </div>
      </div>

      {/* ── Right: detail panel ── */}
      {selected && (
        <div className="flex-1 flex flex-col min-w-0 animate-slide-in">
          {/* Detail header */}
          <div className="px-5 py-4 border-b border-chef-border flex items-start gap-3">
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2.5 flex-wrap">
                <span className="font-mono text-sm font-bold text-chef-text">{selected.name}</span>
                <span className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${formatBadge(selected.format)}`}>
                  {selected.format}
                </span>
                <span className="text-[10px] font-mono text-emerald-400">{selected.schemaVersion}</span>
                {selected.liveType === 'rm-api' && (
                  <span className="text-[10px] text-emerald-400 bg-emerald-500/10 px-1.5 py-0.5 rounded-full flex items-center gap-1">
                    <CheckCircle2 size={9} /> Live API
                  </span>
                )}
                {selected.liveType === 'server-api' && (
                  <span className="text-[10px] text-indigo-400 bg-indigo-500/10 px-1.5 py-0.5 rounded-full flex items-center gap-1">
                    <Cpu size={9} /> In-memory
                  </span>
                )}
              </div>
              <div className="text-xs text-chef-muted mt-1">{selected.description}</div>
              <div className="flex items-center gap-3 mt-2 text-[10px] text-chef-muted flex-wrap">
                <span className="flex items-center gap-1"><Database size={10} />{selected.records} records</span>
                <span>·</span>
                <span>{selected.size}</span>
                <span>·</span>
                <span className="flex items-center gap-1"><Clock size={10} />ingested {selected.lastIngested}</span>
                <span>·</span>
                <span>via {selected.connection}</span>
              </div>
              {/* Refresh result banner */}
              {refreshResult && (
                <div className={`mt-2 flex items-center gap-2 text-[11px] px-3 py-1.5 rounded-lg ${
                  refreshResult.ok
                    ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
                    : 'bg-rose-500/10 text-rose-400 border border-rose-500/20'
                }`}>
                  {refreshResult.ok ? <CheckCircle2 size={12} /> : <AlertCircle size={12} />}
                  {refreshResult.msg}
                </div>
              )}
            </div>
            <div className="flex items-center gap-2 shrink-0">
              {selected.queryDataset && (
                <button
                  onClick={() => handleQueryLink(selected)}
                  className="flex items-center gap-1.5 text-[11px] text-indigo-400 border border-indigo-500/30 rounded-lg px-2.5 py-1.5 hover:bg-indigo-500/10 transition-colors"
                >
                  Query <ArrowRight size={11} />
                </button>
              )}
              <button
                onClick={() => setShowDeleteDialog(true)}
                disabled={deleteBusy}
                className="flex items-center gap-1.5 text-[11px] text-rose-400 border border-rose-500/30 rounded-lg px-2.5 py-1.5 hover:bg-rose-500/10 transition-colors disabled:opacity-50"
              >
                <X size={11} /> Delete
              </button>
              <button
                onClick={handleRefresh}
                disabled={refreshing}
                title="Re-fetch schema from source"
                className="p-1.5 text-chef-muted hover:text-chef-text hover:bg-chef-card rounded-lg transition-colors disabled:opacity-50"
              >
                <RefreshCw size={13} className={refreshing ? 'animate-spin' : ''} />
              </button>
              <button className="p-1.5 text-chef-muted hover:text-chef-text hover:bg-chef-card rounded-lg transition-colors">
                <Download size={13} />
              </button>
              <button
                onClick={() => setSelected(null)}
                className="p-1.5 text-chef-muted hover:text-chef-text hover:bg-chef-card rounded-lg transition-colors"
              >
                <X size={13} />
              </button>
            </div>
          </div>

          {/* Tabs */}
          <div className="flex items-center gap-0 px-5 border-b border-chef-border">
            {(['preview', 'schema', 'runs'] as Tab[]).map(t => (
              <button
                key={t}
                onClick={() => setTab(t)}
                className={`px-4 py-3 text-xs font-medium capitalize transition-colors border-b-2 -mb-px ${
                  tab === t
                    ? 'text-indigo-400 border-indigo-500'
                    : 'text-chef-muted border-transparent hover:text-chef-text'
                }`}
              >
                {t}
              </button>
            ))}
          </div>

          {/* Tab content */}
          <div className="flex-1 overflow-auto">

            {/* ── PREVIEW: Rick & Morty live (server-side) ── */}
            {tab === 'preview' && selected.liveType === 'rm-api' && (
              <div>
                <div className="px-5 py-3 border-b border-chef-border flex items-center gap-2 text-[10px]">
                  <Table size={11} className="text-chef-muted" />
                  {liveLoading ? (
                    <span className="text-indigo-400 flex items-center gap-1.5">
                      <Loader2 size={10} className="animate-spin" />
                      Fetching from rickandmortyapi.com (server-side)…
                    </span>
                  ) : (
                    <span className="text-chef-muted">
                      Showing {Math.min(visibleLiveRows, liveChars.length)} of {liveChars.length} records · live · 10 columns
                    </span>
                  )}
                  <span className="ml-auto flex items-center gap-1 text-emerald-400">
                    <CheckCircle2 size={11} /> Live API · no CORS
                  </span>
                </div>
                {liveLoading ? (
                  <div className="flex items-center justify-center py-20 text-chef-muted gap-2">
                    <Loader2 size={18} className="animate-spin text-indigo-400" />
                    <span className="text-sm">Loading characters…</span>
                  </div>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead className="sticky top-0 bg-chef-surface">
                        <tr className="border-b border-chef-border">
                          {['id','name','status','species','type','gender','origin','location','episodes','created'].map(col => (
                            <th key={col} className="text-left px-4 py-2.5 text-[10px] font-semibold uppercase tracking-wider text-chef-muted whitespace-nowrap font-mono">
                              {col}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {liveChars.slice(0, visibleLiveRows).map((c, i) => (
                          <tr key={i} className="border-b border-chef-border hover:bg-chef-card/50 transition-colors">
                            <td className="px-4 py-2 text-[11px] font-mono text-orange-300">{String(c.id ?? '')}</td>
                            <td className="px-4 py-2 text-[11px] font-mono text-indigo-300 whitespace-nowrap">{String(c.name ?? '')}</td>
                            <td className="px-4 py-2 text-[11px] font-mono">
                              <span className={`px-1.5 py-0.5 rounded text-[10px] ${
                                String(c.status) === 'Alive' ? 'bg-emerald-500/10 text-emerald-400' :
                                String(c.status) === 'Dead'  ? 'bg-rose-500/10 text-rose-400' :
                                'bg-slate-500/10 text-slate-400'
                              }`}>{String(c.status ?? '')}</span>
                            </td>
                            <td className="px-4 py-2 text-[11px] font-mono text-sky-300 whitespace-nowrap">{String(c.species ?? '')}</td>
                            <td className="px-4 py-2 text-[11px] font-mono text-chef-muted">{String((c as unknown as Record<string,unknown>).type ?? '')}</td>
                            <td className="px-4 py-2 text-[11px] font-mono text-chef-muted">{String(c.gender ?? '')}</td>
                            <td className="px-4 py-2 text-[11px] font-mono text-violet-300 whitespace-nowrap max-w-[120px] truncate">{String(c.origin?.name ?? c.origin ?? '')}</td>
                            <td className="px-4 py-2 text-[11px] font-mono text-chef-muted whitespace-nowrap max-w-[120px] truncate">{String(c.location?.name ?? c.location ?? '')}</td>
                            <td className="px-4 py-2 text-[11px] font-mono text-amber-300 text-right">{String((c as unknown as Record<string,unknown>).episodes ?? c.episode?.length ?? '')}</td>
                            <td className="px-4 py-2 text-[11px] font-mono text-chef-muted whitespace-nowrap">{String(c.created ?? '').split('T')[0]}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {liveChars.length > LIVE_PREVIEW_PAGE_SIZE && (
                      <div className="px-5 py-3 text-[10px] text-chef-muted border-t border-chef-border flex items-center gap-3">
                        <span>Showing {Math.min(visibleLiveRows, liveChars.length)} of {liveChars.length} records.</span>
                        {visibleLiveRows < liveChars.length && (
                          <button
                            onClick={() => setVisibleLiveRows(prev => Math.min(prev + LIVE_PREVIEW_PAGE_SIZE, liveChars.length))}
                            className="text-indigo-400 hover:underline"
                          >
                            Load more
                          </button>
                        )}
                        <button onClick={() => handleQueryLink(selected)} className="text-indigo-400 hover:underline">
                          Open in Query Editor →
                        </button>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}

            {/* ── PREVIEW: synthetic events (server-side /api/query) ── */}
            {tab === 'preview' && selected.liveType === 'server-api' && (
              <div>
                <div className="px-5 py-3 border-b border-chef-border flex items-center gap-2 text-[10px]">
                  <Table size={11} className="text-chef-muted" />
                  {serverPreviewLoading ? (
                    <span className="text-indigo-400 flex items-center gap-1.5">
                      <Loader2 size={10} className="animate-spin" />
                      Scanning 500K events via /api/query…
                    </span>
                  ) : serverPreview ? (
                    <span className="text-chef-muted">
                      Showing {serverPreview.rowCount} of {selected.records} records · {serverPreview.durationMs}ms
                    </span>
                  ) : null}
                  <span className="ml-auto flex items-center gap-1 text-indigo-400">
                    <Cpu size={11} /> Server-side · no CORS
                  </span>
                </div>
                {serverPreviewLoading ? (
                  <div className="flex items-center justify-center py-20 text-chef-muted gap-2">
                    <Loader2 size={18} className="animate-spin text-indigo-400" />
                    <span className="text-sm">Loading preview…</span>
                  </div>
                ) : serverPreview ? (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead className="sticky top-0 bg-chef-surface">
                        <tr className="border-b border-chef-border">
                          {serverPreview.columns.map(col => (
                            <th key={col} className="text-left px-4 py-2.5 text-[10px] font-semibold uppercase tracking-wider text-chef-muted whitespace-nowrap font-mono">
                              {col}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {serverPreview.rows.slice(0, visibleServerRows).map((row, i) => (
                          <tr key={i} className="border-b border-chef-border hover:bg-chef-card/50 transition-colors">
                            {serverPreview.columns.map((col, j) => (
                              <td key={col} className={`px-4 py-2 text-[11px] font-mono whitespace-nowrap ${
                                col === 'id'         ? 'text-orange-300' :
                                col === 'user_id'    ? 'text-sky-300' :
                                col === 'amount'     ? 'text-emerald-300 text-right' :
                                col === 'country'    ? 'text-amber-300' :
                                col === 'device'     ? 'text-violet-300' :
                                col === 'ts'         ? 'text-chef-muted' :
                                'text-chef-text'
                              }`}>
                                {col === 'event_type' ? (
                                  <span className={`px-1.5 py-0.5 rounded text-[10px] ${eventTypeBadge(row[j])}`}>
                                    {row[j]}
                                  </span>
                                ) : row[j]}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    <div className="px-5 py-3 text-[10px] text-chef-muted border-t border-chef-border flex items-center gap-2">
                      <span>Showing {Math.min(visibleServerRows, serverPreview.rows.length)} of {serverPreview.rows.length} preview rows — served from /api/query server-side.</span>
                      {visibleServerRows < serverPreview.rows.length && (
                        <button
                          onClick={() => setVisibleServerRows(prev => Math.min(prev + LIVE_PREVIEW_PAGE_SIZE, serverPreview.rows.length))}
                          className="text-indigo-400 hover:underline"
                        >
                          Load more
                        </button>
                      )}
                      <button onClick={() => handleQueryLink(selected)} className="text-indigo-400 hover:underline flex items-center gap-1">
                        Query all 500K <ArrowRight size={10} />
                      </button>
                    </div>
                  </div>
                ) : null}
              </div>
            )}

            {/* ── PREVIEW: HTTP dataset with stored sample rows ── */}
            {tab === 'preview' && !selected.liveType && selectedSampleRows.length > 0 && selected.schema && (
              <div>
                <div className="px-5 py-3 border-b border-chef-border flex items-center gap-2 text-[10px] text-chef-muted">
                  <Table size={11} />
                  <span>
                    Showing {Math.min(visibleStoredRows, selectedSampleRows.length)} of {selectedSampleRows.length} loaded rows · {selected.schema.length} fields · fetched server-side
                  </span>
                  <span className="ml-auto flex items-center gap-1 text-emerald-400">
                    <CheckCircle2 size={11} /> Real data
                  </span>
                </div>
                <FlatSampleTable schema={selected.schema} rows={selectedSampleRows.slice(0, visibleStoredRows)} />
                <div className="px-5 py-3 text-[10px] text-chef-muted border-t border-chef-border flex items-center gap-3">
                  <span>Showing {Math.min(visibleStoredRows, selectedSampleRows.length)} of {selected.totalRows?.toLocaleString() ?? '?'} rows fetched from source.</span>
                  {visibleStoredRows < selectedSampleRows.length && (
                    <button
                      onClick={() => setVisibleStoredRows(prev => Math.min(prev + DATASET_PREVIEW_PAGE_SIZE, selectedSampleRows.length))}
                      className="text-indigo-400 hover:underline"
                    >
                      Load more
                    </button>
                  )}
                  <button onClick={handleRefresh} className="text-indigo-400 hover:underline">
                    Refresh from source →
                  </button>
                </div>
              </div>
            )}

            {/* ── PREVIEW: no live data available ── */}
            {tab === 'preview' && !selected.liveType && selectedSampleRows.length === 0 && (
              <div className="flex flex-col items-center justify-center py-20 gap-4 text-center px-8">
                <div className="w-12 h-12 rounded-xl bg-chef-card border border-chef-border flex items-center justify-center">
                  <Table size={20} className="text-chef-muted" />
                </div>
                <div>
                  <div className="text-sm font-semibold text-chef-text mb-1">No live preview available</div>
                  <div className="text-xs text-chef-muted leading-relaxed max-w-xs">
                    {selected.source === 'http' || selected.url || selected.connectorId
                      ? 'Click Refresh to fetch real sample data from the linked source server-side.'
                      : `Live preview for ${selected.source} sources requires an active connection. Connect this source to fetch data.`
                    }
                  </div>
                </div>
                {canRefreshFromSource && (
                  <button
                    onClick={handleRefresh}
                    disabled={refreshing}
                    className="flex items-center gap-2 text-xs bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-2 rounded-lg transition-colors disabled:opacity-50"
                  >
                    {refreshing ? <><Loader2 size={12} className="animate-spin" /> Fetching…</> : <><RefreshCw size={12} /> Fetch from Source</>}
                  </button>
                )}
              </div>
            )}

            {/* ── SCHEMA TAB ── */}
            {tab === 'schema' && (
              <div className="p-5">
                {selected.schema && selected.schema.length > 0 ? (
                  <>
                    <div className="flex items-center justify-between mb-4">
                      <div>
                        <div className="text-sm font-semibold text-chef-text">Inferred Schema</div>
                        <div className="text-[11px] text-chef-muted mt-0.5">
                          {selected.schemaVersion} · {selected.schema.length} fields · {selected.schema.filter(f => !f.nullable).length} required
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="text-[10px] text-emerald-400 bg-emerald-500/10 px-2 py-1 rounded-full">
                          {selected.liveType || selected.sampleRows ? 'Inferred' : 'Declared'}
                        </span>
                        <button
                          onClick={handleRefresh}
                          disabled={refreshing || !canRefreshFromSource}
                          title={canRefreshFromSource ? 'Re-infer schema from source' : 'No source connected for refresh'}
                          className="text-[11px] text-chef-muted hover:text-chef-text border border-chef-border rounded-md px-2.5 py-1 transition-colors flex items-center gap-1.5 disabled:opacity-40"
                        >
                          <RefreshCw size={11} className={refreshing ? 'animate-spin' : ''} />
                          {refreshing ? 'Refreshing…' : 'Refresh'}
                        </button>
                      </div>
                    </div>
                    <FlatSchemaTable schema={selected.schema} />
                  </>
                ) : (
                  <div className="flex flex-col items-center justify-center py-16 gap-4 text-center">
                    <Database size={28} className="text-chef-muted" />
                    <div>
                      <div className="text-sm font-semibold text-chef-text mb-1">Schema not available</div>
                      <div className="text-xs text-chef-muted">
                        {canRefreshFromSource
                          ? 'Click Refresh to infer the schema by fetching the linked source.'
                          : 'Connect this source to infer its schema.'}
                      </div>
                    </div>
                    {canRefreshFromSource && (
                      <button
                        onClick={handleRefresh}
                        disabled={refreshing}
                        className="flex items-center gap-2 text-xs bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-2 rounded-lg transition-colors disabled:opacity-50"
                      >
                        {refreshing ? <><Loader2 size={12} className="animate-spin" /> Inferring…</> : <><RefreshCw size={12} /> Infer from Source</>}
                      </button>
                    )}
                  </div>
                )}
              </div>
            )}

            {/* ── RUNS TAB ── */}
            {tab === 'runs' && (
              <div>
                <div className="px-5 py-3 border-b border-chef-border flex items-center gap-2 text-[10px] text-chef-muted">
                  <GitBranch size={11} />
                  <span>Last 30 ingestion runs</span>
                  <button className="ml-auto flex items-center gap-1.5 bg-indigo-600 hover:bg-indigo-500 text-white text-xs px-2.5 py-1 rounded-md transition-colors">
                    <Upload size={11} /> Trigger Run
                  </button>
                </div>
                {SEED_RUNS[selected.id] ? (
                  <table className="w-full">
                    <thead className="sticky top-0 bg-chef-surface">
                      <tr className="border-b border-chef-border">
                        <th className="text-left px-5 py-2.5 text-[10px] font-semibold uppercase tracking-wider text-chef-muted">Run ID</th>
                        <th className="text-left px-5 py-2.5 text-[10px] font-semibold uppercase tracking-wider text-chef-muted">Status</th>
                        <th className="text-left px-5 py-2.5 text-[10px] font-semibold uppercase tracking-wider text-chef-muted">Started</th>
                        <th className="text-left px-5 py-2.5 text-[10px] font-semibold uppercase tracking-wider text-chef-muted">Duration</th>
                        <th className="text-right px-5 py-2.5 text-[10px] font-semibold uppercase tracking-wider text-chef-muted">Records</th>
                        <th className="text-right px-5 py-2.5 text-[10px] font-semibold uppercase tracking-wider text-chef-muted">Bytes In</th>
                      </tr>
                    </thead>
                    <tbody>
                      {SEED_RUNS[selected.id].map(r => (
                        <tr key={r.id} className="border-b border-chef-border hover:bg-chef-card/50 transition-colors">
                          <td className="px-5 py-3 text-[11px] font-mono text-indigo-300">{r.id}</td>
                          <td className="px-5 py-3"><StatusBadge status={r.status} /></td>
                          <td className="px-5 py-3 text-[11px] text-chef-muted font-mono">{r.started}</td>
                          <td className="px-5 py-3 text-[11px] text-chef-text font-mono">{r.duration}</td>
                          <td className="px-5 py-3 text-[11px] font-mono text-right text-chef-text">{r.records}</td>
                          <td className="px-5 py-3 text-[11px] font-mono text-right text-chef-muted">{r.bytes}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <div className="flex flex-col items-center justify-center py-16 gap-3 text-center text-chef-muted">
                    <GitBranch size={24} />
                    <div className="text-xs">No run history for this dataset yet.</div>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      )}

      {/* New Dataset Wizard */}
      {showWizard && (
        <NewDatasetWizard
          onClose={() => setShowWizard(false)}
          onCreated={() => {
            setShowWizard(false)
            loadDatasets()
          }}
        />
      )}

      <ConfirmDialog
        open={showDeleteDialog && !!selected}
        title={selected ? `Delete ${selected.name}?` : 'Delete dataset?'}
        description="This removes the dataset from the workspace."
        details={selected ? [
          selected.connectorId ? 'The source connector will remain, but this dataset materialization will be deleted.' : 'This dataset will be deleted immediately.',
          'Pipelines that read from or write to this dataset will also be deleted.',
        ] : []}
        confirmLabel="Delete dataset"
        tone="danger"
        busy={deleteBusy}
        onCancel={() => {
          if (deleteBusy) return
          setShowDeleteDialog(false)
        }}
        onConfirm={() => void handleDeleteDataset()}
      />
    </div>
  )
}
