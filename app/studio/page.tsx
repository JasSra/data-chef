'use client'

import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import {
  RefreshCw, Play, ChevronRight, ChevronDown, Search, X,
  Table2, Eye, Code2, GitBranch, Layers, Database,
  MessageSquare, Radio, Zap, AlertCircle, Loader2,
  Copy, Check, KeyRound, Trash2, Download, Info,
  Terminal, Eraser, Pencil, FileJson, GripHorizontal,
  Send, Activity, Clock, BarChart2, Hash, List, Star,
} from 'lucide-react'

// ─── Types ────────────────────────────────────────────────────────────────────

interface Connector { id: string; name: string; type: string; status?: string }

interface CatalogResult {
  columns: string[]; rows: string[][]
  rowCount: number; totalRows: number; durationMs?: number; error?: string
  capabilities?: Record<string, unknown>
}

interface DataResult {
  columns: string[]; rows: string[][]
  rowCount: number; totalRows: number; durationMs?: number; error?: string
}

interface QueryLogEntry {
  id: string
  sql: string
  type: 'SELECT' | 'UPDATE' | 'DELETE' | 'INSERT' | 'DDL' | 'OTHER'
  status: 'ok' | 'error'
  durationMs?: number
  rowCount?: number
  rowsAffected?: number
  error?: string
  timestamp: Date
}

interface QueueMessage {
  id: string
  timestamp: string
  topic?: string
  queue?: string
  payload: string
  qos?: string
  retained?: string
}

// ─── Constants ────────────────────────────────────────────────────────────────

const SQL_TYPES = new Set(['postgresql', 'mysql', 'mssql'])
const QUEUE_TYPES = new Set(['rabbitmq', 'mqtt'])
const REDIS_TYPES = new Set(['redis'])
const SUPPORTED_TYPES = new Set([...SQL_TYPES, ...QUEUE_TYPES, ...REDIS_TYPES])

const isSql = (t: string) => SQL_TYPES.has(t)
const isQueue = (t: string) => QUEUE_TYPES.has(t)
const isRedis = (t: string) => REDIS_TYPES.has(t)

const SQL_KEYWORDS = [
  'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN',
  'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET', 'INSERT', 'UPDATE',
  'DELETE', 'CREATE', 'DROP', 'ALTER', 'WITH', 'AS', 'ON', 'AND', 'OR',
  'NOT', 'IN', 'IS', 'NULL', 'LIKE', 'BETWEEN', 'EXISTS', 'DISTINCT',
  'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'CAST', 'COALESCE', 'CASE', 'WHEN',
  'THEN', 'ELSE', 'END', 'TOP', 'FETCH', 'NEXT', 'ROWS', 'ONLY',
]

const REDIS_COMMANDS = [
  'SCAN', 'GET', 'MGET', 'HGETALL', 'HMGET', 'LRANGE', 'SMEMBERS', 'ZRANGE',
  'XRANGE', 'XREAD', 'XINFO', 'TYPE', 'EXISTS', 'TTL', 'PTTL', 'RANDOMKEY',
  'INFO', 'MEMORY STATS', 'FT.SEARCH', 'FT._LIST', 'JSON.GET', 'TS.RANGE',
  'PUBSUB CHANNELS',
]

const COL_TYPES = ['VARCHAR', 'NVARCHAR', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT',
  'DECIMAL', 'FLOAT', 'BIT', 'BOOLEAN', 'TEXT', 'NTEXT', 'DATE', 'DATETIME',
  'DATETIME2', 'TIMESTAMP', 'UNIQUEIDENTIFIER', 'UUID', 'JSON', 'CHAR', 'NCHAR']

const TYPE_ICON: Record<string, React.ReactNode> = {
  postgresql: <Database size={13} className="text-sky-400" />,
  mysql: <Database size={13} className="text-orange-400" />,
  mssql: <Database size={13} className="text-blue-400" />,
  rabbitmq: <MessageSquare size={13} className="text-amber-400" />,
  mqtt: <Radio size={13} className="text-emerald-400" />,
  redis: <Zap size={13} className="text-rose-400" />,
}

const LOG_TYPE_COLOR: Record<QueryLogEntry['type'], string> = {
  SELECT: 'text-sky-400',
  UPDATE: 'text-amber-400',
  DELETE: 'text-rose-400',
  INSERT: 'text-emerald-400',
  DDL: 'text-violet-400',
  OTHER: 'text-chef-muted',
}

// ─── Module-level helpers ─────────────────────────────────────────────────────

function wordAtCursor(text: string, pos: number): { word: string; start: number; end: number } {
  let start = pos
  while (start > 0 && /\w/.test(text[start - 1])) start--
  let end = pos
  while (end < text.length && /\w/.test(text[end])) end++
  return { word: text.slice(start, end), start, end }
}

function detectQueryType(sql: string): QueryLogEntry['type'] {
  const s = sql.trim().toUpperCase()
  if (s.startsWith('SELECT')) return 'SELECT'
  if (s.startsWith('UPDATE')) return 'UPDATE'
  if (s.startsWith('DELETE')) return 'DELETE'
  if (s.startsWith('INSERT')) return 'INSERT'
  if (/^(CREATE|DROP|ALTER|TRUNCATE|EXEC|CALL|RENAME)/.test(s)) return 'DDL'
  return 'OTHER'
}

function buildDDL(tableFqn: string, sr: CatalogResult, pkColumns: string[], ct: string): string {
  const cols = sr.columns.map(c => c.toLowerCase())
  const ni = cols.indexOf('column_name')
  const ti = cols.indexOf('data_type')
  const li = cols.findIndex(c => c.includes('maximum_length') || c.includes('char_length'))
  const nuli = cols.indexOf('is_nullable')
  const di = cols.findIndex(c => c.includes('default'))
  const qi = (n: string) => ct === 'mssql' ? `[${n}]` : ct === 'mysql' ? `\`${n}\`` : `"${n}"`
  const colDefs = sr.rows.map(row => {
    const name = row[ni >= 0 ? ni : 0] ?? ''
    const type = (row[ti >= 0 ? ti : 1] ?? '').toUpperCase()
    const len = li >= 0 && row[li] && row[li] !== 'null' ? row[li] : ''
    const nullable = nuli >= 0 ? row[nuli] : 'YES'
    const def = di >= 0 && row[di] && row[di] !== 'null' ? row[di] : ''
    let d = `  ${qi(name)} ${type}`
    if (len) d += `(${len})`
    if (def) d += ` DEFAULT ${def}`
    d += nullable === 'NO' ? ' NOT NULL' : ' NULL'
    return d
  })
  if (pkColumns.length > 0) colDefs.push(`  PRIMARY KEY (${pkColumns.map(qi).join(', ')})`)
  return `CREATE TABLE ${tableFqn} (\n${colDefs.join(',\n')}\n)`
}

function buildInserts(tableFqn: string, columns: string[], rows: string[][], ct: string): string {
  const qi = (n: string) => ct === 'mssql' ? `[${n}]` : ct === 'mysql' ? `\`${n}\`` : `"${n}"`
  const qv = (v: string) => v === '' ? 'NULL' : `'${v.replace(/'/g, "''")}'`
  const colList = columns.map(qi).join(', ')
  return rows.map(row => `INSERT INTO ${tableFqn} (${colList}) VALUES (${row.map(qv).join(', ')});`).join('\n')
}

function downloadText(content: string, filename: string, mime = 'text/plain') {
  const blob = new Blob([content], { type: mime })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url; a.download = filename; a.click()
  URL.revokeObjectURL(url)
}

// ─── Small components ─────────────────────────────────────────────────────────

function Spinner() { return <Loader2 size={14} className="animate-spin text-indigo-400" /> }

function TreeSection({ title, icon, items, selected, onSelect, loading }: {
  title: string; icon: React.ReactNode; items: string[]
  selected: string | null; onSelect: (item: string) => void; loading?: boolean
}) {
  const [open, setOpen] = useState(true)
  const [filter, setFilter] = useState('')
  const filtered = filter ? items.filter(i => i.toLowerCase().includes(filter.toLowerCase())) : items
  return (
    <div className="mb-1">
      <button onClick={() => setOpen(o => !o)} className="flex items-center gap-1.5 w-full px-2 py-1 text-[11px] font-semibold text-chef-muted uppercase tracking-widest hover:text-chef-text transition-colors">
        {open ? <ChevronDown size={10} /> : <ChevronRight size={10} />}
        {icon}
        <span className="flex-1 text-left">{title}</span>
        {loading ? <Spinner /> : <span className="font-mono">{items.length}</span>}
      </button>
      {open && (
        <div>
          {items.length > 8 && (
            <div className="px-2 pb-1">
              <div className="flex items-center gap-1 bg-chef-surface rounded px-1.5 py-0.5 border border-chef-border">
                <Search size={10} className="text-chef-muted shrink-0" />
                <input value={filter} onChange={e => setFilter(e.target.value)} placeholder="filter…" className="bg-transparent text-[11px] text-chef-text placeholder:text-chef-muted outline-none flex-1 min-w-0" />
              </div>
            </div>
          )}
          {filtered.map(item => (
            <button key={item} onClick={() => onSelect(item)} className={`w-full text-left px-3 py-1 text-[12px] truncate rounded transition-colors ${selected === item ? 'bg-indigo-500/15 text-indigo-300' : 'text-chef-muted hover:text-chef-text hover:bg-white/[0.04]'}`}>
              {item}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ─── DataGrid ─────────────────────────────────────────────────────────────────

interface DataGridProps {
  columns: string[]; rows: string[][]; filter: string
  pkColumns?: string[]; tableFqn?: string; connectorType?: string
  exportName?: string
  onExecute?: (sql: string) => Promise<{ rowsAffected?: number; totalRows?: number; rowCount?: number; error?: string; durationMs?: number }>
  onRefresh?: () => void
}

function DataGrid({ columns, rows, filter, pkColumns = [], tableFqn, connectorType, exportName, onExecute, onRefresh }: DataGridProps) {
  const [sortCol, setSortCol] = useState<number | null>(null)
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc')
  const [selectedIdxs, setSelectedIdxs] = useState<Set<number>>(new Set())
  const [editCell, setEditCell] = useState<{ ri: number; ci: number } | null>(null)
  const [editValue, setEditValue] = useState('')
  const [executing, setExecuting] = useState(false)
  const [execMsg, setExecMsg] = useState<{ ok: boolean; msg: string } | null>(null)
  const [copied, setCopied] = useState<number | null>(null)

  useEffect(() => { setSelectedIdxs(new Set()); setEditCell(null); setSortCol(null) }, [rows])

  const filteredRows = useMemo(() => {
    if (!filter.trim()) return rows
    const q = filter.toLowerCase()
    return rows.filter(row => row.some(cell => cell.toLowerCase().includes(q)))
  }, [rows, filter])

  const sortedRows = useMemo(() => {
    if (sortCol === null) return filteredRows
    return [...filteredRows].sort((a, b) => {
      const va = a[sortCol] ?? '', vb = b[sortCol] ?? ''
      const na = Number(va), nb = Number(vb)
      const cmp = !isNaN(na) && !isNaN(nb) && va !== '' && vb !== '' ? na - nb : va < vb ? -1 : va > vb ? 1 : 0
      return sortDir === 'asc' ? cmp : -cmp
    })
  }, [filteredRows, sortCol, sortDir])

  function toggleSort(ci: number) {
    if (sortCol === ci) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortCol(ci); setSortDir('asc') }
    setSelectedIdxs(new Set())
  }
  function toggleRow(ri: number) {
    setSelectedIdxs(prev => { const n = new Set(prev); n.has(ri) ? n.delete(ri) : n.add(ri); return n })
  }
  function toggleAll() {
    if (selectedIdxs.size === sortedRows.length) setSelectedIdxs(new Set())
    else setSelectedIdxs(new Set(sortedRows.map((_, i) => i)))
  }

  const qi = (n: string) => connectorType === 'mssql' ? `[${n}]` : connectorType === 'mysql' ? `\`${n}\`` : `"${n}"`
  const qv = (v: string) => `'${v.replace(/'/g, "''")}'`

  function buildWhere(row: string[]) {
    return pkColumns.map(pk => {
      const ci = columns.findIndex(c => c.toLowerCase() === pk.toLowerCase())
      return ci >= 0 ? `${qi(columns[ci])} = ${qv(row[ci])}` : null
    }).filter(Boolean).join(' AND ')
  }

  function showMsg(ok: boolean, msg: string) {
    setExecMsg({ ok, msg }); setTimeout(() => setExecMsg(null), 3500)
  }

  async function saveEdit() {
    if (!editCell) return
    const row = sortedRows[editCell.ri]
    const colName = columns[editCell.ci]
    const originalVal = row[editCell.ci]
    setEditCell(null)
    if (!onExecute || !tableFqn || pkColumns.length === 0 || editValue === originalVal) return
    const where = buildWhere(row)
    if (!where) return
    setExecuting(true)
    const result = await onExecute(`UPDATE ${tableFqn} SET ${qi(colName)} = ${qv(editValue)} WHERE ${where}`)
    setExecuting(false)
    if (result.error) showMsg(false, result.error)
    else { showMsg(true, '1 row updated'); onRefresh?.() }
  }

  async function deleteSelected() {
    if (!onExecute || !tableFqn || pkColumns.length === 0 || selectedIdxs.size === 0) return
    if (!confirm(`Delete ${selectedIdxs.size} row(s)? This cannot be undone.`)) return
    const pkIdx = columns.findIndex(c => pkColumns.some(pk => pk.toLowerCase() === c.toLowerCase()))
    if (pkIdx < 0) { showMsg(false, 'No primary key column found'); return }
    const vals = [...selectedIdxs].map(ri => qv(sortedRows[ri][pkIdx])).join(', ')
    setExecuting(true)
    const result = await onExecute(`DELETE FROM ${tableFqn} WHERE ${qi(columns[pkIdx])} IN (${vals})`)
    setExecuting(false); setSelectedIdxs(new Set())
    if (result.error) showMsg(false, result.error)
    else { showMsg(true, `${result.rowsAffected ?? result.totalRows ?? selectedIdxs.size} row(s) deleted`); onRefresh?.() }
  }

  function exportCsv() {
    const esc = (v: string) => `"${v.replace(/"/g, '""')}"`
    const lines = [columns.map(esc).join(','), ...sortedRows.map(row => row.map(esc).join(','))]
    const blob = new Blob([lines.join('\n')], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url; a.download = `${(exportName ?? 'export').replace(/[^\w.-]/g, '_')}.csv`; a.click()
    URL.revokeObjectURL(url)
  }

  function copyRow(ri: number) {
    const obj = Object.fromEntries(columns.map((col, ci) => [col, sortedRows[ri][ci]]))
    navigator.clipboard.writeText(JSON.stringify(obj, null, 2))
    setCopied(ri); setTimeout(() => setCopied(null), 1500)
  }

  if (!columns.length) return null
  const canEdit = !!onExecute && !!tableFqn && pkColumns.length > 0

  return (
    <div className="flex flex-col flex-1 min-h-0 overflow-hidden">
      <div className="flex items-center gap-2 px-3 py-1 border-b border-chef-border/50 bg-chef-surface/50 shrink-0 text-[11px]">
        <span className="text-chef-muted font-mono">
          {filteredRows.length !== rows.length ? `${filteredRows.length} / ${rows.length} rows` : `${sortedRows.length} rows`}
        </span>
        {selectedIdxs.size > 0 && <span className="text-indigo-400 font-mono">{selectedIdxs.size} selected</span>}
        <div className="flex-1" />
        {executing && <Loader2 size={12} className="animate-spin text-indigo-400" />}
        {execMsg && <span className={`font-mono ${execMsg.ok ? 'text-emerald-400' : 'text-rose-400'}`}>{execMsg.msg}</span>}
        {canEdit && selectedIdxs.size > 0 && (
          <button onClick={deleteSelected} className="flex items-center gap-1 px-2 py-0.5 bg-rose-500/10 hover:bg-rose-500/20 text-rose-400 rounded transition-colors">
            <Trash2 size={11} />Delete {selectedIdxs.size}
          </button>
        )}
        {!canEdit && pkColumns.length === 0 && isSql(connectorType ?? '') && (
          <span className="text-chef-muted/40 text-[10px]">no PK — editing disabled</span>
        )}
        <button onClick={exportCsv} className="flex items-center gap-1 px-2 py-0.5 bg-chef-card hover:bg-white/[0.06] text-chef-muted hover:text-chef-text rounded border border-chef-border/50 transition-colors">
          <Download size={11} />CSV
        </button>
      </div>
      <div className="overflow-auto flex-1 text-[11px] font-mono">
        <table className="w-full border-collapse">
          <thead className="sticky top-0 z-10">
            <tr className="bg-chef-surface border-b border-chef-border">
              <th className="w-8 px-2 text-center shrink-0">
                <input type="checkbox" checked={sortedRows.length > 0 && selectedIdxs.size === sortedRows.length} onChange={toggleAll} className="accent-indigo-500 cursor-pointer" />
              </th>
              <th className="w-6 px-1" />
              {columns.map((col, ci) => (
                <th key={col} onClick={() => toggleSort(ci)} className="px-2 py-1.5 text-left text-chef-muted font-semibold whitespace-nowrap border-r border-chef-border/50 last:border-r-0 cursor-pointer hover:text-chef-text hover:bg-white/[0.03] select-none">
                  <span className="flex items-center gap-1">
                    {col}
                    {sortCol === ci && <span className="text-indigo-400 font-normal">{sortDir === 'asc' ? '↑' : '↓'}</span>}
                    {pkColumns.some(pk => pk.toLowerCase() === col.toLowerCase()) && <KeyRound size={9} className="text-amber-400 shrink-0" />}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row, ri) => (
              <tr key={ri} onClick={() => toggleRow(ri)} className={`border-b border-chef-border/30 group cursor-pointer transition-colors ${selectedIdxs.has(ri) ? 'bg-indigo-500/10' : 'hover:bg-white/[0.03]'}`}>
                <td className="px-2 text-center" onClick={e => e.stopPropagation()}>
                  <input type="checkbox" checked={selectedIdxs.has(ri)} onChange={() => toggleRow(ri)} className="accent-indigo-500 cursor-pointer" />
                </td>
                <td className="px-1 opacity-0 group-hover:opacity-100 transition-opacity" onClick={e => e.stopPropagation()}>
                  <button onClick={() => copyRow(ri)} className="text-chef-muted hover:text-chef-text" title="Copy row as JSON">
                    {copied === ri ? <Check size={10} className="text-emerald-400" /> : <Copy size={10} />}
                  </button>
                </td>
                {row.map((cell, ci) => (
                  <td key={ci} onDoubleClick={canEdit ? e => { e.stopPropagation(); setEditCell({ ri, ci }); setEditValue(cell) } : undefined} className={`px-2 py-1 whitespace-nowrap max-w-[220px] border-r border-chef-border/20 last:border-r-0 ${canEdit ? 'cursor-text' : ''} ${cell === '' ? 'text-chef-muted/40' : 'text-chef-text'}`} title={canEdit ? 'Double-click to edit' : cell}>
                    {editCell?.ri === ri && editCell?.ci === ci ? (
                      <input autoFocus value={editValue} onChange={e => setEditValue(e.target.value)} onBlur={saveEdit} onKeyDown={e => { if (e.key === 'Enter') saveEdit(); if (e.key === 'Escape') setEditCell(null) }} onClick={e => e.stopPropagation()} className="w-full min-w-[100px] bg-indigo-500/10 border border-indigo-500/50 rounded px-1 py-0 outline-none text-chef-text" />
                    ) : (
                      <span className="block truncate">{cell === '' ? <span className="text-chef-muted/30">null</span> : cell}</span>
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {sortedRows.length === 0 && <div className="text-center py-8 text-chef-muted text-xs">No rows match filter</div>}
      </div>
    </div>
  )
}

// ─── Redis Key Viewer ─────────────────────────────────────────────────────────

const REDIS_TYPE_COLOR: Record<string, string> = {
  string: 'text-sky-400 border-sky-500/30 bg-sky-500/5',
  hash: 'text-amber-400 border-amber-500/30 bg-amber-500/5',
  list: 'text-emerald-400 border-emerald-500/30 bg-emerald-500/5',
  set: 'text-violet-400 border-violet-500/30 bg-violet-500/5',
  zset: 'text-rose-400 border-rose-500/30 bg-rose-500/5',
  stream: 'text-indigo-400 border-indigo-500/30 bg-indigo-500/5',
  json: 'text-lime-400 border-lime-500/30 bg-lime-500/5',
  timeseries: 'text-cyan-400 border-cyan-500/30 bg-cyan-500/5',
}

function jsonHighlight(text: string): string {
  if (!text) return ''
  const escaped = text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  return escaped.replace(
    /("(?:[^"\\]|\\.)*"\s*:?|\btrue\b|\bfalse\b|\bnull\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g,
    (m) => {
      if (m.endsWith(':')) return `<span class="text-sky-300">${m}</span>`
      if (m.startsWith('"')) return `<span class="text-emerald-300">${m}</span>`
      if (m === 'true' || m === 'false') return `<span class="text-amber-400">${m}</span>`
      if (m === 'null') return `<span class="text-rose-400">${m}</span>`
      return `<span class="text-violet-400">${m}</span>`
    }
  )
}

function JsonTree({ value }: { value: string }) {
  const [pretty, setPretty] = useState(true)
  let parsed: unknown = null
  let isJson = false
  try { parsed = JSON.parse(value); isJson = true } catch {}
  const display = isJson && pretty ? JSON.stringify(parsed, null, 2) : value
  return (
    <div className="flex flex-col flex-1 min-h-0">
      <div className="flex items-center gap-2 px-3 py-1 border-b border-chef-border/30 shrink-0 text-[11px]">
        <span className="text-lime-400 font-semibold">JSON</span>
        <div className="flex-1" />
        {isJson && (
          <button onClick={() => setPretty(p => !p)} className="text-chef-muted hover:text-chef-text transition-colors">
            {pretty ? 'raw' : 'pretty'}
          </button>
        )}
        <button onClick={() => navigator.clipboard.writeText(display)} className="flex items-center gap-1 text-chef-muted hover:text-chef-text transition-colors">
          <Copy size={10} />copy
        </button>
      </div>
      <div className="flex-1 overflow-auto p-3">
        <pre className="text-[11px] font-mono leading-relaxed"
          dangerouslySetInnerHTML={{ __html: jsonHighlight(display) }} />
      </div>
    </div>
  )
}

function TtlBadge({ ttl }: { ttl: number }) {
  if (ttl === -1) return <span className="text-[10px] text-chef-muted/60 font-mono border border-chef-border/30 rounded px-1 py-0.5">no expiry</span>
  if (ttl === -2) return <span className="text-[10px] text-rose-400 font-mono border border-rose-500/30 rounded px-1 py-0.5">expired</span>
  const h = Math.floor(ttl / 3600), m = Math.floor((ttl % 3600) / 60), s = ttl % 60
  const label = h > 0 ? `${h}h ${m}m` : m > 0 ? `${m}m ${s}s` : `${s}s`
  return <span className="text-[10px] text-amber-400 font-mono border border-amber-500/30 rounded px-1 py-0.5 flex items-center gap-1"><Clock size={9} />{label}</span>
}

function RedisKeyViewer({ keyName, keyType, keyTtl, dataResult, onRefresh }: {
  keyName: string; keyType: string | null; keyTtl: number
  dataResult: DataResult | null; onRefresh?: () => void
}) {
  const [filter, setFilter] = useState('')
  const type = keyType ?? 'unknown'
  const typeColor = REDIS_TYPE_COLOR[type] ?? 'text-chef-muted border-chef-border bg-chef-surface/50'

  if (!dataResult) return <div className="flex-1 flex items-center justify-center text-chef-muted text-xs"><Loader2 size={14} className="animate-spin mr-2" />Loading key…</div>

  const { columns, rows } = dataResult

  // ── Header ──
  const header = (
    <div className="flex items-center gap-2 px-3 py-2 border-b border-chef-border/40 shrink-0">
      <span className={`text-[10px] font-semibold uppercase tracking-wider px-1.5 py-0.5 rounded border font-mono ${typeColor}`}>{type}</span>
      <span className="text-[12px] font-mono text-chef-text truncate max-w-[300px]" title={keyName}>{keyName}</span>
      <TtlBadge ttl={keyTtl} />
      <div className="flex-1" />
      <span className="text-[11px] font-mono text-chef-muted/60">{rows.length} items</span>
      {onRefresh && <button onClick={onRefresh} className="p-1 text-chef-muted hover:text-chef-text rounded transition-colors"><RefreshCw size={11} /></button>}
    </div>
  )

  // ── STRING ──
  if (type === 'string') {
    const value = rows[0]?.[columns.indexOf('value')] ?? rows[0]?.[0] ?? ''
    const isJsonLike = (value.startsWith('{') || value.startsWith('['))
    return (
      <div className="flex flex-col flex-1 min-h-0 overflow-hidden">
        {header}
        {isJsonLike ? (
          <JsonTree value={value} />
        ) : (
          <div className="flex-1 overflow-auto p-4">
            <div className="bg-chef-surface border border-chef-border rounded-lg p-4 group relative">
              <button onClick={() => navigator.clipboard.writeText(value)} className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity text-chef-muted hover:text-chef-text p-1 rounded"><Copy size={12} /></button>
              <pre className="text-[13px] font-mono text-chef-text whitespace-pre-wrap break-all leading-relaxed">{value || <span className="text-chef-muted/40 italic">empty string</span>}</pre>
              <div className="mt-2 pt-2 border-t border-chef-border/30 text-[10px] text-chef-muted/60 font-mono">{value.length} chars · {new Blob([value]).size} bytes</div>
            </div>
          </div>
        )}
      </div>
    )
  }

  // ── HASH ──
  if (type === 'hash') {
    // HGETALL returns one row with many columns; transpose to field/value pairs
    const pairs = columns.map((col, i) => [col, rows[0]?.[i] ?? ''])
    const filtered = filter ? pairs.filter(([f, v]) => f.toLowerCase().includes(filter) || v.toLowerCase().includes(filter)) : pairs
    return (
      <div className="flex flex-col flex-1 min-h-0 overflow-hidden">
        {header}
        <div className="flex items-center gap-2 px-3 py-1.5 border-b border-chef-border/30 shrink-0">
          <Search size={10} className="text-chef-muted shrink-0" />
          <input value={filter} onChange={e => setFilter(e.target.value.toLowerCase())} placeholder="filter fields…" className="flex-1 bg-transparent text-[11px] text-chef-text placeholder:text-chef-muted/50 outline-none" />
          <span className="text-[10px] font-mono text-chef-muted/60">{filtered.length}/{pairs.length} fields</span>
        </div>
        <div className="flex-1 overflow-auto">
          <table className="w-full border-collapse text-[11px] font-mono">
            <thead className="sticky top-0 bg-chef-surface border-b border-chef-border z-10">
              <tr>
                <th className="text-left px-3 py-1.5 text-chef-muted font-semibold w-1/3">field</th>
                <th className="text-left px-3 py-1.5 text-chef-muted font-semibold">value</th>
                <th className="w-6" />
              </tr>
            </thead>
            <tbody>
              {filtered.map(([field, value], i) => {
                const isJson = (value.startsWith('{') || value.startsWith('['))
                return (
                  <tr key={i} className="border-b border-chef-border/20 hover:bg-white/[0.03] group">
                    <td className="px-3 py-1.5 text-amber-400/90 font-semibold">{field}</td>
                    <td className="px-3 py-1.5 text-chef-text max-w-[400px]">
                      {isJson ? <span className="text-lime-400/80 italic text-[10px]">[json] </span> : null}
                      <span className="truncate block" title={value}>{value || <span className="text-chef-muted/30">empty</span>}</span>
                    </td>
                    <td className="px-1 opacity-0 group-hover:opacity-100 transition-opacity">
                      <button onClick={() => navigator.clipboard.writeText(value)} className="text-chef-muted hover:text-chef-text p-0.5 rounded"><Copy size={9} /></button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    )
  }

  // ── LIST ──
  if (type === 'list') {
    const idxCol = columns.indexOf('index'), valCol = columns.indexOf('value') >= 0 ? columns.indexOf('value') : 1
    const filtered = filter ? rows.filter(r => r.some(c => c.toLowerCase().includes(filter))) : rows
    return (
      <div className="flex flex-col flex-1 min-h-0 overflow-hidden">
        {header}
        <div className="flex items-center gap-2 px-3 py-1.5 border-b border-chef-border/30 shrink-0">
          <Search size={10} className="text-chef-muted shrink-0" />
          <input value={filter} onChange={e => setFilter(e.target.value.toLowerCase())} placeholder="filter…" className="flex-1 bg-transparent text-[11px] text-chef-text placeholder:text-chef-muted/50 outline-none" />
        </div>
        <div className="flex-1 overflow-auto">
          {filtered.map((row, ri) => {
            const idx = idxCol >= 0 ? row[idxCol] : String(ri)
            const val = row[valCol] ?? row[0] ?? ''
            return (
              <div key={ri} className="flex items-start gap-2 px-3 py-1.5 border-b border-chef-border/20 hover:bg-white/[0.03] group text-[11px] font-mono">
                <span className="text-emerald-400/60 w-8 text-right shrink-0 text-[10px] mt-0.5">{idx}</span>
                <span className="flex-1 text-chef-text break-all leading-relaxed">{val || <span className="text-chef-muted/30 italic">empty</span>}</span>
                <button onClick={() => navigator.clipboard.writeText(val)} className="opacity-0 group-hover:opacity-100 transition-opacity text-chef-muted hover:text-chef-text p-0.5 rounded shrink-0"><Copy size={9} /></button>
              </div>
            )
          })}
        </div>
      </div>
    )
  }

  // ── SET ──
  if (type === 'set') {
    const valCol = columns.indexOf('value') >= 0 ? columns.indexOf('value') : 0
    const members = rows.map(r => r[valCol] ?? r[0] ?? '')
    const filtered = filter ? members.filter(m => m.toLowerCase().includes(filter)) : members
    return (
      <div className="flex flex-col flex-1 min-h-0 overflow-hidden">
        {header}
        <div className="flex items-center gap-2 px-3 py-1.5 border-b border-chef-border/30 shrink-0">
          <Search size={10} className="text-chef-muted shrink-0" />
          <input value={filter} onChange={e => setFilter(e.target.value.toLowerCase())} placeholder="filter members…" className="flex-1 bg-transparent text-[11px] text-chef-text placeholder:text-chef-muted/50 outline-none" />
        </div>
        <div className="flex-1 overflow-auto p-3">
          <div className="flex flex-wrap gap-1.5">
            {filtered.map((m, i) => (
              <button key={i} onClick={() => navigator.clipboard.writeText(m)} title="Click to copy"
                className="px-2 py-0.5 bg-violet-500/10 hover:bg-violet-500/20 border border-violet-500/20 rounded-full text-[11px] font-mono text-violet-300 transition-colors">
                {m}
              </button>
            ))}
          </div>
          {filtered.length === 0 && <div className="text-center py-8 text-chef-muted/50 text-xs">No members match</div>}
        </div>
      </div>
    )
  }

  // ── ZSET ──
  if (type === 'zset') {
    const memberCol = columns.indexOf('member'), scoreCol = columns.indexOf('score')
    const maxScore = Math.max(...rows.map(r => Number(r[scoreCol] ?? 0)))
    const filtered = filter ? rows.filter(r => r.some(c => c.toLowerCase().includes(filter))) : rows
    return (
      <div className="flex flex-col flex-1 min-h-0 overflow-hidden">
        {header}
        <div className="flex-1 overflow-auto">
          <table className="w-full border-collapse text-[11px] font-mono">
            <thead className="sticky top-0 bg-chef-surface border-b border-chef-border z-10">
              <tr>
                <th className="text-left px-3 py-1.5 text-chef-muted font-semibold w-8">#</th>
                <th className="text-left px-3 py-1.5 text-chef-muted font-semibold">member</th>
                <th className="text-right px-3 py-1.5 text-chef-muted font-semibold w-24">score</th>
                <th className="px-3 py-1.5 w-32" />
              </tr>
            </thead>
            <tbody>
              {filtered.map((row, ri) => {
                const member = row[memberCol >= 0 ? memberCol : 0]
                const score = Number(row[scoreCol >= 0 ? scoreCol : 1] ?? 0)
                const pct = maxScore > 0 ? (score / maxScore) * 100 : 0
                return (
                  <tr key={ri} className="border-b border-chef-border/20 hover:bg-white/[0.03]">
                    <td className="px-3 py-1.5 text-chef-muted/50">{ri + 1}</td>
                    <td className="px-3 py-1.5 text-rose-300">{member}</td>
                    <td className="px-3 py-1.5 text-right text-chef-text">{score}</td>
                    <td className="px-3 py-1.5">
                      <div className="h-1.5 bg-chef-border/30 rounded-full overflow-hidden">
                        <div className="h-full bg-rose-500/60 rounded-full transition-all" style={{ width: `${pct}%` }} />
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    )
  }

  // ── STREAM ──
  if (type === 'stream') {
    const idCol = columns.indexOf('id')
    const fieldCols = columns.filter(c => c !== 'id' && c !== 'key' && c !== 'ttl' && c !== 'type')
    const [expandedId, setExpandedId] = useState<string | null>(null)
    return (
      <div className="flex flex-col flex-1 min-h-0 overflow-hidden">
        {header}
        <div className="flex-1 overflow-auto">
          {rows.map((row, ri) => {
            const entryId = row[idCol >= 0 ? idCol : 0] ?? String(ri)
            const tsMs = entryId.includes('-') ? parseInt(entryId.split('-')[0]) : null
            const tsLabel = tsMs ? new Date(tsMs).toLocaleTimeString('en', { hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 3 }) : entryId
            const isExpanded = expandedId === entryId
            const preview = fieldCols.slice(0, 2).map(f => `${f}=${row[columns.indexOf(f)] ?? ''}`).join(' · ')
            return (
              <div key={ri} className="border-b border-chef-border/20">
                <button onClick={() => setExpandedId(isExpanded ? null : entryId)} className="w-full flex items-center gap-3 px-3 py-2 hover:bg-white/[0.03] text-[11px] font-mono text-left">
                  <span className="text-indigo-400/70 text-[10px] shrink-0">{tsLabel}</span>
                  <span className="text-chef-muted/60 truncate flex-1">{preview}</span>
                  <span className="text-chef-muted/40 text-[10px] shrink-0">{fieldCols.length} fields</span>
                  {isExpanded ? <ChevronDown size={10} className="text-chef-muted shrink-0" /> : <ChevronRight size={10} className="text-chef-muted shrink-0" />}
                </button>
                {isExpanded && (
                  <div className="px-4 pb-2 pt-1 bg-indigo-500/5 border-t border-indigo-500/10">
                    <div className="text-[10px] font-mono text-chef-muted/50 mb-1">Entry ID: {entryId}</div>
                    <div className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5">
                      {fieldCols.map(f => (
                        <React.Fragment key={f}>
                          <span className="text-[11px] font-mono text-indigo-300/80 font-semibold">{f}</span>
                          <span className="text-[11px] font-mono text-chef-text break-all">{row[columns.indexOf(f)] ?? ''}</span>
                        </React.Fragment>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )
          })}
        </div>
      </div>
    )
  }

  // ── JSON ──
  if (type === 'json') {
    // Data is flattened — rebuild a readable display
    const pairs = columns.filter(c => c !== 'key' && c !== 'ttl' && c !== 'type').map((col, i) => [col, rows[0]?.[i] ?? ''])
    return (
      <div className="flex flex-col flex-1 min-h-0 overflow-hidden">
        {header}
        <div className="flex-1 overflow-auto p-3 space-y-1">
          {pairs.map(([k, v], i) => {
            const depth = (k.match(/\./g) ?? []).length
            return (
              <div key={i} className="flex items-start gap-2 text-[11px] font-mono" style={{ paddingLeft: `${depth * 12}px` }}>
                <span className="text-lime-400/80 shrink-0">{k.split('.').pop()}</span>
                <span className="text-chef-muted/50">:</span>
                <span className="text-chef-text break-all">{v || <span className="text-chef-muted/30">null</span>}</span>
              </div>
            )
          })}
        </div>
      </div>
    )
  }

  // ── TIMESERIES ──
  if (type === 'timeseries') {
    const tsCol = columns.indexOf('timestamp'), valCol = columns.indexOf('value') >= 0 ? columns.indexOf('value') : 1
    const points = rows.map(r => ({ ts: Number(r[tsCol] ?? 0), val: Number(r[valCol] ?? 0) }))
    const vals = points.map(p => p.val)
    const minVal = Math.min(...vals), maxVal = Math.max(...vals)
    const range = maxVal - minVal || 1
    const avg = vals.reduce((a, b) => a + b, 0) / (vals.length || 1)
    // SVG sparkline
    const W = 400, H = 80, pad = 4
    const px = (i: number) => pad + (i / Math.max(points.length - 1, 1)) * (W - pad * 2)
    const py = (v: number) => pad + (1 - (v - minVal) / range) * (H - pad * 2)
    const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${px(i).toFixed(1)},${py(p.val).toFixed(1)}`).join(' ')
    return (
      <div className="flex flex-col flex-1 min-h-0 overflow-hidden">
        {header}
        <div className="px-4 pt-3 pb-2 border-b border-chef-border/30 shrink-0">
          <div className="flex items-center gap-4 text-[11px] font-mono mb-3">
            {[{ label: 'min', val: minVal, cls: 'text-sky-400' }, { label: 'max', val: maxVal, cls: 'text-rose-400' }, { label: 'avg', val: avg.toFixed(2), cls: 'text-emerald-400' }, { label: 'pts', val: points.length, cls: 'text-chef-muted' }].map(({ label, val, cls }) => (
              <span key={label} className="text-chef-muted/60">{label}: <span className={cls}>{val}</span></span>
            ))}
          </div>
          {points.length > 1 && (
            <svg viewBox={`0 0 ${W} ${H}`} className="w-full h-16" preserveAspectRatio="none">
              <defs>
                <linearGradient id="tsGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#22d3ee" stopOpacity="0.3" />
                  <stop offset="100%" stopColor="#22d3ee" stopOpacity="0" />
                </linearGradient>
              </defs>
              <path d={`${pathD} L${px(points.length - 1).toFixed(1)},${H} L${px(0).toFixed(1)},${H} Z`} fill="url(#tsGrad)" />
              <path d={pathD} fill="none" stroke="#22d3ee" strokeWidth="1.5" />
              {points.map((p, i) => i % Math.max(1, Math.floor(points.length / 20)) === 0
                ? <circle key={i} cx={px(i)} cy={py(p.val)} r="2" fill="#22d3ee" />
                : null)}
            </svg>
          )}
        </div>
        <div className="flex-1 overflow-auto">
          <table className="w-full border-collapse text-[11px] font-mono">
            <thead className="sticky top-0 bg-chef-surface border-b border-chef-border z-10">
              <tr>
                <th className="text-left px-3 py-1.5 text-chef-muted font-semibold">timestamp</th>
                <th className="text-left px-3 py-1.5 text-chef-muted font-semibold">datetime</th>
                <th className="text-right px-3 py-1.5 text-chef-muted font-semibold">value</th>
              </tr>
            </thead>
            <tbody>
              {points.map((p, i) => (
                <tr key={i} className="border-b border-chef-border/20 hover:bg-white/[0.03]">
                  <td className="px-3 py-1 text-chef-muted/60">{p.ts}</td>
                  <td className="px-3 py-1 text-chef-text">{p.ts > 1e10 ? new Date(p.ts).toISOString() : new Date(p.ts * 1000).toISOString()}</td>
                  <td className="px-3 py-1 text-right text-cyan-400">{p.val}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    )
  }

  // ── Fallback: raw DataGrid ──
  return (
    <div className="flex flex-col flex-1 min-h-0 overflow-hidden">
      {header}
      <DataGrid columns={columns} rows={rows} filter={filter} exportName={keyName} />
    </div>
  )
}

// ─── Queue Viewer ──────────────────────────────────────────────────────────────

function JsonPayloadView({ payload }: { payload: string }) {
  let parsed: unknown = null
  let isJson = false
  try { parsed = JSON.parse(payload); isJson = true } catch {}
  if (!isJson) return <pre className="text-[11px] font-mono text-chef-text whitespace-pre-wrap break-all leading-relaxed">{payload || <span className="text-chef-muted/40 italic">empty</span>}</pre>
  return <pre className="text-[11px] font-mono leading-relaxed" dangerouslySetInnerHTML={{ __html: jsonHighlight(JSON.stringify(parsed, null, 2)) }} />
}

interface QueueViewerProps {
  connectorType: string
  selectedObject: string | null
  messages: QueueMessage[]
  loading: boolean
  autoRefresh: boolean
  refreshIntervalMs: number
  onToggleAutoRefresh: () => void
  onIntervalChange: (ms: number) => void
  onRefresh: () => void
  onClearMessages: () => void
  publishTopic: string
  publishPayload: string
  publishRunning: boolean
  publishMsg: { ok: boolean; msg: string } | null
  onPublishTopicChange: (v: string) => void
  onPublishPayloadChange: (v: string) => void
  onPublish: () => void
}

function QueueViewer({
  connectorType, selectedObject, messages, loading, autoRefresh, refreshIntervalMs,
  onToggleAutoRefresh, onIntervalChange, onRefresh, onClearMessages,
  publishTopic, publishPayload, publishRunning, publishMsg,
  onPublishTopicChange, onPublishPayloadChange, onPublish,
}: QueueViewerProps) {
  const [selectedMsg, setSelectedMsg] = useState<QueueMessage | null>(null)
  const [showPublish, setShowPublish] = useState(false)
  const listRef = useRef<HTMLDivElement>(null)
  const [autoScroll, setAutoScroll] = useState(true)

  useEffect(() => {
    if (autoScroll && listRef.current) {
      listRef.current.scrollTop = 0 // newest at top
    }
  }, [messages, autoScroll])

  const label = connectorType === 'mqtt' ? 'topic' : 'queue'

  function formatPayloadPreview(p: string): string {
    if (!p) return '(empty)'
    const trimmed = p.trim()
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
      try { return JSON.stringify(JSON.parse(trimmed)).slice(0, 80) } catch {}
    }
    return trimmed.slice(0, 80)
  }

  return (
    <div className="flex flex-col flex-1 min-h-0 overflow-hidden">
      {/* Toolbar */}
      <div className="flex items-center gap-2 px-3 py-2 border-b border-chef-border/40 shrink-0">
        {connectorType === 'mqtt' ? <Radio size={12} className="text-emerald-400" /> : <MessageSquare size={12} className="text-amber-400" />}
        <span className="text-[12px] font-medium text-chef-text">{selectedObject ?? `all ${label}s`}</span>
        <span className="text-[10px] font-mono text-chef-muted/60 border border-chef-border/30 rounded px-1">{messages.length} msgs</span>

        {/* Live badge */}
        {autoRefresh && (
          <span className="flex items-center gap-1 text-[10px] font-semibold text-emerald-400 bg-emerald-500/10 border border-emerald-500/20 rounded px-1.5 py-0.5">
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />LIVE
          </span>
        )}

        <div className="flex-1" />
        <label className="flex items-center gap-1 text-[11px] text-chef-muted cursor-pointer">
          <input type="checkbox" checked={autoScroll} onChange={e => setAutoScroll(e.target.checked)} className="accent-indigo-500" />
          auto-scroll
        </label>
        <select value={refreshIntervalMs} onChange={e => onIntervalChange(Number(e.target.value))} className="bg-chef-card border border-chef-border text-[11px] text-chef-muted rounded px-1 py-0.5 outline-none">
          {[1000, 3000, 5000, 10000].map(ms => <option key={ms} value={ms}>{ms / 1000}s</option>)}
        </select>
        <button onClick={onToggleAutoRefresh} className={`flex items-center gap-1 px-2 py-0.5 text-[11px] rounded border transition-colors ${autoRefresh ? 'bg-emerald-500/10 border-emerald-500/20 text-emerald-400' : 'bg-chef-surface border-chef-border text-chef-muted hover:text-chef-text'}`}>
          <Activity size={11} />{autoRefresh ? 'Stop' : 'Live'}
        </button>
        <button onClick={onRefresh} disabled={loading} className="p-1.5 text-chef-muted hover:text-chef-text disabled:opacity-40 rounded transition-colors">
          {loading ? <Loader2 size={11} className="animate-spin" /> : <RefreshCw size={11} />}
        </button>
        <button onClick={onClearMessages} className="p-1.5 text-chef-muted hover:text-rose-400 rounded transition-colors" title="Clear messages">
          <Trash2 size={11} />
        </button>
        <button onClick={() => setShowPublish(p => !p)} className={`flex items-center gap-1 px-2 py-0.5 text-[11px] rounded border transition-colors ${showPublish ? 'bg-indigo-500/15 border-indigo-500/20 text-indigo-400' : 'bg-chef-surface border-chef-border text-chef-muted hover:text-chef-text'}`}>
          <Send size={11} />Publish
        </button>
      </div>

      {/* Publish form */}
      {showPublish && (
        <div className="border-b border-chef-border/40 bg-[#080c14] px-3 py-2.5 shrink-0 space-y-2">
          <div className="flex items-center gap-2">
            <span className="text-[11px] text-chef-muted w-16 shrink-0">{connectorType === 'mqtt' ? 'Topic' : 'Queue'}</span>
            <input value={publishTopic} onChange={e => onPublishTopicChange(e.target.value)} placeholder={selectedObject ?? (connectorType === 'mqtt' ? 'my/topic' : 'my.queue')} className="flex-1 bg-chef-surface border border-chef-border rounded px-2 py-1 text-[11px] font-mono text-chef-text outline-none focus:border-indigo-500/50" />
          </div>
          <div className="flex items-start gap-2">
            <span className="text-[11px] text-chef-muted w-16 shrink-0 mt-1">Payload</span>
            <textarea value={publishPayload} onChange={e => onPublishPayloadChange(e.target.value)} rows={3} placeholder='{"key": "value"}' className="flex-1 bg-chef-surface border border-chef-border rounded px-2 py-1 text-[11px] font-mono text-chef-text outline-none focus:border-indigo-500/50 resize-none" />
          </div>
          <div className="flex items-center gap-2">
            <div className="flex-1" />
            {publishMsg && <span className={`text-[11px] font-mono ${publishMsg.ok ? 'text-emerald-400' : 'text-rose-400'}`}>{publishMsg.msg}</span>}
            <button onClick={onPublish} disabled={publishRunning || !publishPayload.trim()} className="flex items-center gap-1.5 px-3 py-1.5 bg-indigo-500 hover:bg-indigo-600 disabled:opacity-40 text-white text-[11px] rounded transition-colors">
              {publishRunning ? <Loader2 size={11} className="animate-spin" /> : <Send size={11} />}
              Send
            </button>
          </div>
        </div>
      )}

      {/* Split layout: list + detail */}
      <div className="flex flex-1 min-h-0">
        {/* Message list */}
        <div ref={listRef} className="w-72 shrink-0 border-r border-chef-border overflow-y-auto">
          {messages.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-full gap-3 text-chef-muted">
              {loading ? <Loader2 size={20} className="animate-spin" /> : <MessageSquare size={24} className="text-chef-border" />}
              <span className="text-xs">{loading ? 'Fetching messages…' : 'No messages yet'}</span>
              {!loading && <button onClick={onRefresh} className="text-[11px] text-indigo-400 hover:text-indigo-300">Fetch now</button>}
            </div>
          ) : (
            messages.map((msg) => (
              <button key={msg.id} onClick={() => setSelectedMsg(msg)} className={`w-full text-left px-3 py-2 border-b border-chef-border/30 transition-colors ${selectedMsg?.id === msg.id ? 'bg-indigo-500/10 border-l-2 border-l-indigo-500' : 'hover:bg-white/[0.03]'}`}>
                <div className="flex items-center gap-1.5 mb-0.5">
                  <span className="text-[10px] font-mono text-chef-muted/60 truncate flex-1">{msg.topic ?? msg.queue ?? '—'}</span>
                  {msg.qos && <span className="text-[9px] text-amber-400/70 border border-amber-500/20 rounded px-0.5">QoS{msg.qos}</span>}
                  {msg.retained === 'true' && <span className="text-[9px] text-violet-400/70 border border-violet-500/20 rounded px-0.5">R</span>}
                </div>
                <div className="text-[11px] font-mono text-chef-text/80 truncate">{formatPayloadPreview(msg.payload)}</div>
                <div className="text-[10px] text-chef-muted/40 font-mono mt-0.5">{new Date(msg.timestamp).toLocaleTimeString('en', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}</div>
              </button>
            ))
          )}
        </div>

        {/* Detail panel */}
        <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
          {selectedMsg ? (
            <>
              <div className="flex items-center gap-2 px-3 py-2 border-b border-chef-border/30 shrink-0 text-[11px]">
                <span className="text-indigo-400 font-mono text-[10px]">{selectedMsg.topic ?? selectedMsg.queue ?? '—'}</span>
                <span className="text-chef-muted/40">·</span>
                <span className="text-chef-muted/60 font-mono">{new Date(selectedMsg.timestamp).toISOString()}</span>
                {selectedMsg.qos && <span className="ml-auto text-[10px] text-amber-400/70">QoS {selectedMsg.qos}</span>}
                <button onClick={() => navigator.clipboard.writeText(selectedMsg.payload)} className="flex items-center gap-1 text-chef-muted hover:text-chef-text transition-colors ml-2">
                  <Copy size={11} />copy
                </button>
              </div>
              <div className="flex-1 overflow-auto p-3">
                <JsonPayloadView payload={selectedMsg.payload} />
              </div>
            </>
          ) : (
            <div className="flex-1 flex items-center justify-center text-chef-muted/50 text-xs">
              Select a message to inspect
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

// ─── Query bar ────────────────────────────────────────────────────────────────

function QueryBar({ value, onChange, onRun, running, suggestions }: {
  value: string; onChange: (v: string) => void; onRun: () => void; running: boolean; suggestions: string[]
}) {
  const ref = useRef<HTMLTextAreaElement>(null)
  const [showSuggestions, setShowSuggestions] = useState(false)
  const [suggestionIdx, setSuggestionIdx] = useState(0)
  const [filteredSugg, setFilteredSugg] = useState<string[]>([])

  function recompute(text: string, pos: number) {
    const { word } = wordAtCursor(text, pos)
    if (word.length >= 2) {
      const q = word.toUpperCase()
      const matches = suggestions.filter(s => s.toUpperCase().startsWith(q) && s.toUpperCase() !== q).slice(0, 8)
      setFilteredSugg(matches); setShowSuggestions(matches.length > 0); setSuggestionIdx(0)
    } else { setShowSuggestions(false) }
  }

  function applySuggestion(sug: string) {
    const el = ref.current; if (!el) return
    const pos = el.selectionStart
    const { start, end } = wordAtCursor(value, pos)
    onChange(value.slice(0, start) + sug + value.slice(end))
    setShowSuggestions(false)
    setTimeout(() => { el.focus(); const p = start + sug.length; el.setSelectionRange(p, p) }, 0)
  }

  function handleKey(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (showSuggestions) {
      if (e.key === 'ArrowDown') { e.preventDefault(); setSuggestionIdx(i => Math.min(i + 1, filteredSugg.length - 1)); return }
      if (e.key === 'ArrowUp') { e.preventDefault(); setSuggestionIdx(i => Math.max(i - 1, 0)); return }
      if ((e.key === 'Tab' || e.key === 'Enter') && filteredSugg[suggestionIdx]) { e.preventDefault(); applySuggestion(filteredSugg[suggestionIdx]); return }
      if (e.key === 'Escape') { setShowSuggestions(false); return }
    }
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); onRun() }
  }

  return (
    <div className="relative">
      <div className="flex items-start gap-2 bg-[#0d1117] border border-chef-border rounded-lg overflow-hidden focus-within:border-indigo-500/50">
        <textarea ref={ref} value={value} onChange={e => { onChange(e.target.value); recompute(e.target.value, e.target.selectionStart) }} onKeyDown={handleKey} onClick={e => recompute(value, (e.target as HTMLTextAreaElement).selectionStart)} rows={value.split('\n').length > 3 ? Math.min(value.split('\n').length, 6) : 3} className="flex-1 bg-transparent text-[12px] font-mono text-chef-text placeholder:text-chef-muted/50 px-3 py-2 outline-none resize-none" placeholder="SELECT * FROM table — ⌘↵ to run" spellCheck={false} />
        <button onClick={onRun} disabled={running} className="mt-2 mr-2 flex items-center gap-1.5 px-3 py-1.5 bg-indigo-500 hover:bg-indigo-600 disabled:opacity-50 disabled:cursor-not-allowed text-white text-[11px] font-medium rounded-md transition-colors shrink-0">
          {running ? <Loader2 size={12} className="animate-spin" /> : <Play size={12} />}
          Run
        </button>
      </div>
      {showSuggestions && (
        <div className="absolute top-full left-0 z-50 mt-0.5 bg-chef-card border border-chef-border rounded-lg shadow-xl overflow-hidden w-64">
          {filteredSugg.map((s, i) => (
            <button key={s} onMouseDown={() => applySuggestion(s)} className={`w-full text-left px-3 py-1.5 text-[11px] font-mono transition-colors ${i === suggestionIdx ? 'bg-indigo-500/20 text-indigo-300' : 'text-chef-text hover:bg-white/[0.04]'}`}>{s}</button>
          ))}
        </div>
      )}
    </div>
  )
}

// ─── Bottom Panel ─────────────────────────────────────────────────────────────

interface BottomPanelProps {
  height: number
  onResizeStart: (e: React.MouseEvent) => void
  tab: 'log' | 'tools' | 'export'
  onTabChange: (t: 'log' | 'tools' | 'export') => void
  onClose: () => void
  queryLog: QueryLogEntry[]
  onClearLog: () => void
  onLoadSql: (sql: string) => void
  connectorType: string
  selectedObject: string | null
  tableFqn?: string
  schemaResult: CatalogResult | null
  dataResult: DataResult | null
  pkColumns: string[]
  onExecute?: (sql: string) => Promise<{ rowsAffected?: number; totalRows?: number; rowCount?: number; error?: string; durationMs?: number }>
  onRefresh?: () => void
}

function BottomPanel({
  height, onResizeStart, tab, onTabChange, onClose,
  queryLog, onClearLog, onLoadSql,
  connectorType, selectedObject, tableFqn, schemaResult, dataResult, pkColumns,
  onExecute, onRefresh,
}: BottomPanelProps) {
  const canTools = isSql(connectorType) && !!selectedObject && !!tableFqn && !!onExecute
  const canExport = !!(dataResult && dataResult.rows.length > 0)

  // Tools state
  const [renameVal, setRenameVal] = useState('')
  const [truncateConf, setTruncateConf] = useState('')
  const [dropConf, setDropConf] = useState('')
  const [addColName, setAddColName] = useState('')
  const [addColType, setAddColType] = useState('NVARCHAR')
  const [addColSize, setAddColSize] = useState('255')
  const [addColNull, setAddColNull] = useState(true)
  const [toolRunning, setToolRunning] = useState(false)
  const [toolMsg, setToolMsg] = useState<{ ok: boolean; msg: string } | null>(null)

  function showToolMsg(ok: boolean, msg: string) {
    setToolMsg({ ok, msg }); setTimeout(() => setToolMsg(null), 4000)
  }

  async function runTool(sql: string, onSuccess?: () => void) {
    if (!onExecute) return
    setToolRunning(true)
    const result = await onExecute(sql)
    setToolRunning(false)
    if (result.error) showToolMsg(false, result.error)
    else { showToolMsg(true, `Done · ${result.durationMs ?? 0}ms`); onRefresh?.(); onSuccess?.() }
  }

  function buildRenameSQL(newName: string) {
    if (connectorType === 'mssql') return `EXEC sp_rename '${tableFqn}', '${newName}', 'OBJECT'`
    if (connectorType === 'mysql') return `RENAME TABLE ${tableFqn} TO \`${newName}\``
    return `ALTER TABLE ${tableFqn} RENAME TO "${newName}"`
  }

  function buildAddColSQL() {
    const qi = (n: string) => connectorType === 'mssql' ? `[${n}]` : connectorType === 'mysql' ? `\`${n}\`` : `"${n}"`
    const sizeClause = ['VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR', 'DECIMAL'].includes(addColType) && addColSize ? `(${addColSize})` : ''
    const kw = connectorType === 'mssql' ? 'ADD' : 'ADD COLUMN'
    return `ALTER TABLE ${tableFqn} ${kw} ${qi(addColName)} ${addColType}${sizeClause} ${addColNull ? 'NULL' : 'NOT NULL'}`
  }

  function copyDDL() {
    if (!schemaResult || !tableFqn) return
    const ddl = buildDDL(tableFqn, schemaResult, pkColumns, connectorType)
    navigator.clipboard.writeText(ddl)
    showToolMsg(true, 'DDL copied to clipboard')
  }

  return (
    <div className="border-t border-chef-border bg-[#080a0f] shrink-0 flex flex-col" style={{ height }}>
      {/* Resize grip */}
      <div onMouseDown={onResizeStart} className="flex items-center justify-center h-2 cursor-ns-resize hover:bg-indigo-500/10 shrink-0 group">
        <GripHorizontal size={12} className="text-chef-border group-hover:text-indigo-500/50" />
      </div>

      {/* Header */}
      <div className="flex items-center gap-1 px-3 h-7 border-b border-chef-border/50 shrink-0">
        <Terminal size={11} className="text-chef-muted mr-1" />
        {(['log', 'tools', 'export'] as const).map(t => {
          const disabled = (t === 'tools' && !canTools) || (t === 'export' && !canExport)
          return (
            <button
              key={t}
              onClick={() => !disabled && onTabChange(t)}
              disabled={disabled}
              className={`px-2.5 py-0.5 text-[11px] capitalize rounded transition-colors disabled:opacity-30 disabled:cursor-default ${tab === t ? 'bg-indigo-500/15 text-indigo-400' : 'text-chef-muted hover:text-chef-text'}`}
            >
              {t === 'log' && queryLog.length > 0 ? `Log (${queryLog.length})` : t === 'tools' ? 'Table Tools' : t === 'export' ? 'Export' : t}
            </button>
          )
        })}
        <div className="flex-1" />
        {toolMsg && <span className={`text-[10px] font-mono ${toolMsg.ok ? 'text-emerald-400' : 'text-rose-400'}`}>{toolMsg.msg}</span>}
        {toolRunning && <Loader2 size={11} className="animate-spin text-indigo-400" />}
        {selectedObject && <span className="text-[10px] text-chef-muted/50 font-mono">{selectedObject}</span>}
        <button onClick={onClose} className="p-0.5 ml-1 text-chef-muted hover:text-chef-text rounded"><X size={11} /></button>
      </div>

      {/* Content */}
      <div className="flex-1 min-h-0 overflow-y-auto">

        {/* ── Query Log ── */}
        {tab === 'log' && (
          <div className="h-full flex flex-col">
            {queryLog.length === 0 ? (
              <div className="flex-1 flex items-center justify-center text-[11px] text-chef-muted/50">
                No queries yet — run a query or edit a cell to see history
              </div>
            ) : (
              <>
                <div className="flex items-center justify-between px-3 py-1 border-b border-chef-border/30 shrink-0">
                  <span className="text-[10px] text-chef-muted">{queryLog.length} entries</span>
                  <button onClick={onClearLog} className="text-[10px] text-chef-muted hover:text-chef-text transition-colors">Clear</button>
                </div>
                <div className="flex-1 overflow-y-auto">
                  {queryLog.map(entry => (
                    <div key={entry.id} className="flex items-start gap-2 px-3 py-1.5 border-b border-chef-border/20 hover:bg-white/[0.02] group text-[11px]">
                      <span className="text-chef-muted/40 font-mono shrink-0 mt-0.5 text-[10px]">
                        {entry.timestamp.toLocaleTimeString('en', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                      </span>
                      <span className={`font-mono font-semibold shrink-0 w-14 text-[10px] mt-0.5 ${LOG_TYPE_COLOR[entry.type]}`}>{entry.type}</span>
                      <span className={`flex-1 font-mono truncate ${entry.status === 'error' ? 'text-rose-300' : 'text-chef-text'}`} title={entry.sql}>
                        {entry.sql.replace(/\s+/g, ' ').slice(0, 120)}
                      </span>
                      <div className="flex items-center gap-2 shrink-0 text-[10px] font-mono">
                        {entry.status === 'ok' ? (
                          <>
                            {entry.durationMs != null && <span className="text-chef-muted/60">{entry.durationMs}ms</span>}
                            {entry.rowCount != null && <span className="text-sky-400/70">{entry.rowCount} rows</span>}
                            {entry.rowsAffected != null && entry.type !== 'SELECT' && <span className="text-emerald-400/70">{entry.rowsAffected} affected</span>}
                          </>
                        ) : (
                          <span className="text-rose-400/70 truncate max-w-[120px]" title={entry.error}>{entry.error?.slice(0, 40)}</span>
                        )}
                      </div>
                      <div className="flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity shrink-0">
                        <button onClick={() => navigator.clipboard.writeText(entry.sql)} title="Copy SQL" className="text-chef-muted hover:text-chef-text"><Copy size={10} /></button>
                        <button onClick={() => onLoadSql(entry.sql)} title="Load into editor" className="text-chef-muted hover:text-indigo-400"><Play size={10} /></button>
                      </div>
                    </div>
                  ))}
                </div>
              </>
            )}
          </div>
        )}

        {/* ── Table Tools ── */}
        {tab === 'tools' && canTools && (
          <div className="p-3 space-y-4 text-[12px]">

            {/* Stats */}
            <div className="flex flex-wrap gap-3 text-[11px] font-mono bg-chef-surface/50 rounded-lg px-3 py-2 border border-chef-border/40">
              {[
                { label: 'rows', value: dataResult?.totalRows?.toLocaleString() ?? '—' },
                { label: 'cols', value: schemaResult ? String(schemaResult.rows.length) : '—' },
                { label: 'pk', value: pkColumns.length ? pkColumns.join(', ') : 'none' },
              ].map(({ label, value }) => (
                <span key={label} className="text-chef-muted">{label}: <span className="text-chef-text">{value}</span></span>
              ))}
            </div>

            {/* Rename */}
            <div>
              <div className="text-[11px] text-chef-muted mb-1.5 flex items-center gap-1"><Pencil size={10} /> Rename Table</div>
              <div className="flex gap-2">
                <input value={renameVal} onChange={e => setRenameVal(e.target.value)} placeholder="New table name" className="flex-1 bg-chef-surface border border-chef-border rounded px-2 py-1 text-[11px] text-chef-text outline-none focus:border-indigo-500/50 font-mono" />
                <button onClick={() => renameVal.trim() && runTool(buildRenameSQL(renameVal.trim()), () => setRenameVal(''))} disabled={!renameVal.trim() || toolRunning} className="px-3 py-1 bg-indigo-500/10 hover:bg-indigo-500/20 text-indigo-400 text-[11px] rounded border border-indigo-500/20 disabled:opacity-40 disabled:cursor-not-allowed">
                  Rename
                </button>
              </div>
            </div>

            {/* Add Column */}
            <div>
              <div className="text-[11px] text-chef-muted mb-1.5 flex items-center gap-1"><Code2 size={10} /> Add Column</div>
              <div className="flex gap-2 flex-wrap">
                <input value={addColName} onChange={e => setAddColName(e.target.value)} placeholder="column_name" className="w-36 bg-chef-surface border border-chef-border rounded px-2 py-1 text-[11px] text-chef-text outline-none focus:border-indigo-500/50 font-mono" />
                <select value={addColType} onChange={e => setAddColType(e.target.value)} className="bg-chef-surface border border-chef-border rounded px-2 py-1 text-[11px] text-chef-text outline-none">
                  {COL_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                </select>
                {['VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR', 'DECIMAL'].includes(addColType) && (
                  <input value={addColSize} onChange={e => setAddColSize(e.target.value)} placeholder="size" className="w-16 bg-chef-surface border border-chef-border rounded px-2 py-1 text-[11px] text-chef-text outline-none font-mono" />
                )}
                <label className="flex items-center gap-1 text-[11px] text-chef-muted cursor-pointer">
                  <input type="checkbox" checked={addColNull} onChange={e => setAddColNull(e.target.checked)} className="accent-indigo-500" />
                  nullable
                </label>
                <button onClick={() => addColName.trim() && runTool(buildAddColSQL(), () => setAddColName(''))} disabled={!addColName.trim() || toolRunning} className="px-3 py-1 bg-indigo-500/10 hover:bg-indigo-500/20 text-indigo-400 text-[11px] rounded border border-indigo-500/20 disabled:opacity-40 disabled:cursor-not-allowed">
                  Add
                </button>
              </div>
              {addColName && (
                <div className="mt-1.5 font-mono text-[10px] text-chef-muted/60 bg-chef-surface/50 rounded px-2 py-1 border border-chef-border/30">
                  {buildAddColSQL()}
                </div>
              )}
            </div>

            {/* Copy DDL */}
            <div>
              <div className="text-[11px] text-chef-muted mb-1.5 flex items-center gap-1"><Code2 size={10} /> Schema DDL</div>
              <div className="flex gap-2">
                <button onClick={copyDDL} disabled={!schemaResult} className="flex items-center gap-1.5 px-3 py-1 bg-chef-surface hover:bg-white/[0.06] text-chef-muted hover:text-chef-text text-[11px] rounded border border-chef-border/50 disabled:opacity-40 transition-colors">
                  <Copy size={11} />Copy CREATE TABLE
                </button>
                {schemaResult && (
                  <button onClick={() => downloadText(buildDDL(tableFqn!, schemaResult, pkColumns, connectorType), `${selectedObject}_ddl.sql`)} className="flex items-center gap-1.5 px-3 py-1 bg-chef-surface hover:bg-white/[0.06] text-chef-muted hover:text-chef-text text-[11px] rounded border border-chef-border/50 transition-colors">
                    <Download size={11} />Download .sql
                  </button>
                )}
              </div>
            </div>

            {/* Danger Zone */}
            <div className="border border-rose-500/20 rounded-lg p-3 bg-rose-500/5 space-y-3">
              <div className="text-[11px] font-semibold text-rose-400/80 uppercase tracking-wider">Danger Zone</div>

              {/* Truncate */}
              <div>
                <div className="text-[11px] text-chef-muted mb-1 flex items-center gap-1"><Eraser size={10} className="text-amber-400" /> Truncate Table <span className="text-chef-muted/50">— removes all rows, keeps structure</span></div>
                <div className="flex gap-2">
                  <input value={truncateConf} onChange={e => setTruncateConf(e.target.value)} placeholder={`Type "${selectedObject}" to confirm`} className="flex-1 bg-chef-surface border border-chef-border rounded px-2 py-1 text-[11px] font-mono text-chef-text outline-none focus:border-amber-500/50" />
                  <button onClick={() => runTool(`TRUNCATE TABLE ${tableFqn}`, () => setTruncateConf(''))} disabled={truncateConf !== selectedObject || toolRunning} className="px-3 py-1 bg-amber-500/10 hover:bg-amber-500/20 text-amber-400 text-[11px] rounded border border-amber-500/20 disabled:opacity-40 disabled:cursor-not-allowed">
                    Truncate
                  </button>
                </div>
              </div>

              {/* Drop Table */}
              <div>
                <div className="text-[11px] text-chef-muted mb-1 flex items-center gap-1"><Trash2 size={10} className="text-rose-400" /> Drop Table <span className="text-chef-muted/50">— permanently deletes table and all data</span></div>
                <div className="flex gap-2">
                  <input value={dropConf} onChange={e => setDropConf(e.target.value)} placeholder={`Type "DROP ${selectedObject}" to confirm`} className="flex-1 bg-chef-surface border border-chef-border rounded px-2 py-1 text-[11px] font-mono text-chef-text outline-none focus:border-rose-500/50" />
                  <button onClick={() => runTool(`DROP TABLE ${tableFqn}`, () => { setDropConf(''); onRefresh?.() })} disabled={dropConf !== `DROP ${selectedObject}` || toolRunning} className="px-3 py-1 bg-rose-500/10 hover:bg-rose-500/20 text-rose-400 text-[11px] rounded border border-rose-500/20 disabled:opacity-40 disabled:cursor-not-allowed">
                    Drop
                  </button>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* ── Export ── */}
        {tab === 'export' && canExport && dataResult && (
          <div className="p-3 space-y-3 text-[12px]">
            <div className="text-[11px] text-chef-muted">
              Exporting <span className="text-chef-text font-mono">{dataResult.rows.length.toLocaleString()}</span> rows × <span className="text-chef-text font-mono">{dataResult.columns.length}</span> columns
              {selectedObject && <> from <span className="text-indigo-400 font-mono">{selectedObject}</span></>}
            </div>

            <div className="grid grid-cols-2 gap-2">
              {/* CSV */}
              <button
                onClick={() => {
                  const esc = (v: string) => `"${v.replace(/"/g, '""')}"`
                  const lines = [dataResult.columns.map(esc).join(','), ...dataResult.rows.map(row => row.map(esc).join(','))]
                  downloadText(lines.join('\n'), `${selectedObject ?? 'export'}.csv`, 'text/csv')
                }}
                className="flex items-center gap-2 px-3 py-2.5 bg-chef-surface hover:bg-white/[0.06] text-chef-text rounded border border-chef-border/50 transition-colors text-[11px]"
              >
                <Download size={13} className="text-emerald-400" />
                <div className="text-left">
                  <div className="font-medium">Export CSV</div>
                  <div className="text-[10px] text-chef-muted">Comma-separated values</div>
                </div>
              </button>

              {/* JSON */}
              <button
                onClick={() => {
                  const arr = dataResult.rows.map(row => Object.fromEntries(dataResult.columns.map((col, ci) => [col, row[ci] === '' ? null : row[ci]])))
                  downloadText(JSON.stringify(arr, null, 2), `${selectedObject ?? 'export'}.json`, 'application/json')
                }}
                className="flex items-center gap-2 px-3 py-2.5 bg-chef-surface hover:bg-white/[0.06] text-chef-text rounded border border-chef-border/50 transition-colors text-[11px]"
              >
                <FileJson size={13} className="text-amber-400" />
                <div className="text-left">
                  <div className="font-medium">Export JSON</div>
                  <div className="text-[10px] text-chef-muted">Array of objects</div>
                </div>
              </button>

              {/* SQL INSERTs */}
              {tableFqn && (
                <button
                  onClick={() => downloadText(buildInserts(tableFqn, dataResult.columns, dataResult.rows, connectorType), `${selectedObject ?? 'export'}_inserts.sql`)}
                  className="flex items-center gap-2 px-3 py-2.5 bg-chef-surface hover:bg-white/[0.06] text-chef-text rounded border border-chef-border/50 transition-colors text-[11px]"
                >
                  <Code2 size={13} className="text-sky-400" />
                  <div className="text-left">
                    <div className="font-medium">SQL INSERTs</div>
                    <div className="text-[10px] text-chef-muted">INSERT INTO statements</div>
                  </div>
                </button>
              )}

              {/* Full backup: DDL + INSERTs */}
              {tableFqn && schemaResult && (
                <button
                  onClick={() => {
                    const ddl = buildDDL(tableFqn, schemaResult, pkColumns, connectorType)
                    const inserts = buildInserts(tableFqn, dataResult.columns, dataResult.rows, connectorType)
                    const header = `-- Backup of ${tableFqn}\n-- Generated at ${new Date().toISOString()}\n-- ${dataResult.rows.length} rows\n\n`
                    downloadText(header + ddl + '\n\n' + inserts, `${selectedObject ?? 'export'}_backup.sql`)
                  }}
                  className="flex items-center gap-2 px-3 py-2.5 bg-chef-surface hover:bg-white/[0.06] text-chef-text rounded border border-chef-border/50 transition-colors text-[11px]"
                >
                  <Download size={13} className="text-violet-400" />
                  <div className="text-left">
                    <div className="font-medium">Full Backup</div>
                    <div className="text-[10px] text-chef-muted">DDL + INSERT statements</div>
                  </div>
                </button>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

// ─── Main page ────────────────────────────────────────────────────────────────

export default function StudioPage() {
  // ── Connectors
  const [connectors, setConnectors] = useState<Connector[]>([])
  const [connectorId, setConnectorId] = useState('')
  const connectorType = connectors.find(c => c.id === connectorId)?.type ?? ''

  // ── Database / schema / object
  const [databases, setDatabases] = useState<string[]>([])
  const [selectedDb, setSelectedDb] = useState('')
  const [schemas, setSchemas] = useState<string[]>([])
  const [selectedSchema, setSelectedSchema] = useState('')
  const [selectedObject, setSelectedObject] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'data' | 'schema' | 'indexes' | 'info'>('data')

  // ── Tree items
  const [tables, setTables] = useState<string[]>([])
  const [views, setViews] = useState<string[]>([])
  const [procedures, setProcedures] = useState<string[]>([])
  const [queues, setQueues] = useState<string[]>([])
  const [redisKeys, setRedisKeys] = useState<string[]>([])
  const [redisIndexes, setRedisIndexes] = useState<string[]>([])
  const [redisStreams, setRedisStreams] = useState<string[]>([])

  // ── Query bar
  const [sqlQuery, setSqlQuery] = useState('')
  const [queryRunning, setQueryRunning] = useState(false)

  // ── Data
  const [dataResult, setDataResult] = useState<DataResult | null>(null)
  const [schemaResult, setSchemaResult] = useState<CatalogResult | null>(null)
  const [indexResult, setIndexResult] = useState<CatalogResult | null>(null)
  const [pkColumns, setPkColumns] = useState<string[]>([])
  const [loading, setLoading] = useState(false)
  const [treeLoading, setTreeLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(500)
  const [rowFilter, setRowFilter] = useState('')

  // ── Bottom panel
  const [panelOpen, setPanelOpen] = useState(false)
  const [panelTab, setPanelTab] = useState<'log' | 'tools' | 'export'>('log')
  const [panelHeight, setPanelHeight] = useState(220)
  const [queryLog, setQueryLog] = useState<QueryLogEntry[]>([])
  const isDragging = useRef(false)
  const dragStartY = useRef(0)
  const dragStartH = useRef(0)

  // ── Redis type metadata
  const [redisKeyType, setRedisKeyType] = useState<string | null>(null)
  const [redisKeyTtl, setRedisKeyTtl] = useState<number>(-1)

  // ── Queue viewer
  const [queueMessages, setQueueMessages] = useState<QueueMessage[]>([])
  const [autoRefresh, setAutoRefresh] = useState(false)
  const [refreshIntervalMs, setRefreshIntervalMs] = useState(3000)
  const [publishTopic, setPublishTopic] = useState('')
  const [publishPayload, setPublishPayload] = useState('')
  const [publishRunning, setPublishRunning] = useState(false)
  const [publishMsg, setPublishMsg] = useState<{ ok: boolean; msg: string } | null>(null)
  const autoRefreshTimer = useRef<ReturnType<typeof setInterval> | null>(null)

  // Resize drag
  useEffect(() => {
    function onMove(e: MouseEvent) {
      if (!isDragging.current) return
      const delta = dragStartY.current - e.clientY
      setPanelHeight(Math.max(120, Math.min(500, dragStartH.current + delta)))
    }
    function onUp() { isDragging.current = false }
    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', onUp)
    return () => { document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp) }
  }, [])

  function handleResizeStart(e: React.MouseEvent) {
    isDragging.current = true
    dragStartY.current = e.clientY
    dragStartH.current = panelHeight
  }

  // ── Auto-refresh for queues
  useEffect(() => {
    if (autoRefreshTimer.current) clearInterval(autoRefreshTimer.current)
    if (autoRefresh && selectedObject && isQueue(connectorType)) {
      autoRefreshTimer.current = setInterval(() => {
        loadQueueData(selectedObject)
      }, refreshIntervalMs)
    }
    return () => { if (autoRefreshTimer.current) clearInterval(autoRefreshTimer.current) }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRefresh, refreshIntervalMs, selectedObject, connectorType])

  // ── Query logger
  function logQuery(sql: string, result: { durationMs?: number; rowsAffected?: number; rowCount?: number; totalRows?: number; error?: string }) {
    const type = detectQueryType(sql)
    const entry: QueryLogEntry = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
      sql: sql.trim(), type,
      status: result.error ? 'error' : 'ok',
      durationMs: result.durationMs,
      rowCount: type === 'SELECT' ? (result.rowCount ?? result.totalRows) : undefined,
      rowsAffected: type !== 'SELECT' ? (result.rowsAffected ?? result.totalRows ?? result.rowCount) : undefined,
      error: result.error,
      timestamp: new Date(),
    }
    setQueryLog(prev => [entry, ...prev].slice(0, 50))
  }

  // ── Load connectors (only types Studio can browse)
  useEffect(() => {
    fetch('/api/connectors')
      .then(r => r.ok ? r.json() : [])
      .then((list: Connector[]) => {
        const supported = list.filter(c => SUPPORTED_TYPES.has(c.type))
        setConnectors(supported)
        if (supported.length > 0) setConnectorId(supported[0].id)
      })
      .catch(() => {})
  }, [])

  // ── When connector changes
  useEffect(() => {
    if (!connectorId || !connectorType) return
    setSelectedDb(''); setSelectedSchema(''); setSelectedObject(null)
    setDatabases([]); setSchemas([]); setTables([]); setViews([]); setProcedures([])
    setQueues([]); setRedisKeys([]); setRedisIndexes([]); setRedisStreams([])
    setDataResult(null); setSchemaResult(null); setIndexResult(null)
    setPkColumns([]); setSqlQuery(''); setError(null)
    setQueueMessages([]); setAutoRefresh(false); setRedisKeyType(null)
    if (isSql(connectorType)) loadDatabases()
    else if (isQueue(connectorType)) loadQueueTree()
    else if (isRedis(connectorType)) loadRedisTree()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connectorId, connectorType])

  useEffect(() => {
    if (!connectorId || !isSql(connectorType)) return
    setSelectedSchema(''); setSelectedObject(null); setSchemas([])
    setTables([]); setViews([]); setProcedures([]); setPkColumns([]); setDataResult(null)
    loadSchemas()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedDb])

  useEffect(() => {
    if (!connectorId || !isSql(connectorType) || !selectedSchema) return
    setSelectedObject(null); setTables([]); setViews([]); setProcedures([])
    setPkColumns([]); setDataResult(null)
    loadTree()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedSchema])

  async function fetchCatalog(kind: string, extra: Record<string, string> = {}): Promise<CatalogResult> {
    const params = new URLSearchParams({ connectorId, kind, ...extra })
    if (selectedDb) params.set('db', selectedDb)
    if (selectedSchema && !extra.schema) params.set('schema', selectedSchema)
    const res = await fetch(`/api/browser/catalog?${params}`)
    return res.json()
  }

  async function loadDatabases() {
    try {
      const r = await fetchCatalog('databases'); if (r.error) return
      const cols = r.columns.map(c => c.toLowerCase())
      const col = cols.indexOf('database_name') >= 0 ? cols.indexOf('database_name')
        : cols.indexOf('datname') >= 0 ? cols.indexOf('datname')
        : cols.findIndex(c => c.includes('name'))
      const list = r.rows.map(row => row[col >= 0 ? col : 0]).filter(Boolean)
      setDatabases(list); if (list.length > 0) setSelectedDb(list[0])
    } catch { /* ignore */ }
  }

  async function loadSchemas() {
    try {
      const r = await fetchCatalog('schemas'); if (r.error) return
      const cols = r.columns.map(c => c.toLowerCase())
      const col = cols.indexOf('schema_name') >= 0 ? cols.indexOf('schema_name') : cols.findIndex(c => c.includes('schema'))
      const SYSTEM_SCHEMAS = new Set(['db_accessadmin','db_backupoperator','db_datareader','db_datawriter','db_ddladmin','db_denydatareader','db_denydatawriter','db_owner','db_securityadmin','information_schema','sys','guest','pg_catalog','pg_toast','performance_schema','mysql'])
      const all = r.rows.map(row => row[col >= 0 ? col : 0]).filter(Boolean)
      setSchemas(all)
      const preferred = all.find(s => s === 'dbo' || s === 'public') ?? all.find(s => !SYSTEM_SCHEMAS.has(s.toLowerCase())) ?? all[0]
      if (preferred) setSelectedSchema(preferred)
      else loadTree()
    } catch { /* ignore */ }
  }

  async function loadTree() {
    if (!connectorId) return
    setTreeLoading(true)
    try {
      const [t, v, p] = await Promise.all([fetchCatalog('tables'), fetchCatalog('views'), fetchCatalog('procedures')])
      const nameCol = (r: CatalogResult) => {
        const cols = r.columns.map(c => c.toLowerCase())
        return cols.indexOf('table_name') >= 0 ? cols.indexOf('table_name')
          : cols.indexOf('routine_name') >= 0 ? cols.indexOf('routine_name')
          : cols.findIndex(c => c === 'name' || (c.includes('name') && !c.includes('schema') && !c.includes('type') && !c.includes('default')))
      }
      setTables(t.rows.map(row => row[Math.max(0, nameCol(t))]).filter(Boolean))
      setViews(v.rows.map(row => row[Math.max(0, nameCol(v))]).filter(Boolean))
      setProcedures(p.rows.map(row => row[Math.max(0, nameCol(p))]).filter(Boolean))
    } catch { /* ignore */ }
    finally { setTreeLoading(false) }
  }

  async function loadQueueTree() {
    setTreeLoading(true)
    try {
      const kind = connectorType === 'mqtt' ? 'topics' : 'queues'
      const r = await fetchCatalog(kind); if (r.error) { setQueues([]); return }
      const col = r.columns.findIndex(c => c.toLowerCase().includes('name') || c.toLowerCase().includes('queue') || c.toLowerCase().includes('topic'))
      setQueues(r.rows.map(row => row[col >= 0 ? col : 0]).filter(Boolean))
    } catch { /* ignore */ }
    finally { setTreeLoading(false) }
  }

  async function loadRedisTree() {
    setTreeLoading(true)
    try {
      const [keys, indexes, streams] = await Promise.all([
        fetchCatalog('keys', { pattern: '*' }),
        fetchCatalog('indexes').catch(() => ({ columns: [], rows: [], rowCount: 0, totalRows: 0, error: 'no search' })),
        fetchCatalog('streams').catch(() => ({ columns: [], rows: [], rowCount: 0, totalRows: 0 })),
      ])
      const keyCol = keys.columns.findIndex(c => c === 'key')
      setRedisKeys(keys.rows.map(row => row[keyCol >= 0 ? keyCol : 0]).filter(Boolean))
      if (!indexes.error) {
        const iCol = indexes.columns.findIndex(c => c === 'index')
        setRedisIndexes(indexes.rows.map(row => row[iCol >= 0 ? iCol : 0]).filter(Boolean))
      }
      const sCol = streams.columns.findIndex(c => c === 'key')
      setRedisStreams(streams.rows.map(row => row[sCol >= 0 ? sCol : 0]).filter(Boolean))
    } catch { /* ignore */ }
    finally { setTreeLoading(false) }
  }

  const loadData = useCallback(async (obj: string, pg = 1, customSql?: string) => {
    if (!connectorId) return
    setLoading(true); setError(null); setRowFilter('')
    try {
      const body: Record<string, unknown> = { connectorId, page: pg, pageSize }
      if (isSql(connectorType)) {
        body.table = obj; body.schema = selectedSchema
        if (selectedDb) body.db = selectedDb
        if (customSql) body.query = customSql
      } else if (connectorType === 'rabbitmq') { body.queue = obj }
      else if (connectorType === 'mqtt') { body.topic = obj }
      else if (isRedis(connectorType)) {
        body.query = redisIndexes.includes(obj) ? `FT.SEARCH ${obj} * LIMIT 0 100`
          : redisStreams.includes(obj) ? `XRANGE ${obj} - + COUNT 100` : `HGETALL ${obj}`
      }
      const res = await fetch('/api/browser/data', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      const data: DataResult = await res.json()
      if (data.error) throw new Error(data.error)
      setDataResult(data); setPage(pg)
      logQuery(customSql ?? (isSql(connectorType) ? `-- browse: ${obj} page ${pg}` : `browse: ${obj}`), { rowCount: data.rowCount, totalRows: data.totalRows, durationMs: data.durationMs })
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally { setLoading(false) }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connectorId, connectorType, selectedSchema, selectedDb, redisIndexes, redisStreams, pageSize])

  async function loadQueueData(obj: string) {
    if (!connectorId) return
    setLoading(true)
    try {
      const body: Record<string, unknown> = { connectorId, pageSize: 100 }
      if (connectorType === 'rabbitmq') body.queue = obj
      else body.topic = obj
      const res = await fetch('/api/browser/data', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      const data: DataResult = await res.json()
      if (data.error || !data.rows.length) return
      const cols = data.columns
      const msgs: QueueMessage[] = data.rows.map((row, i) => {
        const get = (name: string) => { const ci = cols.indexOf(name); return ci >= 0 ? row[ci] : '' }
        return {
          id: `${Date.now()}-${i}-${Math.random().toString(36).slice(2)}`,
          timestamp: get('ts') || new Date().toISOString(),
          topic: get('topic') || undefined,
          queue: get('queue') || undefined,
          payload: get('payload'),
          qos: get('qos') || undefined,
          retained: get('retained') || undefined,
        }
      })
      setQueueMessages(prev => [...msgs, ...prev].slice(0, 500))
    } catch { /* ignore */ }
    finally { setLoading(false) }
  }

  function getRedisDataCmd(key: string, type: string): string {
    const k = key.includes(' ') ? `"${key.replace(/"/g, '\\"')}"` : key
    switch (type) {
      case 'string': return `GET ${k}`
      case 'hash': return `HGETALL ${k}`
      case 'list': return `LRANGE ${k} 0 499`
      case 'set': return `SMEMBERS ${k}`
      case 'zset': return `ZRANGE ${k} 0 499 WITHSCORES`
      case 'stream': return `XRANGE ${k} - + COUNT 100`
      case 'json':
      case 'ReJSON-RL': return `JSON.GET ${k} $`
      case 'timeseries':
      case 'TSDB-TYPE': return `TS.RANGE ${k} - +`
      default: return `HGETALL ${k}`
    }
  }

  async function loadRedisKeyFull(key: string) {
    if (!connectorId) return
    setLoading(true); setError(null); setRowFilter(''); setRedisKeyType(null)
    try {
      const base = { connectorId }
      // Detect type + TTL in parallel
      const [typeRes, ttlRes] = await Promise.all([
        fetch('/api/browser/data', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...base, query: `TYPE ${key}` }) }).then(r => r.json() as Promise<DataResult>),
        fetch('/api/browser/data', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...base, query: `TTL ${key}` }) }).then(r => r.json() as Promise<DataResult>),
      ])
      const rawType = typeRes.rows[0]?.[0] ?? 'string'
      const normalizedType = rawType === 'ReJSON-RL' ? 'json' : rawType === 'TSDB-TYPE' ? 'timeseries' : rawType
      const ttl = Number(ttlRes.rows[0]?.[0] ?? -1)
      setRedisKeyType(normalizedType)
      setRedisKeyTtl(ttl)
      const dataCmd = getRedisDataCmd(key, normalizedType)
      const dataRes = await fetch('/api/browser/data', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...base, query: dataCmd }) })
      const data: DataResult = await dataRes.json()
      if (data.error) throw new Error(data.error)
      setDataResult(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally { setLoading(false) }
  }

  async function publishMessage() {
    if (!connectorId || !publishPayload.trim()) return
    const target = publishTopic || selectedObject || ''
    setPublishRunning(true)
    try {
      const body: Record<string, unknown> = { connectorId, payload: publishPayload }
      if (connectorType === 'mqtt') body.topic = target
      else body.queue = target
      const res = await fetch('/api/browser/publish', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      const result = await res.json() as { ok?: boolean; error?: string; durationMs?: number }
      if (result.error) setPublishMsg({ ok: false, msg: result.error })
      else { setPublishMsg({ ok: true, msg: `Sent in ${result.durationMs ?? 0}ms` }); setPublishPayload('') }
    } catch (e) {
      setPublishMsg({ ok: false, msg: e instanceof Error ? e.message : String(e) })
    } finally {
      setPublishRunning(false)
      setTimeout(() => setPublishMsg(null), 4000)
    }
  }

  async function loadSchema(obj: string) {
    try { const r = await fetchCatalog('columns', { table: obj }); setSchemaResult(r.error ? null : r) }
    catch { setSchemaResult(null) }
  }

  async function loadIndexes(obj: string) {
    try { const r = await fetchCatalog('indexes', { table: obj }); setIndexResult(r.error ? null : r) }
    catch { setIndexResult(null) }
  }

  async function loadPrimaryKeys(obj: string) {
    try {
      const r = await fetchCatalog('primarykeys', { table: obj })
      if (r.error || !r.rows.length) { setPkColumns([]); return }
      const ci = r.columns.findIndex(c => c.toLowerCase().includes('column'))
      setPkColumns(r.rows.map(row => row[ci >= 0 ? ci : 0]).filter(Boolean))
    } catch { setPkColumns([]) }
  }

  // ── Execute DML
  async function executeQuery(sql: string) {
    const res = await fetch('/api/browser/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ connectorId, db: selectedDb || undefined, query: sql }),
    })
    const result = await res.json() as { rowsAffected?: number; totalRows?: number; rowCount?: number; error?: string; durationMs?: number }
    logQuery(sql, result)
    return result
  }

  // ── Table FQN for SQL generation
  const tableFqn = useMemo(() => {
    if (!selectedObject || !isSql(connectorType)) return undefined
    if (connectorType === 'mssql') return `${selectedSchema ? `[${selectedSchema}].` : ''}[${selectedObject}]`
    if (connectorType === 'mysql') return `${selectedDb ? `\`${selectedDb}\`.` : selectedSchema ? `\`${selectedSchema}\`.` : ''}\`${selectedObject}\``
    return `${selectedSchema ? `"${selectedSchema}".` : ''}"${selectedObject}"`
  }, [selectedObject, connectorType, selectedSchema, selectedDb])

  async function selectObject(obj: string) {
    setSelectedObject(obj); setActiveTab('data')
    setSqlQuery(buildDefaultSql(obj))
    if (isRedis(connectorType)) {
      setRedisKeyType(null)
      await loadRedisKeyFull(obj)
    } else if (isQueue(connectorType)) {
      setQueueMessages([])
      await loadQueueData(obj)
    } else {
      await loadData(obj, 1)
      if (isSql(connectorType)) { loadSchema(obj); loadIndexes(obj); loadPrimaryKeys(obj) }
    }
  }

  function buildDefaultSql(obj: string): string {
    if (!isSql(connectorType)) return ''
    if (connectorType === 'mssql') return `SELECT TOP 100 *\nFROM [${selectedSchema || 'dbo'}].[${obj}]`
    return `SELECT *\nFROM "${selectedSchema || 'public'}"."${obj}"\nLIMIT 100`
  }

  async function runSql() {
    if (!sqlQuery.trim() || !connectorId) return
    setQueryRunning(true); setActiveTab('data'); setError(null); setRowFilter('')
    try {
      const body: Record<string, unknown> = { connectorId, query: sqlQuery, schema: selectedSchema, page: 1, pageSize: 500 }
      if (selectedDb) body.db = selectedDb
      const res = await fetch('/api/browser/data', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
      const data: DataResult = await res.json()
      if (data.error) throw new Error(data.error)
      setDataResult(data); setPage(1); setSelectedObject(null); setPkColumns([])
      logQuery(sqlQuery, { rowCount: data.rowCount, totalRows: data.totalRows, durationMs: data.durationMs })
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally { setQueryRunning(false) }
  }

  const suggestions = useMemo(() => {
    const base = isSql(connectorType) ? SQL_KEYWORDS : isRedis(connectorType) ? REDIS_COMMANDS : []
    return [...base, ...tables, ...views, ...procedures, ...queues, ...redisKeys, ...redisIndexes, ...redisStreams]
  }, [connectorType, tables, views, procedures, queues, redisKeys, redisIndexes, redisStreams])

  const totalPages = dataResult ? Math.max(1, Math.ceil(dataResult.totalRows / pageSize)) : 1
  const hasConnector = !!connectorId && !!connectorType

  return (
    <div className="flex flex-col h-full bg-[#08090e] overflow-hidden">

      {/* ── Top bar ── */}
      <div className="flex items-center gap-2 px-3 h-12 border-b border-chef-border shrink-0 bg-chef-surface">
        <div className="flex items-center gap-1.5 min-w-0">
          <div className="shrink-0">{TYPE_ICON[connectorType] ?? <Database size={13} className="text-chef-muted" />}</div>
          <select value={connectorId} onChange={e => setConnectorId(e.target.value)} className="bg-transparent text-chef-text text-[12px] outline-none cursor-pointer max-w-[140px] truncate">
            {connectors.length === 0 && <option value="">No connectors</option>}
            {connectors.map(c => <option key={c.id} value={c.id} className="bg-chef-card">{c.name}</option>)}
          </select>
        </div>

        {isSql(connectorType) && databases.length > 0 && (
          <><span className="text-chef-muted/40">/</span>
          <select value={selectedDb} onChange={e => setSelectedDb(e.target.value)} className="bg-transparent text-chef-text text-[12px] outline-none cursor-pointer max-w-[120px] truncate">
            {databases.map(db => <option key={db} value={db} className="bg-chef-card">{db}</option>)}
          </select></>
        )}

        {isSql(connectorType) && schemas.length > 0 && (
          <><span className="text-chef-muted/40">/</span>
          <select value={selectedSchema} onChange={e => setSelectedSchema(e.target.value)} className="bg-transparent text-chef-text text-[12px] outline-none cursor-pointer max-w-[120px] truncate">
            {schemas.map(s => <option key={s} value={s} className="bg-chef-card">{s}</option>)}
          </select></>
        )}

        <div className="flex-1" />

        {hasConnector && (
          <div className="text-[11px] text-chef-muted font-mono">
            {isSql(connectorType) && `${tables.length}t ${views.length}v ${procedures.length}p`}
            {isQueue(connectorType) && `${queues.length} ${connectorType === 'mqtt' ? 'topics' : 'queues'}`}
            {isRedis(connectorType) && `${redisKeys.length} keys`}
          </div>
        )}

        <button
          onClick={() => setPanelOpen(o => !o)}
          className={`p-1.5 rounded transition-colors ${panelOpen ? 'text-indigo-400 bg-indigo-500/10' : 'text-chef-muted hover:text-chef-text hover:bg-white/[0.04]'}`}
          title="Toggle debug / tools panel"
        >
          <Terminal size={13} />
        </button>

        <button
          onClick={() => { if (isSql(connectorType)) loadSchemas(); else if (isQueue(connectorType)) loadQueueTree(); else if (isRedis(connectorType)) loadRedisTree() }}
          className="p-1.5 text-chef-muted hover:text-chef-text hover:bg-white/[0.04] rounded transition-colors"
          title="Refresh"
        >
          <RefreshCw size={13} />
        </button>
      </div>

      {/* ── Body ── */}
      <div className="flex flex-1 min-h-0">

        {/* Left tree */}
        <div className="w-52 shrink-0 border-r border-chef-border bg-chef-surface flex flex-col overflow-hidden">
          <div className="flex-1 overflow-y-auto py-2 px-1">
            {!hasConnector && <div className="text-center py-8 text-[11px] text-chef-muted">No connector selected</div>}

            {isSql(connectorType) && (
              <>
                <TreeSection title="Tables" icon={<Table2 size={11} />} items={tables} selected={selectedObject} onSelect={selectObject} loading={treeLoading} />
                <TreeSection title="Views" icon={<Eye size={11} />} items={views} selected={selectedObject} onSelect={selectObject} loading={treeLoading} />
                <TreeSection title="Procedures" icon={<Code2 size={11} />} items={procedures} selected={selectedObject} onSelect={selectObject} loading={treeLoading} />
              </>
            )}

            {isQueue(connectorType) && (
              <TreeSection title={connectorType === 'mqtt' ? 'Topics' : 'Queues'} icon={connectorType === 'mqtt' ? <Radio size={11} /> : <MessageSquare size={11} />} items={queues} selected={selectedObject} onSelect={selectObject} loading={treeLoading} />
            )}

            {isRedis(connectorType) && (
              <>
                <TreeSection title="Keys" icon={<KeyRound size={11} />} items={redisKeys} selected={selectedObject} onSelect={selectObject} loading={treeLoading} />
                {redisIndexes.length > 0 && <TreeSection title="Search Indexes" icon={<Search size={11} />} items={redisIndexes} selected={selectedObject} onSelect={selectObject} />}
                {redisStreams.length > 0 && <TreeSection title="Streams" icon={<GitBranch size={11} />} items={redisStreams} selected={selectedObject} onSelect={selectObject} />}
              </>
            )}
          </div>
        </div>

        {/* Right content */}
        <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
          {!hasConnector ? (
            <div className="flex-1 flex items-center justify-center text-chef-muted text-sm">Select a connector to start browsing</div>
          ) : (
            <>
              {(isSql(connectorType) || isRedis(connectorType)) && (
                <div className="px-3 pt-3 pb-2 border-b border-chef-border shrink-0">
                  <QueryBar value={sqlQuery} onChange={setSqlQuery} onRun={runSql} running={queryRunning} suggestions={suggestions} />
                </div>
              )}

              {/* Header */}
              <div className="flex items-center gap-2 px-3 py-2 border-b border-chef-border shrink-0">
                <div className="flex items-center gap-1.5 flex-1 min-w-0">
                  {selectedObject ? (
                    <>
                      <Layers size={12} className="text-indigo-400 shrink-0" />
                      <span className="text-[12px] font-medium text-chef-text truncate">{selectedObject}</span>
                      {dataResult && <span className="text-[10px] text-chef-muted font-mono ml-1">{dataResult.totalRows.toLocaleString()} rows{dataResult.durationMs != null && ` · ${dataResult.durationMs}ms`}</span>}
                      {pkColumns.length > 0 && <span className="text-[10px] text-amber-400/70 font-mono flex items-center gap-0.5"><KeyRound size={9} />{pkColumns.join(', ')}</span>}
                    </>
                  ) : (
                    <span className="text-[12px] text-chef-muted">Select an object from the tree</span>
                  )}
                </div>

                {isSql(connectorType) && selectedObject && (
                  <div className="flex items-center gap-0.5">
                    {(['data', 'schema', 'indexes', 'info'] as const).map(tab => (
                      <button key={tab} onClick={() => setActiveTab(tab)} className={`px-2.5 py-1 text-[11px] rounded capitalize transition-colors flex items-center gap-1 ${activeTab === tab ? 'bg-indigo-500/15 text-indigo-400' : 'text-chef-muted hover:text-chef-text'}`}>
                        {tab === 'info' && <Info size={10} />}{tab}
                      </button>
                    ))}
                  </div>
                )}
                {isRedis(connectorType) && selectedObject && redisKeyType && (
                  <span className={`text-[10px] font-semibold uppercase tracking-wider px-1.5 py-0.5 rounded border font-mono ${REDIS_TYPE_COLOR[redisKeyType] ?? 'text-chef-muted border-chef-border'}`}>{redisKeyType}</span>
                )}

                {activeTab === 'data' && dataResult && dataResult.rows.length > 0 && (
                  <div className="flex items-center gap-1 bg-chef-surface border border-chef-border rounded px-2 py-0.5">
                    <Search size={10} className="text-chef-muted shrink-0" />
                    <input value={rowFilter} onChange={e => setRowFilter(e.target.value)} placeholder="filter rows…" className="bg-transparent text-[11px] text-chef-text placeholder:text-chef-muted outline-none w-28" />
                    {rowFilter && <button onClick={() => setRowFilter('')} className="text-chef-muted hover:text-chef-text"><X size={10} /></button>}
                  </div>
                )}
              </div>

              {/* Content */}
              <div className="flex-1 min-h-0 overflow-hidden flex flex-col">
                {error && (
                  <div className="mx-3 mt-3 flex items-start gap-2 bg-rose-500/10 border border-rose-500/20 rounded-lg px-3 py-2.5 text-[12px] text-rose-300">
                    <AlertCircle size={14} className="shrink-0 mt-0.5" />
                    <span className="font-mono break-all">{error}</span>
                  </div>
                )}

                {loading && <div className="flex items-center justify-center flex-1 gap-2 text-chef-muted text-xs"><Loader2 size={16} className="animate-spin" />Loading…</div>}

                {!loading && !error && activeTab === 'data' && isRedis(connectorType) && selectedObject && (
                  <RedisKeyViewer
                    keyName={selectedObject}
                    keyType={redisKeyType}
                    keyTtl={redisKeyTtl}
                    dataResult={dataResult}
                    onRefresh={() => loadRedisKeyFull(selectedObject)}
                  />
                )}

                {!error && activeTab === 'data' && isQueue(connectorType) && (
                  <QueueViewer
                    connectorType={connectorType}
                    selectedObject={selectedObject}
                    messages={queueMessages}
                    loading={loading}
                    autoRefresh={autoRefresh}
                    refreshIntervalMs={refreshIntervalMs}
                    onToggleAutoRefresh={() => setAutoRefresh(p => !p)}
                    onIntervalChange={setRefreshIntervalMs}
                    onRefresh={() => selectedObject && loadQueueData(selectedObject)}
                    onClearMessages={() => setQueueMessages([])}
                    publishTopic={publishTopic}
                    publishPayload={publishPayload}
                    publishRunning={publishRunning}
                    publishMsg={publishMsg}
                    onPublishTopicChange={setPublishTopic}
                    onPublishPayloadChange={setPublishPayload}
                    onPublish={publishMessage}
                  />
                )}

                {!loading && !error && activeTab === 'data' && isSql(connectorType) && dataResult && (
                  <>
                    <DataGrid columns={dataResult.columns} rows={dataResult.rows} filter={rowFilter} pkColumns={pkColumns} tableFqn={tableFqn} connectorType={connectorType} exportName={selectedObject ?? undefined} onExecute={selectedObject ? executeQuery : undefined} onRefresh={selectedObject ? () => loadData(selectedObject, page) : undefined} />
                    <div className="flex items-center justify-between px-3 py-1.5 border-t border-chef-border bg-chef-surface shrink-0">
                      <button disabled={page <= 1} onClick={() => selectedObject && loadData(selectedObject, page - 1)} className="px-2 py-0.5 text-[11px] text-chef-muted hover:text-chef-text disabled:opacity-30 disabled:cursor-not-allowed">← prev</button>
                      <div className="flex items-center gap-3">
                        <span className="text-[11px] font-mono text-chef-muted">
                          {dataResult.totalRows > pageSize ? `page ${page}/${totalPages} · ${dataResult.totalRows.toLocaleString()} rows` : `${dataResult.rowCount.toLocaleString()} of ${dataResult.totalRows.toLocaleString()} rows`}
                        </span>
                        <select value={pageSize} onChange={e => { setPageSize(Number(e.target.value)); selectedObject && loadData(selectedObject, 1) }} className="bg-chef-card border border-chef-border text-[11px] text-chef-muted rounded px-1 py-0.5 outline-none">
                          {[100, 500, 1000, 2000].map(n => <option key={n} value={n}>{n}/page</option>)}
                        </select>
                      </div>
                      <button disabled={page >= totalPages} onClick={() => selectedObject && loadData(selectedObject, page + 1)} className="px-2 py-0.5 text-[11px] text-chef-muted hover:text-chef-text disabled:opacity-30 disabled:cursor-not-allowed">next →</button>
                    </div>
                  </>
                )}

                {!loading && !error && activeTab === 'schema' && (
                  schemaResult ? <DataGrid columns={schemaResult.columns} rows={schemaResult.rows} filter={rowFilter} /> : <div className="text-center py-8 text-chef-muted text-xs">No schema info</div>
                )}

                {!loading && !error && activeTab === 'indexes' && (
                  indexResult ? <DataGrid columns={indexResult.columns} rows={indexResult.rows} filter={rowFilter} /> : <div className="text-center py-8 text-chef-muted text-xs">No indexes</div>
                )}

                {!loading && !error && activeTab === 'info' && selectedObject && (
                  <div className="p-4 overflow-y-auto flex-1">
                    <div className="max-w-md">
                      <h3 className="text-[12px] font-semibold text-chef-text mb-3 flex items-center gap-1.5"><Info size={13} className="text-indigo-400" />Table Info</h3>
                      <dl className="space-y-0 text-[12px]">
                        {[{ label: 'Object', value: selectedObject }, { label: 'Schema', value: selectedSchema || '—' }, { label: 'Database', value: selectedDb || '—' }, { label: 'Connector', value: connectorType.toUpperCase() }].map(({ label, value }) => (
                          <div key={label} className="flex border-b border-chef-border/30 py-1.5">
                            <dt className="w-28 text-chef-muted shrink-0">{label}</dt>
                            <dd className="text-chef-text font-mono truncate">{value}</dd>
                          </div>
                        ))}
                        <div className="mt-2 pt-1" />
                        {[{ label: 'Row Count', value: dataResult?.totalRows.toLocaleString() ?? '—' }, { label: 'Columns', value: schemaResult ? String(schemaResult.rows.length) : '—' }, { label: 'Primary Key', value: pkColumns.length ? pkColumns.join(', ') : 'none detected' }, { label: 'Indexes', value: indexResult ? String(indexResult.rows.length) : '—' }, { label: 'Query Time', value: dataResult?.durationMs != null ? `${dataResult.durationMs}ms` : '—' }].map(({ label, value }) => (
                          <div key={label} className="flex border-b border-chef-border/30 py-1.5">
                            <dt className="w-28 text-chef-muted shrink-0">{label}</dt>
                            <dd className={`font-mono ${label === 'Primary Key' && pkColumns.length === 0 ? 'text-chef-muted/50 italic' : 'text-chef-text'}`}>{value}</dd>
                          </div>
                        ))}
                      </dl>
                      {pkColumns.length > 0 ? (
                        <div className="mt-4 p-2.5 bg-amber-500/5 border border-amber-500/20 rounded-lg text-[11px] text-amber-400/80">
                          <KeyRound size={11} className="inline mr-1.5" />Inline editing and row deletion enabled — double-click any cell to edit.
                        </div>
                      ) : isSql(connectorType) ? (
                        <div className="mt-4 p-2.5 bg-chef-surface border border-chef-border/50 rounded-lg text-[11px] text-chef-muted">No primary key detected. Inline editing requires a primary key.</div>
                      ) : null}
                    </div>
                  </div>
                )}

                {!loading && !error && !dataResult && !selectedObject && !isQueue(connectorType) && (
                  <div className="flex-1 flex flex-col items-center justify-center gap-3 text-chef-muted">
                    <Database size={32} className="text-chef-border" />
                    <div className="text-sm">Select an object from the tree to preview data</div>
                    {isSql(connectorType) && <div className="text-xs text-chef-muted/60">or write a query above and press ⌘↵</div>}
                  </div>
                )}
              </div>
            </>
          )}
        </div>
      </div>

      {/* ── Bottom panel ── */}
      {panelOpen && (
        <BottomPanel
          height={panelHeight}
          onResizeStart={handleResizeStart}
          tab={panelTab}
          onTabChange={t => {
            setPanelTab(t)
            if (t === 'tools' && (!isSql(connectorType) || !selectedObject)) return
          }}
          onClose={() => setPanelOpen(false)}
          queryLog={queryLog}
          onClearLog={() => setQueryLog([])}
          onLoadSql={sql => { setSqlQuery(sql); setPanelOpen(false) }}
          connectorType={connectorType}
          selectedObject={selectedObject}
          tableFqn={tableFqn}
          schemaResult={schemaResult}
          dataResult={dataResult}
          pkColumns={pkColumns}
          onExecute={isSql(connectorType) ? executeQuery : undefined}
          onRefresh={selectedObject ? () => loadData(selectedObject, page) : undefined}
        />
      )}
    </div>
  )
}
