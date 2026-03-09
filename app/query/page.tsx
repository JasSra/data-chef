'use client'

import { useState, useEffect, useRef, useCallback } from 'react'
import {
  Play, ChevronDown, Copy, Download,
  Zap, Table, Code2, AlertCircle, Loader2, CheckCircle2,
  ArrowRight, History, ChevronRight, X, Server,
  BarChart2, Clock, Plus, Trash2, Database, Save,
} from 'lucide-react'
import { useAppSettings } from '@/components/SettingsProvider'

/* ── Types ──────────────────────────────────────────────────────────────────── */
type Lang = 'sql' | 'jsonpath' | 'jmespath' | 'kql' | 'redis'

interface SchemaField { field: string; type: string }

interface DatasetMeta {
  id:     string
  name:   string
  badge:  string
  desc:   string
  sourceType: 'dataset' | 'connector'
  sourceId: string
  connectorType?: string
  resource?: string
  fields: string[]      // just names — for autocomplete
  schema: SchemaField[] // full schema — for the fields panel
}

interface QResult {
  columns: string[]; rows: string[][]
  rowCount: number; totalRows: number
  bytesScanned: number; durationMs: number
  kqlTranslated?: string; provider?: string; error?: string
}

interface HistoryEntry {
  id: string; lang: Lang; dataset: string
  query: string; rowCount: number
  durationMs: number; ts: number; error?: string
  redisMode?: RedisMode
  redisValueType?: RedisValueType
}

interface SavedQuery { id: string; lang: Lang; name: string; query: string }

interface ObservabilityConnector { id: string; name: string; type: string }
interface SavedObservabilityQuery { id: string; name: string; kql: string; createdAt: number }
type RedisMode = 'command' | 'search' | 'json' | 'timeseries' | 'stream' | 'catalog'
type RedisValueType = 'auto' | 'string' | 'hash' | 'list' | 'set' | 'zset' | 'json' | 'timeseries' | 'stream' | 'search'
type RedisCatalogKind = 'commands' | 'capabilities' | 'keyspaces' | 'keys' | 'indexes' | 'streams'

interface RedisCapabilitySnapshot {
  serverKind: 'redis' | 'redis-stack'
  redisVersion: string
  modules: string[]
  supportsSearch: boolean
  supportsJson: boolean
  supportsTimeSeries: boolean
  supportsBloom: boolean
  supportsGraph: boolean
  supportsStreams: boolean
  dbCount: number | null
  error?: string
}

interface RedisCatalogResult {
  columns: string[]
  rows: string[][]
  rowCount: number
  totalRows: number
  durationMs: number
  redisMode: RedisMode
  capabilities?: RedisCapabilitySnapshot
  catalogMeta?: Record<string, unknown>
  error?: string
}

const AI_TIMESPAN_PRESETS = [
  { label: 'Last 1h',  value: 'PT1H'  },
  { label: 'Last 6h',  value: 'PT6H'  },
  { label: 'Last 24h', value: 'PT24H' },
  { label: 'Last 7d',  value: 'P7D'   },
  { label: 'Last 30d', value: 'P30D'  },
]

function defaultObservabilityKql(type: string): string {
  if (type === 'elasticsearch') return 'logs\n| where @timestamp > ago(24h)\n| limit 100'
  if (type === 'datadog') return 'logs\n| where status:error\n| limit 100'
  return 'requests\n| where timestamp > ago(24h)\n| summarize count() by bin(timestamp, 1h)\n| order by timestamp asc'
}

/* ── Language config ─────────────────────────────────────────────────────────── */
const langMeta: Record<Lang, { label: string; color: string }> = {
  sql:      { label: 'SQL',      color: 'text-sky-400'    },
  jsonpath: { label: 'JSONPath', color: 'text-violet-400' },
  jmespath: { label: 'JMESPath', color: 'text-emerald-400'},
  kql:      { label: 'KQL',      color: 'text-amber-400'  },
  redis:    { label: 'Redis',    color: 'text-red-400'    },
}

const REDIS_MODE_TEMPLATES: Record<RedisMode, string> = {
  command: 'SCAN 0 MATCH * COUNT 50',
  search: 'FT.SEARCH idx:documents "*" LIMIT 0 25',
  json: 'JSON.GET user:42 $',
  timeseries: 'TS.RANGE metrics:cpu - +',
  stream: 'XRANGE orders:stream - + COUNT 50',
  catalog: 'commands',
}

/* ── Field type colours ──────────────────────────────────────────────────────── */
const TYPE_COLOR: Record<string, string> = {
  integer:   'text-orange-400',
  float:     'text-amber-400',
  string:    'text-emerald-400',
  timestamp: 'text-sky-400',
  date:      'text-sky-300',
  boolean:   'text-rose-400',
  object:    'text-violet-400',
  array:     'text-violet-300',
}

/* ── Saved queries for live datasets ─────────────────────────────────────────── */
const savedQueriesMap: Record<string, SavedQuery[]> = {
  'rick-morty-characters': [
    { id: 'q1', lang: 'sql',      name: 'Species breakdown',       query: `SELECT species, COUNT(*) AS total\nFROM characters\nGROUP BY species\nORDER BY total DESC\nLIMIT 20` },
    { id: 'q2', lang: 'sql',      name: 'Active species (HAVING)',  query: `SELECT species, COUNT(*) AS total\nFROM characters\nGROUP BY species\nHAVING total > 5\nORDER BY total DESC` },
    { id: 'q3', lang: 'sql',      name: 'Multi-episode characters', query: `SELECT name, species, episodes, origin\nFROM characters\nWHERE episodes > 20\nORDER BY episodes DESC\nLIMIT 20` },
    { id: 'q4', lang: 'sql',      name: 'Distinct origins',         query: `SELECT DISTINCT origin\nFROM characters\nWHERE status = 'Alive'\nORDER BY origin ASC\nLIMIT 30` },
    { id: 'q5', lang: 'sql',      name: 'Not Human or Alien',       query: `SELECT species, COUNT(*) AS cnt\nFROM characters\nWHERE species NOT IN ('Human', 'Alien')\nGROUP BY species\nORDER BY cnt DESC` },
    { id: 'q6', lang: 'kql',      name: 'Dead by species (KQL)',    query: `characters\n| where status == "Dead"\n| summarize count() by species\n| order by count_ desc\n| limit 10` },
    { id: 'q7', lang: 'jsonpath', name: 'Alive human names',        query: `$[?(@.status=="Alive" && @.species=="Human")].name` },
    { id: 'q8', lang: 'jmespath', name: 'Project name+origin',      query: `[?status=='Alive'].{name: name, species: species, origin: origin.name}` },
  ],
  events: [
    { id: 'e1', lang: 'sql',  name: 'Revenue by country',       query: `SELECT country, COUNT(*) AS orders, SUM(amount) AS revenue\nFROM events\nWHERE event_type = 'purchase'\nGROUP BY country\nORDER BY revenue DESC\nLIMIT 10` },
    { id: 'e2', lang: 'sql',  name: 'High-volume countries',    query: `SELECT country, COUNT(*) AS orders, SUM(amount) AS revenue\nFROM events\nWHERE event_type = 'purchase'\nGROUP BY country\nHAVING orders > 5000\nORDER BY revenue DESC` },
    { id: 'e3', lang: 'sql',  name: 'Events by type + device',  query: `SELECT event_type, device, COUNT(*) AS cnt\nFROM events\nGROUP BY event_type, device\nORDER BY cnt DESC` },
    { id: 'e4', lang: 'sql',  name: 'Top 20 users by spend',    query: `SELECT user_id, SUM(amount) AS total_spend, COUNT(*) AS txns\nFROM events\nWHERE event_type = 'purchase'\nGROUP BY user_id\nORDER BY total_spend DESC\nLIMIT 20` },
    { id: 'e5', lang: 'sql',  name: 'Daily purchase volume',    query: `SELECT ts, COUNT(*) AS orders, SUM(amount) AS revenue\nFROM events\nWHERE event_type = 'purchase'\nGROUP BY ts\nORDER BY ts DESC\nLIMIT 30` },
    { id: 'e6', lang: 'sql',  name: 'Non-purchase events',      query: `SELECT event_type, COUNT(*) AS cnt\nFROM events\nWHERE event_type NOT IN ('purchase', 'view')\nGROUP BY event_type\nORDER BY cnt DESC` },
    { id: 'e7', lang: 'sql',  name: 'Distinct active countries',query: `SELECT DISTINCT country\nFROM events\nWHERE event_type = 'purchase'\nORDER BY country ASC` },
    { id: 'e8', lang: 'kql',  name: 'Mobile errors (KQL)',      query: `events\n| where event_type == "error" and device == "mobile"\n| summarize count() by country\n| order by count_ desc\n| limit 10` },
  ],
}

/* ── SQL keywords for autocomplete ──────────────────────────────────────────── */
const SQL_KW = [
  'SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT',
  'DISTINCT', 'AS', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN',
  'IS', 'NULL', 'DESC', 'ASC', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX',
  'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'JOIN', 'LEFT', 'INNER', 'ON',
  'COALESCE', 'CAST',
]

/* ── Syntax highlighter ──────────────────────────────────────────────────────── */
const SQL_TOKEN = /(--[^\n]*)|(\/\*[\s\S]*?\*\/)|('(?:[^'\\]|\\.)*')|("(?:[^"\\]|\\.)*")|(\b(?:SELECT|FROM|WHERE|GROUP|BY|ORDER|HAVING|LIMIT|DISTINCT|AS|AND|OR|NOT|IN|LIKE|BETWEEN|IS|NULL|DESC|ASC|COUNT|SUM|AVG|MIN|MAX|CASE|WHEN|THEN|ELSE|END|JOIN|LEFT|INNER|OUTER|ON|USING|CROSS|FULL)\b)|(\b\d+(?:\.\d+)?\b)/gi

function highlightSQL(code: string): string {
  const esc = code.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  return esc.replace(SQL_TOKEN, (m, lineComment, blockComment, singleStr, doubleStr, keyword, num) => {
    if (lineComment || blockComment) return `<span style="color:#64748b;font-style:italic">${m}</span>`
    if (singleStr || doubleStr)      return `<span style="color:#34d399">${m}</span>`
    if (keyword)                     return `<span style="color:#60a5fa;font-weight:600">${m}</span>`
    if (num)                         return `<span style="color:#fb923c">${m}</span>`
    return m
  })
}

const KQL_TOKEN = /(\/\/[^\n]*)|('(?:[^'\\]|\\.)*')|("(?:[^"\\]|\\.)*")|(\b(?:where|summarize|order|by|sort|project|limit|take|top|count|sum|avg|min|max|and|or|not|contains|startswith|endswith|has)\b)|(\b\d+(?:\.\d+)?\b)/gi

function highlightKQL(code: string): string {
  const esc = code.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  return esc.replace(KQL_TOKEN, (m, comment, singleStr, doubleStr, keyword, num) => {
    if (comment)              return `<span style="color:#64748b;font-style:italic">${m}</span>`
    if (singleStr||doubleStr) return `<span style="color:#34d399">${m}</span>`
    if (keyword)              return `<span style="color:#fbbf24;font-weight:600">${m}</span>`
    if (num)                  return `<span style="color:#fb923c">${m}</span>`
    return m
  })
}

function highlightCode(code: string, lang: Lang): string {
  if (lang === 'sql' || lang === 'kql')
    return lang === 'sql' ? highlightSQL(code) : highlightKQL(code)
  // JSONPath / JMESPath: highlight strings and operators
  const esc = code.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  return esc
    .replace(/("(?:[^"\\]|\\.)*")/g, '<span style="color:#34d399">$1</span>')
    .replace(/('(?:[^'\\]|\\.)*')/g, '<span style="color:#34d399">$1</span>')
    .replace(/(\$|@|\*|\?|\.\.)/g,    '<span style="color:#60a5fa">$1</span>')
}

/* ── Helpers ─────────────────────────────────────────────────────────────────── */
function fmtBytes(b: number) {
  if (b >= 1e9) return `${(b / 1e9).toFixed(1)} GB`
  if (b >= 1e6) return `${(b / 1e6).toFixed(1)} MB`
  if (b >= 1e3) return `${(b / 1e3).toFixed(0)} KB`
  return `${b} B`
}
function fmtNum(n: number) {
  return n >= 1e6 ? `${(n / 1e6).toFixed(1)}M` :
         n >= 1e3 ? `${(n / 1e3).toFixed(0)}K` : String(n)
}
function timeAgo(ts: number) {
  const s = Math.floor((Date.now() - ts) / 1000)
  if (s < 60)   return `${s}s ago`
  if (s < 3600) return `${Math.floor(s / 60)}m ago`
  return `${Math.floor(s / 3600)}h ago`
}

type SortDirection = 'asc' | 'desc'
type SortState = { column: string; direction: SortDirection }

function formatSqlIdentifier(column: string) {
  return /^\w+$/.test(column) ? column : `"${column.replace(/"/g, '""')}"`
}

function formatKqlIdentifier(column: string) {
  return /^\w+$/.test(column) ? column : `['${column.replace(/'/g, "''")}']`
}

function detectQuerySort(queryText: string, currentLang: Lang): SortState | null {
  if (currentLang === 'sql') {
    const upper = queryText.toUpperCase()
    const orderMatches = [...upper.matchAll(/\bORDER\s+BY\b/g)]
    const orderIdx = orderMatches.length ? (orderMatches[orderMatches.length - 1].index ?? -1) : -1
    if (orderIdx === -1) return null
    const limitMatches = [...upper.matchAll(/\bLIMIT\b/g)]
    const limitIdx = limitMatches.length ? (limitMatches[limitMatches.length - 1].index ?? -1) : -1
    const clause = queryText.slice(orderIdx, limitIdx > orderIdx ? limitIdx : undefined)
      .replace(/^ORDER\s+BY\s+/i, '')
      .trim()
    const match = clause.match(/^(.+?)(?:\s+(ASC|DESC))?\s*$/i)
    if (!match) return null
    const rawColumn = match[1].trim()
    return {
      column: rawColumn.replace(/^"(.*)"$/, '$1'),
      direction: match[2]?.toLowerCase() === 'desc' ? 'desc' : 'asc',
    }
  }

  if (currentLang === 'kql') {
    const stages = queryText.replace(/\/\/[^\n]*/g, '').split('|').map(s => s.trim()).filter(Boolean)
    const sortStage = stages.slice().reverse().find(stage => /^(order|sort)\s+by\s+/i.test(stage))
    if (!sortStage) return null
    const clause = sortStage.replace(/^(order|sort)\s+by\s+/i, '').trim()
    const match = clause.match(/^(.+?)(?:\s+(asc|desc))?\s*$/i)
    if (!match) return null
    return {
      column: match[1].trim().replace(/^\['(.+)'\]$/, '$1').replace(/''/g, "'"),
      direction: match[2]?.toLowerCase() === 'desc' ? 'desc' : 'asc',
    }
  }

  return null
}

function rewriteSqlSort(queryText: string, column: string, direction: SortDirection) {
  const trimmed = queryText.trim().replace(/;+\s*$/, '')
  const upper = trimmed.toUpperCase()
  const orderMatches = [...upper.matchAll(/\bORDER\s+BY\b/g)]
  const limitMatches = [...upper.matchAll(/\bLIMIT\b/g)]
  const orderIdx = orderMatches.length ? (orderMatches[orderMatches.length - 1].index ?? -1) : -1
  const limitIdx = limitMatches.length ? (limitMatches[limitMatches.length - 1].index ?? -1) : -1
  const sortClause = `ORDER BY ${formatSqlIdentifier(column)} ${direction.toUpperCase()}`

  if (orderIdx !== -1 && (limitIdx === -1 || orderIdx < limitIdx)) {
    const before = trimmed.slice(0, orderIdx).trimEnd()
    const after = limitIdx > orderIdx ? trimmed.slice(limitIdx).trim() : ''
    return after ? `${before}\n${sortClause}\n${after}` : `${before}\n${sortClause}`
  }

  if (limitIdx !== -1) {
    const before = trimmed.slice(0, limitIdx).trimEnd()
    const after = trimmed.slice(limitIdx).trim()
    return `${before}\n${sortClause}\n${after}`
  }

  return `${trimmed}\n${sortClause}`
}

function rewriteKqlSort(queryText: string, column: string, direction: SortDirection) {
  const stages = queryText.split('|').map(s => s.trim()).filter(Boolean)
  if (stages.length === 0) return queryText

  const [source, ...rawOps] = stages
  const ops = rawOps.filter(op => !/^(order|sort)\s+by\s+/i.test(op))
  const sortClause = `order by ${formatKqlIdentifier(column)} ${direction}`
  const limitIdx = ops.findIndex(op => /^(limit|take|top)\s+\d+/i.test(op))

  if (limitIdx === -1) ops.push(sortClause)
  else ops.splice(limitIdx, 0, sortClause)

  return [source, ...ops.map(op => `| ${op}`)].join('\n')
}

function normalizeObjectLikeCell(cell: string) {
  if (!cell.includes('[object Object]')) return cell
  const parts = cell.split(',').map(part => part.trim())
  if (parts.every(part => part === '[object Object]')) {
    return `[Object × ${parts.length}]`
  }
  return cell.replace(/\[object Object\]/g, '{...}')
}

function renderCellValue(cell: string) {
  return normalizeObjectLikeCell(cell)
}

function encodeDelimitedValue(value: string, delimiter: ',' | '\t') {
  const needsQuotes = value.includes(delimiter) || value.includes('\n') || value.includes('\r') || value.includes('"')
  const escaped = value.replace(/"/g, '""')
  return needsQuotes ? `"${escaped}"` : escaped
}

function rowsToDelimited(columns: string[], rows: string[][], delimiter: ',' | '\t') {
  return [
    columns.map(column => encodeDelimitedValue(column, delimiter)).join(delimiter),
    ...rows.map(row => row.map(cell => encodeDelimitedValue(renderCellValue(cell), delimiter)).join(delimiter)),
  ].join('\n')
}

const HISTORY_KEY = 'datachef:queryHistory'
function loadHistory(): HistoryEntry[] {
  try { return JSON.parse(localStorage.getItem(HISTORY_KEY) ?? '[]') } catch { return [] }
}
function saveHistory(h: HistoryEntry[]) {
  try { localStorage.setItem(HISTORY_KEY, JSON.stringify(h.slice(0, 50))) } catch { /* noop */ }
}

/* ── Fallback datasets (shown immediately before API loads) ──────────────────── */
const FALLBACK_DATASETS: DatasetMeta[] = []
const GENERIC_QUERYABLE_CONNECTOR_TYPES = ['http', 'postgresql', 'mysql', 'mongodb', 's3', 'sftp', 'bigquery', 'azureb2c', 'azureentraid']

/* ── Page ─────────────────────────────────────────────────────────────────────── */
export default function QueryPage() {
  const { settings } = useAppSettings()
  const [allDatasets,   setAllDatasets]   = useState<DatasetMeta[]>(FALLBACK_DATASETS)
  const [dataset,       setDataset]       = useState('')
  const [lang,          setLang]          = useState<Lang>('sql')
  const [query,         setQuery]         = useState('SELECT *\nFROM dataset\nLIMIT 100')
  const [results,       setResults]       = useState<QResult | null>(null)
  const [running,       setRunning]       = useState(false)
  const [queryError,    setQueryError]    = useState<string | null>(null)
  const [showLangMenu,  setShowLangMenu]  = useState(false)
  const [showDataMenu,  setShowDataMenu]  = useState(false)
  const [showHistory,   setShowHistory]   = useState(false)
  const [showSchema,    setShowSchema]    = useState(true)
  const [history,       setHistory]       = useState<HistoryEntry[]>([])
  const [sortState,     setSortState]     = useState<SortState | null>(null)
  const [selectedRows,  setSelectedRows]  = useState<number[]>([])
  const [lastSelectedRow, setLastSelectedRow] = useState<number | null>(null)
  const [copyFeedback,  setCopyFeedback]  = useState<'selected' | 'all' | null>(null)

  /* Observability mode */
  const [aiConnectors,     setAiConnectors]     = useState<ObservabilityConnector[]>([])
  const [genericConnectors, setGenericConnectors] = useState<ObservabilityConnector[]>([])
  const [aiConnectorId,    setAiConnectorId]    = useState<string | null>(null)
  const [aiTimespan,       setAiTimespan]       = useState('PT24H')
  const [showAiTimeMenu,   setShowAiTimeMenu]   = useState(false)
  const [showAiConnMenu,   setShowAiConnMenu]   = useState(false)
  const [aiSavedQueries,   setAiSavedQueries]   = useState<SavedObservabilityQuery[]>([])
  const [showSaveAiInput,  setShowSaveAiInput]  = useState(false)
  const [saveAiName,       setSaveAiName]       = useState('')
  const [storeLocally,     setStoreLocally]     = useState(false)
  const [storeDatasetName, setStoreDatasetName] = useState('')
  const [storedNotice,     setStoredNotice]     = useState<string | null>(null)
  const [redisConnectors,  setRedisConnectors]  = useState<ObservabilityConnector[]>([])
  const [redisConnectorId, setRedisConnectorId] = useState<string | null>(null)
  const [showRedisConnMenu, setShowRedisConnMenu] = useState(false)
  const [redisMode,        setRedisMode]        = useState<RedisMode>('command')
  const [redisValueType,   setRedisValueType]   = useState<RedisValueType>('auto')
  const [redisCatalogKind, setRedisCatalogKind] = useState<RedisCatalogKind>('commands')
  const [redisCapabilities, setRedisCapabilities] = useState<RedisCapabilitySnapshot | null>(null)
  const [redisCatalog,     setRedisCatalog]     = useState<Array<Record<string, string>>>([])
  const isAiMode = aiConnectorId !== null
  const isRedisMode = redisConnectorId !== null

  /* Autocomplete */
  const [acList,   setAcList]   = useState<string[]>([])
  const [acIdx,    setAcIdx]    = useState(0)
  const [acAnchor, setAcAnchor] = useState<{ top: number; left: number } | null>(null)
  const acWordStart = useRef(-1)

  const textareaRef  = useRef<HTMLTextAreaElement>(null)
  const highlightRef = useRef<HTMLDivElement>(null)
  const copyFeedbackTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const dsMeta = allDatasets.find(d => d.id === dataset) ?? allDatasets[0]
  const autoRanRef = useRef(false)
  const isGenericConnectorMode = dsMeta?.sourceType === 'connector'
  const visibleRowCount = results?.rows.length ?? 0
  const hasSelection = selectedRows.length > 0
  const allVisibleSelected = visibleRowCount > 0 && selectedRows.length === visibleRowCount
  const someVisibleSelected = selectedRows.length > 0 && selectedRows.length < visibleRowCount
  const canRewriteSort = !!results && (lang === 'sql' || lang === 'kql')

  function defaultQueryFor(dsId: string, l: Lang): string {
    const meta = allDatasets.find(d => d.id === dsId)
    if (meta?.sourceType === 'connector') {
      if (meta.connectorType === 'postgresql' || meta.connectorType === 'mysql' || meta.connectorType === 'bigquery') {
        return 'SELECT *\nFROM source_rows\nLIMIT 100'
      }
      if (meta.connectorType === 'azureb2c' || meta.connectorType === 'azureentraid') {
        return 'SELECT *\nFROM source_rows\nLIMIT 100'
      }
      return l === 'kql'
        ? 'source_rows\n| limit 100'
        : l === 'jsonpath'
        ? '$[*]'
        : l === 'jmespath'
        ? '[*]'
        : 'SELECT *\nFROM source_rows\nLIMIT 100'
    }
    const qs = savedQueriesMap[dsId]
    if (qs) return qs.find(q => q.lang === l)?.query ?? qs[0].query
    const tbl = (meta?.name ?? dsId).replace(/-/g, '_')
    if (l === 'redis') return REDIS_MODE_TEMPLATES.command
    if (l === 'kql') return `${tbl}\n| limit 50`
    return `SELECT *\nFROM ${tbl}\nLIMIT 100`
  }

  function getSavedQueries(): SavedQuery[] {
    if (savedQueriesMap[dataset]) return savedQueriesMap[dataset]
    const tbl = (dsMeta?.name ?? dataset).replace(/-/g, '_')
    const fields = dsMeta?.fields ?? []
    const gf = fields.find(f =>
      !['id','user_id','event_id','created_at','timestamp','ts','created','url','image'].includes(f)
    ) ?? fields[1] ?? '*'
    return [
      { id:'g1', lang:'sql', name:'All records',    query:`SELECT *\nFROM ${tbl}\nLIMIT 100` },
      { id:'g2', lang:'sql', name:'Count total',    query:`SELECT COUNT(*) AS total\nFROM ${tbl}` },
      { id:'g3', lang:'sql', name:`Group by ${gf}`, query:`SELECT ${gf}, COUNT(*) AS cnt\nFROM ${tbl}\nGROUP BY ${gf}\nORDER BY cnt DESC\nLIMIT 20` },
      { id:'g4', lang:'kql', name:'KQL sample',     query:`${tbl}\n| limit 50` },
    ]
  }

  /* Load history + fetch datasets + observability connectors */
  useEffect(() => {
    setHistory(loadHistory())
    fetch('/api/connectors')
      .then(r => r.json())
      .then((list: ObservabilityConnector[]) => {
        setAiConnectors(list.filter(c => ['appinsights', 'azuremonitor', 'elasticsearch', 'datadog'].includes(c.type)))
        setRedisConnectors(list.filter(c => c.type === 'redis'))
        setGenericConnectors(list.filter(c => GENERIC_QUERYABLE_CONNECTOR_TYPES.includes(c.type)))
      })
      .catch(() => {})
    fetch('/api/datasets')
      .then(r => r.json())
      .then((list: Array<{
        name: string; records: string; description: string
        queryDataset: string | null
        schema: Array<{ field: string; type: string }> | null
      }>) => {
        const metas: DatasetMeta[] = list.map(d => ({
          id:     d.queryDataset ?? d.name,
          name:   d.name,
          badge:  d.records,
          desc:   d.description,
          sourceType: 'dataset',
          sourceId: d.queryDataset ?? d.name,
          fields: d.schema?.map(f => f.field) ?? [],
          schema: d.schema?.map(f => ({ field: f.field, type: f.type })) ?? [],
        }))
        const connectorMetas: DatasetMeta[] = genericConnectors.map(connector => ({
          id: `connector:${connector.id}`,
          name: connector.name,
          badge: 'live',
          desc: `${connector.type} connector`,
          sourceType: 'connector',
          sourceId: connector.id,
          connectorType: connector.type,
          fields: [],
          schema: [],
        }))
        const combined = [...metas, ...connectorMetas]
        setAllDatasets(combined)
        if (!dataset && combined[0]) {
          const preferred = settings?.queryEngine.defaultDataset
            ? combined.find(meta => meta.id === settings.queryEngine.defaultDataset)
            : null
          const initial = preferred ?? combined[0]
          setDataset(initial.id)
          setQuery(defaultQueryFor(initial.id, 'sql'))
        }

        const jumpTo = localStorage.getItem('datachef:jumpToDataset')
        if (jumpTo) {
          localStorage.removeItem('datachef:jumpToDataset')
          const found = combined.find(d => d.id === jumpTo || d.name === jumpTo)
          if (found) {
            setDataset(found.id)
            const qs = savedQueriesMap[found.id]
            if (qs) { setLang(qs[0].lang); setQuery(qs[0].query) }
            else    { setLang('sql'); setQuery(defaultQueryFor(found.id, 'sql')) }
          }
        }
      })
      .catch(() => { /* keep fallback */ })
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [genericConnectors, settings?.queryEngine.defaultDataset])

  useEffect(() => {
    function syncHistory() {
      setHistory(loadHistory())
    }

    window.addEventListener('storage', syncHistory)
    return () => window.removeEventListener('storage', syncHistory)
  }, [])

  useEffect(() => {
    setSelectedRows([])
    setLastSelectedRow(null)
    setCopyFeedback(null)
    setSortState(results ? detectQuerySort(query, lang) : null)
  }, [results, query, lang])

  useEffect(() => () => {
    if (copyFeedbackTimerRef.current) clearTimeout(copyFeedbackTimerRef.current)
  }, [])

  function handleLangChange(l: Lang) {
    setLang(l); setQuery(defaultQueryFor(dataset, l))
    setResults(null); setQueryError(null); setShowLangMenu(false)
  }

  function handleDatasetChange(d: string) {
    setAiConnectorId(null)
    setRedisConnectorId(null)
    const nextLang = lang === 'redis' ? 'sql' : lang
    setLang(nextLang)
    setDataset(d); setQuery(defaultQueryFor(d, nextLang))
    setResults(null); setQueryError(null); setShowDataMenu(false); dismissAc()
  }

  function loadSaved(sq: SavedQuery) {
    setLang(sq.lang); setQuery(sq.query)
    setResults(null); setQueryError(null); dismissAc()
  }

  /* Insert field at cursor */
  function insertField(field: string) {
    const ta = textareaRef.current
    if (!ta) return
    const start = ta.selectionStart
    const end   = ta.selectionEnd
    const next  = query.slice(0, start) + field + query.slice(end)
    setQuery(next)
    setTimeout(() => {
      ta.selectionStart = start + field.length
      ta.selectionEnd   = start + field.length
      ta.focus()
    }, 0)
  }

  /* ── Autocomplete ──────────────────────────────────────────────────────────── */
  function dismissAc() { setAcList([]); setAcAnchor(null) }

  function triggerAc(value: string, cursor: number) {
    const before = value.slice(0, cursor)
    const m = before.match(/(\w+)$/)
    if (!m || m[1].length < 1) { dismissAc(); return }

    const word     = m[1]
    const wordStart = cursor - word.length
    const low      = word.toLowerCase()

    const fields = dsMeta?.fields ?? []
    const redisItems = redisCatalog.flatMap(item => Object.values(item))
    const items  = [...new Set([...fields, ...SQL_KW, ...redisItems])]
      .filter(c => c.toLowerCase().startsWith(low) && c.toLowerCase() !== low)
      .slice(0, 8)

    if (items.length === 0) { dismissAc(); return }

    /* Cursor pixel position — relative to the editor wrapper div (after gutter) */
    const lines  = before.split('\n')
    const lineN  = lines.length - 1
    const colN   = lines[lineN].length
    const LINE_H = 20.8  // 13 * 1.6
    const CHAR_W = 7.8
    const PAD_T  = 16    // pt-4

    setAcAnchor({ top: PAD_T + (lineN + 1) * LINE_H, left: colN * CHAR_W })
    acWordStart.current = wordStart
    setAcList(items)
    setAcIdx(0)
  }

  function applyCompletion(item: string) {
    const wordStart = acWordStart.current
    const wordEnd   = textareaRef.current?.selectionStart ?? wordStart
    setQuery(query.slice(0, wordStart) + item + query.slice(wordEnd))
    dismissAc()
    setTimeout(() => {
      if (textareaRef.current) {
        const pos = wordStart + item.length
        textareaRef.current.selectionStart = pos
        textareaRef.current.selectionEnd   = pos
        textareaRef.current.focus()
      }
    }, 0)
  }

  function handleQueryChange(e: React.ChangeEvent<HTMLTextAreaElement>) {
    setQuery(e.target.value)
    triggerAc(e.target.value, e.target.selectionStart)
  }

  function handleQueryKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (acList.length > 0) {
      if (e.key === 'ArrowDown') { e.preventDefault(); setAcIdx(i => Math.min(i+1, acList.length-1)); return }
      if (e.key === 'ArrowUp')   { e.preventDefault(); setAcIdx(i => Math.max(i-1, 0)); return }
      if (e.key === 'Tab' || (e.key === 'Enter' && !e.metaKey && !e.ctrlKey)) {
        e.preventDefault(); applyCompletion(acList[acIdx]); return
      }
      if (e.key === 'Escape') { dismissAc(); return }
    }
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); handleRun() }
  }

  /* Sync highlight layer scroll with textarea */
  function handleScroll() {
    if (highlightRef.current && textareaRef.current) {
      highlightRef.current.scrollTop  = textareaRef.current.scrollTop
      highlightRef.current.scrollLeft = textareaRef.current.scrollLeft
    }
  }

  /* ── Run query ─────────────────────────────────────────────────────────────── */
  const handleRun = useCallback(async (queryOverride?: string) => {
    if (running) return
    const effectiveQuery = queryOverride ?? query
    setRunning(true); setResults(null); setQueryError(null); setStoredNotice(null); dismissAc()

    /* ── Observability branch ── */
    if (isAiMode) {
      try {
        const res = await fetch('/api/observability/query', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ connectorId: aiConnectorId, kql: effectiveQuery, timespan: aiTimespan }),
        })
        const data: QResult = await res.json()

        if (data.error) {
          setQueryError(data.error)
        } else {
          setResults(data)

          if (storeLocally && aiConnectorId) {
            const connector = aiConnectors.find(item => item.id === aiConnectorId)
            const name = storeDatasetName.trim() || `${connector?.name ?? 'observability'} snapshot`
            try {
              const schema = data.columns.map(field => ({ field, type: 'string', nullable: true, example: '' }))
              const sampleRows = data.rows.slice(0, 5).map(row =>
                Object.fromEntries(data.columns.map((column, index) => [column, row[index] ?? null])),
              )
              const response = await fetch('/api/datasets', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  name,
                  source: 'conn',
                  format: 'JSON',
                  connectorId: aiConnectorId,
                  connection: connector?.name ?? 'Observability',
                  resource: effectiveQuery,
                  description: `${connector?.name ?? 'Observability'} materialized query result`,
                  schema,
                  sampleRows,
                  totalRows: data.totalRows,
                }),
              })
              if (!response.ok) {
                const failure = await response.json().catch(() => ({ error: 'Dataset creation failed' }))
                throw new Error(String(failure.error ?? 'Dataset creation failed'))
              }
              setStoredNotice(`Saved as "${name}"`)
              setTimeout(() => setStoredNotice(null), 3000)
            } catch (error: unknown) {
              setStoredNotice(error instanceof Error ? error.message : String(error))
              setTimeout(() => setStoredNotice(null), 3000)
            }
          }

          const entry: HistoryEntry = {
            id: crypto.randomUUID(), lang: 'kql',
            dataset: aiConnectorId ?? 'observability',
            query: effectiveQuery, rowCount: data.rowCount, durationMs: data.durationMs, ts: Date.now(),
          }
          const next = [entry, ...history].slice(0, 50); setHistory(next); saveHistory(next)
        }
      } catch (e: unknown) {
        setQueryError(e instanceof Error ? e.message : String(e))
      } finally { setRunning(false) }
      return
    }

    if (isRedisMode) {
      try {
        const res = await fetch('/api/redis/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            connectorId: redisConnectorId,
            mode: redisMode,
            query: redisMode === 'catalog' ? effectiveQuery || redisCatalogKind : effectiveQuery,
            valueType: redisValueType,
            catalog: redisCatalogKind,
            rowLimit: settings?.queryEngine.maxRows,
          }),
        })
        const data: QResult & { capabilities?: RedisCapabilitySnapshot } = await res.json()
        if (data.error) {
          setQueryError(data.error)
        } else {
          setResults({
            ...data,
            bytesScanned: 0,
          })
          if (data.capabilities) setRedisCapabilities(data.capabilities)
        }
        const entry: HistoryEntry = {
          id: crypto.randomUUID(),
          lang: 'redis',
          dataset: redisConnectorId ?? 'redis',
          query: effectiveQuery,
          rowCount: data.rowCount ?? 0,
          durationMs: data.durationMs ?? 0,
          ts: Date.now(),
          error: data.error,
          redisMode,
          redisValueType,
        }
        const next = [entry, ...history].slice(0, 50); setHistory(next); saveHistory(next)
      } catch (e: unknown) {
        setQueryError(e instanceof Error ? e.message : String(e))
      } finally { setRunning(false) }
      return
    }

    /* ── Standard dataset / live connector branch ── */
    try {
      const activeSource = allDatasets.find(item => item.id === dataset)
      const res  = await fetch('/api/query', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: effectiveQuery,
          lang,
          dataset,
          sourceType: activeSource?.sourceType ?? 'dataset',
          sourceId: activeSource?.sourceId ?? dataset,
          resource: activeSource?.resource,
          rowLimit: settings?.queryEngine.maxRows,
        }),
      })
      const data: QResult = await res.json()

      if (data.error) {
        setQueryError(data.error)
        const entry: HistoryEntry = { id: crypto.randomUUID(), lang, dataset, query: effectiveQuery, rowCount: 0, durationMs: data.durationMs ?? 0, ts: Date.now(), error: data.error }
        const next = [entry, ...history].slice(0, 50); setHistory(next); saveHistory(next)
      } else {
        setResults(data)
        if (activeSource?.sourceType === 'connector' && storeLocally) {
          const name = storeDatasetName.trim() || `${activeSource.name} snapshot`
          const schema = data.columns.map(field => ({ field, type: 'string', nullable: true, example: '' }))
          const sampleRows = data.rows.slice(0, 5).map(row =>
            Object.fromEntries(data.columns.map((column, index) => [column, row[index] ?? null])),
          )
          const response = await fetch('/api/datasets', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              name,
              source: 'conn',
              format: 'JSON',
              connectorId: activeSource.sourceId,
              connection: activeSource.name,
              resource: effectiveQuery,
              description: `${activeSource.name} materialized query result`,
              schema,
              sampleRows,
              totalRows: data.totalRows,
              sourceRef: {
                sourceType: 'connector',
                sourceId: activeSource.sourceId,
                resource: query,
              },
              materialization: {
                kind: 'connector',
                sourceType: 'connector',
                sourceId: activeSource.sourceId,
                resource: effectiveQuery,
                refreshMode: 'manual',
              },
            }),
          })
        if (response.ok) {
          setStoredNotice(`Saved as "${name}"`)
          setTimeout(() => setStoredNotice(null), 3000)
        }
      }
        const entry: HistoryEntry = { id: crypto.randomUUID(), lang, dataset, query: effectiveQuery, rowCount: data.rowCount, durationMs: data.durationMs, ts: Date.now() }
        const next = [entry, ...history].slice(0, 50); setHistory(next); saveHistory(next)
      }
    } catch (e: unknown) {
      setQueryError(e instanceof Error ? e.message : String(e))
    } finally { setRunning(false) }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query, lang, dataset, running, history, isAiMode, isRedisMode, aiConnectorId, aiTimespan, redisConnectorId, redisMode, redisValueType, redisCatalogKind, storeLocally, storeDatasetName, settings?.queryEngine.maxRows, allDatasets])

  function showCopyFeedback(kind: 'selected' | 'all') {
    setCopyFeedback(kind)
    if (copyFeedbackTimerRef.current) clearTimeout(copyFeedbackTimerRef.current)
    copyFeedbackTimerRef.current = setTimeout(() => setCopyFeedback(null), 1600)
  }

  async function copyRowsToClipboard(mode: 'selected' | 'all') {
    if (!results) return
    const indexes = mode === 'selected' && selectedRows.length > 0
      ? selectedRows
      : results.rows.map((_, index) => index)
    const rows = indexes.map(index => results.rows[index])
    const payload = rowsToDelimited(results.columns, rows, '\t')
    await navigator.clipboard.writeText(payload)
    showCopyFeedback(mode === 'selected' && selectedRows.length > 0 ? 'selected' : 'all')
  }

  function handleToggleAllRows() {
    if (!results) return
    if (allVisibleSelected) {
      setSelectedRows([])
      setLastSelectedRow(null)
      return
    }
    setSelectedRows(results.rows.map((_, index) => index))
    setLastSelectedRow(results.rows.length > 0 ? results.rows.length - 1 : null)
  }

  function handleToggleRow(index: number, event: React.MouseEvent<HTMLInputElement>) {
    event.preventDefault()
    if (!results) return
    setSelectedRows(prev => {
      const next = new Set(prev)
      if (event.shiftKey && lastSelectedRow !== null) {
        const [start, end] = [lastSelectedRow, index].sort((left, right) => left - right)
        if (!event.metaKey && !event.ctrlKey) next.clear()
        for (let rowIndex = start; rowIndex <= end; rowIndex++) next.add(rowIndex)
      } else if (event.metaKey || event.ctrlKey) {
        if (next.has(index)) next.delete(index)
        else next.add(index)
      } else {
        const isOnlySelected = prev.length === 1 && prev[0] === index
        next.clear()
        if (!isOnlySelected) next.add(index)
      }
      return Array.from(next).sort((left, right) => left - right)
    })
    setLastSelectedRow(index)
  }

  async function handleHeaderSort(column: string) {
    if (!canRewriteSort) return
    const nextDirection: SortDirection =
      sortState?.column === column && sortState.direction === 'asc' ? 'desc' : 'asc'
    const nextQuery = lang === 'kql'
      ? rewriteKqlSort(query, column, nextDirection)
      : rewriteSqlSort(query, column, nextDirection)

    setSortState({ column, direction: nextDirection })
    setQuery(nextQuery)
    await handleRun(nextQuery)
  }

  useEffect(() => {
    if (!settings?.queryEngine.autoExecuteOnOpen || autoRanRef.current || isAiMode || isRedisMode || !dataset || !query.trim()) return
    autoRanRef.current = true
    void handleRun()
  }, [dataset, handleRun, isAiMode, isRedisMode, query, settings?.queryEngine.autoExecuteOnOpen])

  /* Load saved queries when observability connector changes */
  useEffect(() => {
    if (!aiConnectorId) return
    fetch(`/api/observability/saved-queries?connectorId=${aiConnectorId}`)
      .then(r => r.json())
      .then((qs: SavedObservabilityQuery[]) => setAiSavedQueries(qs))
      .catch(() => setAiSavedQueries([]))
  }, [aiConnectorId])

  useEffect(() => {
    if (!redisConnectorId) return
    fetch(`/api/redis/query?connectorId=${redisConnectorId}`)
      .then(r => r.json())
      .then((caps: RedisCapabilitySnapshot) => { if (!caps.error) setRedisCapabilities(caps) })
      .catch(() => setRedisCapabilities(null))
  }, [redisConnectorId])

  useEffect(() => {
    if (!redisConnectorId) return
    fetch(`/api/redis/catalog?connectorId=${redisConnectorId}&catalog=${redisCatalogKind}&limit=50`)
      .then(r => r.json())
      .then((payload: RedisCatalogResult) => {
        if (payload.error) {
          setRedisCatalog([])
          return
        }
        if (payload.capabilities) setRedisCapabilities(payload.capabilities)
        const rows = payload.rows.map(row => Object.fromEntries(payload.columns.map((column, index) => [column, row[index] ?? ''])))
        setRedisCatalog(rows)
      })
      .catch(() => setRedisCatalog([]))
  }, [redisCatalogKind, redisConnectorId])

  function handleSelectAiConnector(id: string | null) {
    setAiConnectorId(id)
    setRedisConnectorId(null)
    setShowDataMenu(false)
    setShowAiConnMenu(false)
    setResults(null); setQueryError(null); dismissAc()
    if (id) {
      const connector = aiConnectors.find(item => item.id === id)
      setLang('kql')
      setQuery(defaultObservabilityKql(connector?.type ?? 'appinsights'))
    } else {
      setLang('sql')
    }
  }

  function handleSelectRedisConnector(id: string | null) {
    setRedisConnectorId(id)
    setAiConnectorId(null)
    setShowDataMenu(false)
    setShowRedisConnMenu(false)
    setResults(null); setQueryError(null); dismissAc()
    if (id) {
      setLang('redis')
      setQuery(REDIS_MODE_TEMPLATES[redisMode])
    } else {
      setLang('sql')
    }
  }

  async function handleSaveAiQuery() {
    if (!aiConnectorId || !saveAiName.trim() || !query.trim()) return
    try {
      const res = await fetch('/api/observability/saved-queries', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ connectorId: aiConnectorId, name: saveAiName.trim(), kql: query }),
      })
      const q = await res.json() as SavedObservabilityQuery
      setAiSavedQueries(prev => [...prev, q])
      setSaveAiName(''); setShowSaveAiInput(false)
    } catch {}
  }

  async function handleDeleteAiQuery(queryId: string) {
    if (!aiConnectorId) return
    await fetch(`/api/observability/saved-queries?connectorId=${aiConnectorId}&queryId=${queryId}`, { method: 'DELETE' })
    setAiSavedQueries(prev => prev.filter(q => q.id !== queryId))
  }

  const aiConnector = aiConnectors.find(c => c.id === aiConnectorId) ?? null
  const aiConnectorName = aiConnector?.name ?? 'Observability'
  const redisConnector = redisConnectors.find(c => c.id === redisConnectorId) ?? null
  const redisConnectorName = redisConnector?.name ?? 'Redis'
  const aiTimespanLabel = AI_TIMESPAN_PRESETS.find(p => p.value === aiTimespan)?.label ?? aiTimespan
  const lineCount    = query.split('\n').length
  const savedQueries = getSavedQueries()
  const highlighted  = highlightCode(query, lang)

  /* Shared history panel (used in both modes) */
  const historyPanel = (
    <div className="border-t border-chef-border">
      <button onClick={() => setShowHistory(v => !v)}
        className="w-full flex items-center gap-2 px-3 py-2.5 text-[11px] text-chef-muted hover:text-chef-text hover:bg-chef-card/50 transition-colors"
      >
        <History size={11} /><span>History</span>
        {history.length > 0 && <span className="ml-auto text-[10px] bg-chef-card px-1.5 py-0.5 rounded font-mono">{history.length}</span>}
        <ChevronRight size={10} className={`ml-auto transition-transform ${showHistory ? 'rotate-90' : ''}`} />
      </button>
      {showHistory && (
        <div className="border-t border-chef-border/50 max-h-48 overflow-auto">
          {history.length === 0
            ? <div className="px-3 py-4 text-[10px] text-chef-muted text-center">No history yet</div>
            : history.map(h => (
              <button key={h.id}
                onClick={() => {
                  const obs = aiConnectors.find(conn => conn.id === h.dataset)
                  const redis = redisConnectors.find(conn => conn.id === h.dataset)
                  setLang(h.lang)
                  setQuery(h.query)
                  if (obs) {
                    setAiConnectorId(obs.id)
                    setRedisConnectorId(null)
                  } else if (redis) {
                    setRedisConnectorId(redis.id)
                    setAiConnectorId(null)
                    setRedisMode(h.redisMode ?? 'command')
                    setRedisValueType(h.redisValueType ?? 'auto')
                  } else {
                    setAiConnectorId(null)
                    setRedisConnectorId(null)
                    setDataset(h.dataset)
                  }
                  setResults(null)
                  setQueryError(null)
                }}
                className="w-full text-left px-3 py-2 hover:bg-chef-card transition-colors border-b border-chef-border/20 group"
              >
                <div className="flex items-center gap-1.5 mb-0.5">
                  <span className={`text-[9px] font-mono ${langMeta[h.lang].color}`}>{langMeta[h.lang].label}</span>
                  {h.error
                    ? <span className="text-[9px] text-rose-400 ml-auto">error</span>
                    : <span className="text-[9px] text-chef-muted ml-auto">{fmtNum(h.rowCount)}r · {h.durationMs}ms</span>}
                </div>
                <div className="text-[10px] text-chef-muted truncate font-mono">{h.query.trim().split('\n')[0]}</div>
                <div className="text-[9px] text-chef-border mt-0.5">{timeAgo(h.ts)}</div>
              </button>
            ))}
        </div>
      )}
    </div>
  )

  return (
    <div className="flex h-full">

      {/* ── Left sidebar ────────────────────────────────────────────────────── */}
      <div className="w-56 shrink-0 border-r border-chef-border flex flex-col bg-chef-surface">

        {!isAiMode && !isRedisMode ? (
          <>
            {/* Source picker */}
            <div className="px-3 py-3 border-b border-chef-border relative">
              <div className="text-[9px] font-semibold uppercase tracking-widest text-chef-muted mb-1.5">Source</div>
              <button
                onClick={() => setShowDataMenu(v => !v)}
                className="w-full flex items-center gap-2 text-left px-2.5 py-1.5 rounded-md border border-chef-border bg-chef-bg hover:border-indigo-500/40 transition-colors"
              >
                <Server size={11} className="text-indigo-400 shrink-0" />
                <span className="text-[11px] font-mono text-chef-text flex-1 truncate">{dsMeta?.name ?? dataset}</span>
                <span className="text-[10px] text-emerald-400 font-mono shrink-0">{dsMeta?.badge ?? '—'}</span>
                <ChevronDown size={10} className="text-chef-muted shrink-0" />
              </button>
              {showDataMenu && (
                <div className="absolute left-3 right-3 top-full mt-1 bg-chef-card border border-chef-border rounded-lg shadow-xl shadow-black/40 z-30 py-1 animate-fade-in max-h-72 overflow-auto">
                  <div className="px-3 pt-2 pb-1 text-[9px] font-semibold uppercase tracking-widest text-chef-border">Datasets & Query Sources</div>
                  {allDatasets.map(ds => (
                    <button key={ds.id} onClick={() => handleDatasetChange(ds.id)}
                      className={`w-full text-left px-3 py-2.5 hover:bg-chef-card-hover transition-colors ${dataset === ds.id ? 'text-indigo-400' : 'text-chef-text'}`}
                    >
                      <div className="flex items-center justify-between">
                        <span className="text-[11px] font-mono truncate max-w-[120px]">{ds.name}</span>
                        <span className={`text-[10px] font-mono shrink-0 ${ds.sourceType === 'connector' ? 'text-cyan-400' : 'text-emerald-400'}`}>{ds.badge}</span>
                      </div>
                      <div className="text-[10px] text-chef-muted mt-0.5 truncate">{ds.desc}</div>
                    </button>
                  ))}

                  {redisConnectors.length > 0 && (
                    <>
                      <div className="mx-3 my-1 border-t border-chef-border/50" />
                      <div className="px-3 pt-1 pb-1 text-[9px] font-semibold uppercase tracking-widest text-red-400">Redis</div>
                      {redisConnectors.map(connector => (
                        <button key={connector.id} onClick={() => handleSelectRedisConnector(connector.id)}
                          className={`w-full text-left px-3 py-2.5 hover:bg-chef-card-hover transition-colors ${redisConnectorId === connector.id ? 'text-red-400' : 'text-chef-text'}`}
                        >
                          <div className="flex items-center justify-between gap-2">
                            <span className="text-[11px] font-mono truncate">{connector.name}</span>
                            <span className="text-[10px] font-mono shrink-0 text-red-400">redis</span>
                          </div>
                          <div className="text-[10px] text-chef-muted mt-0.5 truncate">Dedicated Redis query modes and catalog tools</div>
                        </button>
                      ))}
                    </>
                  )}

                  {aiConnectors.length > 0 && (
                    <>
                      <div className="mx-3 my-1 border-t border-chef-border/50" />
                      <div className="px-3 pt-1 pb-1 text-[9px] font-semibold uppercase tracking-widest text-cyan-400">Observe</div>
                      {aiConnectors.map(connector => (
                        <button key={connector.id} onClick={() => handleSelectAiConnector(connector.id)}
                          className={`w-full text-left px-3 py-2.5 hover:bg-chef-card-hover transition-colors ${aiConnectorId === connector.id ? 'text-cyan-400' : 'text-chef-text'}`}
                        >
                          <div className="flex items-center justify-between gap-2">
                            <span className="text-[11px] font-mono truncate">{connector.name}</span>
                            <span className="text-[10px] font-mono shrink-0 text-cyan-400">{connector.type}</span>
                          </div>
                          <div className="text-[10px] text-chef-muted mt-0.5 truncate">KQL-first observability querying</div>
                        </button>
                      ))}
                    </>
                  )}
                </div>
              )}
            </div>

            {/* Schema fields panel */}
            <div className="border-b border-chef-border">
              <button
                onClick={() => setShowSchema(v => !v)}
                className="w-full flex items-center gap-1.5 px-3 py-2 text-[9px] font-semibold uppercase tracking-widest text-chef-muted hover:text-chef-text transition-colors"
              >
                <span>Schema</span>
                <span className="normal-case text-chef-border font-normal ml-0.5">{dsMeta?.schema.length ?? 0} fields</span>
                <ChevronRight size={9} className={`ml-auto transition-transform duration-150 ${showSchema ? 'rotate-90' : ''}`} />
              </button>
              {showSchema && dsMeta && (
                <div className="px-3 pb-2.5 flex flex-wrap gap-1">
                  {dsMeta.schema.map(f => (
                    <button
                      key={f.field}
                      onClick={() => insertField(f.field)}
                      title={`${f.field}: ${f.type} — click to insert`}
                      className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded border border-chef-border bg-chef-bg hover:bg-indigo-600/20 hover:border-indigo-500/50 transition-colors group"
                    >
                      <span className="text-[10px] font-mono text-chef-text-dim group-hover:text-indigo-300">{f.field}</span>
                      <span className={`text-[8px] font-mono ${TYPE_COLOR[f.type] ?? 'text-chef-muted'}`}>{f.type.slice(0,3)}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>

            {isGenericConnectorMode && (
              <div className="px-3 py-2.5 border-b border-chef-border">
                <label className="flex items-center gap-2 cursor-pointer select-none">
                  <div
                    onClick={() => setStoreLocally(v => !v)}
                    className={`w-7 h-4 rounded-full relative transition-colors cursor-pointer ${storeLocally ? 'bg-cyan-600' : 'bg-chef-border'}`}
                  >
                    <div className={`absolute top-0.5 w-3 h-3 rounded-full bg-white transition-transform ${storeLocally ? 'translate-x-3.5' : 'translate-x-0.5'}`} />
                  </div>
                  <span className="text-[10px] text-chef-muted">Materialize as dataset</span>
                </label>
                {storeLocally && (
                  <input
                    type="text"
                    value={storeDatasetName}
                    onChange={e => setStoreDatasetName(e.target.value)}
                    placeholder="Dataset name…"
                    className="mt-1.5 w-full px-2 py-1 bg-chef-bg border border-chef-border rounded text-[11px] font-mono text-chef-text placeholder-chef-border focus:outline-none focus:border-cyan-500/50"
                  />
                )}
              </div>
            )}

            {/* Saved queries */}
            <div className="px-3 py-2 border-b border-chef-border flex items-center justify-between">
              <div className="text-[9px] font-semibold uppercase tracking-widest text-chef-muted">Saved Queries</div>
            </div>
            <div className="flex-1 overflow-auto py-1">
              {savedQueries.map(sq => (
                <button key={sq.id} onClick={() => loadSaved(sq)}
                  className="w-full text-left px-3 py-2.5 hover:bg-chef-card transition-colors group border-b border-chef-border/30"
                >
                  <div className="text-[11px] text-chef-text group-hover:text-indigo-300 transition-colors leading-tight">{sq.name}</div>
                  <div className={`text-[10px] font-mono mt-0.5 ${langMeta[sq.lang].color}`}>{langMeta[sq.lang].label}</div>
                </button>
              ))}
            </div>

            {historyPanel}
          </>
        ) : isRedisMode ? (
          <>
            <div className="px-3 py-3 border-b border-chef-border relative">
              <div className="flex items-center justify-between mb-1.5">
                <div className="flex items-center gap-1.5">
                  <Database size={10} className="text-red-400" />
                  <div className="text-[9px] font-semibold uppercase tracking-widest text-red-400">Redis</div>
                </div>
                <button
                  onClick={() => handleSelectRedisConnector(null)}
                  className="text-[9px] text-chef-muted hover:text-chef-text transition-colors"
                  title="Back to sources"
                >
                  ← sources
                </button>
              </div>
              <button
                onClick={() => setShowRedisConnMenu(v => !v)}
                className="w-full flex items-center gap-2 text-left px-2.5 py-1.5 rounded-md border border-red-500/30 bg-chef-bg hover:border-red-500/60 transition-colors"
              >
                <Database size={11} className="text-red-400 shrink-0" />
                <span className="text-[11px] font-mono text-chef-text flex-1 truncate">{redisConnectorName}</span>
                {redisConnectors.length > 1 && <ChevronDown size={10} className="text-chef-muted shrink-0" />}
              </button>
              {showRedisConnMenu && redisConnectors.length > 1 && (
                <div className="absolute left-3 right-3 top-full mt-1 bg-chef-card border border-chef-border rounded-lg shadow-xl z-30 py-1 animate-fade-in">
                  {redisConnectors.map(c => (
                    <button key={c.id} onClick={() => handleSelectRedisConnector(c.id)}
                      className={`w-full text-left px-3 py-2 text-[11px] font-mono hover:bg-chef-card-hover transition-colors ${c.id === redisConnectorId ? 'text-red-400' : 'text-chef-text'}`}
                    >
                      {c.name}
                    </button>
                  ))}
                </div>
              )}
            </div>

            <div className="px-3 py-2.5 border-b border-chef-border">
              <div className="text-[9px] font-semibold uppercase tracking-widest text-chef-muted mb-2">Mode</div>
              <div className="grid grid-cols-2 gap-1">
                {(['command', 'search', 'json', 'timeseries', 'stream', 'catalog'] as RedisMode[]).map(mode => (
                  <button key={mode} onClick={() => { setRedisMode(mode); setQuery(REDIS_MODE_TEMPLATES[mode]) }}
                    className={`text-[10px] px-1.5 py-1 rounded border transition-colors font-mono ${
                      redisMode === mode ? 'border-red-500/60 bg-red-500/10 text-red-400' : 'border-chef-border text-chef-muted hover:text-chef-text'
                    }`}
                  >
                    {mode}
                  </button>
                ))}
              </div>
            </div>

            <div className="px-3 py-2.5 border-b border-chef-border">
              <div className="text-[9px] font-semibold uppercase tracking-widest text-chef-muted mb-2">Value Type</div>
              <select
                value={redisValueType}
                onChange={e => setRedisValueType(e.target.value as RedisValueType)}
                className="w-full bg-chef-bg border border-chef-border rounded text-[11px] font-mono px-2 py-1 text-chef-text"
              >
                {['auto', 'string', 'hash', 'list', 'set', 'zset', 'json', 'timeseries', 'stream', 'search'].map(type => (
                  <option key={type} value={type}>{type}</option>
                ))}
              </select>
              {redisCapabilities && (
                <div className="mt-2 text-[10px] text-chef-muted leading-snug">
                  {redisCapabilities.serverKind} · Redis {redisCapabilities.redisVersion || 'unknown'}
                  {redisCapabilities.modules.length ? ` · ${redisCapabilities.modules.join(', ')}` : ''}
                </div>
              )}
            </div>

            <div className="px-3 py-2 border-b border-chef-border">
              <div className="flex items-center justify-between mb-2">
                <div className="text-[9px] font-semibold uppercase tracking-widest text-chef-muted">Catalog</div>
                <select
                  value={redisCatalogKind}
                  onChange={e => setRedisCatalogKind(e.target.value as RedisCatalogKind)}
                  className="bg-chef-bg border border-chef-border rounded text-[10px] font-mono px-1.5 py-0.5 text-chef-text"
                >
                  {['commands', 'capabilities', 'keyspaces', 'keys', 'indexes', 'streams'].map(kind => (
                    <option key={kind} value={kind}>{kind}</option>
                  ))}
                </select>
              </div>
              <div className="max-h-48 overflow-auto space-y-1">
                {redisCatalog.length === 0
                  ? <div className="text-[10px] text-chef-muted text-center py-3">No catalog data</div>
                  : redisCatalog.map((row, index) => (
                    <button key={`${index}-${Object.values(row).join('-')}`}
                      onClick={() => {
                        const first = Object.values(row)[0]
                        if (first) setQuery(String(first))
                      }}
                      className="w-full text-left px-2 py-1.5 rounded border border-chef-border/40 hover:bg-chef-card transition-colors"
                    >
                      <div className="text-[10px] font-mono text-chef-text truncate">{Object.values(row)[0] ?? 'item'}</div>
                      <div className="text-[9px] text-chef-muted truncate">{Object.values(row).slice(1).join(' · ')}</div>
                    </button>
                  ))}
              </div>
            </div>

            {historyPanel}
          </>
        ) : (
          <>
            {/* Observability mode: connector selector */}
            <div className="px-3 py-3 border-b border-chef-border relative">
              <div className="flex items-center justify-between mb-1.5">
                <div className="flex items-center gap-1.5">
                  <BarChart2 size={10} className="text-cyan-400" />
                  <div className="text-[9px] font-semibold uppercase tracking-widest text-cyan-400">Observability</div>
                </div>
                <button
                  onClick={() => handleSelectAiConnector(null)}
                  className="text-[9px] text-chef-muted hover:text-chef-text transition-colors"
                  title="Back to sources"
                >
                  ← sources
                </button>
              </div>
              <button
                onClick={() => setShowAiConnMenu(v => !v)}
                className="w-full flex items-center gap-2 text-left px-2.5 py-1.5 rounded-md border border-cyan-500/30 bg-chef-bg hover:border-cyan-500/60 transition-colors"
              >
                <BarChart2 size={11} className="text-cyan-400 shrink-0" />
                <span className="text-[11px] font-mono text-chef-text flex-1 truncate">{aiConnectorName}</span>
                {aiConnectors.length > 1 && <ChevronDown size={10} className="text-chef-muted shrink-0" />}
              </button>
              {showAiConnMenu && aiConnectors.length > 1 && (
                <div className="absolute left-3 right-3 top-full mt-1 bg-chef-card border border-chef-border rounded-lg shadow-xl z-30 py-1 animate-fade-in">
                  {aiConnectors.map(c => (
                    <button key={c.id} onClick={() => handleSelectAiConnector(c.id)}
                      className={`w-full text-left px-3 py-2 text-[11px] font-mono hover:bg-chef-card-hover transition-colors ${c.id === aiConnectorId ? 'text-cyan-400' : 'text-chef-text'}`}
                    >
                      {c.name}
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Time range presets */}
            <div className="px-3 py-2.5 border-b border-chef-border">
              <div className="flex items-center gap-1.5 mb-2">
                <Clock size={9} className="text-chef-muted" />
                <div className="text-[9px] font-semibold uppercase tracking-widest text-chef-muted">Time Range</div>
              </div>
              <div className="grid grid-cols-2 gap-1">
                {AI_TIMESPAN_PRESETS.map(p => (
                  <button key={p.value} onClick={() => setAiTimespan(p.value)}
                    className={`text-[10px] px-1.5 py-1 rounded border transition-colors font-mono ${
                      aiTimespan === p.value
                        ? 'border-cyan-500/60 bg-cyan-500/10 text-cyan-400'
                        : 'border-chef-border text-chef-muted hover:text-chef-text hover:border-chef-border'
                    }`}
                  >
                    {p.label}
                  </button>
                ))}
              </div>
            </div>

            {/* Store locally toggle */}
            <div className="px-3 py-2.5 border-b border-chef-border">
              <label className="flex items-center gap-2 cursor-pointer select-none">
                <div
                  onClick={() => setStoreLocally(v => !v)}
                  className={`w-7 h-4 rounded-full relative transition-colors cursor-pointer ${storeLocally ? 'bg-cyan-600' : 'bg-chef-border'}`}
                >
                  <div className={`absolute top-0.5 w-3 h-3 rounded-full bg-white transition-transform ${storeLocally ? 'translate-x-3.5' : 'translate-x-0.5'}`} />
                </div>
                <span className="text-[10px] text-chef-muted">Materialize as dataset</span>
              </label>
              {storeLocally && (
                <input
                  type="text" value={storeDatasetName}
                  onChange={e => setStoreDatasetName(e.target.value)}
                  placeholder="Dataset name…"
                  className="mt-1.5 w-full px-2 py-1 bg-chef-bg border border-chef-border rounded text-[11px] font-mono text-chef-text placeholder-chef-border focus:outline-none focus:border-cyan-500/50"
                />
              )}
            </div>

            {/* Saved AI queries */}
            <div className="px-3 py-2 border-b border-chef-border flex items-center justify-between">
              <div className="text-[9px] font-semibold uppercase tracking-widest text-chef-muted">Saved Queries</div>
              <button
                onClick={() => setShowSaveAiInput(v => !v)}
                className="flex items-center gap-1 text-[9px] text-indigo-400 hover:text-indigo-300 transition-colors"
              >
                <Save size={9} /> Save
              </button>
            </div>
            {showSaveAiInput && (
              <div className="px-3 py-2 border-b border-chef-border/50 bg-chef-bg/50">
                <div className="flex gap-1.5">
                  <input
                    type="text" value={saveAiName}
                    onChange={e => setSaveAiName(e.target.value)}
                    onKeyDown={e => { if (e.key === 'Enter') handleSaveAiQuery() }}
                    placeholder="Query name…"
                    className="flex-1 px-2 py-1 bg-chef-bg border border-chef-border rounded text-[11px] font-mono text-chef-text placeholder-chef-border focus:outline-none focus:border-indigo-500/50"
                    autoFocus
                  />
                  <button onClick={handleSaveAiQuery}
                    className="px-2 py-1 bg-indigo-600 hover:bg-indigo-500 text-white text-[10px] rounded transition-colors"
                  >
                    OK
                  </button>
                </div>
              </div>
            )}
            <div className="flex-1 overflow-auto py-1">
              {aiSavedQueries.length === 0
                ? <div className="px-3 py-4 text-[10px] text-chef-muted text-center">No saved queries</div>
                : aiSavedQueries.map(sq => (
                  <div key={sq.id} className="group border-b border-chef-border/30 flex items-start">
                    <button className="flex-1 text-left px-3 py-2.5 hover:bg-chef-card transition-colors"
                      onClick={() => { setQuery(sq.kql); setResults(null); setQueryError(null) }}
                    >
                      <div className="text-[11px] text-chef-text group-hover:text-cyan-300 transition-colors leading-tight">{sq.name}</div>
                      <div className="text-[10px] font-mono text-amber-400 mt-0.5">KQL</div>
                    </button>
                    <button onClick={() => handleDeleteAiQuery(sq.id)}
                      className="shrink-0 px-2 py-3 text-chef-border hover:text-rose-400 transition-colors opacity-0 group-hover:opacity-100"
                    >
                      <Trash2 size={9} />
                    </button>
                  </div>
                ))
              }
            </div>

            {historyPanel}
          </>
        )}
      </div>

      {/* ── Main editor area ──────────────────────────────────────────────────── */}
      <div className="flex-1 flex flex-col min-w-0">

        {/* Status bar */}
        {isAiMode ? (
          <div className="flex items-center gap-2 px-4 py-1.5 border-b border-chef-border bg-cyan-900/10 text-[10px] font-mono shrink-0">
            <BarChart2 size={10} className="text-cyan-400" />
            <span className="text-cyan-400">Observability · direct query</span>
            <span className="text-chef-muted">·</span>
            <span className="text-chef-muted truncate">{aiConnectorName}</span>
            <span className="text-chef-muted ml-auto shrink-0">⌘↵ to run</span>
          </div>
        ) : isRedisMode ? (
          <div className="flex items-center gap-2 px-4 py-1.5 border-b border-chef-border bg-red-900/10 text-[10px] font-mono shrink-0">
            <Database size={10} className="text-red-400" />
            <span className="text-red-400">Redis · capability-aware query</span>
            <span className="text-chef-muted">·</span>
            <span className="text-chef-muted truncate">{redisConnectorName}</span>
            <span className="text-chef-muted ml-auto shrink-0">⌘↵ to run</span>
          </div>
        ) : (
          <div className="flex items-center gap-2 px-4 py-1.5 border-b border-chef-border bg-emerald-900/10 text-[10px] font-mono shrink-0">
            <CheckCircle2 size={10} className="text-emerald-400" />
            <span className="text-emerald-400">server-side execution</span>
            <span className="text-chef-muted">·</span>
            <span className="text-chef-muted truncate">{dsMeta?.desc ?? dataset}</span>
            <span className="text-chef-muted ml-auto shrink-0">⌘↵ to run</span>
          </div>
        )}

        {/* Toolbar */}
        <div className="h-11 border-b border-chef-border bg-chef-surface flex items-center gap-2 px-4 shrink-0">
          {/* Language selector — locked to KQL in AI mode */}
          <div className="relative">
            {isAiMode ? (
              <div className="flex items-center gap-1.5 text-xs font-mono font-semibold px-2.5 py-1 rounded-md border border-amber-500/30 bg-chef-bg text-amber-400">
                KQL <span className="text-[9px] text-chef-border normal-case font-normal">locked</span>
              </div>
            ) : isRedisMode ? (
              <div className="flex items-center gap-1.5 text-xs font-mono font-semibold px-2.5 py-1 rounded-md border border-red-500/30 bg-chef-bg text-red-400">
                REDIS <span className="text-[9px] text-chef-border normal-case font-normal">locked</span>
              </div>
            ) : (
              <button
                onClick={() => { setShowLangMenu(v => !v); setShowDataMenu(false) }}
                className={`flex items-center gap-1.5 text-xs font-mono font-semibold px-2.5 py-1 rounded-md border border-chef-border bg-chef-bg hover:border-indigo-500/50 transition-colors ${langMeta[lang].color}`}
              >
                {langMeta[lang].label} <ChevronDown size={11} />
              </button>
            )}
            {showLangMenu && !isAiMode && !isRedisMode && (
              <div className="absolute top-full left-0 mt-1 w-32 bg-chef-card border border-chef-border rounded-lg shadow-xl shadow-black/40 z-20 py-1 animate-fade-in">
                {(Object.entries(langMeta) as [Lang, typeof langMeta.sql][]).map(([key, meta]) => (
                  <button key={key} onClick={() => handleLangChange(key)}
                    className={`w-full text-left px-3 py-2 text-xs font-mono hover:bg-chef-card-hover transition-colors ${lang === key ? meta.color : 'text-chef-muted hover:text-chef-text'}`}
                  >
                    {meta.label}
                  </button>
                ))}
              </div>
            )}
          </div>

          <div className="w-px h-5 bg-chef-border mx-1" />

          {/* Dataset / connector chip */}
          {isAiMode ? (
            <div className="flex items-center gap-1.5 text-[11px] text-cyan-400 border border-cyan-500/30 rounded-md px-2.5 py-1 bg-chef-bg">
              <BarChart2 size={11} />
              <span className="font-mono truncate max-w-[120px]">{aiConnectorName}</span>
              <span className="text-[9px] text-chef-muted font-mono shrink-0">{aiTimespanLabel}</span>
            </div>
          ) : isRedisMode ? (
            <div className="flex items-center gap-1.5 text-[11px] text-red-400 border border-red-500/30 rounded-md px-2.5 py-1 bg-chef-bg">
              <Database size={11} />
              <span className="font-mono truncate max-w-[120px]">{redisConnectorName}</span>
              <span className="text-[9px] text-chef-muted font-mono shrink-0">{redisMode}</span>
            </div>
          ) : (
            <div className="flex items-center gap-1.5 text-[11px] text-chef-muted border border-chef-border rounded-md px-2.5 py-1 bg-chef-bg">
              <Server size={11} />
              <span className="font-mono truncate max-w-[140px]">{dsMeta?.name ?? dataset}</span>
              <span className="text-emerald-400 font-mono">{dsMeta?.badge ?? '—'}</span>
            </div>
          )}

          <div className="flex-1" />

          <button onClick={() => navigator.clipboard?.writeText(query)}
            className="p-1.5 text-chef-muted hover:text-chef-text transition-colors" title="Copy query">
            <Copy size={13} />
          </button>

          <button onClick={() => void handleRun()} disabled={running}
            className="flex items-center gap-1.5 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white text-xs font-semibold px-3 py-1.5 rounded-lg transition-colors"
          >
            {running ? <><Loader2 size={12} className="animate-spin" /> Running…</> : <><Play size={12} fill="currentColor" /> Run</>}
          </button>
        </div>

        {/* Editor + results */}
        <div className="flex-1 flex flex-col min-h-0">

          {/* Code area */}
          <div className="flex-1 flex min-h-0 bg-chef-bg"
            onClick={() => { setShowLangMenu(false); setShowDataMenu(false) }}
          >
            {/* Line numbers */}
            <div className="select-none w-10 pt-4 text-right pr-2.5 text-[11px] font-mono text-chef-border shrink-0 leading-[1.6] overflow-hidden">
              {Array.from({ length: Math.max(lineCount, 20) }, (_, i) => <div key={i+1}>{i+1}</div>)}
            </div>

            {/* Editor wrapper — highlight layer + transparent textarea stacked */}
            <div className="flex-1 relative overflow-hidden">
              {/* Syntax highlight layer (behind textarea) */}
              <div
                ref={highlightRef}
                aria-hidden="true"
                className="absolute inset-0 pt-4 pb-4 pr-4 text-[13px] font-mono leading-[1.6] pointer-events-none overflow-hidden whitespace-pre-wrap break-words"
                style={{ wordBreak: 'break-word' }}
                dangerouslySetInnerHTML={{ __html: highlighted + '\u200b' }}
              />

              {/* Transparent textarea (captures input, shows caret) */}
              <textarea
                ref={textareaRef}
                value={query}
                onChange={handleQueryChange}
                onKeyDown={handleQueryKeyDown}
                onScroll={handleScroll}
                onBlur={() => setTimeout(dismissAc, 150)}
                onClick={e => triggerAc(query, (e.target as HTMLTextAreaElement).selectionStart)}
                spellCheck={false}
                className="absolute inset-0 pt-4 pb-4 pr-4 bg-transparent text-transparent text-[13px] font-mono leading-[1.6] outline-none resize-none"
                style={{ caretColor: '#818cf8', WebkitTextFillColor: 'transparent' }}
                placeholder=""
              />

              {/* Autocomplete dropdown */}
              {acAnchor && acList.length > 0 && (
                <div
                  className="absolute z-50 bg-chef-card border border-chef-border rounded-lg shadow-xl shadow-black/50 py-1 min-w-[180px]"
                  style={{ top: acAnchor.top, left: acAnchor.left }}
                >
                  {acList.map((item, i) => {
                    const fieldType = dsMeta?.schema.find(f => f.field === item)?.type
                    const isKw = SQL_KW.includes(item)
                    return (
                      <button key={item}
                        onMouseDown={e => { e.preventDefault(); applyCompletion(item) }}
                        className={`w-full text-left px-3 py-1.5 text-[11px] font-mono transition-colors flex items-center gap-2 ${
                          i === acIdx ? 'bg-indigo-600 text-white' : 'text-chef-text hover:bg-chef-card-hover'
                        }`}
                      >
                        <span className="flex-1">{item}</span>
                        {fieldType && (
                          <span className={`text-[9px] shrink-0 ${i === acIdx ? 'text-indigo-200' : (TYPE_COLOR[fieldType] ?? 'text-chef-muted')}`}>
                            {fieldType}
                          </span>
                        )}
                        {isKw && !fieldType && (
                          <span className={`text-[9px] shrink-0 ${i === acIdx ? 'text-indigo-200' : 'text-chef-border'}`}>kw</span>
                        )}
                      </button>
                    )
                  })}
                  <div className="px-3 pt-1 pb-0.5 text-[9px] text-chef-muted border-t border-chef-border/50 mt-0.5">
                    ↑↓ · Tab/↵ accept · Esc dismiss
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* KQL translation hint */}
          {results?.kqlTranslated && (
            <div className="bg-amber-900/10 border-t border-amber-500/20 px-4 py-2 flex items-start gap-2 text-[11px] shrink-0">
              <ArrowRight size={11} className="text-amber-400 mt-0.5 shrink-0" />
              <div>
                <span className="text-amber-400 font-semibold">{isAiMode ? 'Translated Query: ' : isRedisMode ? 'Redis Result: ' : 'KQL → SQL: '}</span>
                <code className="font-mono text-amber-300">{results.kqlTranslated}</code>
              </div>
            </div>
          )}

          {/* Stored notice */}
          {storedNotice && (
            <div className="bg-cyan-900/10 border-t border-cyan-500/20 px-4 py-2 flex items-center gap-2 text-[11px] shrink-0">
              <Database size={11} className="text-cyan-400" />
              <span className="text-cyan-400">{storedNotice}</span>
            </div>
          )}

          {/* Results / error panel */}
          {(results || running || queryError) && (
            <div className="border-t-2 border-indigo-500/30 bg-chef-surface flex flex-col" style={{ maxHeight: '45%', minHeight: '160px' }}>
              <div className="flex items-center gap-3 px-4 py-2 border-b border-chef-border shrink-0">
                <div className="flex items-center gap-1.5 text-xs font-medium">
                  <Table size={12} className="text-indigo-400" />
                  <span className="text-chef-text">Results</span>
                  {results && <span className="text-chef-muted">({fmtNum(results.rowCount)} rows)</span>}
                </div>
                {results && (
                  <div className="flex items-center gap-3 text-[10px] font-mono text-chef-muted">
                    <span className="flex items-center gap-1 text-emerald-400"><Zap size={10} /> {results.durationMs}ms</span>
                    {!isAiMode && !isRedisMode && <span>{fmtBytes(results.bytesScanned)} scanned</span>}
                    <span>{fmtNum(results.totalRows)} rows total</span>
                    {isAiMode && <span className="flex items-center gap-1 text-cyan-400"><BarChart2 size={10} /> Observability</span>}
                    {isRedisMode && <span className="flex items-center gap-1 text-red-400"><Database size={10} /> Redis</span>}
                  </div>
                )}
                <div className="flex-1" />
                {results && results.rows.length > 0 && (
                  <button
                    onClick={() => void copyRowsToClipboard('selected')}
                    className="flex items-center gap-1 text-[11px] text-chef-muted hover:text-chef-text transition-colors"
                    title={hasSelection ? 'Copy selected rows' : 'Copy all visible rows'}
                  >
                    {copyFeedback === 'selected' || (copyFeedback === 'all' && !hasSelection)
                      ? <CheckCircle2 size={11} className="text-emerald-400" />
                      : <Copy size={11} />}
                    {hasSelection ? `Copy selected (${selectedRows.length})` : 'Copy all visible'}
                  </button>
                )}
                {results && results.rows.length > 0 && (
                  <button
                    onClick={() => {
                      const csv = rowsToDelimited(results.columns, results.rows, ',')
                      const a = document.createElement('a')
                      a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }))
                      a.download = `results_${isAiMode ? 'observability' : isRedisMode ? 'redis' : dataset}_${Date.now()}.csv`
                      a.click()
                    }}
                    className="flex items-center gap-1 text-[11px] text-chef-muted hover:text-chef-text transition-colors"
                  >
                    <Download size={11} /> Export CSV
                  </button>
                )}
                <button onClick={() => { setResults(null); setQueryError(null) }} className="p-1 text-chef-muted hover:text-chef-text transition-colors">
                  <X size={12} />
                </button>
              </div>

              {running ? (
                <div className="flex-1 flex items-center justify-center gap-3 text-chef-muted">
                  <Loader2 size={16} className="animate-spin text-indigo-400" />
                  <span className="text-sm font-mono">{isAiMode ? 'Querying observability source…' : isRedisMode ? 'Querying Redis…' : 'Executing on server…'}</span>
                </div>
              ) : queryError ? (
                <div className="flex-1 flex items-start gap-2 p-4 text-rose-400">
                  <AlertCircle size={14} className="shrink-0 mt-0.5" />
                  <div>
                    <div className="text-xs font-semibold mb-1">Query error</div>
                    <pre className="text-[11px] font-mono whitespace-pre-wrap">{queryError}</pre>
                  </div>
                </div>
              ) : results && results.rowCount === 0 ? (
                <div className="flex-1 flex items-center justify-center text-chef-muted text-sm">Query returned 0 rows</div>
              ) : results ? (
                <div className="flex-1 overflow-auto">
                  <table className="w-full">
                    <thead className="sticky top-0 bg-chef-surface z-10">
                      <tr className="border-b border-chef-border">
                        <th className="px-4 py-2 w-12">
                          <input
                            type="checkbox"
                            checked={allVisibleSelected}
                            ref={node => {
                              if (node) node.indeterminate = someVisibleSelected
                            }}
                            onChange={handleToggleAllRows}
                            className="h-3.5 w-3.5 rounded border-chef-border bg-chef-bg text-indigo-500 focus:ring-indigo-500/40"
                            aria-label="Select all visible rows"
                          />
                        </th>
                        {results.columns.map(col => (
                          <th key={col} className="text-left px-4 py-2 text-[10px] font-semibold uppercase tracking-wider text-chef-muted font-mono whitespace-nowrap">
                            {canRewriteSort ? (
                              <button
                                onClick={() => void handleHeaderSort(col)}
                                className={`flex items-center gap-1 transition-colors ${
                                  sortState?.column === col ? 'text-chef-text' : 'text-chef-muted hover:text-chef-text'
                                }`}
                                title={`Sort by ${col}`}
                              >
                                <span>{col}</span>
                                <span className={`text-[9px] ${sortState?.column === col ? 'text-indigo-300' : 'text-chef-border'}`}>
                                  {sortState?.column === col ? (sortState.direction === 'asc' ? '↑' : '↓') : '↕'}
                                </span>
                              </button>
                            ) : (
                              col
                            )}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {results.rows.map((row, i) => (
                        <tr
                          key={i}
                          className={`border-b border-chef-border/50 transition-colors ${
                            selectedRows.includes(i) ? 'bg-indigo-500/10' : 'hover:bg-chef-card/50'
                          }`}
                        >
                          <td className="px-4 py-2">
                            <input
                              type="checkbox"
                              checked={selectedRows.includes(i)}
                              onClick={event => handleToggleRow(i, event)}
                              readOnly
                              className="h-3.5 w-3.5 rounded border-chef-border bg-chef-bg text-indigo-500 focus:ring-indigo-500/40"
                              aria-label={`Select row ${i + 1}`}
                            />
                          </td>
                          {row.map((cell, j) => (
                            <td key={j} className={`px-4 py-2 text-[12px] font-mono whitespace-nowrap ${
                              cell === '∅'        ? 'text-chef-muted italic' :
                              cell === 'Alive'    ? 'text-emerald-400' :
                              cell === 'Dead'     ? 'text-rose-400' :
                              cell === 'unknown'  ? 'text-slate-400' :
                              cell === 'purchase' ? 'text-sky-400' :
                              cell === 'refund'   ? 'text-rose-400' :
                              cell === 'error'    ? 'text-rose-500' :
                              cell === 'mobile'   ? 'text-violet-400' :
                              cell === 'desktop'  ? 'text-indigo-400' :
                              j === 0 && !isNaN(Number(cell)) ? 'text-orange-300' :
                              'text-chef-text'
                            }`} title={renderCellValue(cell)}>
                              {renderCellValue(cell)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : null}
            </div>
          )}
        </div>

        {/* Status bar */}
        <div className="h-7 border-t border-chef-border bg-chef-bg flex items-center px-4 gap-4 text-[10px] font-mono shrink-0">
          {isAiMode ? (
            <>
              <span className="flex items-center gap-1 font-semibold text-amber-400">
                <Code2 size={10} /> KQL
              </span>
              <span className="flex items-center gap-1 text-cyan-400">
                <Clock size={10} /> {aiTimespanLabel}
              </span>
              <span className="text-chef-muted">{lineCount} lines</span>
            </>
          ) : (
            <>
              <span className={`flex items-center gap-1 font-semibold ${isRedisMode ? 'text-red-400' : langMeta[lang].color}`}>
                <Code2 size={10} /> {isRedisMode ? 'REDIS' : langMeta[lang].label}
              </span>
              {isRedisMode && <span className="text-chef-muted">{redisMode}</span>}
              <span className="text-chef-muted">{lineCount} lines</span>
            </>
          )}
          <div className="flex-1" />
          {results && (
            <>
              <span className="flex items-center gap-1 text-emerald-400"><Zap size={10} /> {results.durationMs}ms</span>
              {!isAiMode && !isRedisMode && <span className="text-chef-muted">{fmtBytes(results.bytesScanned)} scanned</span>}
              <span className="text-chef-muted">{fmtNum(results.rowCount)} rows returned</span>
            </>
          )}
          {!results && !running && (
            <span className="text-chef-muted">
              {isAiMode ? `Ready · ${aiConnectorName}` : isRedisMode ? `Ready · ${redisConnectorName}` : 'Ready · server-side'}
            </span>
          )}
          {running && <span className="text-indigo-400 flex items-center gap-1"><Loader2 size={10} className="animate-spin" /> Executing…</span>}
        </div>
      </div>
    </div>
  )
}
