'use client'

import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import {
  Plus, Play, Loader2, ChevronRight, ChevronDown, Search, Clock,
  Trash2, Copy, RotateCcw, AlertTriangle, Check, X, Zap, ArrowRightLeft,
  Gauge, Globe, Filter, Columns, SortAsc, Hash, Link2, Braces,
  FileJson, List, Shield, Eye, MousePointer, GitCompare, Workflow,
  RefreshCw, Pause, FolderOpen, Save, BookmarkPlus, MoreVertical,
} from 'lucide-react'
import ApiServiceWizard from '@/components/ApiServiceWizard'
import ApiFlowCanvas from '@/components/ApiFlowCanvas'
import { getServiceBrandIcon } from '@/lib/service-brand-icons'

/* ── Types ─────────────────────────────────────────────────────────── */

interface ApiService {
  id: string; name: string; description: string; baseUrl: string; swaggerUrl: string
  status: string; activeVersion: string; allowPrivate?: boolean
  customHeaders?: Record<string, string>
  versions: { version: string; endpointCount: number; openApiVersion: string }[]
  tags: string[]; auth: { scheme: string; apiKeyName?: string; apiKeyLocation?: string; apiKeyValue?: string; bearerToken?: string; basicUsername?: string; basicPassword?: string }
}

interface SchemaRegistry {
  endpoints: EndpointInfo[]; tags: string[]; allParamNames: string[]
  enumsByParam: Record<string, unknown[]>; schemas: Record<string, unknown>
}

interface EndpointInfo {
  path: string; method: string; summary?: string; description?: string
  tags: string[]; parameters: ParamInfo[]; deprecated: boolean
  operationId?: string
}

interface ParamInfo {
  name: string; in: string; type: string; required: boolean
  description?: string; enum?: unknown[]; default?: unknown
}

interface ExecResult {
  data: unknown; columns: string[]; rows: unknown[][]; totalCount: number
  status: number; headers: Record<string, string>; requestHeaders: Record<string, string>
  timing: { totalMs: number; ttfbMs: number; steps: StepInfo[] }
  validation: { valid: boolean; issues: ValidationIssue[] }
  cached: boolean; chainSteps?: ChainStepInfo[]
  benchmark?: BenchmarkResult; error?: string
}

interface StepInfo { url: string; method: string; latencyMs: number; status: number }
interface ChainStepInfo { url: string; method: string; status: number; latencyMs: number; recordCount: number }
interface ValidationIssue { path: string; expected: string; actual: string; severity: 'error' | 'warning' }
interface BenchmarkResult {
  runs: number; concurrency: number; totalDurationMs: number
  avgLatencyMs: number; p50LatencyMs: number; p95LatencyMs: number; p99LatencyMs: number
  minLatencyMs: number; maxLatencyMs: number; requestsPerSecond: number
  errorCount: number; successCount: number; totalBytesTransferred: number
}

interface QueryHistoryItem { query: string; timestamp: number; status: number; latencyMs: number }

interface ApiCollection {
  id: string; name: string; description: string; serviceId: string
  queries: { id: string; name: string; query: string; createdAt: number }[]
  createdAt: number; updatedAt: number
}

const POLL_INTERVALS = [
  { label: 'Off', ms: 0 },
  { label: '5s', ms: 5000 },
  { label: '10s', ms: 10000 },
  { label: '30s', ms: 30000 },
  { label: '1m', ms: 60000 },
  { label: '5m', ms: 300000 },
]

/* ── Autocomplete suggestion types ─────────────────────────────────── */

interface Suggestion { label: string; detail?: string; insertText: string; type: string }

const STAGE_SUGGESTIONS: Suggestion[] = [
  { label: 'endpoint', type: 'keyword', detail: 'Select endpoint', insertText: 'endpoint("' },
  { label: 'where', type: 'keyword', detail: 'Filter results', insertText: 'where(' },
  { label: 'select', type: 'keyword', detail: 'Choose fields', insertText: 'select(' },
  { label: 'order_by', type: 'keyword', detail: 'Sort results', insertText: 'order_by(' },
  { label: 'limit', type: 'keyword', detail: 'Limit results', insertText: 'limit(' },
  { label: 'offset', type: 'keyword', detail: 'Skip results', insertText: 'offset(' },
  { label: 'body', type: 'keyword', detail: 'Set request body', insertText: 'body(' },
  { label: 'header', type: 'keyword', detail: 'Add custom header', insertText: 'header("' },
  { label: 'chain', type: 'keyword', detail: 'Chain endpoint', insertText: 'chain("' },
  { label: 'group_by', type: 'keyword', detail: 'Group results', insertText: 'group_by(' },
  { label: 'aggregate', type: 'keyword', detail: 'Aggregate values', insertText: 'aggregate(' },
  { label: 'no_cache', type: 'keyword', detail: 'Bypass cache', insertText: 'no_cache' },
  { label: 'benchmark', type: 'keyword', detail: 'Throughput test', insertText: 'benchmark(runs: ' },
]

/* ── Path prefix tree ──────────────────────────────────────────────── */

interface PathTreeNode {
  segment: string          // relative label from parent (may be compressed, e.g. "api/v2")
  fullPath: string         // absolute path prefix, used as the expand/collapse key
  endpoints: EndpointInfo[]
  children: PathTreeNode[]
}

function buildPathTree(endpoints: EndpointInfo[]): PathTreeNode[] {
  // Internal mutable node
  type M = { segment: string; fullPath: string; endpoints: EndpointInfo[]; children: Map<string, M> }
  const root: M = { segment: '', fullPath: '', endpoints: [], children: new Map() }

  for (const ep of endpoints) {
    const parts = ep.path.split('/').filter(Boolean)
    let node = root
    let path = ''
    for (const part of parts) {
      path += '/' + part
      if (!node.children.has(part)) {
        node.children.set(part, { segment: part, fullPath: path, endpoints: [], children: new Map() })
      }
      node = node.children.get(part)!
    }
    node.endpoints.push(ep)
  }

  // Sort: folders (with children) first a-z, then leaf nodes a-z
  function sortNodes(nodes: PathTreeNode[]): PathTreeNode[] {
    const folders = nodes.filter(n => n.children.length > 0).sort((a, b) => a.segment.localeCompare(b.segment))
    const leaves = nodes.filter(n => n.children.length === 0).sort((a, b) => a.segment.localeCompare(b.segment))
    return [...folders, ...leaves]
  }

  // Compress single-child nodes that have no endpoints into their parent
  function compress(node: M): PathTreeNode {
    const children: PathTreeNode[] = sortNodes([...node.children.values()].map(compress))
    // Merge: this node has no endpoints, exactly one child → absorb the child's label
    if (node.endpoints.length === 0 && children.length === 1 && node.segment !== '') {
      const c = children[0]
      return { segment: `${node.segment}/${c.segment}`, fullPath: c.fullPath, endpoints: c.endpoints, children: c.children }
    }
    return { segment: node.segment, fullPath: node.fullPath, endpoints: node.endpoints, children }
  }

  return sortNodes([...root.children.values()].map(compress))
}

function countTreeEndpoints(node: PathTreeNode): number {
  return node.endpoints.length + node.children.reduce((s, c) => s + countTreeEndpoints(c), 0)
}

/* ── Method badge ──────────────────────────────────────────────────── */

function MethodBadge({ method }: { method: string }) {
  const colors: Record<string, string> = {
    GET: 'bg-emerald-500/20 text-emerald-400',
    POST: 'bg-amber-500/20 text-amber-400',
    PUT: 'bg-blue-500/20 text-blue-400',
    DELETE: 'bg-rose-500/20 text-rose-400',
    PATCH: 'bg-purple-500/20 text-purple-400',
  }
  return <span className={`px-1.5 py-0.5 rounded text-[10px] font-mono font-bold ${colors[method] ?? 'bg-chef-border text-chef-muted'}`}>{method}</span>
}

/* ── Main Page ─────────────────────────────────────────────────────── */

export default function ApiExplorerPage() {
  // Services
  const [services, setServices] = useState<ApiService[]>([])
  const [selectedService, setSelectedService] = useState<string>('')
  const [wizardOpen, setWizardOpen] = useState(false)

  // Schema
  const [schema, setSchema] = useState<SchemaRegistry | null>(null)
  const [schemaLoading, setSchemaLoading] = useState(false)
  const [schemaPanelOpen, setSchemaPanelOpen] = useState(true)
  const [expandedTags, setExpandedTags] = useState<Set<string>>(new Set())
  const [expandedPathNodes, setExpandedPathNodes] = useState<Set<string>>(new Set())
  const [selectedEndpoint, setSelectedEndpoint] = useState<EndpointInfo | null>(null)

  // Query
  const [query, setQuery] = useState('')
  const [executing, setExecuting] = useState(false)
  const [result, setResult] = useState<ExecResult | null>(null)
  const [execError, setExecError] = useState('')
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const highlightContainerRef = useRef<HTMLDivElement>(null)

  // Autocomplete
  const [suggestions, setSuggestions] = useState<Suggestion[]>([])
  const [sugIdx, setSugIdx] = useState(0)
  const [showSuggestions, setShowSuggestions] = useState(false)

  // Results tab
  const [resultTab, setResultTab] = useState<'table' | 'json' | 'headers' | 'validation' | 'perf'>('table')

  // Compare mode (diff)
  const [compareMode, setCompareMode] = useState(false)
  const [compareQuery, setCompareQuery] = useState('')
  const [compareResult, setCompareResult] = useState<ExecResult | null>(null)

  // Flow Canvas
  const [showFlowCanvas, setShowFlowCanvas] = useState(false)

  // Polling / auto-refresh
  const [pollIntervalMs, setPollIntervalMs] = useState(0)
  const [pollActive, setPollActive] = useState(false)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Service management
  const [showServiceMenu, setShowServiceMenu] = useState(false)
  const [deleteConfirm, setDeleteConfirm] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [sidebarWidth, setSidebarWidth] = useState(260)

  // Edit service modal
  const [showEditService, setShowEditService] = useState(false)
  const [editSaving, setEditSaving] = useState(false)
  const [editSaveError, setEditSaveError] = useState('')
  const [editName, setEditName] = useState('')
  const [editDescription, setEditDescription] = useState('')
  const [editBaseUrl, setEditBaseUrl] = useState('')
  const [editHeaders, setEditHeaders] = useState<{key: string; value: string}[]>([])
  const [editAuthScheme, setEditAuthScheme] = useState('none')
  const [editApiKeyName, setEditApiKeyName] = useState('')
  const [editApiKeyLocation, setEditApiKeyLocation] = useState('header')
  const [editApiKeyValue, setEditApiKeyValue] = useState('')
  const [editBearerToken, setEditBearerToken] = useState('')
  const [editBasicUser, setEditBasicUser] = useState('')
  const [editBasicPass, setEditBasicPass] = useState('')

  // Endpoint management
  const [showEndpointManager, setShowEndpointManager] = useState(false)
  const [excludedEndpoints, setExcludedEndpoints] = useState<Set<string>>(new Set())
  const [allEndpointsList, setAllEndpointsList] = useState<{key: string; method: string; path: string; summary?: string; tag: string}[]>([])

  // Collections
  const [collections, setCollections] = useState<ApiCollection[]>([])
  const [showSaveModal, setShowSaveModal] = useState(false)
  const [newCollName, setNewCollName] = useState('')
  const [saveToCollId, setSaveToCollId] = useState('')
  const [saveQueryName, setSaveQueryName] = useState('')
  const [expandedColls, setExpandedColls] = useState<Set<string>>(new Set())

  // History
  const [history, setHistory] = useState<QueryHistoryItem[]>([])

  // Load services
  useEffect(() => { loadServices() }, [])

  async function loadServices() {
    try {
      const res = await fetch('/api/api-services')
      if (res.ok) setServices(await res.json())
    } catch { /* ignore */ }
  }

  // Load collections
  useEffect(() => { loadCollections() }, [selectedService])

  async function loadCollections() {
    try {
      const url = selectedService
        ? `/api/api-collections?serviceId=${selectedService}`
        : '/api/api-collections'
      const res = await fetch(url)
      if (res.ok) setCollections(await res.json())
    } catch { /* ignore */ }
  }

  // Refresh service spec
  async function handleRefreshService() {
    if (!selectedService) return
    setRefreshing(true)
    setShowServiceMenu(false)
    try {
      const res = await fetch(`/api/api-services/${selectedService}/refresh`, { method: 'POST' })
      if (res.ok) {
        loadServices()
        // Reload schema
        setSchemaLoading(true)
        const sr = await fetch(`/api/api-services/${selectedService}/schema`)
        if (sr.ok) setSchema(await sr.json())
        setSchemaLoading(false)
      }
    } catch { /* ignore */ }
    setRefreshing(false)
  }

  // Delete service
  async function handleDeleteService() {
    if (!selectedService) return
    try {
      const res = await fetch(`/api/api-services/${selectedService}`, { method: 'DELETE' })
      if (res.ok) {
        setSelectedService('')
        setQuery('')
        setResult(null)
        setSchema(null)
        setDeleteConfirm(false)
        setShowServiceMenu(false)
        loadServices()
      }
    } catch { /* ignore */ }
  }

  // Open edit service modal
  function openEditService() {
    const svc = services.find(s => s.id === selectedService)
    if (!svc) return
    setEditName(svc.name)
    setEditDescription(svc.description ?? '')
    setEditBaseUrl(svc.baseUrl)
    const hdrs = Object.entries(svc.customHeaders ?? {}).map(([key, value]) => ({ key, value }))
    setEditHeaders(hdrs.length > 0 ? hdrs : [{ key: '', value: '' }])
    setEditAuthScheme(svc.auth.scheme ?? 'none')
    setEditApiKeyName(svc.auth.apiKeyName ?? '')
    setEditApiKeyLocation(svc.auth.apiKeyLocation ?? 'header')
    setEditApiKeyValue(svc.auth.apiKeyValue ?? '')
    setEditBearerToken(svc.auth.bearerToken ?? '')
    setEditBasicUser(svc.auth.basicUsername ?? '')
    setEditBasicPass(svc.auth.basicPassword ?? '')
    setEditSaveError('')
    setShowServiceMenu(false)
    setShowEditService(true)
  }

  async function handleSaveEditService() {
    if (!selectedService) return
    setEditSaving(true)
    setEditSaveError('')
    try {
      // Build customHeaders map (skip empty keys)
      const customHeaders: Record<string, string> = {}
      for (const { key, value } of editHeaders) {
        if (key.trim()) customHeaders[key.trim()] = value
      }

      // Build auth object
      const auth: Record<string, unknown> = { scheme: editAuthScheme }
      if (editAuthScheme === 'api_key') {
        auth.apiKeyName = editApiKeyName; auth.apiKeyLocation = editApiKeyLocation; auth.apiKeyValue = editApiKeyValue
      } else if (editAuthScheme === 'bearer') {
        auth.bearerToken = editBearerToken
      } else if (editAuthScheme === 'basic') {
        auth.basicUsername = editBasicUser; auth.basicPassword = editBasicPass
      }

      const res = await fetch(`/api/api-services/${selectedService}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: editName.trim(),
          description: editDescription,
          baseUrl: editBaseUrl.trim().replace(/\/+$/, ''),
          customHeaders,
          auth,
        }),
      })
      const data = await res.json()
      if (res.ok) {
        // Patch local service list immediately from response so the chip & selector update right away
        setServices(prev => prev.map(s =>
          s.id === selectedService
            ? { ...s, name: data.name ?? s.name, baseUrl: data.baseUrl ?? s.baseUrl, customHeaders: data.customHeaders ?? s.customHeaders }
            : s
        ))
        await loadServices()   // full refresh to sync all fields
        setShowEditService(false)
      } else {
        setEditSaveError(data.error ?? 'Save failed — please try again')
      }
    } catch (e) {
      setEditSaveError(e instanceof Error ? e.message : 'Network error')
    }
    setEditSaving(false)
  }

  // Open endpoint manager — fetch the full spec (un-excluded) to show all endpoints
  async function openEndpointManager() {
    if (!selectedService) return
    try {
      // Load the current service to get excludedEndpoints
      const svcRes = await fetch(`/api/api-services/${selectedService}`)
      const svc = svcRes.ok ? await svcRes.json() : null
      const excluded = new Set<string>(svc?.excludedEndpoints ?? [])
      setExcludedEndpoints(excluded)

      // Temporarily get the full schema (we use a query param to bypass exclusions)
      const schemaRes = await fetch(`/api/api-services/${selectedService}/schema?full=1`)
      const fullSchema = schemaRes.ok ? await schemaRes.json() : null
      if (fullSchema?.endpoints) {
        setAllEndpointsList(fullSchema.endpoints.map((ep: { method: string; path: string; summary?: string; tags?: string[] }) => ({
          key: `${ep.method} ${ep.path}`,
          method: ep.method,
          path: ep.path,
          summary: ep.summary,
          tag: ep.tags?.[0] ?? 'default',
        })))
      }
      setShowEndpointManager(true)
    } catch { /* ignore */ }
  }

  async function saveEndpointExclusions() {
    if (!selectedService) return
    try {
      await fetch(`/api/api-services/${selectedService}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ excludedEndpoints: [...excludedEndpoints] }),
      })
      setShowEndpointManager(false)
      // Reload schema to reflect changes
      setSchemaLoading(true)
      const schemaRes = await fetch(`/api/api-services/${selectedService}/schema`)
      if (schemaRes.ok) setSchema(await schemaRes.json())
      setSchemaLoading(false)
    } catch { /* ignore */ }
  }

  // Polling / auto-refresh
  useEffect(() => {
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null }
    if (pollActive && pollIntervalMs > 0 && query.trim()) {
      pollRef.current = setInterval(() => { executeQuery() }, pollIntervalMs)
    }
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [pollActive, pollIntervalMs, query]) // eslint-disable-line react-hooks/exhaustive-deps

  // Save query to collection
  async function handleSaveQuery() {
    if (!saveQueryName.trim()) return
    try {
      // Create new collection if needed
      let colId = saveToCollId
      if (colId === '__new__' && newCollName.trim()) {
        const res = await fetch('/api/api-collections', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: newCollName, serviceId: selectedService }),
        })
        if (res.ok) {
          const col = await res.json()
          colId = col.id
        }
      }
      if (!colId || colId === '__new__') return
      await fetch(`/api/api-collections/${colId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'addQuery', name: saveQueryName, query }),
      })
      setShowSaveModal(false)
      setSaveQueryName('')
      setNewCollName('')
      loadCollections()
    } catch { /* ignore */ }
  }

  async function handleDeleteCollection(colId: string) {
    await fetch(`/api/api-collections/${colId}`, { method: 'DELETE' })
    loadCollections()
  }

  async function handleRemoveQuery(colId: string, queryId: string) {
    await fetch(`/api/api-collections/${colId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'removeQuery', queryId }),
    })
    loadCollections()
  }

  // Load schema when service changes
  useEffect(() => {
    if (!selectedService) { setSchema(null); return }
    setSchemaLoading(true)
    fetch(`/api/api-services/${selectedService}/schema`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { setSchema(data); setSchemaLoading(false) })
      .catch(() => setSchemaLoading(false))
  }, [selectedService])

  // Auto-select first service (prefer localStorage-saved service)
  useEffect(() => {
    if (services.length === 0) return
    const saved = typeof window !== 'undefined' ? localStorage.getItem('api-explorer-service') : null
    if (saved && services.some(s => s.id === saved)) {
      if (!selectedService) setSelectedService(saved)
    } else if (!selectedService) {
      setSelectedService(services[0].id)
    }
  }, [services]) // eslint-disable-line react-hooks/exhaustive-deps

  // Persist selected service
  useEffect(() => {
    if (selectedService && typeof window !== 'undefined') {
      localStorage.setItem('api-explorer-service', selectedService)
    }
  }, [selectedService])

  // Restore saved query on mount (once)
  useEffect(() => {
    const saved = typeof window !== 'undefined' ? localStorage.getItem('api-explorer-query') : null
    if (saved) setQuery(saved)
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Persist query (debounced)
  useEffect(() => {
    const id = setTimeout(() => {
      if (typeof window !== 'undefined') localStorage.setItem('api-explorer-query', query)
    }, 600)
    return () => clearTimeout(id)
  }, [query])

  // Generate starter query when service selected
  const activeService = useMemo(() => services.find(s => s.id === selectedService), [services, selectedService])

  useEffect(() => {
    if (activeService && !query) {
      setQuery(`service("${activeService.name}")\n| endpoint("")\n| limit(10)`)
    }
  }, [activeService]) // eslint-disable-line react-hooks/exhaustive-deps

  /* ── Autocomplete logic ──────────────────────────────────────────── */

  const updateSuggestions = useCallback((text: string, cursorPos: number) => {
    const before = text.slice(0, cursorPos)

    // After pipe — suggest stages
    if (/\|\s*$/.test(before)) {
      setSuggestions(STAGE_SUGGESTIONS)
      setShowSuggestions(true)
      setSugIdx(0)
      return
    }

    // Inside endpoint("...") — suggest endpoint paths
    const epMatch = before.match(/endpoint\s*\(\s*"([^"]*)$/)
    if (epMatch && schema) {
      const partial = epMatch[1].toLowerCase()
      const eps = schema.endpoints
        .filter(e => e.path.toLowerCase().includes(partial) || (e.summary ?? '').toLowerCase().includes(partial))
        .slice(0, 15)
        .map(e => ({
          label: `${e.method} ${e.path}`,
          detail: e.summary ?? e.operationId,
          insertText: `${e.path}"${e.method !== 'GET' ? `, ${e.method}` : ''}`,
          type: 'endpoint',
        }))
      setSuggestions(eps)
      setShowSuggestions(eps.length > 0)
      setSugIdx(0)
      return
    }

    // Inside where() — suggest param names
    const whereMatch = before.match(/where\s*\([^)]*(?:,\s*|^\s*|\(\s*)([a-zA-Z_]*)$/)
    if (whereMatch && schema) {
      const partial = whereMatch[1].toLowerCase()
      const params = schema.allParamNames
        .filter(p => p.toLowerCase().includes(partial))
        .slice(0, 15)
        .map(p => ({
          label: p,
          detail: schema.enumsByParam[p] ? `enum: ${(schema.enumsByParam[p] as string[]).slice(0, 3).join(', ')}` : 'parameter',
          insertText: p,
          type: 'param',
        }))
      setSuggestions(params)
      setShowSuggestions(params.length > 0)
      setSugIdx(0)
      return
    }

    // Inside select()/group_by() — suggest field names from response schema
    const fieldMatch = before.match(/(?:select|group_by)\s*\([^)]*(?:,\s*|^\s*|\(\s*)([a-zA-Z_]*)$/)
    if (fieldMatch && result?.columns) {
      const partial = fieldMatch[1].toLowerCase()
      const fields = result.columns
        .filter(c => c.toLowerCase().includes(partial))
        .map(c => ({ label: c, insertText: c, type: 'field', detail: 'response field' }))
      setSuggestions(fields)
      setShowSuggestions(fields.length > 0)
      setSugIdx(0)
      return
    }

    // Inside service("...") — suggest service names
    const svcMatch = before.match(/service\s*\(\s*"([^"]*)$/)
    if (svcMatch) {
      const partial = svcMatch[1].toLowerCase()
      const svcs = services
        .filter(s => s.name.toLowerCase().includes(partial))
        .map(s => ({ label: s.name, detail: `${s.versions[0]?.endpointCount ?? '?'} endpoints`, insertText: `${s.name}"`, type: 'service' }))
      setSuggestions(svcs)
      setShowSuggestions(svcs.length > 0)
      setSugIdx(0)
      return
    }

    // Partial stage keyword
    const kwMatch = before.match(/\|\s*([a-z_]+)$/)
    if (kwMatch) {
      const partial = kwMatch[1].toLowerCase()
      const filtered = STAGE_SUGGESTIONS.filter(s => s.label.includes(partial))
      setSuggestions(filtered)
      setShowSuggestions(filtered.length > 0)
      setSugIdx(0)
      return
    }

    setShowSuggestions(false)
  }, [schema, services, result])

  function applySuggestion(sug: Suggestion) {
    const ta = textareaRef.current
    if (!ta) return
    const pos = ta.selectionStart
    const before = query.slice(0, pos)
    const after = query.slice(pos)

    // Find the start of the partial text to replace
    let replaceStart = pos
    if (sug.type === 'endpoint') {
      const m = before.match(/endpoint\s*\(\s*"[^"]*$/)
      if (m) replaceStart = before.lastIndexOf('"') + 1
    } else if (sug.type === 'param' || sug.type === 'field') {
      const m = before.match(/[a-zA-Z_]*$/)
      if (m) replaceStart = pos - m[0].length
    } else if (sug.type === 'service') {
      const m = before.match(/service\s*\(\s*"[^"]*$/)
      if (m) replaceStart = before.lastIndexOf('"') + 1
    } else {
      // Stage keyword
      const m = before.match(/\|\s*[a-z_]*$/)
      if (m) replaceStart = before.lastIndexOf('|') + 1
      else replaceStart = pos
    }

    const newQuery = query.slice(0, replaceStart) + (sug.type === 'keyword' ? ' ' : '') + sug.insertText + after
    setQuery(newQuery)
    setShowSuggestions(false)

    setTimeout(() => {
      const newPos = replaceStart + (sug.type === 'keyword' ? 1 : 0) + sug.insertText.length
      ta.focus()
      ta.setSelectionRange(newPos, newPos)
    }, 0)
  }

  /* ── Execute ─────────────────────────────────────────────────────── */

  async function executeQuery(q?: string) {
    const queryToRun = q ?? query
    if (!queryToRun.trim()) return

    // Detect SQL and show a helpful hint before hitting the server
    const sqlKeyword = /^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|WITH|SHOW|DESCRIBE|EXPLAIN)\b/i.exec(queryToRun)
    if (sqlKeyword) {
      setExecError(
        `SQL is not supported — this editor uses ApiQL syntax.\n\nTry:\n  service("${activeService?.name ?? 'my-api'}")\n  | endpoint("/your/path")\n  | limit(10)\n\nFor filtering: | where(param = value)\nFor chaining:  | chain("/next/path", bind: id = id)`
      )
      return
    }

    setExecuting(true)
    setExecError('')
    setResult(null)
    try {
      const res = await fetch('/api/api-proxy/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: queryToRun }),
      })
      const data = await res.json()
      if (!res.ok) {
        setExecError(data.error ?? 'Execution failed')
        return
      }
      setResult(data)
      setResultTab('table')
      // Record history
      setHistory(prev => [{
        query: queryToRun,
        timestamp: Date.now(),
        status: data.status,
        latencyMs: data.timing?.totalMs ?? 0,
      }, ...prev.slice(0, 49)])
    } catch (e) {
      setExecError(e instanceof Error ? e.message : 'Network error')
    } finally {
      setExecuting(false)
    }
  }

  async function executeCompare() {
    if (!compareQuery.trim()) return
    try {
      const res = await fetch('/api/api-proxy/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: compareQuery }),
      })
      const data = await res.json()
      if (res.ok) setCompareResult(data)
    } catch { /* ignore */ }
  }

  /* ── Click-to-Query (Viral Feature #1) ───────────────────────────── */

  function handleCellClick(colName: string, value: unknown, e: React.MouseEvent) {
    if (!result || !schema) return

    if (e.button === 2) return // right click handled separately

    const strValue = typeof value === 'string' ? `"${value}"` : String(value)

    // Check if this field maps to a path param on another endpoint
    const possibleEndpoints = schema.endpoints.filter(ep =>
      ep.path.includes(`{${colName}}`) || ep.path.includes(`{${colName}Id}`)
    )

    if (possibleEndpoints.length > 0) {
      const ep = possibleEndpoints[0]
      const newStage = `\n| chain("${ep.path}", bind: ${colName.replace(/Id$/, '')} = ${colName})`
      setQuery(prev => prev.trimEnd() + newStage)
    } else {
      // Add as where filter
      const newStage = `\n| where(${colName} = ${strValue})`
      setQuery(prev => prev.trimEnd() + newStage)
    }
  }

  function handleCellRightClick(colName: string, value: unknown, e: React.MouseEvent) {
    e.preventDefault()
    const strValue = typeof value === 'string' ? `"${value}"` : String(value)

    // Simple context menu via prompt-like approach — add to select
    const options = [
      `Filter: | where(${colName} = ${strValue})`,
      `Exclude: | where(${colName} != ${strValue})`,
      `Select: | select(${colName})`,
    ]

    // For now, add as filter (proper context menu can be added later)
    setQuery(prev => prev.trimEnd() + `\n| where(${colName} = ${strValue})`)
  }

  /* ── Schema panel endpoint click ─────────────────────────────────── */

  function insertEndpoint(ep: EndpointInfo) {
    setSelectedEndpoint(ep)
    const svcName = activeService?.name ?? ''
    const methodSuffix = ep.method !== 'GET' ? `, ${ep.method}` : ''
    const requiredParams = ep.parameters.filter(p => p.required)
    let whereClause = ''
    if (requiredParams.length > 0) {
      whereClause = `\n| where(${requiredParams.map(p => `${p.name} = ""`).join(', ')})`
    }
    setQuery(`service("${svcName}")\n| endpoint("${ep.path}"${methodSuffix})${whereClause}\n| limit(20)`)
  }

  /* ── Keyboard handling ───────────────────────────────────────────── */

  function handleKeyDown(e: React.KeyboardEvent) {
    if (showSuggestions && suggestions.length > 0) {
      if (e.key === 'ArrowDown') { e.preventDefault(); setSugIdx(i => Math.min(i + 1, suggestions.length - 1)) }
      else if (e.key === 'ArrowUp') { e.preventDefault(); setSugIdx(i => Math.max(i - 1, 0)) }
      else if (e.key === 'Enter' || e.key === 'Tab') { e.preventDefault(); applySuggestion(suggestions[sugIdx]) }
      else if (e.key === 'Escape') { setShowSuggestions(false) }
      return
    }

    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault()
      executeQuery()
    }
  }

  /* ── Syntax highlight (single-pass tokenizer) ────────────────────── */

  function highlightQuery(text: string): string {
    const KW = new Set(['service','endpoint','where','select','order_by','limit','offset','body','header','chain','group_by','aggregate','no_cache','benchmark'])
    const FN = new Set(['count','sum','avg','min','max','GET','POST','PUT','DELETE','PATCH'])
    const OP = new Set(['contains','startswith','and','or','as','asc','desc','bind','runs','concurrency'])
    const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    let out = '', i = 0
    while (i < text.length) {
      const ch = text[i]
      // Line comment
      if (ch === '/' && text[i + 1] === '/') {
        let j = i; while (j < text.length && text[j] !== '\n') j++
        out += `<span class="cmt">${esc(text.slice(i, j))}</span>`; i = j; continue
      }
      // String literal
      if (ch === '"') {
        let j = i + 1
        while (j < text.length && text[j] !== '"') { if (text[j] === '\\') j++; j++ }
        out += `<span class="str">${esc(text.slice(i, j + 1))}</span>`; i = j + 1; continue
      }
      // Pipe
      if (ch === '|') { out += '<span class="op">|</span>'; i++; continue }
      // Operators
      if ('=!<>'.includes(ch)) {
        const op = text[i + 1] === '=' ? ch + '=' : ch
        out += `<span class="op">${esc(op)}</span>`; i += op.length; continue
      }
      // Number
      if (/\d/.test(ch) && (i === 0 || /\W/.test(text[i - 1]))) {
        let j = i; while (j < text.length && /[\d.]/.test(text[j])) j++
        out += `<span class="num">${text.slice(i, j)}</span>`; i = j; continue
      }
      // Identifier / keyword
      if (/[a-zA-Z_]/.test(ch)) {
        let j = i; while (j < text.length && /\w/.test(text[j])) j++
        const w = text.slice(i, j)
        if (KW.has(w)) out += `<span class="kw">${w}</span>`
        else if (FN.has(w)) out += `<span class="fn">${w}</span>`
        else if (OP.has(w)) out += `<span class="op">${w}</span>`
        else out += esc(w)
        i = j; continue
      }
      out += esc(ch); i++
    }
    return out
  }

  /* ── Render ──────────────────────────────────────────────────────── */

  const endpointsByTag = useMemo(() => {
    if (!schema) return new Map<string, EndpointInfo[]>()
    const map = new Map<string, EndpointInfo[]>()
    for (const ep of schema.endpoints) {
      const tag = ep.tags[0] ?? 'default'
      const list = map.get(tag) ?? []
      list.push(ep)
      map.set(tag, list)
    }
    return map
  }, [schema])

  return (
    <div className="flex flex-col h-full overflow-hidden">
      {/* Top bar */}
      <div className="flex items-center gap-3 px-4 py-2.5 border-b border-chef-border bg-chef-surface/80 shrink-0">
        <div className="flex items-center gap-2 flex-1 min-w-0">
          {(() => {
            const svc = services.find(s => s.id === selectedService)
            if (svc) {
              const brand = getServiceBrandIcon(svc.name, svc.baseUrl)
              return <i className={`${brand.faClass} ${brand.color} shrink-0`} style={{ fontSize: '15px' }} />
            }
            return <Globe size={15} className="text-indigo-400 shrink-0" />
          })()}
          <span className="text-sm font-semibold text-chef-text">API Explorer</span>
          <span className="text-chef-muted text-xs">/</span>

          {/* Service selector */}
          <select
            value={selectedService}
            onChange={e => { setSelectedService(e.target.value); setQuery(''); setResult(null); setDeleteConfirm(false); setShowServiceMenu(false) }}
            className="bg-chef-card border border-chef-border rounded-lg px-2.5 py-1 text-sm text-chef-text focus:outline-none focus:border-indigo-500/50 min-w-[160px]"
          >
            <option value="">Select service...</option>
            {services.map(s => (
              <option key={s.id} value={s.id}>{s.name} (v{s.activeVersion})</option>
            ))}
          </select>

          {/* Service context menu */}
          {selectedService && (
            <div className="relative">
              <button
                onClick={() => { setShowServiceMenu(!showServiceMenu); setDeleteConfirm(false) }}
                className="p-1 rounded text-chef-muted hover:text-chef-text hover:bg-white/[0.04] transition-colors"
                title="Service options"
              >
                <MoreVertical size={14} />
              </button>
              {showServiceMenu && (
                <div className="absolute top-full left-0 mt-1 bg-chef-card border border-chef-border rounded-lg shadow-xl py-1 z-50 min-w-[200px]">
                  {!deleteConfirm ? (
                    <>
                      {/* Service info */}
                      {(() => {
                        const svc = services.find(s => s.id === selectedService)
                        if (!svc) return null
                        const activeVer = svc.versions.find(v => v.version === svc.activeVersion)
                        return (
                          <div className="px-3 py-2 border-b border-chef-border/50">
                            <div className="text-[10px] font-semibold text-chef-text truncate">{svc.name}</div>
                            <div className="text-[9px] text-chef-muted mt-0.5 truncate">{svc.baseUrl}</div>
                            {activeVer && (
                              <div className="text-[9px] text-indigo-400 mt-0.5">v{svc.activeVersion} · {activeVer.endpointCount} endpoints</div>
                            )}
                            <div className={`text-[9px] mt-0.5 ${svc.status === 'active' ? 'text-emerald-400' : svc.status === 'error' ? 'text-rose-400' : 'text-amber-400'}`}>
                              {svc.status}
                            </div>
                          </div>
                        )
                      })()}
                      <button
                        onClick={handleRefreshService}
                        disabled={refreshing}
                        className="w-full flex items-center gap-2 px-3 py-1.5 text-xs text-chef-text hover:bg-white/[0.04] transition-colors disabled:opacity-50"
                      >
                        <RotateCcw size={12} className={refreshing ? 'animate-spin' : ''} />
                        {refreshing ? 'Refreshing…' : 'Refresh spec'}
                      </button>
                      <button
                        onClick={() => {
                          const svc = services.find(s => s.id === selectedService)
                          if (svc) navigator.clipboard.writeText(svc.baseUrl)
                          setShowServiceMenu(false)
                        }}
                        className="w-full flex items-center gap-2 px-3 py-1.5 text-xs text-chef-text hover:bg-white/[0.04] transition-colors"
                      >
                        <Copy size={12} />
                        Copy base URL
                      </button>
                      {(() => {
                        const svc = services.find(s => s.id === selectedService)
                        if (!svc?.swaggerUrl) return null
                        return (
                          <button
                            onClick={() => { navigator.clipboard.writeText(svc.swaggerUrl); setShowServiceMenu(false) }}
                            className="w-full flex items-center gap-2 px-3 py-1.5 text-xs text-chef-text hover:bg-white/[0.04] transition-colors"
                          >
                            <Link2 size={12} />
                            Copy spec URL
                          </button>
                        )
                      })()}
                      <button
                        onClick={openEditService}
                        className="w-full flex items-center gap-2 px-3 py-1.5 text-xs text-chef-text hover:bg-white/[0.04] transition-colors"
                      >
                        <FolderOpen size={12} />
                        Edit service
                      </button>
                      <div className="border-t border-chef-border/50 mt-1 pt-1">
                        <button
                          onClick={() => setDeleteConfirm(true)}
                          className="w-full flex items-center gap-2 px-3 py-1.5 text-xs text-rose-400 hover:bg-rose-500/10 transition-colors"
                        >
                          <Trash2 size={12} />
                          Remove service
                        </button>
                      </div>
                    </>
                  ) : (
                    <div className="px-3 py-2">
                      <p className="text-[10px] text-rose-400 mb-1.5">Delete this service and all its specs?</p>
                      <div className="flex gap-1.5">
                        <button
                          onClick={handleDeleteService}
                          className="flex-1 px-2 py-1 text-[10px] bg-rose-500 text-white rounded hover:bg-rose-600 transition-colors"
                        >
                          Confirm Delete
                        </button>
                        <button
                          onClick={() => { setDeleteConfirm(false); setShowServiceMenu(false) }}
                          className="flex-1 px-2 py-1 text-[10px] bg-chef-border text-chef-muted rounded hover:bg-chef-border/80 transition-colors"
                        >
                          Cancel
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {/* Base URL chip — click to open Edit service */}
          {activeService && (
            <button
              onClick={openEditService}
              className="hidden sm:flex items-center gap-1.5 max-w-[220px] px-2 py-0.5 rounded-md bg-chef-bg border border-chef-border text-[11px] text-chef-muted hover:text-chef-text hover:border-indigo-500/40 transition-colors group"
              title={`Base URL: ${activeService.baseUrl} — click to edit`}
            >
              <Globe size={10} className="shrink-0 group-hover:text-indigo-400 transition-colors" />
              <span className="truncate font-mono">{activeService.baseUrl}</span>
            </button>
          )}

          <button
            onClick={() => setWizardOpen(true)}
            className="flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-indigo-500/10 text-indigo-400 hover:bg-indigo-500/20 text-xs font-medium transition-colors"
          >
            <Plus size={12} />
            Add Service
          </button>
        </div>

        <div className="flex items-center gap-1.5">
          <button
            onClick={() => { setShowFlowCanvas(!showFlowCanvas); if (!showFlowCanvas) setCompareMode(false) }}
            className={`flex items-center gap-1 px-2 py-1 rounded-lg text-xs transition-colors ${
              showFlowCanvas ? 'bg-purple-500/20 text-purple-400' : 'text-chef-muted hover:text-chef-text hover:bg-white/[0.04]'
            }`}
            title="Flow Canvas"
          >
            <Workflow size={12} />
            Flow
          </button>
          <button
            onClick={() => { setCompareMode(!compareMode); if (!compareMode) setShowFlowCanvas(false) }}
            className={`flex items-center gap-1 px-2 py-1 rounded-lg text-xs transition-colors ${
              compareMode ? 'bg-amber-500/20 text-amber-400' : 'text-chef-muted hover:text-chef-text hover:bg-white/[0.04]'
            }`}
            title="Compare mode"
          >
            <GitCompare size={12} />
            Diff
          </button>
          <button
            onClick={() => setSchemaPanelOpen(!schemaPanelOpen)}
            className="p-1.5 rounded-lg text-chef-muted hover:text-chef-text hover:bg-white/[0.04] transition-colors"
            title="Toggle schema panel"
          >
            <Braces size={14} />
          </button>
        </div>
      </div>

      {/* Main area */}
      <div className="flex-1 flex min-h-0">
        {/* Schema panel */}
        {schemaPanelOpen && (
          <div className="shrink-0 border-r border-chef-border bg-chef-surface overflow-y-auto" style={{ width: sidebarWidth }}>
            <div className="px-3 py-2 border-b border-chef-border/50 flex items-center justify-between">
              <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest">Endpoints</div>
              {selectedService && (
                <button
                  onClick={openEndpointManager}
                  className="text-[9px] text-chef-muted hover:text-indigo-400 transition-colors"
                  title="Manage endpoints — show/hide individual endpoints"
                >
                  Manage
                </button>
              )}
            </div>

            {schemaLoading ? (
              <div className="flex items-center justify-center py-8">
                <Loader2 size={16} className="animate-spin text-chef-muted" />
              </div>
            ) : schema ? (
              <div className="py-1">
                {[...endpointsByTag.entries()].map(([tag, endpoints]) => {
                  const tree = buildPathTree(endpoints)
                  const totalEndpoints = endpoints.length

                  // parentPath: the fullPath of the containing folder ('' at root)
                  // Used to strip the known prefix so we only show the relative portion
                  function renderNode(node: PathTreeNode, depth: number, parentPath: string): React.ReactNode {
                    const hasChildren = node.children.length > 0
                    const isExpanded = expandedPathNodes.has(node.fullPath)
                    // Base indent: tag rows use pl-3 (12px), each depth adds 10px
                    const indent = 12 + depth * 10

                    // What to show as the endpoint path label: strip the parent folder prefix
                    const epLabel = (ep: EndpointInfo) => {
                      const rel = ep.path.slice(parentPath.length)
                      return rel || '/'
                    }

                    return (
                      <div key={node.fullPath}>
                        {/* Folder row */}
                        {hasChildren && (
                          <button
                            onClick={() => setExpandedPathNodes(prev => {
                              const next = new Set(prev)
                              next.has(node.fullPath) ? next.delete(node.fullPath) : next.add(node.fullPath)
                              return next
                            })}
                            className="w-full flex items-center gap-1 text-chef-muted hover:text-chef-text hover:bg-white/[0.04] transition-colors py-1.5 pr-2"
                            style={{ paddingLeft: `${indent}px` }}
                          >
                            {isExpanded ? <ChevronDown size={10} className="shrink-0" /> : <ChevronRight size={10} className="shrink-0" />}
                            <span className="font-mono text-[11px] truncate flex-1 text-left">{node.segment}</span>
                            {!isExpanded && (
                              <span className="text-[9px] text-chef-muted/60 shrink-0">{countTreeEndpoints(node)}</span>
                            )}
                          </button>
                        )}

                        {/* Endpoints at this exact path — shown if leaf OR if folder is open */}
                        {(!hasChildren || isExpanded) && node.endpoints.map(ep => (
                          <div
                            key={`${ep.method}-${ep.path}`}
                            className={`group flex items-center gap-2 py-1.5 text-xs hover:bg-white/[0.04] transition-colors cursor-pointer pr-1.5 ${
                              selectedEndpoint?.path === ep.path && selectedEndpoint?.method === ep.method
                                ? 'bg-indigo-500/10 text-indigo-400'
                                : 'text-chef-muted hover:text-chef-text'
                            }`}
                            style={{ paddingLeft: `${indent + (hasChildren ? 14 : 4)}px` }}
                            onClick={() => insertEndpoint(ep)}
                            title={ep.path}
                          >
                            <MethodBadge method={ep.method} />
                            <span className="truncate font-mono text-[11px] flex-1">{epLabel(ep)}</span>
                            <CopyBtn value={ep.path} />
                          </div>
                        ))}

                        {/* Recurse into children — parentPath advances to this node's fullPath */}
                        {hasChildren && isExpanded && node.children.map(child => renderNode(child, depth + 1, node.fullPath))}
                      </div>
                    )
                  }

                  return (
                    <div key={tag}>
                      <button
                        onClick={() => {
                          const next = new Set(expandedTags)
                          if (next.has(tag)) {
                            next.delete(tag)
                          } else {
                            next.add(tag)
                            // Auto-expand first-level path nodes when opening a tag
                            setExpandedPathNodes(prev => {
                              const np = new Set(prev)
                              for (const node of tree) {
                                if (node.children.length > 0) np.add(node.fullPath)
                              }
                              return np
                            })
                          }
                          setExpandedTags(next)
                        }}
                        className="w-full flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-chef-muted hover:text-chef-text hover:bg-white/[0.04] transition-colors"
                      >
                        {expandedTags.has(tag) ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                        <span className="truncate">{tag}</span>
                        <span className="ml-auto text-[10px] text-chef-muted">{totalEndpoints}</span>
                      </button>

                      {expandedTags.has(tag) && tree.map(node => renderNode(node, 0, ''))}
                    </div>
                  )
                })}
              </div>
            ) : (
              <div className="px-3 py-4 text-xs text-chef-muted text-center">
                Select a service to browse endpoints
              </div>
            )}

          </div>
        )}

        {/* Drag handle */}
        {schemaPanelOpen && (
          <div
            className="w-1 shrink-0 cursor-col-resize hover:bg-indigo-500/40 active:bg-indigo-500 bg-transparent transition-colors"
            onMouseDown={e => {
              e.preventDefault()
              const startX = e.clientX
              const startWidth = sidebarWidth
              const onMove = (ev: MouseEvent) => setSidebarWidth(Math.max(160, Math.min(480, startWidth + ev.clientX - startX)))
              const onUp = () => {
                document.removeEventListener('mousemove', onMove)
                document.removeEventListener('mouseup', onUp)
              }
              document.addEventListener('mousemove', onMove)
              document.addEventListener('mouseup', onUp)
            }}
          />
        )}

        {/* Center: Query editor + Results OR Flow Canvas */}
        <div className="flex-1 flex flex-col min-w-0">
          {showFlowCanvas ? (
            <ApiFlowCanvas
              services={services.map(s => ({ id: s.id, name: s.name }))}
              initialServiceId={selectedService ?? undefined}
              onQueryGenerated={(q) => { setQuery(q); setShowFlowCanvas(false) }}
              onClose={() => setShowFlowCanvas(false)}
            />
          ) : (
          <>
          {/* Query editor — grows to fill available space when no result is shown */}
          <div className={`border-b border-chef-border bg-chef-surface ${result || execError ? 'shrink-0' : 'flex-1 flex flex-col'}`}>
            <div className={`${result || execError ? '' : 'flex-1 min-h-0'} ${compareMode ? 'grid grid-cols-2 divide-x divide-chef-border' : 'relative'}`}>
              {/* Main query */}
              <div className={`relative ${result || execError ? '' : 'h-full'}`}>
                <div
                  ref={highlightContainerRef}
                  className="absolute top-0 left-0 right-0 bottom-0 pointer-events-none px-3 py-2 overflow-hidden"
                >
                  <pre className="code-area whitespace-pre-wrap break-all" dangerouslySetInnerHTML={{ __html: highlightQuery(query) }} />
                </div>
                <textarea
                  ref={textareaRef}
                  value={query}
                  onChange={e => {
                    setQuery(e.target.value)
                    updateSuggestions(e.target.value, e.target.selectionStart)
                    // auto-resize (only when shrunk — when flex-1, height is managed by container)
                    if (result || execError) {
                      const el = e.target; el.style.height = 'auto'; el.style.height = el.scrollHeight + 'px'
                    }
                  }}
                  onScroll={e => {
                    if (highlightContainerRef.current) {
                      highlightContainerRef.current.scrollTop = e.currentTarget.scrollTop
                      highlightContainerRef.current.scrollLeft = e.currentTarget.scrollLeft
                    }
                  }}
                  onKeyDown={handleKeyDown}
                  onBlur={() => setTimeout(() => setShowSuggestions(false), 150)}
                  className="query-editor-ta code-area w-full h-full bg-transparent text-transparent caret-chef-text px-3 py-2 resize-none focus:outline-none"
                  style={{ minHeight: '120px' }}
                  placeholder='service("name") | endpoint("/path") | limit(10)'
                  spellCheck={false}
                />

                {/* Autocomplete dropdown */}
                {showSuggestions && suggestions.length > 0 && (
                  <div className="absolute z-30 left-3 top-full mt-1 w-80 max-h-60 overflow-y-auto bg-chef-card border border-chef-border rounded-xl shadow-2xl shadow-black/40">
                    {suggestions.map((sug, i) => (
                      <button
                        key={sug.label + i}
                        onMouseDown={e => { e.preventDefault(); applySuggestion(sug) }}
                        onMouseEnter={() => setSugIdx(i)}
                        className={`w-full flex items-center gap-2 px-3 py-1.5 text-xs transition-colors ${
                          i === sugIdx ? 'bg-indigo-500/15 text-indigo-300' : 'text-chef-text hover:bg-white/[0.04]'
                        }`}
                      >
                        <span className={`w-1.5 h-1.5 rounded-full shrink-0 ${
                          sug.type === 'keyword' ? 'bg-indigo-400' :
                          sug.type === 'endpoint' ? 'bg-emerald-400' :
                          sug.type === 'param' ? 'bg-amber-400' :
                          sug.type === 'service' ? 'bg-cyan-400' : 'bg-chef-muted'
                        }`} />
                        <span className="font-mono font-medium truncate">{sug.label}</span>
                        {sug.detail && <span className="ml-auto text-chef-muted truncate text-[10px]">{sug.detail}</span>}
                      </button>
                    ))}
                  </div>
                )}
              </div>

              {/* Compare query (diff mode) */}
              {compareMode && (
                <div className="relative">
                  <textarea
                    value={compareQuery}
                    onChange={e => setCompareQuery(e.target.value)}
                    className="w-full bg-transparent text-chef-text px-3 py-2 text-sm font-mono resize-none focus:outline-none"
                    style={{ minHeight: '120px' }}
                    placeholder="Compare query..."
                    spellCheck={false}
                  />
                </div>
              )}
            </div>

            {/* Run bar */}
            <div className="flex items-center gap-2 px-3 py-1.5 border-t border-chef-border/50 bg-chef-bg/50">
              <button
                onClick={() => executeQuery()}
                disabled={executing || !query.trim()}
                className="flex items-center gap-1.5 px-3 py-1 rounded-lg bg-emerald-500 hover:bg-emerald-600 text-white text-xs font-medium disabled:opacity-50 transition-colors"
              >
                {executing ? <Loader2 size={12} className="animate-spin" /> : <Play size={12} />}
                Run
              </button>

              {compareMode && (
                <button
                  onClick={executeCompare}
                  disabled={!compareQuery.trim()}
                  className="flex items-center gap-1.5 px-3 py-1 rounded-lg bg-amber-500 hover:bg-amber-600 text-white text-xs font-medium disabled:opacity-50 transition-colors"
                >
                  <ArrowRightLeft size={12} />
                  Run Compare
                </button>
              )}

              <span className="text-[10px] text-chef-muted">Ctrl+Enter to run</span>

              {/* Poll / auto-refresh */}
              <div className="flex items-center gap-1 border-l border-chef-border/50 pl-2 ml-1">
                <select
                  value={pollIntervalMs}
                  onChange={e => { const ms = Number(e.target.value); setPollIntervalMs(ms); if (ms === 0) setPollActive(false) }}
                  className="bg-transparent text-[10px] text-chef-muted border-none focus:outline-none cursor-pointer py-0"
                >
                  {POLL_INTERVALS.map(p => (
                    <option key={p.ms} value={p.ms}>{p.label}</option>
                  ))}
                </select>
                {pollIntervalMs > 0 && (
                  <button
                    onClick={() => setPollActive(!pollActive)}
                    className={`p-0.5 rounded transition-colors ${
                      pollActive ? 'text-emerald-400 bg-emerald-500/10 animate-pulse' : 'text-chef-muted hover:text-chef-text'
                    }`}
                    title={pollActive ? 'Stop auto-refresh' : 'Start auto-refresh'}
                  >
                    {pollActive ? <Pause size={10} /> : <RefreshCw size={10} />}
                  </button>
                )}
              </div>

              {/* Copy query */}
              {query.trim() && (
                <CopyQueryBtn query={query} />
              )}

              {/* Save to collection */}
              {query.trim() && (
                <button
                  onClick={() => { setShowSaveModal(true); setSaveQueryName(''); setSaveToCollId(collections[0]?.id ?? '__new__') }}
                  className="flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] text-chef-muted hover:text-indigo-400 hover:bg-indigo-500/10 transition-colors"
                  title="Save to collection"
                >
                  <BookmarkPlus size={10} />
                </button>
              )}

              {/* Result metadata */}
              {result && (
                <div className="ml-auto flex items-center gap-2 text-[10px]">
                  {result.cached && (
                    <span className="flex items-center gap-1 text-emerald-400 bg-emerald-500/10 px-1.5 py-0.5 rounded">
                      <Zap size={10} />cached
                    </span>
                  )}
                  <span className="text-chef-muted">{result.timing.totalMs}ms</span>
                  <span className={`font-mono ${result.status < 400 ? 'text-emerald-400' : 'text-rose-400'}`}>{result.status}</span>
                  <span className="text-chef-muted">{result.totalCount} rows</span>
                  {result.chainSteps && (
                    <span className="text-indigo-400">{result.chainSteps.length} steps</span>
                  )}
                  {result.validation && !result.validation.valid && (
                    <span className="flex items-center gap-0.5 text-amber-400">
                      <AlertTriangle size={10} />
                      {result.validation.issues.length} issues
                    </span>
                  )}
                </div>
              )}
            </div>
          </div>

          {/* Results area — only takes flex-1 when there's something to show */}
          <div className={`${result || execError ? 'flex-1' : 'shrink-0'} overflow-hidden flex flex-col`}>
            {execError && (
              <div className="px-4 py-3 bg-rose-500/10 border-b border-rose-500/20 text-rose-400 text-xs flex items-start gap-2">
                <AlertTriangle size={14} className="shrink-0 mt-0.5" />
                <pre className="whitespace-pre-wrap font-sans">{execError}</pre>
              </div>
            )}

            {result && (
              <>
                {/* Result tabs */}
                <div className="flex items-center gap-0.5 px-3 py-1 border-b border-chef-border/50 bg-chef-surface/50 shrink-0">
                  {([
                    { key: 'table', label: 'Table', icon: List },
                    { key: 'json', label: 'JSON', icon: FileJson },
                    { key: 'headers', label: 'Headers', icon: Braces },
                    { key: 'validation', label: 'Validation', icon: Shield },
                    { key: 'perf', label: 'Performance', icon: Gauge },
                  ] as const).map(({ key, label, icon: Icon }) => (
                    <button
                      key={key}
                      onClick={() => setResultTab(key)}
                      className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs transition-colors ${
                        resultTab === key
                          ? 'bg-indigo-500/15 text-indigo-400 font-medium'
                          : 'text-chef-muted hover:text-chef-text hover:bg-white/[0.04]'
                      }`}
                    >
                      <Icon size={12} />
                      {label}
                      {key === 'validation' && result.validation.issues.length > 0 && (
                        <span className="w-1.5 h-1.5 rounded-full bg-amber-400" />
                      )}
                    </button>
                  ))}
                </div>

                {/* Tab content */}
                <div className="flex-1 overflow-auto">
                  {resultTab === 'table' && (
                    <div className={`${compareMode && compareResult ? 'grid grid-cols-2 divide-x divide-chef-border h-full' : 'h-full'}`}>
                      <ResultTable result={result} onCellClick={handleCellClick} onCellRightClick={handleCellRightClick} />
                      {compareMode && compareResult && (
                        <ResultTable result={compareResult} onCellClick={handleCellClick} onCellRightClick={handleCellRightClick} />
                      )}
                    </div>
                  )}

                  {resultTab === 'json' && (
                    <div className={`${compareMode && compareResult ? 'grid grid-cols-2 divide-x divide-chef-border h-full' : 'h-full'}`}>
                      <div className="relative p-3 overflow-auto group">
                        <button
                          onClick={() => navigator.clipboard.writeText(JSON.stringify(result.data, null, 2))}
                          className="absolute top-3 right-3 opacity-0 group-hover:opacity-100 flex items-center gap-1 px-2 py-1 rounded-lg bg-chef-card border border-chef-border text-[10px] text-chef-muted hover:text-chef-text hover:bg-white/[0.08] transition-all z-10"
                          title="Copy JSON"
                        >
                          <Copy size={10} />
                          Copy
                        </button>
                        <pre className="text-xs font-mono text-chef-text whitespace-pre-wrap">{JSON.stringify(result.data, null, 2)}</pre>
                      </div>
                      {compareMode && compareResult && (
                        <div className="p-3 overflow-auto">
                          <pre className="text-xs font-mono text-chef-text whitespace-pre-wrap">{JSON.stringify(compareResult.data, null, 2)}</pre>
                        </div>
                      )}
                    </div>
                  )}

                  {resultTab === 'headers' && (
                    <div className="p-3 space-y-3">
                      {/* Request Headers */}
                      <div>
                        <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest mb-1.5 flex items-center gap-1.5">
                          <ArrowRightLeft size={9} className="text-amber-400" />
                          Request Headers Sent
                        </div>
                        {Object.keys(result.requestHeaders ?? {}).length === 0 ? (
                          <div className="text-[10px] text-chef-muted/60 italic">No request headers captured</div>
                        ) : (
                          Object.entries(result.requestHeaders).map(([k, v]) => (
                            <div key={k} className="group flex items-start gap-2 text-xs py-0.5">
                              <span className="font-mono text-amber-400/80 shrink-0">{k}:</span>
                              <span className="text-chef-text font-mono break-all flex-1">{v}</span>
                              <CopyBtn value={v} />
                            </div>
                          ))
                        )}
                      </div>

                      {/* Response Headers */}
                      <div className="border-t border-chef-border/40 pt-3">
                        <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest mb-1.5 flex items-center gap-1.5">
                          <ArrowRightLeft size={9} className="text-indigo-400" />
                          Response Headers Received
                        </div>
                        {Object.keys(result.headers).length === 0 ? (
                          <div className="text-[10px] text-chef-muted/60 italic">No response headers</div>
                        ) : (
                          Object.entries(result.headers).map(([k, v]) => (
                            <div key={k} className="group flex items-start gap-2 text-xs py-0.5">
                              <span className="font-mono text-indigo-400 shrink-0">{k}:</span>
                              <span className="text-chef-text font-mono break-all flex-1">{v}</span>
                              <CopyBtn value={v} />
                            </div>
                          ))
                        )}
                      </div>

                      {result.chainSteps && (
                        <div className="border-t border-chef-border/40 pt-3">
                          <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest mb-2">Chain Waterfall</div>
                          {result.chainSteps.map((step, i) => (
                            <div key={i} className="group flex items-center gap-2 text-xs py-0.5">
                              <span className="text-chef-muted w-4">{i + 1}.</span>
                              <MethodBadge method={step.method} />
                              <span className="font-mono text-chef-text truncate flex-1">{step.url}</span>
                              <CopyBtn value={step.url} />
                              <span className={`font-mono ${step.status < 400 ? 'text-emerald-400' : 'text-rose-400'}`}>{step.status}</span>
                              <span className="text-chef-muted">{step.latencyMs}ms</span>
                              <span className="text-chef-muted">{step.recordCount} rows</span>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )}

                  {resultTab === 'validation' && (
                    <div className="p-3 space-y-2">
                      {result.validation.valid ? (
                        <div className="flex items-center gap-2 text-emerald-400 text-sm">
                          <Check size={16} />
                          Response matches schema
                        </div>
                      ) : (
                        result.validation.issues.map((issue, i) => (
                          <div key={i} className={`flex items-start gap-2 px-3 py-2 rounded-lg text-xs ${
                            issue.severity === 'error' ? 'bg-rose-500/10 text-rose-400' : 'bg-amber-500/10 text-amber-400'
                          }`}>
                            {issue.severity === 'error' ? <X size={12} className="shrink-0 mt-0.5" /> : <AlertTriangle size={12} className="shrink-0 mt-0.5" />}
                            <div>
                              <span className="font-mono font-medium">{issue.path}</span>
                              <span className="mx-1.5">—</span>
                              <span>expected {issue.expected}, got {issue.actual}</span>
                            </div>
                          </div>
                        ))
                      )}
                    </div>
                  )}

                  {resultTab === 'perf' && (
                    <div className="p-3 space-y-3">
                      {/* Timing breakdown */}
                      <div className="grid grid-cols-3 gap-3">
                        <MetricBox label="Total" value={`${result.timing.totalMs}ms`} />
                        <MetricBox label="TTFB" value={`${result.timing.ttfbMs}ms`} />
                        <MetricBox label="Response Size" value={formatBytes(JSON.stringify(result.data).length)} />
                      </div>

                      {/* Chain waterfall */}
                      {result.timing.steps.length > 0 && (
                        <div>
                          <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest mb-2">Request Waterfall</div>
                          {result.timing.steps.map((step, i) => (
                            <div key={i} className="flex items-center gap-2 mb-1">
                              <span className="text-[10px] text-chef-muted w-4">{i + 1}</span>
                              <div className="flex-1 h-4 bg-chef-border/30 rounded overflow-hidden relative">
                                <div
                                  className="h-full bg-gradient-to-r from-indigo-500 to-cyan-500 rounded"
                                  style={{ width: `${Math.max((step.latencyMs / result.timing.totalMs) * 100, 5)}%` }}
                                />
                                <span className="absolute inset-0 flex items-center px-2 text-[10px] font-mono text-white">
                                  {step.method} {step.latencyMs}ms
                                </span>
                              </div>
                              <span className={`text-[10px] font-mono ${step.status < 400 ? 'text-emerald-400' : 'text-rose-400'}`}>{step.status}</span>
                            </div>
                          ))}
                        </div>
                      )}

                      {/* Benchmark results */}
                      {result.benchmark && (
                        <div>
                          <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest mb-2">Benchmark Results</div>
                          <div className="grid grid-cols-4 gap-2">
                            <MetricBox label="Avg Latency" value={`${result.benchmark.avgLatencyMs}ms`} />
                            <MetricBox label="P50" value={`${result.benchmark.p50LatencyMs}ms`} />
                            <MetricBox label="P95" value={`${result.benchmark.p95LatencyMs}ms`} />
                            <MetricBox label="P99" value={`${result.benchmark.p99LatencyMs}ms`} />
                            <MetricBox label="Min" value={`${result.benchmark.minLatencyMs}ms`} />
                            <MetricBox label="Max" value={`${result.benchmark.maxLatencyMs}ms`} />
                            <MetricBox label="Req/sec" value={`${result.benchmark.requestsPerSecond}`} />
                            <MetricBox label="Errors" value={`${result.benchmark.errorCount}/${result.benchmark.runs}`} />
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </>
            )}

            {!result && !execError && (
              <div className="py-6 flex items-center justify-center">
                <div className="text-center space-y-1.5">
                  <p className="text-xs text-chef-muted/70">Run a query with <kbd className="font-mono bg-chef-card border border-chef-border px-1 rounded text-[10px]">Ctrl+Enter</kbd></p>
                  <p className="text-[11px] text-chef-muted/50">
                    Type <span className="font-mono text-indigo-400/70">|</span> for autocomplete
                  </p>
                </div>
              </div>
            )}
          </div>
          </>
          )}
        </div>

        {/* Right panel: Endpoint Detail + Collections + History */}
        {(selectedEndpoint || collections.length > 0 || history.length > 0) && (
          <div className="w-[240px] shrink-0 border-l border-chef-border bg-chef-surface overflow-y-auto hidden xl:flex xl:flex-col">

            {/* Endpoint parameters detail */}
            {selectedEndpoint && (
              <div className="border-b border-chef-border/50">
                <div className="px-3 py-2 flex items-center justify-between">
                  <div className="flex items-center gap-1.5 min-w-0">
                    <MethodBadge method={selectedEndpoint.method} />
                    <span className="font-mono text-[10px] text-chef-text truncate">{selectedEndpoint.path}</span>
                  </div>
                  <button onClick={() => setSelectedEndpoint(null)} className="p-0.5 text-chef-muted hover:text-chef-text shrink-0 ml-1">
                    <X size={10} />
                  </button>
                </div>
                {selectedEndpoint.summary && (
                  <div className="px-3 pb-1 text-[10px] text-chef-muted line-clamp-2">{selectedEndpoint.summary}</div>
                )}
                <div className="px-3 pb-2 space-y-2">
                  <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest">
                    Parameters
                    {selectedEndpoint.parameters.length > 0 && (
                      <span className="ml-1.5 text-indigo-400 normal-case font-normal">{selectedEndpoint.parameters.length}</span>
                    )}
                  </div>
                  {selectedEndpoint.parameters.length === 0 ? (
                    <div className="text-[10px] text-chef-muted/60 italic">No parameters</div>
                  ) : (
                    selectedEndpoint.parameters.map(p => (
                      <div key={p.name} className="text-xs">
                        <div className="flex items-center gap-1.5 flex-wrap">
                          <span className="font-mono text-chef-text">{p.name}</span>
                          <span className="text-[9px] text-chef-muted bg-chef-bg px-1 rounded">{p.in}</span>
                          <span className="text-[9px] text-indigo-400/70">{p.type}</span>
                          {p.required && <span className="text-[9px] text-rose-400 font-semibold">req</span>}
                        </div>
                        {p.enum && (
                          <div className="mt-0.5 flex flex-wrap gap-0.5">
                            {(p.enum as string[]).slice(0, 6).map(v => (
                              <span key={String(v)} className="px-1 py-0 rounded text-[9px] bg-indigo-500/10 text-indigo-400 font-mono">{String(v)}</span>
                            ))}
                            {(p.enum as string[]).length > 6 && (
                              <span className="text-[9px] text-chef-muted">+{(p.enum as string[]).length - 6}</span>
                            )}
                          </div>
                        )}
                        {p.default !== undefined && (
                          <div className="text-[9px] text-chef-muted mt-0.5">default: <span className="font-mono text-chef-text/70">{String(p.default)}</span></div>
                        )}
                        {p.description && <div className="text-[9px] text-chef-muted mt-0.5 line-clamp-3">{p.description}</div>}
                      </div>
                    ))
                  )}
                </div>
              </div>
            )}

            {/* Collections */}
            {collections.length > 0 && (
              <>
                <div className="px-3 py-2 border-b border-chef-border/50 flex items-center justify-between">
                  <div className="flex items-center gap-1.5">
                    <FolderOpen size={10} className="text-indigo-400" />
                    <span className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest">Collections</span>
                  </div>
                </div>
                {collections.map(col => (
                  <div key={col.id}>
                    <div className="flex items-center w-full">
                      <button
                        onClick={() => {
                          const next = new Set(expandedColls)
                          next.has(col.id) ? next.delete(col.id) : next.add(col.id)
                          setExpandedColls(next)
                        }}
                        className="flex items-center gap-1.5 flex-1 min-w-0 px-3 py-1.5 text-xs text-chef-text hover:bg-white/[0.04] transition-colors"
                      >
                        {expandedColls.has(col.id) ? <ChevronDown size={10} /> : <ChevronRight size={10} />}
                        <span className="truncate">{col.name}</span>
                        <span className="ml-auto text-[9px] text-chef-muted shrink-0">{col.queries.length}</span>
                      </button>
                      <button
                        onClick={() => handleDeleteCollection(col.id)}
                        className="p-1 mr-1 text-chef-muted hover:text-rose-400 transition-colors shrink-0"
                        title="Delete collection"
                      >
                        <X size={10} />
                      </button>
                    </div>
                    {expandedColls.has(col.id) && col.queries.map(sq => (
                      <div key={sq.id} className="flex items-center group">
                        <button
                          onClick={() => { setQuery(sq.query); executeQuery(sq.query) }}
                          className="flex-1 min-w-0 pl-7 pr-1 py-1.5 text-left hover:bg-white/[0.04] transition-colors"
                        >
                          <div className="text-[10px] text-chef-text truncate">{sq.name}</div>
                          <div className="text-[9px] text-chef-muted font-mono truncate">{sq.query.split('\n')[0]}</div>
                        </button>
                        <button
                          onClick={() => handleRemoveQuery(col.id, sq.id)}
                          className="p-1 mr-1 text-chef-muted hover:text-rose-400 opacity-0 group-hover:opacity-100 transition-all shrink-0"
                          title="Remove query"
                        >
                          <X size={9} />
                        </button>
                      </div>
                    ))}
                  </div>
                ))}
              </>
            )}

            {/* History */}
            {history.length > 0 && (
              <>
                <div className="px-3 py-2 border-b border-chef-border/50 flex items-center justify-between">
                  <div className="flex items-center gap-1.5">
                    <Clock size={10} className="text-chef-muted" />
                    <span className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest">History</span>
                  </div>
                  <button onClick={() => setHistory([])} className="text-chef-muted hover:text-chef-text p-0.5">
                    <Trash2 size={10} />
                  </button>
                </div>
                {history.map((item, i) => (
                  <button
                    key={i}
                    onClick={() => { setQuery(item.query); executeQuery(item.query) }}
                    className="w-full px-3 py-2 text-left hover:bg-white/[0.04] border-b border-chef-border/30 transition-colors"
                  >
                    <div className="text-[10px] font-mono text-chef-text truncate">{item.query.split('\n')[0]}</div>
                    <div className="flex items-center gap-2 mt-0.5 text-[9px] text-chef-muted">
                      <span className={item.status < 400 ? 'text-emerald-400' : 'text-rose-400'}>{item.status}</span>
                      <span>{item.latencyMs}ms</span>
                      <span>{new Date(item.timestamp).toLocaleTimeString()}</span>
                    </div>
                  </button>
                ))}
              </>
            )}
          </div>
        )}
      </div>

      {/* Endpoint manager modal */}
      {showEndpointManager && (
        <div className="fixed inset-0 bg-black/60 z-50 flex items-center justify-center" onClick={() => setShowEndpointManager(false)}>
          <div className="bg-chef-card border border-chef-border rounded-xl shadow-2xl w-[520px] max-h-[80vh] flex flex-col" onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between px-5 py-4 border-b border-chef-border">
              <div>
                <h3 className="text-sm font-semibold text-chef-text">Manage Endpoints</h3>
                <p className="text-[10px] text-chef-muted mt-0.5">
                  Uncheck endpoints to hide them from the schema panel and autocomplete.
                  {excludedEndpoints.size > 0 && <span className="text-amber-400 ml-1">{excludedEndpoints.size} hidden</span>}
                </p>
              </div>
              <div className="flex gap-1.5">
                <button
                  onClick={() => setExcludedEndpoints(new Set())}
                  className="text-[10px] text-emerald-400 hover:text-emerald-300 transition-colors"
                >
                  Show all
                </button>
                <span className="text-chef-border">|</span>
                <button
                  onClick={() => setExcludedEndpoints(new Set(allEndpointsList.map(e => e.key)))}
                  className="text-[10px] text-rose-400 hover:text-rose-300 transition-colors"
                >
                  Hide all
                </button>
              </div>
            </div>
            <div className="flex-1 overflow-y-auto px-2 py-2">
              {(() => {
                const byTag = new Map<string, typeof allEndpointsList>()
                for (const ep of allEndpointsList) {
                  const list = byTag.get(ep.tag) ?? []
                  list.push(ep)
                  byTag.set(ep.tag, list)
                }
                return [...byTag.entries()].map(([tag, eps]) => (
                  <div key={tag} className="mb-2">
                    <div className="flex items-center justify-between px-2 py-1">
                      <span className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest">{tag}</span>
                      <span className="text-[9px] text-chef-muted">
                        {eps.filter(e => !excludedEndpoints.has(e.key)).length}/{eps.length}
                      </span>
                    </div>
                    {eps.map(ep => (
                      <label key={ep.key} className="flex items-center gap-2.5 px-2 py-1.5 rounded-lg hover:bg-white/[0.04] cursor-pointer transition-colors">
                        <input
                          type="checkbox"
                          checked={!excludedEndpoints.has(ep.key)}
                          onChange={() => {
                            const next = new Set(excludedEndpoints)
                            next.has(ep.key) ? next.delete(ep.key) : next.add(ep.key)
                            setExcludedEndpoints(next)
                          }}
                          className="accent-indigo-500 w-3.5 h-3.5 rounded"
                        />
                        <MethodBadge method={ep.method} />
                        <span className="text-xs font-mono text-chef-text truncate flex-1">{ep.path}</span>
                        {ep.summary && <span className="text-[10px] text-chef-muted truncate max-w-[140px]">{ep.summary}</span>}
                      </label>
                    ))}
                  </div>
                ))
              })()}
            </div>
            <div className="flex gap-2 px-5 py-3 border-t border-chef-border">
              <button
                onClick={saveEndpointExclusions}
                className="flex-1 py-1.5 bg-indigo-500 hover:bg-indigo-600 text-white text-xs rounded-lg font-medium transition-colors"
              >
                Save ({allEndpointsList.length - excludedEndpoints.size} visible)
              </button>
              <button
                onClick={() => setShowEndpointManager(false)}
                className="px-4 py-1.5 bg-chef-border text-chef-muted text-xs rounded-lg hover:bg-chef-border/80 transition-colors"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Save to collection modal */}
      {showSaveModal && (
        <div className="fixed inset-0 bg-black/60 z-50 flex items-center justify-center" onClick={() => setShowSaveModal(false)}>
          <div className="bg-chef-card border border-chef-border rounded-xl shadow-2xl w-[380px] p-5" onClick={e => e.stopPropagation()}>
            <div className="flex items-center gap-2 mb-4">
              <Save size={16} className="text-indigo-400" />
              <h3 className="text-sm font-semibold text-chef-text">Save to Collection</h3>
            </div>

            <label className="block text-xs text-chef-muted mb-1">Query name</label>
            <input
              value={saveQueryName}
              onChange={e => setSaveQueryName(e.target.value)}
              placeholder="e.g. List all datasets"
              className="w-full bg-chef-bg border border-chef-border rounded-lg px-3 py-1.5 text-sm text-chef-text focus:outline-none focus:border-indigo-500/50 mb-3"
              autoFocus
            />

            <label className="block text-xs text-chef-muted mb-1">Collection</label>
            <select
              value={saveToCollId}
              onChange={e => setSaveToCollId(e.target.value)}
              className="w-full bg-chef-bg border border-chef-border rounded-lg px-3 py-1.5 text-sm text-chef-text focus:outline-none focus:border-indigo-500/50 mb-2"
            >
              {collections.map(c => (
                <option key={c.id} value={c.id}>{c.name}</option>
              ))}
              <option value="__new__">+ New collection</option>
            </select>

            {saveToCollId === '__new__' && (
              <input
                value={newCollName}
                onChange={e => setNewCollName(e.target.value)}
                placeholder="Collection name"
                className="w-full bg-chef-bg border border-chef-border rounded-lg px-3 py-1.5 text-sm text-chef-text focus:outline-none focus:border-indigo-500/50 mb-2"
              />
            )}

            <div className="flex gap-2 mt-4">
              <button
                onClick={handleSaveQuery}
                disabled={!saveQueryName.trim() || (saveToCollId === '__new__' && !newCollName.trim())}
                className="flex-1 py-1.5 bg-indigo-500 hover:bg-indigo-600 text-white text-xs rounded-lg font-medium disabled:opacity-50 transition-colors"
              >
                Save
              </button>
              <button
                onClick={() => setShowSaveModal(false)}
                className="px-4 py-1.5 bg-chef-border text-chef-muted text-xs rounded-lg hover:bg-chef-border/80 transition-colors"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Edit Service modal */}
      {showEditService && (
        <div className="fixed inset-0 bg-black/60 z-50 flex items-center justify-center" onClick={() => setShowEditService(false)}>
          <div className="bg-chef-card border border-chef-border rounded-xl shadow-2xl w-[540px] max-h-[90vh] flex flex-col" onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between px-5 py-4 border-b border-chef-border shrink-0">
              <div>
                <h3 className="text-sm font-semibold text-chef-text">Edit Service</h3>
                <p className="text-[10px] text-chef-muted mt-0.5">Update service settings, custom headers and auth</p>
              </div>
              <button onClick={() => setShowEditService(false)} className="text-chef-muted hover:text-chef-text transition-colors"><X size={16} /></button>
            </div>

            <div className="flex-1 overflow-y-auto px-5 py-4 space-y-5">
              {/* General */}
              <div>
                <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest mb-2.5">General</div>
                <div className="space-y-2.5">
                  <div>
                    <label className="block text-xs text-chef-muted mb-1">Name</label>
                    <input value={editName} onChange={e => setEditName(e.target.value)} className="w-full bg-chef-bg border border-chef-border rounded-lg px-3 py-1.5 text-sm text-chef-text focus:outline-none focus:border-indigo-500/50" />
                  </div>
                  <div>
                    <label className="block text-xs text-chef-muted mb-1">Description</label>
                    <input value={editDescription} onChange={e => setEditDescription(e.target.value)} className="w-full bg-chef-bg border border-chef-border rounded-lg px-3 py-1.5 text-sm text-chef-text focus:outline-none focus:border-indigo-500/50" placeholder="Optional description" />
                  </div>
                  <div>
                    <label className="block text-xs text-chef-muted mb-1">Base URL</label>
                    <input value={editBaseUrl} onChange={e => setEditBaseUrl(e.target.value)} className="w-full bg-chef-bg border border-chef-border rounded-lg px-3 py-1.5 text-sm text-chef-text font-mono focus:outline-none focus:border-indigo-500/50" placeholder="https://api.example.com/v1" />
                  </div>
                </div>
              </div>

              {/* Custom Headers */}
              <div>
                <div className="flex items-center justify-between mb-2.5">
                  <div>
                    <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest">Custom Headers</div>
                    <div className="text-[9px] text-chef-muted mt-0.5">Injected into every request from this service</div>
                  </div>
                  <button
                    onClick={() => setEditHeaders(prev => [...prev, { key: '', value: '' }])}
                    className="text-[10px] text-indigo-400 hover:text-indigo-300 transition-colors flex items-center gap-1"
                  >
                    <Plus size={10} /> Add header
                  </button>
                </div>
                <div className="space-y-1.5">
                  {editHeaders.map((h, i) => (
                    <div key={i} className="flex gap-2 items-center">
                      <input
                        value={h.key}
                        onChange={e => setEditHeaders(prev => prev.map((r, j) => j === i ? { ...r, key: e.target.value } : r))}
                        placeholder="Header name"
                        className="flex-1 bg-chef-bg border border-chef-border rounded-lg px-2.5 py-1.5 text-xs text-chef-text font-mono focus:outline-none focus:border-indigo-500/50 min-w-0"
                      />
                      <input
                        value={h.value}
                        onChange={e => setEditHeaders(prev => prev.map((r, j) => j === i ? { ...r, value: e.target.value } : r))}
                        placeholder="Value"
                        className="flex-1 bg-chef-bg border border-chef-border rounded-lg px-2.5 py-1.5 text-xs text-chef-text font-mono focus:outline-none focus:border-indigo-500/50 min-w-0"
                      />
                      <button
                        onClick={() => setEditHeaders(prev => prev.filter((_, j) => j !== i))}
                        className="p-1 text-chef-muted hover:text-rose-400 transition-colors shrink-0"
                      >
                        <X size={12} />
                      </button>
                    </div>
                  ))}
                  {editHeaders.length === 0 && (
                    <div className="text-[10px] text-chef-muted text-center py-2 border border-dashed border-chef-border/50 rounded-lg">
                      No custom headers — click &ldquo;Add header&rdquo; to start
                    </div>
                  )}
                </div>
              </div>

              {/* Authentication */}
              <div>
                <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest mb-2.5">Authentication</div>
                <div className="space-y-2.5">
                  <div>
                    <label className="block text-xs text-chef-muted mb-1">Scheme</label>
                    <select value={editAuthScheme} onChange={e => setEditAuthScheme(e.target.value)} className="w-full bg-chef-bg border border-chef-border rounded-lg px-3 py-1.5 text-sm text-chef-text focus:outline-none focus:border-indigo-500/50">
                      <option value="none">None</option>
                      <option value="api_key">API Key</option>
                      <option value="bearer">Bearer Token</option>
                      <option value="basic">Basic Auth</option>
                    </select>
                  </div>

                  {editAuthScheme === 'api_key' && (
                    <>
                      <div className="grid grid-cols-2 gap-2">
                        <div>
                          <label className="block text-xs text-chef-muted mb-1">Key name</label>
                          <input value={editApiKeyName} onChange={e => setEditApiKeyName(e.target.value)} placeholder="X-API-Key" className="w-full bg-chef-bg border border-chef-border rounded-lg px-2.5 py-1.5 text-xs text-chef-text font-mono focus:outline-none focus:border-indigo-500/50" />
                        </div>
                        <div>
                          <label className="block text-xs text-chef-muted mb-1">Location</label>
                          <select value={editApiKeyLocation} onChange={e => setEditApiKeyLocation(e.target.value)} className="w-full bg-chef-bg border border-chef-border rounded-lg px-2.5 py-1.5 text-xs text-chef-text focus:outline-none focus:border-indigo-500/50">
                            <option value="header">Header</option>
                            <option value="query">Query param</option>
                            <option value="cookie">Cookie</option>
                          </select>
                        </div>
                      </div>
                      <div>
                        <label className="block text-xs text-chef-muted mb-1">Key value</label>
                        <input type="password" value={editApiKeyValue} onChange={e => setEditApiKeyValue(e.target.value)} placeholder="sk-..." className="w-full bg-chef-bg border border-chef-border rounded-lg px-2.5 py-1.5 text-xs text-chef-text font-mono focus:outline-none focus:border-indigo-500/50" />
                      </div>
                    </>
                  )}

                  {editAuthScheme === 'bearer' && (
                    <div>
                      <label className="block text-xs text-chef-muted mb-1">Token</label>
                      <input type="password" value={editBearerToken} onChange={e => setEditBearerToken(e.target.value)} placeholder="eyJ..." className="w-full bg-chef-bg border border-chef-border rounded-lg px-2.5 py-1.5 text-xs text-chef-text font-mono focus:outline-none focus:border-indigo-500/50" />
                    </div>
                  )}

                  {editAuthScheme === 'basic' && (
                    <div className="grid grid-cols-2 gap-2">
                      <div>
                        <label className="block text-xs text-chef-muted mb-1">Username</label>
                        <input value={editBasicUser} onChange={e => setEditBasicUser(e.target.value)} className="w-full bg-chef-bg border border-chef-border rounded-lg px-2.5 py-1.5 text-xs text-chef-text font-mono focus:outline-none focus:border-indigo-500/50" />
                      </div>
                      <div>
                        <label className="block text-xs text-chef-muted mb-1">Password</label>
                        <input type="password" value={editBasicPass} onChange={e => setEditBasicPass(e.target.value)} className="w-full bg-chef-bg border border-chef-border rounded-lg px-2.5 py-1.5 text-xs text-chef-text font-mono focus:outline-none focus:border-indigo-500/50" />
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>

            <div className="flex flex-col gap-2 px-5 py-3 border-t border-chef-border shrink-0">
              {editSaveError && (
                <div className="flex items-center gap-1.5 text-[11px] text-rose-400 bg-rose-500/10 border border-rose-500/20 rounded-lg px-3 py-2">
                  <AlertTriangle size={12} className="shrink-0" />
                  {editSaveError}
                </div>
              )}
              <div className="flex gap-2">
                <button
                  onClick={handleSaveEditService}
                  disabled={editSaving || !editName.trim() || !editBaseUrl.trim()}
                  className="flex-1 py-1.5 bg-indigo-500 hover:bg-indigo-600 text-white text-xs rounded-lg font-medium disabled:opacity-50 transition-colors flex items-center justify-center gap-1.5"
                >
                  {editSaving ? <><Loader2 size={12} className="animate-spin" /> Saving…</> : 'Save changes'}
                </button>
                <button onClick={() => { setShowEditService(false); setEditSaveError('') }} className="px-4 py-1.5 bg-chef-border text-chef-muted text-xs rounded-lg hover:bg-chef-border/80 transition-colors">
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Wizard modal */}
      <ApiServiceWizard
        open={wizardOpen}
        onClose={() => setWizardOpen(false)}
        onCreated={() => { loadServices(); setWizardOpen(false) }}
      />
    </div>
  )
}

/* ── Sub-components ────────────────────────────────────────────────── */

function ResultTable({
  result,
  onCellClick,
  onCellRightClick,
}: {
  result: ExecResult
  onCellClick: (col: string, val: unknown, e: React.MouseEvent) => void
  onCellRightClick: (col: string, val: unknown, e: React.MouseEvent) => void
}) {
  if (result.columns.length === 0) {
    return <div className="p-4 text-xs text-chef-muted text-center">No tabular data in response</div>
  }

  return (
    <div className="overflow-auto h-full">
      <table className="w-full text-xs">
        <thead className="sticky top-0 bg-chef-surface z-10">
          <tr>
            {result.columns.map(col => (
              <th key={col} className="group px-3 py-2 text-left font-medium text-chef-muted border-b border-chef-border whitespace-nowrap">
                <div className="flex items-center gap-1">
                  {col}
                  <CopyBtn value={col} />
                </div>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {result.rows.map((row, ri) => (
            <tr key={ri} className="border-b border-chef-border/30 hover:bg-white/[0.02]">
              {row.map((cell, ci) => (
                <td
                  key={ci}
                  className="px-3 py-1.5 text-chef-text font-mono whitespace-nowrap max-w-[300px] truncate cursor-pointer hover:bg-indigo-500/5 hover:text-indigo-300 transition-colors"
                  onClick={e => onCellClick(result.columns[ci], cell, e)}
                  onContextMenu={e => onCellRightClick(result.columns[ci], cell, e)}
                  title={`Click to drill into ${result.columns[ci]}`}
                >
                  {cell === null || cell === undefined ? (
                    <span className="text-chef-muted/50 italic">null</span>
                  ) : typeof cell === 'object' ? (
                    <span className="text-chef-muted">{JSON.stringify(cell)}</span>
                  ) : (
                    String(cell)
                  )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function CopyQueryBtn({ query }: { query: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <button
      onClick={async () => {
        try { await navigator.clipboard.writeText(query) } catch { /* ignore */ }
        setCopied(true)
        setTimeout(() => setCopied(false), 1500)
      }}
      className="flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] text-chef-muted hover:text-chef-text hover:bg-white/[0.04] transition-colors"
      title="Copy query"
    >
      {copied ? <Check size={10} className="text-emerald-400" /> : <Copy size={10} />}
    </button>
  )
}

function CopyBtn({ value, className = '' }: { value: string; className?: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <button
      onClick={async e => {
        e.stopPropagation()
        try { await navigator.clipboard.writeText(value) } catch { /* ignore */ }
        setCopied(true)
        setTimeout(() => setCopied(false), 1500)
      }}
      className={`inline-flex items-center justify-center w-5 h-5 rounded opacity-0 group-hover:opacity-100 hover:!opacity-100 hover:bg-white/10 text-chef-muted hover:text-chef-text transition-all shrink-0 ${className}`}
      title="Copy"
    >
      {copied ? <Check size={10} className="text-emerald-400" /> : <Copy size={10} />}
    </button>
  )
}

function MetricBox({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg bg-chef-card border border-chef-border p-2.5">
      <div className="text-[10px] text-chef-muted uppercase">{label}</div>
      <div className="text-sm font-mono font-medium text-chef-text mt-0.5">{value}</div>
    </div>
  )
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)}MB`
}
