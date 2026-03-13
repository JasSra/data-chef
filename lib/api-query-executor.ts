/**
 * API Query Executor — translates an ApiQL AST into HTTP requests,
 * handles chain queries, response validation, caching, and performance tracking.
 */

import 'server-only'

import type {
  ApiQueryAST, QueryStage, WhereCondition, ChainBinding, AggregationExpr,
} from '@/lib/api-query-parser'
import type { ApiServiceAuth } from '@/lib/api-services'
import { getApiService, getApiServiceAuth, loadSpec } from '@/lib/api-services'
import { validateProxyUrl } from '@/lib/ssrf-guard'
import { makeCacheKey, getCached, setCached } from '@/lib/api-response-cache'
import { recordMetrics, computeBenchmarkResult } from '@/lib/api-perf-tracker'
import type { RequestMetrics, BenchmarkResult } from '@/lib/api-perf-tracker'
import { extractSchemaRegistry } from '@/lib/api-schema-registry'
import type { OpenApiSpec } from '@/lib/openapi-normalizer'

/* ── Types ─────────────────────────────────────────────────────────── */

export interface ExecutionResult {
  data: unknown
  columns: string[]
  rows: unknown[][]
  totalCount: number
  status: number
  headers: Record<string, string>         // response headers
  requestHeaders: Record<string, string>  // request headers that were sent
  timing: TimingInfo
  validation: ValidationResult
  cached: boolean
  chainSteps?: ChainStepInfo[]
  benchmark?: BenchmarkResult
}

export interface TimingInfo {
  totalMs: number
  ttfbMs: number
  steps: { url: string; method: string; latencyMs: number; status: number }[]
}

export interface ValidationResult {
  valid: boolean
  issues: ValidationIssue[]
}

export interface ValidationIssue {
  path: string
  expected: string
  actual: string
  severity: 'error' | 'warning'
}

export interface ChainStepInfo {
  url: string
  method: string
  status: number
  latencyMs: number
  recordCount: number
}

const MAX_CHAIN_DEPTH = 5
const REQUEST_TIMEOUT_MS = 30_000

/* ── Main executor ─────────────────────────────────────────────────── */

export async function executeApiQuery(ast: ApiQueryAST): Promise<ExecutionResult> {
  const t0 = performance.now()

  // Resolve service
  const services = (await import('@/lib/api-services')).getApiServices()
  const service = services.find(s => s.name === ast.service || s.id === ast.service)
  if (!service) {
    throw new Error(`Service not found: "${ast.service}"`)
  }

  const auth = getApiServiceAuth(service.id)
  const baseUrl = service.baseUrl

  const customHeaders: Record<string, string> = service.customHeaders ?? {}
  const allowPrivate = service.allowPrivate ?? false

  // Check for benchmark stage
  const benchmarkStage = ast.stages.find(s => s.type === 'benchmark')
  if (benchmarkStage && benchmarkStage.type === 'benchmark') {
    return executeBenchmark(ast, service.id, baseUrl, auth, benchmarkStage.runs, benchmarkStage.concurrency, customHeaders, allowPrivate)
  }

  // Check for no_cache flag
  const noCache = ast.stages.some(s => s.type === 'no_cache')

  // Build the request from stages
  const request = buildRequest(ast.stages, baseUrl)

  // Check cache first (unless no_cache)
  if (!noCache && request.method === 'GET') {
    const cacheKey = makeCacheKey(service.id, request.method, request.url, request.queryParams)
    const cached = getCached(cacheKey)
    if (cached) {
      const data = cached.data
      const { columns, rows, totalCount } = extractTabularData(data, ast.stages)
      return {
        data,
        columns,
        rows,
        totalCount,
        status: cached.status,
        headers: cached.headers,
        requestHeaders: {},
        timing: { totalMs: Math.round(performance.now() - t0), ttfbMs: 0, steps: [] },
        validation: { valid: true, issues: [] },
        cached: true,
      }
    }
  }

  // Execute the request (potentially with chains)
  const chainStages = ast.stages.filter(s => s.type === 'chain')
  let result: ProxiedResponse

  if (chainStages.length > 0) {
    result = await executeChainedRequests(ast.stages, baseUrl, auth, service.id, customHeaders, allowPrivate)
  } else {
    result = await executeSingleRequest(request, auth, service.id, noCache, customHeaders, allowPrivate)
  }

  // Apply post-processing (select, group_by, aggregate, order_by, limit, offset)
  const { columns, rows, totalCount } = extractTabularData(result.data, ast.stages)

  // Validate response against schema
  const validation = await validateResponse(service.id, result.data, ast.stages)

  const totalMs = Math.round(performance.now() - t0)

  return {
    data: result.data,
    columns,
    rows,
    totalCount,
    status: result.status,
    headers: result.headers,
    requestHeaders: result.requestHeaders,
    timing: { totalMs, ttfbMs: result.ttfbMs, steps: result.steps },
    validation,
    cached: false,
    chainSteps: result.chainSteps,
  }
}

/* ── Request building ──────────────────────────────────────────────── */

interface BuiltRequest {
  method: string
  url: string
  queryParams: Record<string, string>
  headers: Record<string, string>
  body?: string
  path: string
}

function buildRequest(stages: QueryStage[], baseUrl: string): BuiltRequest {
  let method = 'GET'
  let path = '/'
  const queryParams: Record<string, string> = {}
  const headers: Record<string, string> = {}
  let body: string | undefined

  for (const stage of stages) {
    switch (stage.type) {
      case 'endpoint':
        path = stage.path
        method = stage.method
        break
      case 'where':
        for (const cond of stage.conditions) {
          queryParams[cond.field] = String(cond.value)
        }
        break
      case 'header':
        headers[stage.name] = stage.value
        break
      case 'body':
        body = stage.json
        break
      case 'limit':
        queryParams['limit'] = String(stage.count)
        break
      case 'offset':
        queryParams['offset'] = String(stage.count)
        break
      case 'order_by':
        queryParams['order_by'] = `${stage.field} ${stage.direction}`
        break
      case 'select':
        queryParams['select'] = stage.fields.join(',')
        break
      case 'group_by':
        queryParams['group_by'] = stage.fields.join(',')
        break
    }
  }

  const url = `${baseUrl}${path}`
  return { method, url, queryParams, headers, body, path }
}

/* ── HTTP execution ────────────────────────────────────────────────── */

interface ProxiedResponse {
  data: unknown
  status: number
  headers: Record<string, string>        // response headers
  requestHeaders: Record<string, string> // request headers actually sent
  ttfbMs: number
  steps: { url: string; method: string; latencyMs: number; status: number }[]
  chainSteps?: ChainStepInfo[]
}

async function executeSingleRequest(
  request: BuiltRequest,
  auth: ApiServiceAuth | null,
  serviceId: string,
  noCache: boolean,
  customHeaders?: Record<string, string>,
  allowPrivate?: boolean,
): Promise<ProxiedResponse> {
  const url = new URL(request.url)
  for (const [k, v] of Object.entries(request.queryParams)) {
    url.searchParams.set(k, v)
  }

  // SSRF check — honour allowPrivate so intranet services work
  const guard = validateProxyUrl(url.toString(), { allowPrivate })
  if (!guard.valid) throw new Error(`Blocked: ${guard.error}`)

  // Apply headers: service-level custom headers (lowest) → query-level headers → auth (highest)
  const headers: Record<string, string> = {
    'Accept': 'application/json',
    ...(customHeaders ?? {}),
    ...request.headers,
  }
  applyAuth(auth, headers, url)

  const t0 = performance.now()
  let ttfbMs = 0

  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS)

  try {
    const fetchOpts: RequestInit = {
      method: request.method,
      headers,
      signal: controller.signal,
    }
    if (request.body && request.method !== 'GET') {
      fetchOpts.body = request.body
      if (!headers['Content-Type']) headers['Content-Type'] = 'application/json'
    }

    const res = await fetch(url.toString(), fetchOpts)
    ttfbMs = Math.round(performance.now() - t0)
    clearTimeout(timeout)

    const responseHeaders: Record<string, string> = {}
    res.headers.forEach((v, k) => { responseHeaders[k] = v })

    let data: unknown
    const contentType = res.headers.get('content-type') ?? ''
    if (contentType.includes('json')) {
      data = await res.json()
    } else {
      data = await res.text()
    }

    const latencyMs = Math.round(performance.now() - t0)
    const responseSizeBytes = JSON.stringify(data).length

    // Record metrics
    const metrics: RequestMetrics = {
      url: url.toString(),
      method: request.method,
      statusCode: res.status,
      latencyMs,
      ttfbMs,
      responseSizeBytes,
      cached: false,
      timestamp: Date.now(),
    }
    recordMetrics(serviceId, metrics)

    // Cache the response
    if (!noCache) {
      const cacheKey = makeCacheKey(serviceId, request.method, request.url, request.queryParams, request.body)
      setCached(cacheKey, data, responseHeaders, res.status, request.method, res.headers.get('cache-control') ?? undefined)
    }

    // Capture the final request headers (after auth injection)
    const sentHeaders: Record<string, string> = { ...headers }
    // Redact secret values in the display copy
    for (const k of Object.keys(sentHeaders)) {
      const kl = k.toLowerCase()
      if (kl === 'authorization' || kl === 'x-api-key' || kl === 'cookie') {
        const v = sentHeaders[k]
        // Show scheme/prefix but mask the credential part
        const spaceIdx = v.indexOf(' ')
        sentHeaders[k] = spaceIdx > 0 ? `${v.slice(0, spaceIdx)} ****` : '****'
      }
    }

    return {
      data,
      status: res.status,
      headers: responseHeaders,
      requestHeaders: sentHeaders,
      ttfbMs,
      steps: [{ url: url.toString(), method: request.method, latencyMs, status: res.status }],
    }
  } catch (e) {
    clearTimeout(timeout)
    throw e
  }
}

async function executeChainedRequests(
  stages: QueryStage[],
  baseUrl: string,
  auth: ApiServiceAuth | null,
  serviceId: string,
  customHeaders?: Record<string, string>,
  allowPrivate?: boolean,
): Promise<ProxiedResponse> {
  const steps: { url: string; method: string; latencyMs: number; status: number }[] = []
  const chainSteps: ChainStepInfo[] = []
  let currentData: unknown = null
  let lastStatus = 200
  let lastHeaders: Record<string, string> = {}
  let totalTtfb = 0

  // Collect non-chain stages for the first request
  const preChainStages: QueryStage[] = []
  const chainAndPostStages: { chain: QueryStage; post: QueryStage[] }[] = []

  let currentPost: QueryStage[] = []
  let currentChain: QueryStage | null = null

  for (const stage of stages) {
    if (stage.type === 'chain') {
      if (currentChain) {
        chainAndPostStages.push({ chain: currentChain, post: currentPost })
        currentPost = []
      }
      currentChain = stage
    } else if (currentChain) {
      currentPost.push(stage)
    } else {
      preChainStages.push(stage)
    }
  }
  if (currentChain) {
    chainAndPostStages.push({ chain: currentChain, post: currentPost })
  }

  if (chainAndPostStages.length > MAX_CHAIN_DEPTH) {
    throw new Error(`Chain depth ${chainAndPostStages.length} exceeds maximum of ${MAX_CHAIN_DEPTH}`)
  }

  // Execute first request
  const firstReq = buildRequest(preChainStages, baseUrl)
  const firstResult = await executeSingleRequest(firstReq, auth, serviceId, false, customHeaders, allowPrivate)
  currentData = firstResult.data
  lastStatus = firstResult.status
  lastHeaders = firstResult.headers
  let lastRequestHeaders = firstResult.requestHeaders
  totalTtfb += firstResult.ttfbMs
  steps.push(...firstResult.steps)

  const firstRecords = extractRecords(currentData)
  chainSteps.push({
    url: firstReq.url + firstReq.path,
    method: firstReq.method,
    status: firstResult.status,
    latencyMs: firstResult.steps[0]?.latencyMs ?? 0,
    recordCount: firstRecords.length,
  })

  // Execute chain steps
  for (const { chain, post: _post } of chainAndPostStages) {
    if (chain.type !== 'chain') continue

    const records = extractRecords(currentData)
    const allResults: unknown[] = []

    for (const record of records.slice(0, 50)) { // cap at 50 to prevent abuse
      let chainPath = chain.path
      const chainParams: Record<string, string> = {}

      // Apply bindings
      for (const binding of chain.bindings) {
        const value = getNestedValue(record, binding.sourceField)
        if (value !== undefined) {
          // Replace path params {param}
          const pathParam = `{${binding.targetParam}}`
          if (chainPath.includes(pathParam)) {
            chainPath = chainPath.replace(pathParam, String(value))
          } else {
            chainParams[binding.targetParam] = String(value)
          }
        }
      }

      // Also handle implicit path params from record
      const pathParams = chainPath.match(/\{(\w+)\}/g)
      if (pathParams) {
        for (const pp of pathParams) {
          const paramName = pp.slice(1, -1)
          const value = getNestedValue(record, paramName)
          if (value !== undefined) {
            chainPath = chainPath.replace(pp, String(value))
          }
        }
      }

      const chainUrl = `${baseUrl}${chainPath}`
      const chainReq: BuiltRequest = {
        method: chain.method,
        url: chainUrl,
        queryParams: chainParams,
        headers: { ...(customHeaders ?? {}) },
        path: chainPath,
      }

      try {
        const res = await executeSingleRequest(chainReq, auth, serviceId, false, customHeaders, allowPrivate)
        allResults.push(res.data)
        steps.push(...res.steps)
        totalTtfb += res.ttfbMs
        lastStatus = res.status
        lastHeaders = res.headers
        lastRequestHeaders = res.requestHeaders
      } catch {
        // Continue on individual failures
      }
    }

    currentData = allResults.length === 1 ? allResults[0] : allResults

    chainSteps.push({
      url: `${baseUrl}${chain.path}`,
      method: chain.method,
      status: lastStatus,
      latencyMs: steps.slice(-records.length).reduce((sum, s) => sum + s.latencyMs, 0),
      recordCount: allResults.length,
    })
  }

  return {
    data: currentData,
    status: lastStatus,
    headers: lastHeaders,
    requestHeaders: lastRequestHeaders,
    ttfbMs: totalTtfb,
    steps,
    chainSteps,
  }
}

/* ── Benchmark execution ───────────────────────────────────────────── */

async function executeBenchmark(
  ast: ApiQueryAST,
  serviceId: string,
  baseUrl: string,
  auth: ApiServiceAuth | null,
  runs: number,
  concurrency: number,
  customHeaders?: Record<string, string>,
  allowPrivate?: boolean,
): Promise<ExecutionResult> {
  const stagesWithoutBenchmark = ast.stages.filter(s => s.type !== 'benchmark')
  const request = buildRequest(stagesWithoutBenchmark, baseUrl)

  const latencies: number[] = []
  let errors = 0
  let totalBytes = 0
  const t0 = performance.now()

  // Execute in batches of `concurrency`
  for (let i = 0; i < runs; i += concurrency) {
    const batch = Math.min(concurrency, runs - i)
    const promises = Array.from({ length: batch }, async () => {
      try {
        const result = await executeSingleRequest(request, auth, serviceId, true, customHeaders, allowPrivate)
        latencies.push(result.steps[0]?.latencyMs ?? 0)
        totalBytes += JSON.stringify(result.data).length
      } catch {
        errors++
        latencies.push(0)
      }
    })
    await Promise.all(promises)
  }

  const totalDurationMs = performance.now() - t0
  const benchmark = computeBenchmarkResult(latencies, errors, totalBytes, totalDurationMs, runs, concurrency)

  return {
    data: benchmark,
    columns: Object.keys(benchmark),
    rows: [Object.values(benchmark)],
    totalCount: 1,
    status: 200,
    headers: {},
    requestHeaders: {},
    timing: { totalMs: Math.round(totalDurationMs), ttfbMs: 0, steps: [] },
    validation: { valid: true, issues: [] },
    cached: false,
    benchmark,
  }
}

/* ── Auth injection ────────────────────────────────────────────────── */

function applyAuth(auth: ApiServiceAuth | null, headers: Record<string, string>, url: URL): void {
  if (!auth || auth.scheme === 'none') return

  switch (auth.scheme) {
    case 'api_key':
      if (auth.apiKeyLocation === 'header') {
        headers[auth.apiKeyName ?? 'X-API-Key'] = auth.apiKeyValue ?? ''
      } else if (auth.apiKeyLocation === 'query') {
        url.searchParams.set(auth.apiKeyName ?? 'apikey', auth.apiKeyValue ?? '')
      } else if (auth.apiKeyLocation === 'cookie') {
        headers['Cookie'] = `${auth.apiKeyName ?? 'apikey'}=${auth.apiKeyValue ?? ''}`
      }
      break
    case 'bearer':
      headers['Authorization'] = `Bearer ${auth.bearerToken ?? ''}`
      break
    case 'basic': {
      const encoded = Buffer.from(`${auth.basicUsername ?? ''}:${auth.basicPassword ?? ''}`).toString('base64')
      headers['Authorization'] = `Basic ${encoded}`
      break
    }
    // OAuth2 would require token refresh flow — simplified for now
    case 'oauth2':
      if (auth.oauth2ClientSecret) {
        headers['Authorization'] = `Bearer ${auth.oauth2ClientSecret}`
      }
      break
  }
}

/* ── Data extraction helpers ───────────────────────────────────────── */

function extractRecords(data: unknown): Record<string, unknown>[] {
  if (Array.isArray(data)) return data.filter(isRecord)

  if (isRecord(data)) {
    // Common patterns: { results: [...] }, { data: [...] }, { items: [...] }, { records: [...] }
    for (const key of ['results', 'data', 'items', 'records', 'rows', 'entries', 'values']) {
      if (Array.isArray((data as Record<string, unknown>)[key])) {
        return ((data as Record<string, unknown>)[key] as unknown[]).filter(isRecord)
      }
    }
    return [data as Record<string, unknown>]
  }

  return []
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj
  for (const part of parts) {
    if (!isRecord(current)) return undefined
    current = (current as Record<string, unknown>)[part]
  }
  return current
}

function extractTabularData(
  data: unknown,
  stages: QueryStage[],
): { columns: string[]; rows: unknown[][]; totalCount: number } {
  let records = extractRecords(data)

  // Apply client-side where filtering (for chain results or APIs that don't support server-side filtering)
  const whereStage = stages.find(s => s.type === 'where')
  if (whereStage && whereStage.type === 'where') {
    records = records.filter(rec => {
      return whereStage.conditions.every(cond => matchCondition(rec, cond))
    })
  }

  // Apply group_by + aggregate
  const groupByStage = stages.find(s => s.type === 'group_by')
  const aggregateStage = stages.find(s => s.type === 'aggregate')

  if (groupByStage && groupByStage.type === 'group_by') {
    const groups = new Map<string, Record<string, unknown>[]>()
    for (const rec of records) {
      const key = groupByStage.fields.map(f => String(getNestedValue(rec, f) ?? '')).join('::')
      const group = groups.get(key) ?? []
      group.push(rec)
      groups.set(key, group)
    }

    const aggregated: Record<string, unknown>[] = []
    for (const [, groupRecords] of groups) {
      const row: Record<string, unknown> = {}
      // Include group by fields
      for (const f of groupByStage.fields) {
        row[f] = getNestedValue(groupRecords[0], f)
      }
      // Apply aggregations
      if (aggregateStage && aggregateStage.type === 'aggregate') {
        for (const agg of aggregateStage.aggregations) {
          row[agg.alias] = computeAggregation(groupRecords, agg)
        }
      }
      aggregated.push(row)
    }
    records = aggregated
  }

  // Apply order_by
  const orderByStage = stages.find(s => s.type === 'order_by')
  if (orderByStage && orderByStage.type === 'order_by') {
    const dir = orderByStage.direction === 'desc' ? -1 : 1
    records.sort((a, b) => {
      const aVal = getNestedValue(a, orderByStage.field)
      const bVal = getNestedValue(b, orderByStage.field)
      if (aVal === bVal) return 0
      if (aVal === undefined || aVal === null) return 1
      if (bVal === undefined || bVal === null) return -1
      return aVal < bVal ? -dir : dir
    })
  }

  const totalCount = records.length

  // Apply offset
  const offsetStage = stages.find(s => s.type === 'offset')
  if (offsetStage && offsetStage.type === 'offset') {
    records = records.slice(offsetStage.count)
  }

  // Apply limit
  const limitStage = stages.find(s => s.type === 'limit')
  if (limitStage && limitStage.type === 'limit') {
    records = records.slice(0, limitStage.count)
  }

  // Apply select (field projection)
  const selectStage = stages.find(s => s.type === 'select')
  let columns: string[]

  if (selectStage && selectStage.type === 'select') {
    columns = selectStage.fields
    records = records.map(rec => {
      const projected: Record<string, unknown> = {}
      for (const field of selectStage.fields) {
        projected[field] = getNestedValue(rec, field)
      }
      return projected
    })
  } else {
    // Infer columns from first record
    columns = records.length > 0 ? Object.keys(records[0]) : []
  }

  const rows = records.map(rec => columns.map(col => getNestedValue(rec, col)))

  return { columns, rows, totalCount }
}

function matchCondition(rec: Record<string, unknown>, cond: WhereCondition): boolean {
  const value = getNestedValue(rec, cond.field)
  const target = cond.value

  switch (cond.operator) {
    case '=': return value == target
    case '!=': return value != target
    case '>': return (value as number) > (target as number)
    case '<': return (value as number) < (target as number)
    case '>=': return (value as number) >= (target as number)
    case '<=': return (value as number) <= (target as number)
    case 'contains': return String(value).toLowerCase().includes(String(target).toLowerCase())
    case 'startswith': return String(value).toLowerCase().startsWith(String(target).toLowerCase())
    default: return true
  }
}

function computeAggregation(records: Record<string, unknown>[], agg: AggregationExpr): number {
  if (agg.fn === 'count') return records.length

  const values = records
    .map(r => getNestedValue(r, agg.field))
    .filter(v => typeof v === 'number') as number[]

  if (values.length === 0) return 0

  switch (agg.fn) {
    case 'sum': return values.reduce((a, b) => a + b, 0)
    case 'avg': return values.reduce((a, b) => a + b, 0) / values.length
    case 'min': return Math.min(...values)
    case 'max': return Math.max(...values)
    default: return 0
  }
}

/* ── Response validation ───────────────────────────────────────────── */

async function validateResponse(
  serviceId: string,
  data: unknown,
  stages: QueryStage[],
): Promise<ValidationResult> {
  const issues: ValidationIssue[] = []

  try {
    const service = getApiService(serviceId)
    if (!service) return { valid: true, issues: [] }

    const active = service.versions.find(v => v.version === service.activeVersion)
    if (!active) return { valid: true, issues: [] }

    const spec = loadSpec(serviceId, active.specFileName)
    if (!spec) return { valid: true, issues: [] }

    const registry = extractSchemaRegistry(spec as unknown as OpenApiSpec, serviceId, active.version)

    // Find the endpoint being queried
    const endpointStage = stages.find(s => s.type === 'endpoint')
    if (!endpointStage || endpointStage.type !== 'endpoint') return { valid: true, issues: [] }

    const endpoint = registry.endpoints.find(
      e => e.path === endpointStage.path && e.method === endpointStage.method,
    )
    if (!endpoint?.responseSchema) return { valid: true, issues: [] }

    // Validate response structure against schema
    const records = extractRecords(data)
    if (records.length > 0) {
      const sample = records[0]
      const expectedFields = endpoint.responseSchema.properties
      if (expectedFields) {
        for (const [fieldName, fieldInfo] of Object.entries(expectedFields)) {
          if (fieldInfo.required && !(fieldName in sample)) {
            issues.push({
              path: fieldName,
              expected: fieldInfo.type,
              actual: 'missing',
              severity: 'error',
            })
          }
        }

        for (const key of Object.keys(sample)) {
          if (!expectedFields[key]) {
            issues.push({
              path: key,
              expected: 'not in schema',
              actual: typeof sample[key],
              severity: 'warning',
            })
          }
        }
      }
    }
  } catch {
    // Validation is best-effort
  }

  return { valid: issues.filter(i => i.severity === 'error').length === 0, issues }
}
