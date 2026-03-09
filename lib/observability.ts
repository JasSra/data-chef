import 'server-only'

import { executeKQL, executeKQLApiKey, executeKQLWorkspace } from '@/lib/appinsights'
import { getConnector, getConnectorRuntimeConfig, getObservabilityCreds, type ObservabilityCredentials } from '@/lib/connectors'

export type ObservabilityConnectorType = 'appinsights' | 'azuremonitor' | 'elasticsearch' | 'datadog'

export interface ObservabilityQueryResult {
  columns: string[]
  rows: string[][]
  rowCount: number
  totalRows: number
  durationMs: number
  provider: ObservabilityConnectorType
  translatedQuery?: string
  error?: string
}

type KqlPlan = {
  source: string
  where?: string
  orderBy?: string
  limit?: number
  summarizeBy?: string
}

export function isObservabilityConnectorType(type: string): type is ObservabilityConnectorType {
  return type === 'appinsights' || type === 'azuremonitor' || type === 'elasticsearch' || type === 'datadog'
}

export function defaultObservabilityQuery(type: ObservabilityConnectorType): string {
  switch (type) {
    case 'appinsights':
    case 'azuremonitor':
      return 'requests\n| where timestamp > ago(24h)\n| summarize count() by bin(timestamp, 1h)\n| order by timestamp asc'
    case 'elasticsearch':
      return 'logs\n| where @timestamp > ago(24h)\n| limit 100'
    case 'datadog':
      return 'logs\n| where status:error\n| limit 100'
  }
}

function normaliseResult(
  provider: ObservabilityConnectorType,
  columns: string[],
  rows: string[][],
  durationMs: number,
  translatedQuery?: string,
): ObservabilityQueryResult {
  return {
    provider,
    columns,
    rows,
    rowCount: rows.length,
    totalRows: rows.length,
    durationMs,
    translatedQuery,
  }
}

function escapeElasticString(value: string): string {
  return value.replace(/"/g, '\\"')
}

function formatElasticLiteral(value: string): string {
  if (/^-?\d+(\.\d+)?$/.test(value)) return value
  return `"${escapeElasticString(value)}"`
}

function parseKql(kql: string): KqlPlan {
  const pipes = kql.split('|').map(part => part.trim()).filter(Boolean)
  if (pipes.length === 0) throw new Error('Empty query')

  const plan: KqlPlan = { source: pipes[0] }

  for (const raw of pipes.slice(1)) {
    if (/^where\s+/i.test(raw)) {
      if (plan.where) throw new Error('Only one where clause is supported')
      plan.where = raw.replace(/^where\s+/i, '').trim()
      continue
    }
    if (/^(limit|take)\s+\d+$/i.test(raw)) {
      plan.limit = Number(raw.replace(/^(limit|take)\s+/i, ''))
      continue
    }
    if (/^order\s+by\s+/i.test(raw)) {
      plan.orderBy = raw.replace(/^order\s+by\s+/i, '').trim()
      continue
    }
    if (/^summarize\s+count\(\)\s+by\s+/i.test(raw)) {
      plan.summarizeBy = raw.replace(/^summarize\s+count\(\)\s+by\s+/i, '').trim()
      continue
    }
    throw new Error(`Unsupported KQL clause for this provider: "${raw}"`)
  }

  return plan
}

function kqlWhereToElasticQuery(text?: string): string {
  if (!text) return '*'

  return text
    .replace(/\s+and\s+/gi, ' AND ')
    .replace(/\s+or\s+/gi, ' OR ')
    .replace(/==/g, ':')
    .replace(/=\s*/g, ':')
    .replace(/([@A-Za-z0-9_.-]+)\s*:\s*"([^"]+)"/g, (_m, field, value) => `${field}:${formatElasticLiteral(value)}`)
    .replace(/([@A-Za-z0-9_.-]+)\s*:\s*'([^']+)'/g, (_m, field, value) => `${field}:${formatElasticLiteral(value)}`)
    .replace(/([@A-Za-z0-9_.-]+)\s*:\s*([A-Za-z0-9_.:-]+)/g, (_m, field, value) => `${field}:${formatElasticLiteral(value)}`)
}

function translateKqlToElastic(plan: KqlPlan) {
  const index = plan.source || 'logs-*'
  const size = Math.max(1, Math.min(1000, plan.limit ?? 100))
  const queryString = kqlWhereToElasticQuery(plan.where)
  const sortField = plan.orderBy?.split(/\s+/)[0]
  const sortDir = /\bdesc\b/i.test(plan.orderBy ?? '') ? 'desc' : 'asc'

  const body: Record<string, unknown> = {
    size,
    query: { query_string: { query: queryString } },
  }

  if (sortField) {
    body.sort = [{ [sortField]: { order: sortDir } }]
  }

  if (plan.summarizeBy) {
    body.size = 0
    body.aggs = {
      grouped: {
        terms: {
          field: plan.summarizeBy,
          size,
        },
      },
    }
  }

  return { index, body, translated: JSON.stringify(body, null, 2) }
}

function translateKqlToDatadog(plan: KqlPlan, timespan?: string) {
  const query = kqlWhereToElasticQuery(plan.where)
    .replace(/\bAND\b/g, ' AND ')
    .replace(/\bOR\b/g, ' OR ')
  const sortDir = /\basc\b/i.test(plan.orderBy ?? '') ? 'asc' : 'desc'
  const limit = Math.max(1, Math.min(1000, plan.limit ?? 100))

  return {
    translated: JSON.stringify({
      filter: {
        from: timespan === 'PT1H' ? 'now-1h' : timespan === 'PT6H' ? 'now-6h' : timespan === 'P7D' ? 'now-7d' : timespan === 'P30D' ? 'now-30d' : 'now-24h',
        to: 'now',
        query: query === '*' ? '*' : query,
      },
      sort: sortDir === 'asc' ? 'timestamp' : '-timestamp',
      page: { limit },
    }, null, 2),
    query,
    limit,
    sortDir,
  }
}

function rowsFromObjects(items: Array<Record<string, unknown>>) {
  const first = items[0] ?? {}
  const columns = Object.keys(first)
  const rows = items.map(item => columns.map(column => {
    const value = item[column]
    if (value === null || value === undefined) return '∅'
    if (typeof value === 'object') return JSON.stringify(value)
    return String(value)
  }))
  return { columns, rows }
}

async function executeAzureQuery(creds: ObservabilityCredentials, kql: string, timespan?: string): Promise<ObservabilityQueryResult> {
  const started = performance.now()
  if (creds.provider !== 'appinsights' && creds.provider !== 'azuremonitor') {
    throw new Error(`Unsupported Azure observability provider: ${creds.provider}`)
  }

  const azureCreds = creds
  const azureResult = azureCreds.provider === 'appinsights' && azureCreds.authMode === 'api_key'
    ? await executeKQLApiKey(azureCreds.appId, azureCreds.apiKey, kql, timespan)
    : azureCreds.provider === 'appinsights' && azureCreds.mode === 'appinsights'
    ? await executeKQL(azureCreds.appId, azureCreds.tenantId ?? '', azureCreds.clientId ?? '', azureCreds.clientSecret ?? '', kql, timespan)
    : await executeKQLWorkspace(azureCreds.workspaceId ?? '', azureCreds.tenantId ?? '', azureCreds.clientId ?? '', azureCreds.clientSecret ?? '', kql, timespan)

  return {
    provider: azureCreds.provider,
    columns: azureResult.columns,
    rows: azureResult.rows,
    rowCount: azureResult.rowCount,
    totalRows: azureResult.rowCount,
    durationMs: azureResult.durationMs || Math.round(performance.now() - started),
    error: azureResult.error,
  }
}

async function executeElasticQuery(creds: Extract<ObservabilityCredentials, { provider: 'elasticsearch' }>, kql: string): Promise<ObservabilityQueryResult> {
  const started = performance.now()
  const plan = parseKql(kql)
  const translated = translateKqlToElastic(plan)
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  if (creds.authType === 'apikey') {
    headers.Authorization = `ApiKey ${creds.apiKey}`
  } else {
    headers.Authorization = `Basic ${Buffer.from(`${creds.username}:${creds.password}`).toString('base64')}`
  }

  const res = await fetch(`${creds.endpoint.replace(/\/$/, '')}/${encodeURIComponent(translated.index)}/_search`, {
    method: 'POST',
    headers,
    body: JSON.stringify(translated.body),
    signal: AbortSignal.timeout(20_000),
  })
  const durationMs = Math.round(performance.now() - started)
  if (!res.ok) {
    return { provider: 'elasticsearch', columns: [], rows: [], rowCount: 0, totalRows: 0, durationMs, translatedQuery: translated.translated, error: `Elastic API ${res.status}: ${(await res.text()).slice(0, 400)}` }
  }
  const payload = await res.json() as {
    hits?: { hits?: Array<{ _source?: Record<string, unknown>; fields?: Record<string, unknown> }> }
    aggregations?: { grouped?: { buckets?: Array<{ key: string; doc_count: number }> } }
  }

  if (payload.aggregations?.grouped?.buckets) {
    const items = payload.aggregations.grouped.buckets.map(bucket => ({
      [plan.summarizeBy ?? 'group']: bucket.key,
      count_: bucket.doc_count,
    }))
    const { columns, rows } = rowsFromObjects(items)
    return normaliseResult('elasticsearch', columns, rows, durationMs, translated.translated)
  }

  const hits = payload.hits?.hits ?? []
  const items = hits.map(hit => ({ ...(hit._source ?? {}), ...(hit.fields ?? {}) }))
  const { columns, rows } = rowsFromObjects(items)
  return normaliseResult('elasticsearch', columns, rows, durationMs, translated.translated)
}

async function executeDatadogQuery(
  creds: Extract<ObservabilityCredentials, { provider: 'datadog' }>,
  kql: string,
  timespan?: string,
): Promise<ObservabilityQueryResult> {
  const started = performance.now()
  const plan = parseKql(kql)
  const translated = translateKqlToDatadog(plan, timespan)
  const headers = {
    'Content-Type': 'application/json',
    'DD-API-KEY': creds.apiKey,
    'DD-APPLICATION-KEY': creds.applicationKey,
  }

  const res = await fetch(`https://api.${creds.site}/api/v2/logs/events/search`, {
    method: 'POST',
    headers,
    body: translated.translated,
    signal: AbortSignal.timeout(20_000),
  })
  const durationMs = Math.round(performance.now() - started)
  if (!res.ok) {
    return { provider: 'datadog', columns: [], rows: [], rowCount: 0, totalRows: 0, durationMs, translatedQuery: translated.translated, error: `Datadog API ${res.status}: ${(await res.text()).slice(0, 400)}` }
  }

  const payload = await res.json() as {
    data?: Array<{
      id?: string
      attributes?: Record<string, unknown>
    }>
  }

  const items: Array<Record<string, unknown>> = (payload.data ?? []).map(entry => ({
    id: entry.id,
    ...(entry.attributes ?? {}),
  }))

  if (plan.summarizeBy) {
    const summarizeBy = plan.summarizeBy
    const counts = new Map<string, number>()
    for (const item of items) {
      const key = String(item[summarizeBy] ?? '∅')
      counts.set(key, (counts.get(key) ?? 0) + 1)
    }
    const summarized = Array.from(counts.entries()).map(([key, count]) => ({ [summarizeBy]: key, count_: count }))
    const { columns, rows } = rowsFromObjects(summarized)
    return normaliseResult('datadog', columns, rows, durationMs, translated.translated)
  }

  const { columns, rows } = rowsFromObjects(items)
  return normaliseResult('datadog', columns, rows, durationMs, translated.translated)
}

export async function executeLiveObservabilityQuery(
  connectorId: string,
  kql: string,
  options: { timespan?: string } = {},
): Promise<ObservabilityQueryResult> {
  const connector = getConnector(connectorId)
  if (!connector) {
    return { provider: 'appinsights', columns: [], rows: [], rowCount: 0, totalRows: 0, durationMs: 0, error: `Unknown connector "${connectorId}"` }
  }
  if (!isObservabilityConnectorType(connector.type)) {
    return { provider: 'appinsights', columns: [], rows: [], rowCount: 0, totalRows: 0, durationMs: 0, error: `Connector "${connector.name}" is not an observability connector` }
  }

  const creds = getObservabilityCreds(connectorId)
  if (!creds) {
    return { provider: connector.type, columns: [], rows: [], rowCount: 0, totalRows: 0, durationMs: 0, error: `No observability credentials found for connector "${connector.name}"` }
  }

  if (creds.provider === 'appinsights' || creds.provider === 'azuremonitor') {
    return executeAzureQuery(creds, kql, options.timespan)
  }
  if (creds.provider === 'elasticsearch') {
    return executeElasticQuery(creds, kql)
  }
  return executeDatadogQuery(creds, kql, options.timespan)
}

export async function sampleObservabilityRows(
  connectorId: string,
  options: { rowLimit?: number; resource?: string; timespan?: string } = {},
): Promise<Record<string, unknown>[]> {
  const connector = getConnector(connectorId)
  const runtimeConfig = getConnectorRuntimeConfig(connectorId) ?? {}
  const query = String(options.resource ?? runtimeConfig.defaultQuery ?? defaultObservabilityQuery((connector?.type as ObservabilityConnectorType) ?? 'appinsights'))
  const result = await executeLiveObservabilityQuery(connectorId, query, { timespan: options.timespan ?? 'PT24H' })
  if (result.error) throw new Error(result.error)
  const limit = Math.max(1, options.rowLimit ?? 500)
  return result.rows.slice(0, limit).map(row => Object.fromEntries(result.columns.map((column, index) => [column, row[index] ?? null])))
}
