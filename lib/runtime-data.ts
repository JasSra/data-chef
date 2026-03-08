import { getCharacters, flattenCharacter } from '@/lib/rm-api'
import { getSyntheticEvents } from '@/lib/synthetic-data'
import { getDatasets, type DatasetRecord, type SchemaField } from '@/lib/datasets'
import {
  getConnectorRuntimeConfig,
  getConnector,
  getAppInsightsCreds,
  type ConnectorRuntimeConfig,
} from '@/lib/connectors'
import { executeKQL } from '@/lib/appinsights'

export type RuntimeRow = Record<string, unknown>

export interface HttpAuthOptions {
  auth?: string
  apiKeyHeader?: string
  apiKeyValue?: string
  bearerToken?: string
  basicUser?: string
  basicPass?: string
}

export interface RuntimeQueryResult {
  columns: string[]
  rows: string[][]
  rowCount: number
  totalRows: number
  pushedDown: boolean
}

export function inferType(value: unknown): string {
  if (value === null || value === undefined) return 'null'
  if (typeof value === 'boolean') return 'boolean'
  if (typeof value === 'number') return Number.isInteger(value) ? 'integer' : 'float'
  if (typeof value === 'string') {
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(value)) return 'timestamp'
    if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return 'date'
    return 'string'
  }
  if (Array.isArray(value)) return 'array'
  if (typeof value === 'object') return 'object'
  return 'string'
}

export function formatExample(value: unknown): string {
  if (value === null || value === undefined) return 'null'
  if (Array.isArray(value)) return `[${value.length} items]`
  if (typeof value === 'object') {
    const keys = Object.keys(value as object)
    return `{ ${keys.slice(0, 4).join(', ')}${keys.length > 4 ? ', …' : ''} }`
  }
  const s = String(value)
  return s.length > 50 ? `${s.slice(0, 47)}…` : s
}

export function extractArray(data: unknown): unknown[] {
  if (Array.isArray(data)) return data
  if (data && typeof data === 'object') {
    const preferred = ['data', 'items', 'results', 'records', 'rows', 'events', 'list']
    const obj = data as Record<string, unknown>
    for (const key of preferred) {
      if (Array.isArray(obj[key]) && (obj[key] as unknown[]).length > 0) return obj[key] as unknown[]
    }
    for (const val of Object.values(obj)) {
      if (Array.isArray(val) && val.length > 0) return val
    }
  }
  return []
}

export function inferSchema(records: RuntimeRow[], sample = 200): SchemaField[] {
  const recs = records.slice(0, sample)
  const count = recs.length
  if (count === 0) return []

  const typeFreq = new Map<string, Map<string, number>>()
  const nullFreq = new Map<string, number>()
  const examples = new Map<string, unknown>()

  for (const rec of recs) {
    for (const [key, val] of Object.entries(rec)) {
      if (!typeFreq.has(key)) {
        typeFreq.set(key, new Map())
        nullFreq.set(key, 0)
      }
      const t = inferType(val)
      if (t === 'null') {
        nullFreq.set(key, (nullFreq.get(key) ?? 0) + 1)
      } else {
        const m = typeFreq.get(key)!
        m.set(t, (m.get(t) ?? 0) + 1)
        if (!examples.has(key)) examples.set(key, val)
      }
    }
  }

  return Array.from(typeFreq.entries()).map(([field, types]) => {
    let bestType = 'string'
    let bestCount = 0
    for (const [t, c] of types) {
      if (c > bestCount) {
        bestType = t
        bestCount = c
      }
    }
    const nulls = nullFreq.get(field) ?? 0
    return {
      field,
      type: bestType,
      nullable: count > 0 && nulls / count > 0.05,
      example: formatExample(examples.get(field)),
    }
  })
}

export function buildAuthHeaders(auth: HttpAuthOptions, userAgent: string): Record<string, string> {
  const headers: Record<string, string> = {
    Accept: 'application/json, text/plain, */*',
    'User-Agent': userAgent,
  }
  if (auth.auth === 'apikey' && auth.apiKeyHeader && auth.apiKeyValue) {
    headers[auth.apiKeyHeader] = auth.apiKeyValue
  }
  if (auth.auth === 'bearer' && auth.bearerToken) {
    headers.Authorization = `Bearer ${auth.bearerToken}`
  }
  if (auth.auth === 'basic' && auth.basicUser) {
    headers.Authorization = `Basic ${Buffer.from(`${auth.basicUser}:${auth.basicPass ?? ''}`).toString('base64')}`
  }
  return headers
}

export async function fetchHttpRecords(
  url: string,
  auth: HttpAuthOptions = {},
  options: { rowLimit?: number; timeoutMs?: number; userAgent?: string } = {},
): Promise<RuntimeRow[]> {
  const res = await fetch(url, {
    headers: buildAuthHeaders(auth, options.userAgent ?? 'dataChef-runtime/0.1'),
    signal: AbortSignal.timeout(options.timeoutMs ?? 15_000),
  })
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} ${res.statusText}`)
  }

  const text = await res.text()
  let data: unknown
  try {
    data = JSON.parse(text)
  } catch {
    const lines = text.trim().split('\n').filter(Boolean)
    data = lines.slice(0, options.rowLimit ?? 500).map(line => JSON.parse(line))
  }

  const rows = extractArray(data)
  if (rows.length > 0) return rows.slice(0, options.rowLimit ?? rows.length) as RuntimeRow[]
  if (data && typeof data === 'object') return [data as RuntimeRow]
  return []
}

export async function loadRowsFromConnector(
  connectorId: string,
  options: { rowLimit?: number; resource?: string } = {},
): Promise<RuntimeRow[]> {
  const connector = getConnector(connectorId)
  if (!connector) throw new Error(`Unknown connector: "${connectorId}"`)

  const config = getConnectorRuntimeConfig(connectorId) ?? {}
  const rowLimit = options.rowLimit ?? 500

  if (connector.type === 'http') {
    const baseUrl = String(config.url ?? connector.endpoint ?? '')
    if (!baseUrl.startsWith('http')) {
      throw new Error(`Connector "${connector.name}" has no valid HTTP URL`)
    }

    let finalUrl = baseUrl
    if (options.resource) {
      finalUrl = options.resource.startsWith('http')
        ? options.resource
        : new URL(options.resource, baseUrl).toString()
    }

    return fetchHttpRecords(finalUrl, {
      auth: typeof config.auth === 'string' ? config.auth : undefined,
      apiKeyHeader: typeof config.apiKeyHeader === 'string' ? config.apiKeyHeader : undefined,
      apiKeyValue: typeof config.apiKeyValue === 'string' ? config.apiKeyValue : undefined,
      bearerToken: typeof config.bearerToken === 'string' ? config.bearerToken : undefined,
      basicUser: typeof config.basicUser === 'string' ? config.basicUser : undefined,
      basicPass: typeof config.basicPass === 'string' ? config.basicPass : undefined,
    }, { rowLimit, userAgent: 'dataChef-connector/0.1' })
  }

  if (connector.type === 'appinsights') {
    const creds = getAppInsightsCreds(connectorId)
    if (!creds) throw new Error(`Connector "${connector.name}" has no App Insights credentials`)
    const kql = options.resource?.trim() || 'requests | limit 100'
    const result = await executeKQL(
      creds.appId,
      creds.tenantId,
      creds.clientId,
      creds.clientSecret,
      kql,
      'PT24H',
    )
    if (result.error) throw new Error(result.error)
    return result.rows.slice(0, rowLimit).map(values => rowFromColumns(result.columns, values))
  }

  if (connector.type === 'postgresql') {
    return samplePostgresRowsFromConfig(config, options.resource, rowLimit)
  }

  throw new Error(`Connector type "${connector.type}" does not support runtime row loading yet`)
}

function rowFromColumns(columns: string[], values: string[]): RuntimeRow {
  const row: RuntimeRow = {}
  columns.forEach((col, i) => { row[col] = values[i] })
  return row
}

function formatPgValue(value: unknown): string {
  if (value === null || value === undefined) return '∅'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function tableOrSubquery(resource?: string, fallback?: unknown): string {
  const text = String(resource ?? fallback ?? '').trim()
  if (!text) throw new Error('No table or SQL query configured')
  if (/^\s*select\b/i.test(text)) return `(${text}) AS source_rows`
  return text
}

async function getPgClient(config: ConnectorRuntimeConfig) {
  const { Client } = await import('pg')
  const useConnectionString = Boolean(config.useConnectionString)
  const client = new Client(useConnectionString ? {
    connectionString: String(config.connectionString ?? ''),
    ssl: config.ssl ? { rejectUnauthorized: false } : undefined,
  } : {
    host: String(config.host ?? ''),
    port: Number(config.port ?? 5432),
    database: String(config.database ?? ''),
    user: String(config.dbUser ?? ''),
    password: String(config.dbPass ?? ''),
    ssl: config.ssl ? { rejectUnauthorized: false } : undefined,
  })
  await client.connect()
  return client
}

export async function samplePostgresRowsFromConfig(
  config: ConnectorRuntimeConfig,
  resource: string | undefined,
  rowLimit: number,
): Promise<RuntimeRow[]> {
  const client = await getPgClient(config)
  try {
    const source = tableOrSubquery(resource, config.tableOrQuery)
    const result = await client.query(`SELECT * FROM ${source} LIMIT ${Math.max(1, rowLimit)}`)
    return result.rows as RuntimeRow[]
  } finally {
    await client.end()
  }
}

export async function executeDatasetQuery(
  datasetId: string,
  sql: string,
): Promise<RuntimeQueryResult | null> {
  const ds = getDatasetRuntimeRecord(datasetId)
  if (!ds?.connectorId) return null

  const connector = getConnector(ds.connectorId)
  if (!connector) throw new Error(`Unknown connector: "${ds.connectorId}"`)
  if (connector.type !== 'postgresql') return null

  const config = getConnectorRuntimeConfig(ds.connectorId)
  if (!config) throw new Error(`Connector "${connector.name}" has no runtime config`)

  const client = await getPgClient(config)
  try {
    const result = await client.query(sql)
    const columns = result.fields.map((f: { name: string }) => f.name)
    const rows = result.rows.map((row: RuntimeRow) =>
      columns.map((col: string) => formatPgValue(row[col]))
    )
    return {
      columns,
      rows,
      rowCount: rows.length,
      totalRows: rows.length,
      pushedDown: true,
    }
  } finally {
    await client.end()
  }
}

export function getDatasetRuntimeRecord(datasetId: string): DatasetRecord | null {
  return getDatasets().find(d =>
    d.id === datasetId || d.name === datasetId || d.queryDataset === datasetId
  ) ?? null
}

export async function loadDatasetRows(
  datasetId: string,
  options: { rowLimit?: number } = {},
): Promise<RuntimeRow[]> {
  const rowLimit = options.rowLimit ?? 500

  switch (datasetId) {
    case 'rick-morty-characters':
      return (await getCharacters()).map(flattenCharacter).slice(0, rowLimit) as unknown as RuntimeRow[]
    case 'events':
      return getSyntheticEvents().slice(0, rowLimit) as unknown as RuntimeRow[]
  }

  const ds = getDatasetRuntimeRecord(datasetId)
  if (!ds) {
    throw new Error(`Unknown dataset: "${datasetId}"`)
  }

  if (ds.source === 'http' && ds.url?.startsWith('http')) {
    return fetchHttpRecords(ds.url, {}, { rowLimit, userAgent: 'dataChef-query/0.1' })
  }

  if (ds.source === 'conn' && ds.connectorId) {
    return loadRowsFromConnector(ds.connectorId, { rowLimit, resource: ds.resource })
  }

  if (ds.sampleRows?.length) {
    return ds.sampleRows.slice(0, rowLimit) as RuntimeRow[]
  }

  throw new Error(`Dataset "${ds.name}" has no live runtime loader`)
}

export async function loadDatasetRaw(
  datasetId: string,
  options: { rowLimit?: number } = {},
): Promise<unknown[]> {
  if (datasetId === 'rick-morty-characters') {
    return (await getCharacters()).slice(0, options.rowLimit ?? 500)
  }
  return loadDatasetRows(datasetId, options)
}

export function bytesForRows(rows: number): number {
  return rows * 400
}

export function parseSchemaText(schemaText: string): Map<string, string> {
  const expected = new Map<string, string>()
  for (const line of schemaText.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed) continue
    const match = trimmed.match(/^([A-Za-z0-9_.-]+)\s*:\s*([A-Za-z0-9_-]+)/)
    if (match) expected.set(match[1], match[2].toLowerCase())
  }
  return expected
}
