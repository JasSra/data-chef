import 'server-only'
import { getCharacters, flattenCharacter } from '@/lib/rm-api'
import { getSyntheticEvents } from '@/lib/synthetic-data'
import { getDatasets, type DatasetRecord, type SchemaField } from '@/lib/datasets'
import {
  getConnectorRuntimeConfig,
  getConnector,
  getAppInsightsCreds,
  getAzureB2CCreds,
  getAzureDevOpsCreds,
  getAzureEntraIdCreds,
  getObservabilityCreds,
  type ConnectorRuntimeConfig,
} from '@/lib/connectors'
import { executeKQL, executeKQLApiKey, executeKQLWorkspace } from '@/lib/appinsights'
import { fetchAzureB2CRows, fetchAzureEntraIdRows } from '@/lib/azure-graph'
import { fetchAzureDevOpsRows } from '@/lib/azure-devops'
import { fetchGitHubRows } from '@/lib/github'
import { isObservabilityConnectorType, sampleObservabilityRows } from '@/lib/observability'
import { sampleRedisRowsFromConfig } from '@/lib/redis'
import { sampleMssqlRowsFromConfig, executeMssqlQuery } from '@/lib/mssql'
import { browseRabbitQueue } from '@/lib/rabbitmq'
import { subscribeMqttTopic } from '@/lib/mqtt'
import { fetchRssFeed, feedItemsToRows } from '@/lib/rss'
import { collectWsFeed, wsMessagesToRows, type WsFeedConfig } from '@/lib/websocket-feed'
import type { SourceReference, SourceType } from '@/lib/datasets'

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

export interface RuntimeTableQueryResult extends RuntimeQueryResult {
  provider?: string
  kqlTranslated?: string
}

type SupportedRuntimeConnector =
  'http' | 'postgresql' | 'mysql' | 'mongodb' | 's3' | 'sftp' | 'bigquery' | 'redis' | 'mssql' | 'rabbitmq' | 'mqtt' | 'rss' | 'websocket' | 'appinsights' | 'azuremonitor' | 'elasticsearch' | 'datadog' | 'azureb2c' | 'azureentraid' | 'github' | 'azuredevops'

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

function parseCustomHeaders(text: string): Record<string, string> {
  const headers: Record<string, string> = {}
  for (const line of text.split('\n')) {
    const idx = line.indexOf(':')
    if (idx <= 0) continue
    const key = line.slice(0, idx).trim()
    const value = line.slice(idx + 1).trim()
    if (key && value) headers[key] = value
  }
  return headers
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

function parseCsv(text: string): RuntimeRow[] {
  const lines = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim().split('\n')
  if (lines.length < 2) return []

  function split(line: string): string[] {
    const cols: string[] = []
    let cur = ''
    let inQuote = false
    for (let i = 0; i < line.length; i++) {
      const ch = line[i]
      if (ch === '"') {
        if (inQuote && line[i + 1] === '"') {
          cur += '"'
          i++
        } else {
          inQuote = !inQuote
        }
      } else if (ch === ',' && !inQuote) {
        cols.push(cur)
        cur = ''
      } else {
        cur += ch
      }
    }
    cols.push(cur)
    return cols.map(v => v.trim())
  }

  const headers = split(lines[0])
  return lines.slice(1).filter(Boolean).map(line => {
    const values = split(line)
    return Object.fromEntries(headers.map((header, i) => [header, values[i] ?? '']))
  })
}

function matchSftpPattern(name: string, pattern: string): boolean {
  const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*')
  return new RegExp(`^${escaped}$`).test(name)
}

async function readBodyText(body: unknown): Promise<string> {
  if (typeof body === 'string') return body
  if (body && typeof body === 'object' && 'transformToString' in body && typeof body.transformToString === 'function') {
    return body.transformToString()
  }
  if (body && typeof body === 'object' && Symbol.asyncIterator in body) {
    const chunks: Uint8Array[] = []
    for await (const chunk of body as AsyncIterable<Uint8Array | string>) {
      chunks.push(typeof chunk === 'string' ? Buffer.from(chunk) : chunk)
    }
    return Buffer.concat(chunks.map(chunk => Buffer.from(chunk))).toString('utf8')
  }
  throw new Error('Unable to read response body')
}

function inferFormat(explicitFormat: unknown, nameHint = ''): 'json' | 'jsonl' | 'csv' | 'parquet' {
  const fmt = String(explicitFormat ?? '').toLowerCase()
  if (fmt === 'json' || fmt === 'jsonl' || fmt === 'csv' || fmt === 'parquet') return fmt
  const lower = nameHint.toLowerCase()
  if (lower.endsWith('.jsonl') || lower.endsWith('.ndjson')) return 'jsonl'
  if (lower.endsWith('.csv')) return 'csv'
  if (lower.endsWith('.parquet')) return 'parquet'
  return 'json'
}

function parseStructuredText(text: string, format: 'json' | 'jsonl' | 'csv' | 'parquet', rowLimit: number): RuntimeRow[] {
  if (format === 'parquet') {
    throw new Error('Parquet sampling is not implemented yet')
  }
  if (format === 'csv') {
    return parseCsv(text).slice(0, rowLimit)
  }
  if (format === 'jsonl') {
    return text.trim().split('\n').filter(Boolean).slice(0, rowLimit).map(line => JSON.parse(line) as RuntimeRow)
  }

  const data = JSON.parse(text)
  const rows = extractArray(data)
  if (rows.length > 0) return rows.slice(0, rowLimit) as RuntimeRow[]
  if (data && typeof data === 'object') return [data as RuntimeRow]
  return []
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

export async function sampleRowsFromRuntimeConfig(
  type: SupportedRuntimeConnector,
  config: ConnectorRuntimeConfig,
  options: { rowLimit?: number; resource?: string; timespan?: string } = {},
): Promise<RuntimeRow[]> {
  const rowLimit = options.rowLimit ?? 500

  function resolveHttpUrl(baseUrl: string, resource?: string): string {
    const trimmed = (resource ?? '').trim()
    if (!trimmed) return baseUrl
    if (/^https?:\/\//i.test(trimmed)) return trimmed
    try {
      return new URL(trimmed, baseUrl).toString()
    } catch {
      return baseUrl
    }
  }

  switch (type) {
    case 'http':
      return fetchHttpRecords(resolveHttpUrl(String(config.url ?? ''), options.resource), {
        auth: typeof config.auth === 'string' ? config.auth : undefined,
        apiKeyHeader: typeof config.apiKeyHeader === 'string' ? config.apiKeyHeader : undefined,
        apiKeyValue: typeof config.apiKeyValue === 'string' ? config.apiKeyValue : undefined,
        bearerToken: typeof config.bearerToken === 'string' ? config.bearerToken : undefined,
        basicUser: typeof config.basicUser === 'string' ? config.basicUser : undefined,
        basicPass: typeof config.basicPass === 'string' ? config.basicPass : undefined,
      }, { rowLimit, userAgent: 'dataChef-runtime/0.1' })
    case 'postgresql':
      return samplePostgresRowsFromConfig(config, options.resource, rowLimit)
    case 'mysql':
      return sampleMysqlRowsFromConfig(config, options.resource, rowLimit)
    case 'mongodb':
      return sampleMongoRowsFromConfig(config, options.resource, rowLimit)
    case 's3':
      return sampleS3RowsFromConfig(config, rowLimit)
    case 'sftp':
      return sampleSftpRowsFromConfig(config, rowLimit)
    case 'bigquery':
      return sampleBigQueryRowsFromConfig(config, options.resource, rowLimit)
    case 'redis':
      return sampleRedisRowsFromConfig(config, options.resource, rowLimit)
    case 'mssql':
      return sampleMssqlRowsFromConfig(config, options.resource, rowLimit) as Promise<RuntimeRow[]>
    case 'rabbitmq': {
      const queue = options.resource?.trim() || String(config.defaultCatalog ?? 'default')
      const res = await browseRabbitQueue(config, { queue, count: rowLimit })
      return res.rows.map(row => Object.fromEntries(res.columns.map((col, i) => [col, row[i]])))
    }
    case 'mqtt': {
      const res = await subscribeMqttTopic(config, { topic: options.resource || String(config.defaultTopic ?? '#'), limit: rowLimit })
      return res.rows.map(row => Object.fromEntries(res.columns.map((col, i) => [col, row[i]])))
    }
    case 'rss': {
      const customHeaders = parseCustomHeaders(String(config.customHeaders ?? ''))
      const result = await fetchRssFeed(
        String(config.url ?? ''),
        {
          auth: String(config.auth ?? 'none') as 'none' | 'bearer' | 'apikey' | 'basic' | undefined,
          bearerToken: String(config.bearerToken ?? ''),
          apiKeyHeader: String(config.apiKeyHeader ?? ''),
          apiKeyValue: String(config.apiKeyValue ?? ''),
          basicUser: String(config.basicUser ?? ''),
          basicPass: String(config.basicPass ?? ''),
        },
        { rowLimit, customHeaders },
      )
      if (result.error) throw new Error(result.error)
      return feedItemsToRows(result.items).slice(0, rowLimit)
    }
    case 'websocket': {
      const customHeaders = parseCustomHeaders(String(config.customHeaders ?? ''))
      const wsCfg: WsFeedConfig = {
        url: String(config.url ?? ''),
        auth: (String(config.auth ?? 'none')) as WsFeedConfig['auth'],
        bearerToken: String(config.bearerToken ?? ''),
        apiKeyHeader: String(config.apiKeyHeader ?? ''),
        apiKeyValue: String(config.apiKeyValue ?? ''),
        basicUser: String(config.basicUser ?? ''),
        basicPass: String(config.basicPass ?? ''),
        customHeaders,
        subscribeMessage: String(config.subscribeMessage ?? ''),
        windowMs: Number(config.windowMs ?? 5000),
      }
      const result = await collectWsFeed(wsCfg, { limit: rowLimit })
      if (result.error && !result.connected) throw new Error(result.error)
      return wsMessagesToRows(result.messages).slice(0, rowLimit)
    }
    case 'appinsights': {
      const connectorId = String(config.connectorId ?? '')
      if (!connectorId) throw new Error('App Insights connectorId is required')
      const creds = getAppInsightsCreds(connectorId)
      if (!creds) throw new Error('App Insights credentials not found')
      const kql = options.resource?.trim() || 'requests | limit 100'
      const result = creds.authMode === 'api_key'
        ? await executeKQLApiKey(
            creds.appId,
            creds.apiKey,
            kql,
            options.timespan ?? 'PT24H',
          )
        : creds.mode === 'workspace'
        ? await executeKQLWorkspace(
            creds.workspaceId ?? '',
            creds.tenantId ?? '',
            creds.clientId ?? '',
            creds.clientSecret ?? '',
            kql,
            options.timespan ?? 'PT24H',
          )
        : await executeKQL(
            creds.appId,
            creds.tenantId ?? '',
            creds.clientId ?? '',
            creds.clientSecret ?? '',
            kql,
            options.timespan ?? 'PT24H',
          )
      if (result.error) throw new Error(result.error)
      return result.rows.slice(0, rowLimit).map(values => rowFromColumns(result.columns, values))
    }
    case 'azuremonitor':
    case 'elasticsearch':
    case 'datadog': {
      const connectorId = String(config.connectorId ?? '')
      if (!connectorId) throw new Error(`${type} connectorId is required`)
      const creds = getObservabilityCreds(connectorId)
      if (!creds) throw new Error(`${type} credentials not found`)
      return sampleObservabilityRows(connectorId, {
        rowLimit,
        resource: options.resource,
        timespan: options.timespan ?? 'PT24H',
      })
    }
    case 'azureb2c': {
      const connectorId = String(config.connectorId ?? '')
      if (!connectorId) throw new Error('Azure AD B2C connectorId is required')
      const creds = getAzureB2CCreds(connectorId)
      if (!creds) throw new Error('Azure AD B2C credentials not found')
      const result = await fetchAzureB2CRows(creds, options.resource ?? String(config.resource ?? 'users'), {
        rowLimit,
      })
      return result.rows
    }
    case 'azureentraid': {
      const connectorId = String(config.connectorId ?? '')
      if (!connectorId) throw new Error('Azure Entra ID connectorId is required')
      const creds = getAzureEntraIdCreds(connectorId)
      if (!creds) throw new Error('Azure Entra ID credentials not found')
      const result = await fetchAzureEntraIdRows(creds, options.resource ?? String(config.resource ?? 'users'), {
        rowLimit,
      })
      return result.rows
    }
    case 'github': {
      const connectorId = String(config.connectorId ?? '')
      if (!connectorId) throw new Error('GitHub connectorId is required')
      return fetchGitHubRows(connectorId, options.resource ?? String(config.defaultResource ?? 'repos'), {
        rowLimit,
      })
    }
    case 'azuredevops': {
      const connectorId = String(config.connectorId ?? '')
      if (!connectorId) throw new Error('Azure DevOps connectorId is required')
      const creds = getAzureDevOpsCreds(connectorId)
      if (!creds) throw new Error('Azure DevOps credentials not found')
      return fetchAzureDevOpsRows(connectorId, options.resource ?? String(config.defaultResource ?? 'repositories'), {
        rowLimit,
      })
    }
  }

  throw new Error(`Unsupported connector runtime type: ${String(type)}`)
}

export async function loadRowsFromConnector(
  connectorId: string,
  options: { rowLimit?: number; resource?: string; timespan?: string } = {},
): Promise<RuntimeRow[]> {
  const connector = getConnector(connectorId)
  if (!connector) throw new Error(`Unknown connector: "${connectorId}"`)

  const config = getConnectorRuntimeConfig(connectorId) ?? {}
  const rowLimit = options.rowLimit ?? 500

  if (connector.type === 'appinsights') {
    return sampleRowsFromRuntimeConfig('appinsights', { ...config, connectorId }, options)
  }

  if (isObservabilityConnectorType(connector.type)) {
    return sampleRowsFromRuntimeConfig(connector.type, { ...config, connectorId }, options)
  }

  if (connector.type === 'azureb2c') {
    return sampleRowsFromRuntimeConfig('azureb2c', { ...config, connectorId }, options)
  }

  if (connector.type === 'redis') {
    return sampleRowsFromRuntimeConfig('redis', config, options)
  }

  if (connector.type === 'mssql') {
    return sampleMssqlRowsFromConfig(config, options.resource, options.rowLimit ?? 500) as Promise<RuntimeRow[]>
  }

  if (connector.type === 'rabbitmq') {
    const queue = options.resource?.trim() || String(config.defaultCatalog ?? 'default')
    const res = await browseRabbitQueue(config, { queue, count: options.rowLimit ?? 50 })
    return res.rows.map(row => Object.fromEntries(res.columns.map((col, i) => [col, row[i]])))
  }

  if (connector.type === 'mqtt') {
    const res = await subscribeMqttTopic(config, { topic: options.resource || String(config.defaultTopic ?? '#'), limit: options.rowLimit ?? 100 })
    return res.rows.map(row => Object.fromEntries(res.columns.map((col, i) => [col, row[i]])))
  }

  if (connector.type === 'rss') {
    return sampleRowsFromRuntimeConfig('rss', config, options)
  }

  if (connector.type === 'websocket') {
    return sampleRowsFromRuntimeConfig('websocket', config, options)
  }

  if (connector.type === 'azureentraid') {
    return sampleRowsFromRuntimeConfig('azureentraid', { ...config, connectorId }, options)
  }

  if (connector.type === 'github') {
    return sampleRowsFromRuntimeConfig('github', { ...config, connectorId }, options)
  }

  if (connector.type === 'azuredevops') {
    return sampleRowsFromRuntimeConfig('azuredevops', { ...config, connectorId }, options)
  }

  return sampleRowsFromRuntimeConfig(connector.type as SupportedRuntimeConnector, config, {
    ...options,
    rowLimit,
  })
}

export async function loadSourceRows(
  source: SourceReference,
  options: { rowLimit?: number; timespan?: string } = {},
): Promise<RuntimeRow[]> {
  if (source.sourceType === 'dataset') {
    return loadDatasetRows(source.sourceId, options)
  }

  return loadRowsFromConnector(source.sourceId, {
    rowLimit: options.rowLimit,
    resource: source.resource,
    timespan: options.timespan,
  })
}

export async function loadSourceRaw(
  source: SourceReference,
  options: { rowLimit?: number } = {},
): Promise<unknown[]> {
  if (source.sourceType === 'dataset') {
    return loadDatasetRaw(source.sourceId, options)
  }
  return loadSourceRows(source, options)
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

async function sampleMysqlRowsFromConfig(
  config: ConnectorRuntimeConfig,
  resource: string | undefined,
  rowLimit: number,
): Promise<RuntimeRow[]> {
  const mysql = await import('mysql2/promise')
  const useConnectionString = Boolean(config.useConnectionString)
  const connection = useConnectionString
    ? await mysql.createConnection(String(config.connectionString ?? ''))
    : await mysql.createConnection({
        host: String(config.host ?? ''),
        port: Number(config.port ?? 3306),
        database: String(config.database ?? ''),
        user: String(config.dbUser ?? ''),
        password: String(config.dbPass ?? ''),
        ssl: config.ssl ? {} : undefined,
      })
  try {
    const source = tableOrSubquery(resource, config.tableOrQuery)
    const [rows] = await connection.query(`SELECT * FROM ${source} LIMIT ${Math.max(1, rowLimit)}`)
    return rows as RuntimeRow[]
  } finally {
    await connection.end()
  }
}

async function sampleMongoRowsFromConfig(
  config: ConnectorRuntimeConfig,
  resource: string | undefined,
  rowLimit: number,
): Promise<RuntimeRow[]> {
  const { MongoClient } = await import('mongodb')
  const useConnectionString = Boolean(config.useConnectionString)
  const uri = useConnectionString
    ? String(config.connectionString ?? '')
    : `mongodb://${encodeURIComponent(String(config.dbUser ?? ''))}:${encodeURIComponent(String(config.dbPass ?? ''))}@${String(config.host ?? 'localhost')}:${Number(config.port ?? 27017)}`
  const client = new MongoClient(uri)
  await client.connect()
  try {
    const db = client.db(String(config.database ?? ''))
    const collectionName = String(resource ?? config.collection ?? '').trim()
    if (!collectionName) throw new Error('No collection configured')
    const filter = String(config.filter ?? '').trim()
    const parsedFilter = filter ? JSON.parse(filter) : {}
    const rows = await db.collection(collectionName).find(parsedFilter).limit(Math.max(1, rowLimit)).toArray()
    return rows.map(doc => {
      const row: RuntimeRow = {}
      for (const [key, value] of Object.entries(doc)) row[key] = value
      return row
    })
  } finally {
    await client.close()
  }
}

async function sampleS3RowsFromConfig(
  config: ConnectorRuntimeConfig,
  rowLimit: number,
): Promise<RuntimeRow[]> {
  const { S3Client, ListObjectsV2Command, GetObjectCommand } = await import('@aws-sdk/client-s3')
  const client = new S3Client({
    region: String(config.region ?? 'us-east-1'),
    endpoint: config.endpoint ? String(config.endpoint) : undefined,
    forcePathStyle: Boolean(config.endpoint),
    credentials: config.accessKeyId && config.secretAccessKey ? {
      accessKeyId: String(config.accessKeyId),
      secretAccessKey: String(config.secretAccessKey),
    } : undefined,
  })

  const list = await client.send(new ListObjectsV2Command({
    Bucket: String(config.bucket ?? ''),
    Prefix: String(config.prefix ?? ''),
    MaxKeys: 5,
  }))
  const object = list.Contents?.find(item => item.Key) ?? list.Contents?.[0]
  if (!object?.Key) throw new Error('No objects found in bucket/prefix')

  const format = inferFormat(config.format, object.Key)
  const data = await client.send(new GetObjectCommand({
    Bucket: String(config.bucket ?? ''),
    Key: object.Key,
  }))
  const text = await readBodyText(data.Body)
  return parseStructuredText(text, format, rowLimit)
}

async function sampleSftpRowsFromConfig(
  config: ConnectorRuntimeConfig,
  rowLimit: number,
): Promise<RuntimeRow[]> {
  if (String(config.protocol ?? 'sftp') === 'ftp') {
    throw new Error('FTP runtime probing is not supported; use SFTP')
  }
  const { default: SftpClient } = await import('ssh2-sftp-client')
  const client = new SftpClient()
  await client.connect({
    host: String(config.host ?? ''),
    port: Number(config.port ?? 22),
    username: String(config.sftpUser ?? ''),
    password: config.authType === 'password' ? String(config.password ?? '') : undefined,
    privateKey: config.authType === 'privatekey' ? String(config.privateKey ?? '') : undefined,
  })
  try {
    const remotePath = String(config.path ?? '/')
    const entries = await client.list(remotePath)
    const pattern = String(config.filePattern ?? '*')
    const file = entries.find((entry: { name: string }) => matchSftpPattern(entry.name, pattern))
    if (!file) throw new Error('No files found matching the configured pattern')
    const fullPath = `${remotePath.replace(/\/$/, '')}/${file.name}`
    const data = await client.get(fullPath)
    const text = Buffer.isBuffer(data) ? data.toString('utf8') : String(data)
    return parseStructuredText(text, inferFormat(config.format, file.name), rowLimit)
  } finally {
    await client.end()
  }
}

async function sampleBigQueryRowsFromConfig(
  config: ConnectorRuntimeConfig,
  resource: string | undefined,
  rowLimit: number,
): Promise<RuntimeRow[]> {
  const { BigQuery } = await import('@google-cloud/bigquery')
  const credentials = JSON.parse(String(config.serviceAccountJson ?? '{}'))
  const bigquery = new BigQuery({
    projectId: String(config.project ?? credentials.project_id ?? ''),
    credentials,
  })
  const text = String(resource ?? config.tableOrSql ?? '').trim()
  if (!text) throw new Error('No table or SQL query configured')
  const query = /^\s*select\b/i.test(text)
    ? `${text} LIMIT ${Math.max(1, rowLimit)}`
    : `SELECT * FROM \`${String(config.project ?? credentials.project_id ?? '')}.${String(config.dataset ?? '')}.${text}\` LIMIT ${Math.max(1, rowLimit)}`
  const [rows] = await bigquery.query({ query, useLegacySql: false })
  return rows as RuntimeRow[]
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

export async function executeSourceQuery(
  source: SourceReference,
  sql: string,
): Promise<RuntimeQueryResult | null> {
  if (source.sourceType === 'dataset') {
    return executeDatasetQuery(source.sourceId, sql)
  }

  const connector = getConnector(source.sourceId)
  if (!connector) throw new Error(`Unknown connector: "${source.sourceId}"`)
  if (connector.type !== 'postgresql' && connector.type !== 'mysql' && connector.type !== 'mssql') return null

  const config = getConnectorRuntimeConfig(source.sourceId)
  if (!config) throw new Error(`Connector "${connector.name}" has no runtime config`)

  if (connector.type === 'mssql') {
    const result = await executeMssqlQuery(config, { query: sql, rowLimit: 500 })
    const rows = result.rows.map((row: string[]) => result.columns.map((_col: string, i: number) => row[i]))
    return {
      columns: result.columns,
      rows,
      rowCount: result.rowCount,
      totalRows: result.totalRows,
      pushedDown: true,
    }
  }

  if (connector.type === 'mysql') {
    const mysql = await import('mysql2/promise')
    const connection = Boolean(config.connectionMode === 'connectionString')
      ? await mysql.createConnection(String(config.connectionString ?? ''))
      : await mysql.createConnection({
          host: String(config.host ?? ''),
          port: Number(config.port ?? 3306),
          database: String(config.database ?? ''),
          user: String(config.dbUser ?? ''),
          password: String(config.dbPass ?? ''),
        })
    try {
      const [rows] = await connection.query(sql)
      const arr = rows as Array<Record<string, unknown>>
      const columns = arr.length > 0 ? Object.keys(arr[0]) : []
      return {
        columns,
        rows: arr.map(row => columns.map(col => String(row[col] ?? ''))),
        rowCount: arr.length,
        totalRows: arr.length,
        pushedDown: true,
      }
    } finally {
      await connection.end()
    }
  }

  const client = await getPgClient(config)
  try {
    const queryText = source.resource?.trim()
      ? `WITH source_rows AS (${source.resource}) ${sql}`
      : sql
    const result = await client.query(queryText)
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

export function bytesForSource(sourceType: SourceType, rowCount: number): number {
  if (sourceType === 'dataset') return bytesForRows(rowCount)
  return Math.max(bytesForRows(rowCount), rowCount * 200)
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
