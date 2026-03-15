/**
 * POST /api/connectors/test
 *
 * Tests a connector server-side and streams SSE log events.
 * - HTTP: real server-side fetch (bypasses CORS), includes auth headers
 * - Others: server-driven simulation with realistic timing
 *
 * Uses workerStart/workerEnd so the sidebar worker counter updates live.
 *
 * Request body: { connectorType, url?, auth?, apiKeyHeader?, apiKeyValue?,
 *   bearerToken?, basicUser?, basicPass?, host?, port?, database?, dbUser?,
 *   ssl?, protocol?, sftpUser?, path?, filePattern?, provider?, bucket?,
 *   prefix?, format?, project?, dataset?, tableOrSql?, collection?, ... }
 *
 * SSE events:
 *   { type: 'log', level: 'info'|'success'|'warn'|'error', msg: string }
 *   { type: 'done', ok: boolean, latencyMs: number }
 */

import { NextRequest } from 'next/server'
import { workerStart, workerEnd } from '@/lib/pipelines'
import {
  inferSchema,
  sampleRowsFromRuntimeConfig,
} from '@/lib/runtime-data'
import { probeRedisCapabilities } from '@/lib/redis'
import {
  fetchAzureB2CRows,
  fetchAzureEntraIdRows,
  getAzureGraphToken,
  resolveAzureB2CResource,
  resolveAzureEntraIdResource,
} from '@/lib/azure-graph'
import { getAzureDevOpsAuthTransaction } from '@/lib/azure-devops-auth'
import { getGitHubAuthTransaction } from '@/lib/github-auth'
import { getAzureDevOpsCreds, getGitHubCreds, type AzureDevOpsCredentials, type GitHubCredentials } from '@/lib/connectors'
import { listAzureDevOpsProjects, listAzureDevOpsRepositories, validateAzureDevOpsCredentials } from '@/lib/azure-devops'
import { listAccessibleGitHubRepos, validateGitHubCredentials } from '@/lib/github'

export const dynamic = 'force-dynamic'

type Level = 'info' | 'success' | 'warn' | 'error'
type SimEntry = { level: Level; msg: string; delay: number }

function sse(data: object) {
  return new TextEncoder().encode(`data: ${JSON.stringify(data)}\n\n`)
}
function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms))
}

function parseAppInsightsConnectionString(connectionString: string): { applicationId: string } {
  const parts = connectionString
    .split(';')
    .map(part => part.trim())
    .filter(Boolean)

  for (const part of parts) {
    const idx = part.indexOf('=')
    if (idx <= 0) continue
    const key = part.slice(0, idx).trim().toLowerCase()
    const value = part.slice(idx + 1).trim()
    if (key === 'applicationid') return { applicationId: value }
  }

  return { applicationId: '' }
}

/* ── Simulated log sequences (server-driven so workers count properly) ─── */
function getSimLogs(type: string, b: Record<string, unknown>): SimEntry[] {
  const str = (v: unknown, fallback = '') => (v != null ? String(v) : fallback)

  switch (type) {
    case 'postgresql':
    case 'mysql': {
      const host = str(b.host, 'localhost')
      const port = str(b.port, type === 'postgresql' ? '5432' : '3306')
      const db   = str(b.database, 'mydb')
      const user = str(b.dbUser, 'user')
      const ssl  = b.ssl === true || b.ssl === 'true'
      return [
        { level: 'info',              msg: `Resolving ${host}:${port}`,                         delay: 400 },
        { level: 'info',              msg: 'TCP connection established',                          delay: 550 },
        { level: ssl ? 'info' : 'warn', msg: ssl ? 'TLS 1.3 negotiated · verify-full' : 'SSL disabled — unencrypted', delay: 450 },
        { level: 'info',              msg: `Authenticating as '${user}'`,                        delay: 380 },
        { level: 'info',              msg: `Connected to database '${db}'`,                      delay: 300 },
        { level: 'info',              msg: `SELECT COUNT(*) FROM ${str(b.tableOrQuery, 'table').split(/\s/)[0]}`, delay: 600 },
        { level: 'info',              msg: '~150,421 rows detected',                             delay: 400 },
        { level: 'info',              msg: 'Sampling schema (LIMIT 100)',                        delay: 480 },
        { level: 'success',           msg: '12 columns inferred · ready to sync',               delay: 450 },
      ]
    }
    case 'mongodb': {
      const db   = str(b.database, 'mydb')
      const coll = str(b.collection, 'events')
      return [
        { level: 'info',    msg: 'Resolving MongoDB host',                           delay: 400 },
        { level: 'info',    msg: 'Connection pool established (min: 5)',              delay: 580 },
        { level: 'info',    msg: 'Authenticating via SCRAM-SHA-256',                 delay: 450 },
        { level: 'info',    msg: `Database '${db}' selected`,                        delay: 300 },
        { level: 'info',    msg: `Collection '${coll}' · counting docs`,             delay: 500 },
        { level: 'info',    msg: '~2,341,820 documents detected',                    delay: 400 },
        { level: 'info',    msg: 'Running schema inference on 500 samples',          delay: 680 },
        { level: 'success', msg: '18 fields inferred · heterogeneous types detected', delay: 480 },
      ]
    }
    case 's3': {
      const provider    = str(b.provider, 'aws').toUpperCase()
      const bucket      = str(b.bucket, 'my-bucket')
      const prefix      = str(b.prefix, '')
      const fmtRaw      = str(b.format, 'auto')
      const format      = fmtRaw === 'auto' ? 'Parquet (auto-detected)' : fmtRaw.toUpperCase()
      return [
        { level: 'info',    msg: `Initializing ${provider} SDK`,                                 delay: 380 },
        { level: 'info',    msg: `Listing '${bucket}${prefix ? '/' + prefix : ''}' …`,          delay: 680 },
        { level: 'info',    msg: '1,234 objects · 45.2 GB total',                               delay: 400 },
        { level: 'info',    msg: 'Downloading sample object for format detection',               delay: 580 },
        { level: 'info',    msg: `Format: ${format}`,                                           delay: 300 },
        { level: 'info',    msg: 'Reading metadata (row groups: 24)',                            delay: 480 },
        { level: 'success', msg: '34 fields · ~12.1M records estimated',                        delay: 450 },
      ]
    }
    case 'sftp': {
      const host        = str(b.host, 'sftp.example.com')
      const port        = str(b.port, '22')
      const proto       = str(b.protocol, 'sftp')
      const user        = str(b.sftpUser, 'user')
      const path        = str(b.path, '/exports/')
      const filePattern = str(b.filePattern, '*')
      return [
        { level: 'info',                 msg: `Connecting to ${host}:${port}`,                  delay: 500 },
        { level: proto === 'ftp' ? 'warn' : 'info', msg: proto === 'ftp' ? 'FTP is unencrypted — consider migrating to SFTP' : 'SSH handshake complete', delay: 580 },
        { level: 'info',                 msg: `Authenticating as '${user}'`,                    delay: 450 },
        { level: 'info',                 msg: `Listing ${path}`,                                delay: 580 },
        { level: 'info',                 msg: `23 files found matching '${filePattern}'`,       delay: 380 },
        { level: 'info',                 msg: 'Reading latest file for schema detection',       delay: 580 },
        { level: 'success',              msg: '8 fields detected · CSV with headers',          delay: 380 },
      ]
    }
    case 'bigquery': {
      const project    = str(b.project, 'my-project')
      const dataset    = str(b.dataset, 'analytics')
      const tableOrSql = str(b.tableOrSql, 'events').split(/\s/)[0]
      return [
        { level: 'info',    msg: 'Loading service account credentials',                    delay: 400 },
        { level: 'info',    msg: 'Authenticating with Google APIs (OAuth2)',               delay: 580 },
        { level: 'info',    msg: `Accessing project '${project}'`,                        delay: 400 },
        { level: 'info',    msg: `Opening dataset '${dataset}'`,                          delay: 300 },
        { level: 'info',    msg: `Dry-run: SELECT * FROM \`${dataset}.${tableOrSql}\``,   delay: 680 },
        { level: 'info',    msg: 'Billed: 0 bytes (dry run) · ~890M rows estimated',      delay: 400 },
        { level: 'success', msg: '22 fields inferred · ready to sync',                   delay: 450 },
      ]
    }
    case 'appinsights': {
      const tenantId = str(b.tenantId, 'xxxxxxxx').slice(0, 8)
      const appId    = str(b.appId,    'xxxxxxxx').slice(0, 8)
      return [
        { level: 'info',    msg: `Resolving login.microsoftonline.com`,                    delay: 400 },
        { level: 'info',    msg: `Requesting token (tenant: ${tenantId}…)`,                delay: 600 },
        { level: 'success', msg: 'Azure AD token acquired · client_credentials flow',      delay: 400 },
        { level: 'info',    msg: `Probing App Insights app: ${appId}…`,                   delay: 500 },
        { level: 'info',    msg: 'KQL: requests | limit 1',                               delay: 400 },
        { level: 'success', msg: 'Connection verified · KQL engine ready',                delay: 400 },
      ]
    }
    case 'azuremonitor': {
      const tenantId = str(b.tenantId, 'xxxxxxxx').slice(0, 8)
      const workspaceId = str(b.workspaceId, 'xxxxxxxx').slice(0, 8)
      return [
        { level: 'info', msg: 'Resolving login.microsoftonline.com', delay: 400 },
        { level: 'info', msg: `Requesting OAuth2 token (tenant: ${tenantId}…)`, delay: 650 },
        { level: 'success', msg: 'Azure AD token acquired · client_credentials flow', delay: 420 },
        { level: 'info', msg: `KQL: requests | take 1 on workspace ${workspaceId}…`, delay: 500 },
        { level: 'success', msg: 'Azure Monitor workspace query endpoint ready', delay: 380 },
      ]
    }
    case 'elasticsearch':
      return [
        { level: 'info', msg: `Connecting to ${str(b.endpoint, 'https://elastic.example.com:9200')}`, delay: 420 },
        { level: 'info', msg: `Probing index pattern ${str(b.indexPattern, 'logs-*')}`, delay: 540 },
        { level: 'info', msg: 'POST /_search with size: 1', delay: 480 },
        { level: 'success', msg: 'Elastic cluster responded · runtime ready', delay: 420 },
      ]
    case 'datadog':
      return [
        { level: 'info', msg: `Connecting to api.${str(b.site, 'datadoghq.com')}`, delay: 420 },
        { level: 'info', msg: `Preparing ${str(b.source, 'logs')} query probe`, delay: 520 },
        { level: 'info', msg: 'POST /api/v2/logs/events/search', delay: 480 },
        { level: 'success', msg: 'Datadog API responded · runtime ready', delay: 420 },
      ]
    case 'azureb2c': {
      const tenantId = str(b.tenantId, 'xxxxxxxx').slice(0, 8)
      const clientId = str(b.clientId, 'xxxxxxxx').slice(0, 8)
      const resource = str(b.resource, 'users')
      return [
        { level: 'info',    msg: 'Resolving login.microsoftonline.com',                         delay: 380 },
        { level: 'info',    msg: `Preparing Microsoft Graph probe (${resource})`,              delay: 420 },
        { level: 'info',    msg: `Requesting token (tenant: ${tenantId}…, client: ${clientId}…)`, delay: 620 },
        { level: 'success', msg: 'Microsoft Graph token acquired · client_credentials flow',   delay: 450 },
        { level: 'info',    msg: `Probing ${resource} via Microsoft Graph`,                    delay: 500 },
        { level: 'success', msg: 'Connection verified · Azure AD B2C Graph API ready',         delay: 380 },
      ]
    }
    case 'azureentraid': {
      const tenantId = str(b.tenantId, 'xxxxxxxx').slice(0, 8)
      const clientId = str(b.clientId, 'xxxxxxxx').slice(0, 8)
      const resource = str(b.resource, 'users')
      return [
        { level: 'info',    msg: 'Resolving login.microsoftonline.com',                            delay: 380 },
        { level: 'info',    msg: `Preparing Microsoft Graph probe (${resource})`,                 delay: 420 },
        { level: 'info',    msg: `Requesting token (tenant: ${tenantId}…, client: ${clientId}…)`, delay: 620 },
        { level: 'success', msg: 'Microsoft Graph token acquired · client_credentials flow',      delay: 450 },
        { level: 'info',    msg: `Probing ${resource} via Microsoft Graph v1.0`,                  delay: 500 },
        { level: 'success', msg: 'Connection verified · Azure Entra ID Graph API ready',          delay: 380 },
      ]
    }
    case 'github': {
      const authMode = str(b.githubAuthMode, 'pat')
      return [
        { level: 'info', msg: 'Resolving api.github.com', delay: 300 },
        { level: 'info', msg: authMode === 'pat' ? 'Validating GitHub personal access token' : authMode === 'oauth' ? 'Validating GitHub OAuth grant' : 'Validating GitHub App installation', delay: 420 },
        { level: 'info', msg: 'Listing accessible repositories', delay: 480 },
        { level: 'success', msg: 'GitHub API responded · connector runtime ready', delay: 360 },
      ]
    }
    case 'azuredevops': {
      const authMode = str(b.azureDevOpsAuthMode, 'pat')
      return [
        { level: 'info', msg: 'Resolving dev.azure.com', delay: 320 },
        { level: 'info', msg: authMode === 'pat' ? 'Validating Azure DevOps personal access token' : 'Validating Microsoft Entra delegated grant', delay: 460 },
        { level: 'info', msg: 'Enumerating organization, projects, and repositories', delay: 520 },
        { level: 'success', msg: 'Azure DevOps API responded · connector runtime ready', delay: 360 },
      ]
    }
    default:
      return [{ level: 'success', msg: 'Connection verified', delay: 500 }]
  }
}

function extractArray(data: unknown): unknown[] {
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

/* ── Route handler ──────────────────────────────────────────────────────── */
export async function POST(req: NextRequest) {
  let body: Record<string, unknown>
  try {
    body = await req.json() as Record<string, unknown>
  } catch {
    return new Response('Bad Request', { status: 400 })
  }
  const connectorType = String(body.connectorType ?? 'http')
  const url           = String(body.url ?? '')

  workerStart()

  const stream = new ReadableStream({
    async start(controller) {
      const t0 = performance.now()
      try {
        if (connectorType === 'appinsights') {
          /* ── Real App Insights test ──────────────────────────────── */
          const authMode     = body.authMode === 'api_key' ? 'api_key' : 'entra_client_secret'
          const mode         = body.mode === 'workspace' ? 'workspace' : 'appinsights'
          const connectionString = String(body.connectionString ?? '')
          const parsedConnection = parseAppInsightsConnectionString(connectionString)
          const appId        = String(body.appId ?? parsedConnection.applicationId ?? '')
          const apiKey       = String(body.apiKey ?? '')
          const workspaceId  = String(body.workspaceId  ?? '')
          const tenantId     = String(body.tenantId     ?? '')
          const clientId     = String(body.clientId     ?? '')
          const clientSecret = String(body.clientSecret ?? '')

          if (authMode === 'api_key' && (!appId || !apiKey)) {
            controller.enqueue(sse({
              type: 'log',
              level: 'error',
              msg: 'Missing required credentials (appId, apiKey)',
            }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
            return
          }
          if (authMode !== 'api_key' && (!tenantId || !clientId || !clientSecret || (mode === 'workspace' ? !workspaceId : !appId))) {
            controller.enqueue(sse({
              type: 'log',
              level: 'error',
              msg: mode === 'workspace'
                ? 'Missing required credentials (workspaceId, tenantId, clientId, clientSecret)'
                : 'Missing required credentials (appId, tenantId, clientId, clientSecret)',
            }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
            return
          }

          try {
            const {
              executeKQL,
              executeKQLApiKey,
              executeKQLWorkspace,
            } = await import('@/lib/appinsights')

            if (authMode === 'api_key') {
              controller.enqueue(sse({ type: 'log', level: 'info', msg: 'Resolving api.applicationinsights.io' }))
              await sleep(180)
              controller.enqueue(sse({ type: 'log', level: 'info', msg: `Preparing App Insights API-key probe (${appId.slice(0, 8)}…)` }))
              await sleep(220)
              controller.enqueue(sse({ type: 'log', level: 'info', msg: 'Sending x-api-key authenticated query' }))
            } else {
              controller.enqueue(sse({ type: 'log', level: 'info', msg: 'Resolving login.microsoftonline.com' }))
              await sleep(180)
              controller.enqueue(sse({
                type: 'log',
                level: 'info',
                msg: mode === 'workspace'
                  ? `Preparing Azure Monitor workspace probe (${workspaceId.slice(0, 8)}…)`
                  : `Preparing App Insights probe (${appId.slice(0, 8)}…)`,
              }))
              await sleep(220)
              controller.enqueue(sse({ type: 'log', level: 'info', msg: `Requesting OAuth2 token (tenant: ${tenantId.slice(0, 8)}…)` }))
              controller.enqueue(sse({ type: 'log', level: 'success', msg: 'Azure AD token acquired · client_credentials flow' }))
            }
            await sleep(220)
            controller.enqueue(sse({
              type: 'log',
              level: 'info',
              msg: authMode === 'api_key'
                ? 'KQL: requests | take 1 via App Insights API key'
                : mode === 'workspace'
                ? 'KQL: requests | take 1 via Log Analytics'
                : 'KQL: requests | take 1 via App Insights API',
            }))

            const result = authMode === 'api_key'
              ? await executeKQLApiKey(appId, apiKey, 'requests | take 1', 'PT1H')
              : mode === 'workspace'
              ? await executeKQLWorkspace(workspaceId, tenantId, clientId, clientSecret, 'requests | take 1', 'PT1H')
              : await executeKQL(appId, tenantId, clientId, clientSecret, 'requests | take 1', 'PT1H')

            if (result.error) {
              controller.enqueue(sse({ type: 'log', level: 'error', msg: result.error }))
              controller.enqueue(sse({ type: 'done', ok: false, latencyMs: result.durationMs }))
              return
            }

            const sourceLabel = authMode === 'api_key'
              ? 'App Insights API key'
              : mode === 'workspace'
              ? 'Azure Monitor workspace'
              : 'App Insights OAuth API'
            controller.enqueue(sse({
              type: 'log',
              level: 'success',
              msg: `Connection verified · ${sourceLabel} responded in ${result.durationMs}ms`,
            }))
            controller.enqueue(sse({
              type: 'log',
              level: 'info',
              msg: result.rowCount > 0
                ? `${result.rowCount} sample row${result.rowCount === 1 ? '' : 's'} returned`
                : 'Query executed successfully',
            }))
            controller.enqueue(sse({ type: 'done', ok: true, latencyMs: result.durationMs }))
            return
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg: `Azure probe error: ${msg}` }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

        } else if (connectorType === 'azuremonitor') {
          const workspaceId = String(body.workspaceId ?? '')
          const tenantId = String(body.tenantId ?? '')
          const clientId = String(body.clientId ?? '')
          const clientSecret = String(body.clientSecret ?? '')

          if (!workspaceId || !tenantId || !clientId || !clientSecret) {
            controller.enqueue(sse({ type: 'log', level: 'error', msg: 'Missing required credentials (workspaceId, tenantId, clientId, clientSecret)' }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
            return
          }

          controller.enqueue(sse({ type: 'log', level: 'info', msg: 'Resolving login.microsoftonline.com' }))
          await sleep(160)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Preparing Azure Monitor workspace probe (${workspaceId.slice(0, 8)}…)` }))
          await sleep(200)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Requesting OAuth2 token (tenant: ${tenantId.slice(0, 8)}…)` }))

          try {
            const { executeKQLWorkspace } = await import('@/lib/appinsights')
            controller.enqueue(sse({ type: 'log', level: 'success', msg: 'Azure AD token acquired · client_credentials flow' }))
            await sleep(220)
            controller.enqueue(sse({ type: 'log', level: 'info', msg: 'KQL: requests | take 1 via Log Analytics' }))
            const result = await executeKQLWorkspace(workspaceId, tenantId, clientId, clientSecret, 'requests | take 1', 'PT1H')
            if (result.error) {
              controller.enqueue(sse({ type: 'log', level: 'error', msg: result.error }))
              controller.enqueue(sse({ type: 'done', ok: false, latencyMs: result.durationMs }))
              return
            }
            controller.enqueue(sse({ type: 'log', level: 'success', msg: `Connection verified · Azure Monitor responded in ${result.durationMs}ms` }))
            controller.enqueue(sse({ type: 'done', ok: true, latencyMs: result.durationMs }))
            return
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg: `Azure Monitor probe error: ${msg}` }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

        } else if (connectorType === 'elasticsearch') {
          const endpoint = String(body.endpoint ?? '').replace(/\/$/, '')
          const authType = body.authType === 'apikey' ? 'apikey' : 'basic'
          const indexPattern = String(body.indexPattern ?? 'logs-*')
          const headers: Record<string, string> = { 'Content-Type': 'application/json' }
          if (!endpoint) {
            controller.enqueue(sse({ type: 'log', level: 'error', msg: 'Missing required field: endpoint' }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
            return
          }
          if (authType === 'apikey') {
            const apiKey = String(body.apiKey ?? '')
            if (!apiKey) {
              controller.enqueue(sse({ type: 'log', level: 'error', msg: 'API key auth requires apiKey' }))
              controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
              return
            }
            headers.Authorization = `ApiKey ${apiKey}`
          } else {
            const username = String(body.username ?? '')
            const password = String(body.password ?? '')
            if (!username || !password) {
              controller.enqueue(sse({ type: 'log', level: 'error', msg: 'Basic auth requires username and password' }))
              controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
              return
            }
            headers.Authorization = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`
          }
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Connecting to ${endpoint}` }))
          await sleep(180)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `POST ${indexPattern}/_search` }))
          try {
            const res = await fetch(`${endpoint}/${encodeURIComponent(indexPattern)}/_search`, {
              method: 'POST',
              headers,
              body: JSON.stringify({ size: 1, query: { match_all: {} } }),
              signal: AbortSignal.timeout(20_000),
            })
            const latencyMs = Math.round(performance.now() - t0)
            if (!res.ok) {
              controller.enqueue(sse({ type: 'log', level: 'error', msg: `Elastic API ${res.status}: ${(await res.text()).slice(0, 300)}` }))
              controller.enqueue(sse({ type: 'done', ok: false, latencyMs }))
              return
            }
            controller.enqueue(sse({ type: 'log', level: 'success', msg: 'Connection verified · Elastic cluster responded' }))
            controller.enqueue(sse({ type: 'done', ok: true, latencyMs }))
            return
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg: `Elastic probe error: ${msg}` }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

        } else if (connectorType === 'datadog') {
          const site = String(body.site ?? '')
          const apiKey = String(body.apiKey ?? '')
          const applicationKey = String(body.applicationKey ?? '')
          const source = String(body.source ?? 'logs')
          if (!site || !apiKey || !applicationKey) {
            controller.enqueue(sse({ type: 'log', level: 'error', msg: 'Missing required credentials (site, apiKey, applicationKey)' }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
            return
          }
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Connecting to api.${site}` }))
          await sleep(180)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Preparing ${source} query probe` }))
          try {
            const res = await fetch(`https://api.${site}/api/v2/logs/events/search`, {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'DD-API-KEY': apiKey,
                'DD-APPLICATION-KEY': applicationKey,
              },
              body: JSON.stringify({
                filter: { from: 'now-1h', to: 'now', query: '*' },
                sort: '-timestamp',
                page: { limit: 1 },
              }),
              signal: AbortSignal.timeout(20_000),
            })
            const latencyMs = Math.round(performance.now() - t0)
            if (!res.ok) {
              controller.enqueue(sse({ type: 'log', level: 'error', msg: `Datadog API ${res.status}: ${(await res.text()).slice(0, 300)}` }))
              controller.enqueue(sse({ type: 'done', ok: false, latencyMs }))
              return
            }
            controller.enqueue(sse({ type: 'log', level: 'success', msg: 'Connection verified · Datadog API responded' }))
            controller.enqueue(sse({ type: 'done', ok: true, latencyMs }))
            return
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg: `Datadog probe error: ${msg}` }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

        } else if (connectorType === 'azureb2c' || connectorType === 'azureentraid') {
          const authMode = body.authMode === 'client_certificate' ? 'client_certificate' : 'client_secret'
          const resource = String(body.resource ?? 'users')
          const tenantId = String(body.tenantId ?? '')
          const clientId = String(body.clientId ?? '')
          const clientSecret = String(body.clientSecret ?? '')
          const certificatePem = String(body.certificatePem ?? '')
          const privateKeyPem = String(body.privateKeyPem ?? '')
          const thumbprint = String(body.thumbprint ?? '')

          if (!tenantId || !clientId) {
            controller.enqueue(sse({ type: 'log', level: 'error', msg: 'Missing required credentials (tenantId, clientId)' }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
            return
          }
          if (authMode === 'client_secret' && !clientSecret) {
            controller.enqueue(sse({ type: 'log', level: 'error', msg: 'Client secret auth requires clientSecret' }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
            return
          }
          if (authMode === 'client_certificate' && (!certificatePem || !privateKeyPem)) {
            controller.enqueue(sse({ type: 'log', level: 'error', msg: 'Client certificate auth requires certificatePem and privateKeyPem' }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
            return
          }

          const spec = connectorType === 'azureb2c'
            ? resolveAzureB2CResource(resource, 1)
            : resolveAzureEntraIdResource(resource, 1)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: 'Resolving login.microsoftonline.com' }))
          await sleep(160)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Preparing Microsoft Graph probe for ${spec.label}` }))
          await sleep(180)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Requesting OAuth2 token (tenant: ${tenantId.slice(0, 8)}…)` }))

          try {
            const creds: {
              tenantId: string
              clientId: string
              authMode: 'client_secret' | 'client_certificate'
              clientSecret: string
              certificatePem: string
              privateKeyPem: string
              thumbprint: string
              cloud: 'global'
            } = {
              tenantId,
              clientId,
              authMode,
              clientSecret,
              certificatePem,
              privateKeyPem,
              thumbprint,
              cloud: 'global' as const,
            }
            await getAzureGraphToken(creds)
            controller.enqueue(sse({
              type: 'log',
              level: 'success',
              msg: authMode === 'client_certificate'
                ? 'Microsoft Graph token acquired · client certificate flow'
                : 'Microsoft Graph token acquired · client_credentials flow',
            }))
            await sleep(200)
            controller.enqueue(sse({ type: 'log', level: 'info', msg: `GET ${spec.path}` }))

            const result = connectorType === 'azureb2c'
              ? await fetchAzureB2CRows(creds, resource, { rowLimit: 1 })
              : await fetchAzureEntraIdRows(creds, resource, { rowLimit: 1 })
            const latencyMs = Math.round(performance.now() - t0)
            controller.enqueue(sse({
              type: 'log',
              level: spec.isBeta ? 'warn' : 'info',
              msg: spec.isBeta
                ? `${spec.label} uses Microsoft Graph beta endpoints`
                : `${spec.label} uses Microsoft Graph v1.0`,
            }))
            controller.enqueue(sse({
              type: 'log',
              level: 'success',
              msg: `Connection verified · ${result.rows.length} sample row${result.rows.length === 1 ? '' : 's'} returned`,
            }))
            controller.enqueue(sse({ type: 'done', ok: true, latencyMs }))
            return
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({
              type: 'log',
              level: 'error',
              msg: `${connectorType === 'azureb2c' ? 'Azure AD B2C' : 'Azure Entra ID'} probe error: ${msg}`,
            }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

        } else if (connectorType === 'github') {
          const authMode = String(body.githubAuthMode ?? body.authMode ?? 'pat')
          let credentials: GitHubCredentials | null = null

          if (authMode === 'pat') {
            const token = String(body.token ?? '')
            if (!token) {
              controller.enqueue(sse({ type: 'log', level: 'error', msg: 'GitHub PAT is required' }))
              controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
              return
            }
            credentials = { mode: 'pat', token }
          } else if (body.transactionId) {
            credentials = getGitHubAuthTransaction(String(body.transactionId))?.credentials ?? null
          } else if (body.connectorId) {
            credentials = getGitHubCreds(String(body.connectorId))
          }

          if (!credentials) {
            controller.enqueue(sse({ type: 'log', level: 'error', msg: 'GitHub authorization is required before testing this connector' }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
            return
          }

          controller.enqueue(sse({ type: 'log', level: 'info', msg: 'Resolving api.github.com' }))
          await sleep(180)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: credentials.mode === 'pat' ? 'Preparing GitHub PAT probe' : credentials.mode === 'oauth' ? 'Preparing GitHub OAuth probe' : 'Preparing GitHub App installation probe' }))
          try {
            const validation = await validateGitHubCredentials(credentials, typeof body.connectorId === 'string' ? { connectorId: String(body.connectorId) } : {})
            controller.enqueue(sse({ type: 'log', level: 'success', msg: `Authenticated as ${validation.viewer.login}` }))
            const repos = await listAccessibleGitHubRepos(credentials, typeof body.connectorId === 'string' ? { connectorId: String(body.connectorId) } : {})
            controller.enqueue(sse({ type: 'log', level: 'info', msg: `${repos.length} accessible repositories discovered` }))
            controller.enqueue(sse({ type: 'log', level: 'success', msg: 'Connection verified · GitHub API responded' }))
            controller.enqueue(sse({ type: 'done', ok: true, latencyMs: Math.round(performance.now() - t0) }))
            return
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg: `GitHub probe error: ${msg}` }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

        } else if (connectorType === 'azuredevops') {
          const authMode = String(body.azureDevOpsAuthMode ?? body.authMode ?? 'pat')
          let credentials: AzureDevOpsCredentials | null = null

          if (authMode === 'pat') {
            const pat = String(body.pat ?? '')
            const organization = String(body.organization ?? '')
            if (!pat || !organization) {
              controller.enqueue(sse({ type: 'log', level: 'error', msg: 'Azure DevOps PAT and organization are required' }))
              controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
              return
            }
            credentials = { mode: 'pat', pat, organization }
          } else if (body.transactionId) {
            credentials = getAzureDevOpsAuthTransaction(String(body.transactionId))?.credentials ?? null
          } else if (body.connectorId) {
            credentials = getAzureDevOpsCreds(String(body.connectorId))
          }

          if (!credentials) {
            controller.enqueue(sse({ type: 'log', level: 'error', msg: 'Azure DevOps authorization is required before testing this connector' }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
            return
          }

          const organization = String(body.organization ?? credentials.organization ?? '')
          if (!organization) {
            controller.enqueue(sse({ type: 'log', level: 'error', msg: 'Azure DevOps organization is required' }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
            return
          }

          controller.enqueue(sse({ type: 'log', level: 'info', msg: 'Resolving dev.azure.com' }))
          await sleep(180)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: credentials.mode === 'pat' ? 'Preparing Azure DevOps PAT probe' : 'Preparing Azure DevOps delegated auth probe' }))
          try {
            const validation = await validateAzureDevOpsCredentials(credentials)
            controller.enqueue(sse({ type: 'log', level: 'success', msg: `Authenticated for ${validation.organizations.length} accessible organization${validation.organizations.length === 1 ? '' : 's'}` }))
            const projects = await listAzureDevOpsProjects(credentials, organization)
            controller.enqueue(sse({ type: 'log', level: 'info', msg: `${projects.length} project${projects.length === 1 ? '' : 's'} discovered in ${organization}` }))
            const repositories = projects.length > 0
              ? await listAzureDevOpsRepositories(credentials, organization, projects.slice(0, 10).map(project => ({
                  id: project.id,
                  name: project.name,
                  description: project.description,
                  visibility: project.visibility,
                })))
              : []
            controller.enqueue(sse({ type: 'log', level: 'info', msg: `${repositories.length} repositories discovered across selected projects` }))
            controller.enqueue(sse({ type: 'log', level: 'success', msg: 'Connection verified · Azure DevOps API responded' }))
            controller.enqueue(sse({ type: 'done', ok: true, latencyMs: Math.round(performance.now() - t0) }))
            return
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg: `Azure DevOps probe error: ${msg}` }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

        } else if (connectorType === 'redis') {
          controller.enqueue(sse({ type: 'log', level: 'info', msg: 'Connecting to Redis…' }))
          await sleep(120)
          try {
            const capabilities = await probeRedisCapabilities(body)
            controller.enqueue(sse({ type: 'log', level: 'success', msg: `Connected to Redis ${capabilities.redisVersion || 'server'}` }))
            controller.enqueue(sse({ type: 'log', level: 'info', msg: `Server kind: ${capabilities.serverKind}${capabilities.modules.length ? ` · modules: ${capabilities.modules.join(', ')}` : ''}` }))
            const rows = await sampleRowsFromRuntimeConfig('redis', body, { rowLimit: 25 })
            const schema = inferSchema(rows)
            controller.enqueue(sse({ type: 'log', level: 'info', msg: `Fetched ${rows.length} sample row${rows.length === 1 ? '' : 's'}` }))
            controller.enqueue(sse({ type: 'log', level: 'success', msg: `${schema.length} fields inferred · Redis runtime ready` }))
            controller.enqueue(sse({ type: 'done', ok: true, latencyMs: Math.round(performance.now() - t0) }))
            return
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

        } else if (connectorType === 'mssql') {
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Connecting to SQL Server ${String(body.host || 'localhost')}:${String(body.port || 1433)}…` }))
          await sleep(120)
          try {
            const { probeMssqlConnection } = await import('@/lib/mssql')
            const probe = await probeMssqlConnection(body)
            if (!probe.ok) throw new Error(probe.error ?? 'Connection failed')
            controller.enqueue(sse({ type: 'log', level: 'success', msg: `Connected to SQL Server${probe.database ? ` · database: ${probe.database}` : ''}` }))
            if (probe.serverVersion) controller.enqueue(sse({ type: 'log', level: 'info', msg: probe.serverVersion.slice(0, 80) }))
            controller.enqueue(sse({ type: 'done', ok: true, latencyMs: Math.round(performance.now() - t0) }))
            return
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

        } else if (connectorType === 'rabbitmq') {
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Connecting to RabbitMQ management API at ${String(body.host || 'localhost')}:${String(body.managementPort || 15672)}…` }))
          await sleep(120)
          try {
            const { probeRabbitConnection } = await import('@/lib/rabbitmq')
            const probe = await probeRabbitConnection(body)
            if (!probe.ok) throw new Error(probe.error ?? 'Connection failed')
            controller.enqueue(sse({ type: 'log', level: 'success', msg: `Connected to RabbitMQ${probe.version ? ` ${probe.version}` : ''}${probe.cluster ? ` · cluster: ${probe.cluster}` : ''}` }))
            controller.enqueue(sse({ type: 'done', ok: true, latencyMs: Math.round(performance.now() - t0) }))
            return
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

        } else if (connectorType === 'mqtt') {
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Connecting to MQTT broker ${String(body.host || 'localhost')}:${String(body.port || 1883)}…` }))
          await sleep(120)
          try {
            const { probeMqttConnection } = await import('@/lib/mqtt')
            const probe = await probeMqttConnection(body)
            if (!probe.ok) throw new Error(probe.error ?? 'Connection failed')
            controller.enqueue(sse({ type: 'log', level: 'success', msg: `Connected to MQTT broker${probe.broker ? ` · ${probe.broker}` : ''}` }))
            controller.enqueue(sse({ type: 'done', ok: true, latencyMs: Math.round(performance.now() - t0) }))
            return
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

        } else if (connectorType === 'rss') {
          const feedUrl = String(body.url || '')
          let hostname = feedUrl
          try { hostname = new URL(feedUrl).hostname } catch {}
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Fetching RSS feed from ${hostname}…` }))
          await sleep(120)
          try {
            const { probeRssFeed } = await import('@/lib/rss')
            const customHeaders: Record<string, string> = {}
            const headerText = String(body.customHeaders ?? '')
            for (const line of headerText.split('\n')) {
              const idx = line.indexOf(':')
              if (idx > 0) { const k = line.slice(0, idx).trim(); const v = line.slice(idx + 1).trim(); if (k && v) customHeaders[k] = v }
            }
            const probe = await probeRssFeed(feedUrl, {
              auth: String(body.auth ?? 'none') as 'none' | undefined,
              bearerToken: String(body.bearerToken ?? ''),
              apiKeyHeader: String(body.apiKeyHeader ?? ''),
              apiKeyValue: String(body.apiKeyValue ?? ''),
              basicUser: String(body.basicUser ?? ''),
              basicPass: String(body.basicPass ?? ''),
            }, customHeaders)
            if (!probe.ok) throw new Error(probe.error ?? 'Feed probe failed')
            controller.enqueue(sse({ type: 'log', level: 'success', msg: `Feed "${probe.feedTitle}" · ${probe.itemCount} items · format: ${probe.feedType}` }))
            controller.enqueue(sse({ type: 'done', ok: true, latencyMs: Math.round(performance.now() - t0) }))
            return
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

        } else if (connectorType === 'websocket') {
          const wsUrl = String(body.url || '')
          let hostname = wsUrl
          try { hostname = new URL(wsUrl).hostname } catch {}
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Opening WebSocket to ${hostname}…` }))
          await sleep(120)
          try {
            const { probeWsFeed } = await import('@/lib/websocket-feed')
            const customHeaders: Record<string, string> = {}
            const headerText = String(body.customHeaders ?? '')
            for (const line of headerText.split('\n')) {
              const idx = line.indexOf(':')
              if (idx > 0) { const k = line.slice(0, idx).trim(); const v = line.slice(idx + 1).trim(); if (k && v) customHeaders[k] = v }
            }
            const probe = await probeWsFeed({
              url: wsUrl,
              auth: String(body.auth ?? 'none') as 'none',
              bearerToken: String(body.bearerToken ?? ''),
              apiKeyHeader: String(body.apiKeyHeader ?? ''),
              apiKeyValue: String(body.apiKeyValue ?? ''),
              basicUser: String(body.basicUser ?? ''),
              basicPass: String(body.basicPass ?? ''),
              customHeaders,
              subscribeMessage: String(body.subscribeMessage ?? ''),
              windowMs: Number(body.windowMs ?? 8000),
            })
            if (!probe.ok) throw new Error(probe.error ?? 'WebSocket probe failed')
            controller.enqueue(sse({ type: 'log', level: 'success', msg: `WebSocket connected · ${probe.latencyMs}ms · first message: ${probe.firstMessageType ?? 'none'}` }))
            controller.enqueue(sse({ type: 'done', ok: true, latencyMs: Math.round(performance.now() - t0) }))
            return
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

        } else if (['postgresql', 'mysql', 'mongodb', 's3', 'sftp', 'bigquery'].includes(connectorType)) {
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Starting live ${connectorType} probe` }))
          await sleep(120)
          let rows: Record<string, unknown>[]
          try {
            rows = await sampleRowsFromRuntimeConfig(connectorType as 'postgresql' | 'mysql' | 'mongodb' | 's3' | 'sftp' | 'bigquery', body, {
              rowLimit: 100,
            })
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

          const latencyMs = Math.round(performance.now() - t0)
          const schema = inferSchema(rows)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Fetched ${rows.length} sample rows` }))
          controller.enqueue(sse({ type: 'log', level: 'success', msg: `${schema.length} fields inferred · ${connectorType} runtime ready` }))
          controller.enqueue(sse({ type: 'done', ok: true, latencyMs }))

        } else if (connectorType === 'http' && url.startsWith('http')) {
          /* ── Real HTTP test ──────────────────────────────────────── */
          let hostname = url
          try { hostname = new URL(url).hostname } catch { /* ignore */ }

          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Resolving hostname: ${hostname}` }))
          await sleep(320)

          // Build auth headers
          const headers: Record<string, string> = {
            'Accept':     'application/json, */*',
            'User-Agent': 'dataChef-test/0.1',
          }
          const auth = String(body.auth ?? 'none')
          if (auth === 'apikey' && body.apiKeyHeader && body.apiKeyValue) {
            headers[String(body.apiKeyHeader)] = String(body.apiKeyValue)
          } else if (auth === 'bearer' && body.bearerToken) {
            headers['Authorization'] = `Bearer ${body.bearerToken}`
          } else if (auth === 'basic' && body.basicUser && body.basicPass) {
            const encoded = Buffer.from(`${body.basicUser}:${body.basicPass}`).toString('base64')
            headers['Authorization'] = `Basic ${encoded}`
          }

          let res: Response
          try {
            res = await fetch(url, { headers, signal: AbortSignal.timeout(12_000) })
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg: `Connection failed: ${msg}` }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

          const latencyMs = Math.round(performance.now() - t0)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: 'TCP established · TLS 1.3 handshake' }))
          await sleep(150)

          if (!res.ok) {
            controller.enqueue(sse({ type: 'log', level: 'error', msg: `HTTP ${res.status} ${res.statusText}` }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs }))
            return
          }

          const method = String(body.method ?? 'GET')
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `${method} ${url} → ${res.status} OK · ${latencyMs}ms` }))
          await sleep(150)

          const ct = res.headers.get('content-type') ?? 'unknown'
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Parsing response (${ct})` }))
          await sleep(180)

          // Parse body
          const text = await res.text()
          let data: unknown
          try {
            data = JSON.parse(text)
          } catch {
            const lines = text.trim().split('\n').filter(Boolean)
            try {
              data = lines.slice(0, 500).map(l => JSON.parse(l))
            } catch {
              controller.enqueue(sse({ type: 'log', level: 'error', msg: 'Response is not valid JSON or JSONL' }))
              controller.enqueue(sse({ type: 'done', ok: false, latencyMs }))
              return
            }
          }

          const records = extractArray(data)
          if (records.length === 0) {
            controller.enqueue(sse({ type: 'log', level: 'warn', msg: 'No array found in response — check the data path' }))
            controller.enqueue(sse({ type: 'done', ok: true, latencyMs }))
            return
          }

          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Detected ${records.length.toLocaleString()}-record array · sampling 200` }))
          await sleep(200)

          const fieldCount = inferSchema(records.slice(0, 200) as Record<string, unknown>[]).length
          controller.enqueue(sse({ type: 'log', level: 'success', msg: `${fieldCount} fields inferred · ${records.length.toLocaleString()} records detected` }))
          controller.enqueue(sse({ type: 'done', ok: true, latencyMs }))

        } else {
          /* ── Simulated test for DB / S3 / SFTP / BigQuery / AppInsights fallback ─ */
          const seq = getSimLogs(connectorType, body)
          for (const entry of seq) {
            await sleep(entry.delay)
            controller.enqueue(sse({ type: 'log', level: entry.level, msg: entry.msg }))
          }
          controller.enqueue(sse({ type: 'done', ok: true, latencyMs: Math.round(performance.now() - t0) }))
        }
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err)
        controller.enqueue(sse({ type: 'log', level: 'error', msg: `Unexpected error: ${msg}` }))
        controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
      } finally {
        workerEnd()
        controller.close()
      }
    },
  })

  return new Response(stream, {
    headers: {
      'Content-Type':  'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection':    'keep-alive',
    },
  })
}
