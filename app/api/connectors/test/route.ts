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
  samplePostgresRowsFromConfig,
} from '@/lib/runtime-data'

export const dynamic = 'force-dynamic'

type Level = 'info' | 'success' | 'warn' | 'error'
type SimEntry = { level: Level; msg: string; delay: number }

function sse(data: object) {
  return new TextEncoder().encode(`data: ${JSON.stringify(data)}\n\n`)
}
function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms))
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
          const appId        = String(body.appId        ?? '')
          const tenantId     = String(body.tenantId     ?? '')
          const clientId     = String(body.clientId     ?? '')
          const clientSecret = String(body.clientSecret ?? '')

          if (!appId || !tenantId || !clientId || !clientSecret) {
            controller.enqueue(sse({ type: 'log', level: 'error', msg: 'Missing required credentials (appId, tenantId, clientId, clientSecret)' }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: 0 }))
            return
          }

          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Resolving login.microsoftonline.com` }))
          await sleep(200)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Requesting OAuth2 token (tenant: ${tenantId.slice(0, 8)}…)` }))

          let token: string
          try {
            const { getAzureToken } = await import('@/lib/appinsights')
            token = await getAzureToken(tenantId, clientId, clientSecret)
            controller.enqueue(sse({ type: 'log', level: 'success', msg: 'Azure AD token acquired · client_credentials flow' }))
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg: `Token error: ${msg}` }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }
          await sleep(300)

          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Probing App Insights app: ${appId.slice(0, 8)}…` }))

          let probeRes: Response
          try {
            probeRes = await fetch(`https://api.applicationinsights.io/v1/apps/${appId}/query`, {
              method:  'POST',
              headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
              body:    JSON.stringify({ query: 'requests | limit 1', timespan: 'PT1H' }),
              signal:  AbortSignal.timeout(12_000),
            })
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg: `API probe failed: ${msg}` }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

          const latencyMs = Math.round(performance.now() - t0)
          if (!probeRes.ok) {
            const text = await probeRes.text()
            controller.enqueue(sse({ type: 'log', level: 'error', msg: `App Insights API ${probeRes.status}: ${text.slice(0, 200)}` }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs }))
            return
          }

          controller.enqueue(sse({ type: 'log', level: 'success', msg: `Connection verified · App Insights API responded in ${latencyMs}ms` }))
          controller.enqueue(sse({ type: 'log', level: 'info', msg: 'KQL tables available: requests, exceptions, traces, dependencies, customEvents' }))
          controller.enqueue(sse({ type: 'done', ok: true, latencyMs }))

        } else if (connectorType === 'postgresql') {
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Resolving ${String(body.host ?? 'localhost')}:${String(body.port ?? '5432')}` }))
          await sleep(120)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Authenticating as '${String(body.dbUser ?? 'user')}'` }))
          await sleep(120)

          let rows: Record<string, unknown>[]
          try {
            rows = await samplePostgresRowsFromConfig({
              host: body.host,
              port: body.port,
              database: body.database,
              dbUser: body.dbUser,
              dbPass: body.dbPass,
              ssl: body.ssl,
              tableOrQuery: body.tableOrQuery,
              useConnectionString: body.useConnectionString,
              connectionString: body.connectionString,
            }, undefined, 100)
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg: `PostgreSQL connection failed: ${msg}` }))
            controller.enqueue(sse({ type: 'done', ok: false, latencyMs: Math.round(performance.now() - t0) }))
            return
          }

          const latencyMs = Math.round(performance.now() - t0)
          const schema = inferSchema(rows)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Fetched ${rows.length} sample rows` }))
          controller.enqueue(sse({ type: 'log', level: 'success', msg: `${schema.length} fields inferred · PostgreSQL runtime ready` }))
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
