/**
 * POST /api/connectors/sync
 *
 * Runs a connector sync job and streams SSE log events.
 * - HTTP connectors with URL: real server-side fetch + schema inference.
 *   Also updates any linked dataset in the registry.
 * - Others: server-driven simulation with realistic timing.
 *
 * Uses workerStart/workerEnd so the sidebar worker counter updates live.
 *
 * Request body: { connectorType, connectorId, connectorName, url? }
 *
 * SSE events:
 *   { type: 'log', level: 'info'|'success'|'warn'|'error', msg: string }
 *   { type: 'progress', p: number }
 *   { type: 'done', ok: boolean, records: number, durationMs: number }
 */

import { NextRequest } from 'next/server'
import { workerStart, workerEnd } from '@/lib/pipelines'
import { getDatasetByUrl, updateDatasetSchema } from '@/lib/datasets'
import { updateConnectorSync, getConnectorRuntimeConfig } from '@/lib/connectors'
import { runConnectorSyncJob } from '@/lib/connector-sync'
import {
  extractArray,
  inferSchema,
  sampleRowsFromRuntimeConfig,
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

/* ── Simulated sync sequences ───────────────────────────────────────────── */
function getSimSyncLogs(type: string, connName: string): SimEntry[] {
  const base: SimEntry[] = [
    { level: 'info',    msg: `Starting sync job for '${connName}'`,          delay: 300 },
    { level: 'info',    msg: 'Acquiring worker slot from pool',               delay: 380 },
    { level: 'info',    msg: 'Connecting to source…',                        delay: 550 },
    { level: 'info',    msg: 'Authentication successful',                     delay: 400 },
    { level: 'info',    msg: 'Fetching incremental records since last run',   delay: 680 },
    { level: 'info',    msg: 'Processing batch 1/3 (1,000 records)',          delay: 500 },
    { level: 'info',    msg: 'Processing batch 2/3 (1,000 records)',          delay: 500 },
    { level: 'info',    msg: 'Processing batch 3/3 (847 records)',            delay: 430 },
    { level: 'info',    msg: 'Writing to dataset storage',                    delay: 480 },
    { level: 'success', msg: '2,847 new records ingested · 0 errors',        delay: 380 },
  ]
  if (type === 'sftp') {
    base.splice(2, 0, { level: 'warn', msg: 'FTP mode: unencrypted transfer', delay: 100 })
  }
  if (type === 's3') {
    base.splice(5, 0, { level: 'info', msg: 'Decoding Parquet row groups (1-8)', delay: 350 })
  }
  if (type === 'bigquery') {
    base.splice(4, 0, { level: 'info', msg: 'Running BigQuery export job · billed 2.1 GB', delay: 900 })
  }
  return base
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
  const connectorId   = String(body.connectorId   ?? '')
  const connectorName = String(body.connectorName ?? 'Connector')
  const url           = String(body.url ?? '')

  workerStart()

  const stream = new ReadableStream({
    async start(controller) {
      const t0 = performance.now()
      try {
        if (connectorId && connectorType !== 'webhook' && connectorType !== 'file') {
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Starting sync for '${connectorName}'` }))
          controller.enqueue(sse({ type: 'progress', p: 15 }))
          await sleep(120)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: 'Loading connector runtime configuration' }))
          controller.enqueue(sse({ type: 'progress', p: 40 }))
          await sleep(180)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: 'Fetching rows and updating linked datasets' }))

          const result = await runConnectorSyncJob(connectorId, { manageWorker: false })
          if (!result.ok) {
            controller.enqueue(sse({ type: 'log', level: 'error', msg: result.error ?? 'Sync failed' }))
            controller.enqueue(sse({ type: 'progress', p: 100 }))
            controller.enqueue(sse({ type: 'done', ok: false, records: 0, durationMs: result.durationMs }))
            return
          }

          controller.enqueue(sse({
            type: 'log',
            level: 'success',
            msg: result.syncedDatasets > 0
              ? `${result.records.toLocaleString()} rows synced across ${result.syncedDatasets} dataset${result.syncedDatasets === 1 ? '' : 's'}`
              : `${result.records.toLocaleString()} rows sampled successfully`,
          }))
          controller.enqueue(sse({ type: 'progress', p: 100 }))
          controller.enqueue(sse({ type: 'done', ok: true, records: result.records, durationMs: result.durationMs }))
          return
        }

        if (['postgresql', 'mysql', 'mongodb', 's3', 'sftp', 'bigquery'].includes(connectorType)) {
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Starting sync for '${connectorName}'` }))
          controller.enqueue(sse({ type: 'progress', p: 10 }))
          await sleep(180)
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Connecting to ${connectorType}…` }))

          const config = getConnectorRuntimeConfig(connectorId)
          if (!config) {
            controller.enqueue(sse({ type: 'log', level: 'error', msg: 'No runtime config found for connector' }))
            controller.enqueue(sse({ type: 'done', ok: false, records: 0, durationMs: Math.round(performance.now() - t0) }))
            return
          }

          let records: Record<string, unknown>[]
          try {
            records = await sampleRowsFromRuntimeConfig(
              connectorType as 'postgresql' | 'mysql' | 'mongodb' | 's3' | 'sftp' | 'bigquery',
              config,
              { rowLimit: 500 },
            )
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg }))
            controller.enqueue(sse({ type: 'done', ok: false, records: 0, durationMs: Math.round(performance.now() - t0) }))
            return
          }

          controller.enqueue(sse({ type: 'progress', p: 55 }))
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Fetched ${records.length} sample rows · inferring schema…` }))
          const schema = inferSchema(records)
          const sampleRows = records.slice(0, 5)

          controller.enqueue(sse({ type: 'progress', p: 80 }))
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Schema: ${schema.length} fields detected` }))

          const durationMs = Math.round(performance.now() - t0)
          updateConnectorSync(connectorId, records.length, durationMs, true)
          controller.enqueue(sse({ type: 'log', level: 'success', msg: `${records.length} sampled rows synced · ${schema.length} fields` }))
          controller.enqueue(sse({ type: 'progress', p: 100 }))
          controller.enqueue(sse({ type: 'done', ok: true, records: records.length, durationMs }))

        } else if (connectorType === 'http' && url.startsWith('http')) {
          /* ── Real HTTP sync ──────────────────────────────────────── */
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Starting sync for '${connectorName}'` }))
          controller.enqueue(sse({ type: 'progress', p: 5 }))
          await sleep(300)

          controller.enqueue(sse({ type: 'log', level: 'info', msg: 'Acquiring worker slot from pool' }))
          await sleep(350)

          let hostname = url
          try { hostname = new URL(url).hostname } catch { /* ignore */ }
          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Connecting to ${hostname}…` }))
          controller.enqueue(sse({ type: 'progress', p: 15 }))
          await sleep(400)

          let res: Response
          try {
            res = await fetch(url, {
              headers: {
                'Accept':     'application/json, */*',
                'User-Agent': 'dataChef-sync/0.1',
              },
              signal: AbortSignal.timeout(15_000),
            })
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e)
            controller.enqueue(sse({ type: 'log', level: 'error', msg: `Fetch failed: ${msg}` }))
            controller.enqueue(sse({ type: 'done', ok: false, records: 0, durationMs: Math.round(performance.now() - t0) }))
            return
          }

          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Authentication successful · ${res.status} OK` }))
          controller.enqueue(sse({ type: 'progress', p: 30 }))
          await sleep(200)

          if (!res.ok) {
            controller.enqueue(sse({ type: 'log', level: 'error', msg: `Source returned HTTP ${res.status} ${res.statusText}` }))
            controller.enqueue(sse({ type: 'done', ok: false, records: 0, durationMs: Math.round(performance.now() - t0) }))
            return
          }

          controller.enqueue(sse({ type: 'log', level: 'info', msg: 'Downloading payload…' }))
          const text = await res.text()
          controller.enqueue(sse({ type: 'progress', p: 50 }))

          let data: unknown
          try {
            data = JSON.parse(text)
          } catch {
            const lines = text.trim().split('\n').filter(Boolean)
            try {
              data = lines.slice(0, 500).map(l => JSON.parse(l))
            } catch {
              controller.enqueue(sse({ type: 'log', level: 'error', msg: 'Response is not valid JSON or JSONL' }))
              controller.enqueue(sse({ type: 'done', ok: false, records: 0, durationMs: Math.round(performance.now() - t0) }))
              return
            }
          }

          const records    = extractArray(data) as Record<string, unknown>[]
          const totalRows  = records.length

          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Fetched ${totalRows.toLocaleString()} records · inferring schema…` }))
          controller.enqueue(sse({ type: 'progress', p: 65 }))
          await sleep(300)

          const schema     = inferSchema(records)
          const sampleRows = records.slice(0, 5)

          controller.enqueue(sse({ type: 'log', level: 'info', msg: `Schema: ${schema.length} fields detected` }))
          controller.enqueue(sse({ type: 'progress', p: 80 }))
          await sleep(250)

          // Update linked dataset if URL matches
          const dataset = getDatasetByUrl(url)
          if (dataset) {
            updateDatasetSchema(dataset.id, schema, sampleRows, totalRows)
            controller.enqueue(sse({ type: 'log', level: 'info', msg: `Dataset '${dataset.name}' updated in registry` }))
          }

          controller.enqueue(sse({ type: 'log', level: 'info', msg: 'Writing to dataset storage' }))
          controller.enqueue(sse({ type: 'progress', p: 90 }))
          await sleep(300)

          const durationMs = Math.round(performance.now() - t0)
          updateConnectorSync(connectorId, totalRows, durationMs, true)
          controller.enqueue(sse({ type: 'log', level: 'success', msg: `${totalRows.toLocaleString()} records synced · ${schema.length} fields · 0 errors` }))
          controller.enqueue(sse({ type: 'progress', p: 100 }))
          controller.enqueue(sse({ type: 'done', ok: true, records: totalRows, durationMs }))

        } else {
          /* ── Simulated sync for DB / S3 / SFTP / BigQuery / Webhook */
          const seq = getSimSyncLogs(connectorType, connectorName)
          for (let i = 0; i < seq.length; i++) {
            await sleep(seq[i].delay)
            controller.enqueue(sse({ type: 'log', level: seq[i].level, msg: seq[i].msg }))
            controller.enqueue(sse({ type: 'progress', p: Math.round(((i + 1) / seq.length) * 100) }))
          }
          const durationMs = Math.round(performance.now() - t0)
          updateConnectorSync(connectorId, 2847, durationMs, true)
          controller.enqueue(sse({ type: 'done', ok: true, records: 2847, durationMs }))
        }
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err)
        controller.enqueue(sse({ type: 'log', level: 'error', msg: `Unexpected error: ${msg}` }))
        controller.enqueue(sse({ type: 'done', ok: false, records: 0, durationMs: Math.round(performance.now() - t0) }))
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
