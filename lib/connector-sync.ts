import 'server-only'

import {
  getConnectors,
  getConnector,
  updateConnectorFailure,
  updateConnectorSync,
} from '@/lib/connectors'
import { getDatasets, updateDatasetSchema } from '@/lib/datasets'
import { inferSchema, loadRowsFromConnector } from '@/lib/runtime-data'
import { workerEnd, workerStart } from '@/lib/pipelines'

export interface ConnectorSyncResult {
  ok: boolean
  records: number
  durationMs: number
  syncedDatasets: number
  error?: string
}

interface RunConnectorSyncOptions {
  manageWorker?: boolean
}

function isScheduledInterval(value: string): boolean {
  return ['1h', '6h', '24h'].includes(value)
}

export function connectorIntervalMs(value: string): number | null {
  switch (value) {
    case '1h':
      return 60 * 60 * 1000
    case '6h':
      return 6 * 60 * 60 * 1000
    case '24h':
      return 24 * 60 * 60 * 1000
    default:
      return null
  }
}

export async function runConnectorSyncJob(
  connectorId: string,
  options: RunConnectorSyncOptions = {},
): Promise<ConnectorSyncResult> {
  const connector = getConnector(connectorId)
  if (!connector) {
    return { ok: false, records: 0, durationMs: 0, syncedDatasets: 0, error: 'Connector not found' }
  }

  const startedAt = Date.now()
  const manageWorker = options.manageWorker !== false
  if (manageWorker) workerStart()
  try {
    const datasets = getDatasets().filter(dataset => dataset.connectorId === connectorId)

    if (datasets.length === 0) {
      const rows = await loadRowsFromConnector(connectorId, { rowLimit: 500 })
      const durationMs = Date.now() - startedAt
      updateConnectorSync(connectorId, rows.length, durationMs, true)
      return { ok: true, records: rows.length, durationMs, syncedDatasets: 0 }
    }

    let totalRecords = 0
    for (const dataset of datasets) {
      const rows = await loadRowsFromConnector(connectorId, {
        rowLimit: 500,
        resource: dataset.resource,
      })
      totalRecords += rows.length
      updateDatasetSchema(dataset.id, inferSchema(rows), rows.slice(0, 5), rows.length)
    }

    const durationMs = Date.now() - startedAt
    updateConnectorSync(connectorId, totalRecords, durationMs, true)
    return {
      ok: true,
      records: totalRecords,
      durationMs,
      syncedDatasets: datasets.length,
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    const durationMs = Date.now() - startedAt
    updateConnectorFailure(connectorId, message)
    return { ok: false, records: 0, durationMs, syncedDatasets: 0, error: message }
  } finally {
    if (manageWorker) workerEnd()
  }
}

async function runDueConnectorSyncs(): Promise<void> {
  const now = Date.now()
  const connectors = getConnectors().filter(connector =>
    connector.status === 'connected' &&
    isScheduledInterval(connector.syncInterval),
  )

  for (const connector of connectors) {
    const intervalMs = connectorIntervalMs(connector.syncInterval)
    if (!intervalMs) continue
    const lastRunAt = connector.lastRunAt ?? connector.lastSyncAt ?? connector.createdAt
    if (now - lastRunAt < intervalMs) continue
    await runConnectorSyncJob(connector.id)
  }
}

declare global {
  // eslint-disable-next-line no-var
  var __datachefConnectorSchedulerStarted: boolean | undefined
}

export function ensureConnectorSchedulerStarted(): void {
  if (globalThis.__datachefConnectorSchedulerStarted) return
  globalThis.__datachefConnectorSchedulerStarted = true

  void runDueConnectorSyncs()
  setInterval(() => {
    void runDueConnectorSyncs()
  }, 30_000)
}
