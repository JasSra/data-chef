import { NextRequest, NextResponse } from 'next/server'
import {
  exportConnectors,
  importConnectors,
  relativeTime,
  fmtRecords,
  getSparkValues,
  type ConnectorTransferRecord,
} from '@/lib/connectors'

export const dynamic = 'force-dynamic'

function toResponse(connector: ReturnType<typeof importConnectors>[number]) {
  const lastSync = connector.lastSyncAt
    ? relativeTime(connector.lastSyncAt)
    : connector.status === 'connected' ? 'live' : 'never'

  return {
    id: connector.id,
    name: connector.name,
    type: connector.type,
    status: connector.status,
    authMethod: connector.authMethod,
    endpoint: connector.endpoint,
    description: connector.description,
    datasets: connector.datasets,
    syncInterval: connector.syncInterval,
    latencyMs: connector.latencyMs,
    lastSync,
    recordsSynced: connector.recordsRaw > 0 ? `${fmtRecords(connector.recordsRaw)} total` : '—',
    sparkValues: getSparkValues(connector.syncHistory),
  }
}

function normalizePayload(body: unknown): ConnectorTransferRecord[] {
  if (Array.isArray(body)) return body as ConnectorTransferRecord[]
  if (body && typeof body === 'object') {
    const obj = body as { connectors?: unknown }
    if (Array.isArray(obj.connectors)) return obj.connectors as ConnectorTransferRecord[]
  }
  return []
}

export async function GET(req: NextRequest) {
  const ids = req.nextUrl.searchParams.getAll('id')
  const connectors = exportConnectors(ids.length > 0 ? ids : undefined)

  return NextResponse.json({
    version: 1,
    exportedAt: new Date().toISOString(),
    connectors,
  })
}

export async function POST(req: NextRequest) {
  let body: unknown
  try {
    body = await req.json()
  } catch {
    return NextResponse.json({ error: 'Bad Request' }, { status: 400 })
  }

  const records = normalizePayload(body)
  if (records.length === 0) {
    return NextResponse.json({ error: 'No connectors found in import payload' }, { status: 400 })
  }

  const imported = importConnectors(records)

  return NextResponse.json({
    imported: imported.length,
    connectors: imported.map(toResponse),
  })
}
