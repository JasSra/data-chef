import { NextRequest, NextResponse } from 'next/server'

import { getDataset, updateDatasetSchema } from '@/lib/datasets'
import { getConnector } from '@/lib/connectors'
import { inferSchema, loadRowsFromConnector } from '@/lib/runtime-data'

export const dynamic = 'force-dynamic'

function extractArray(data: unknown): unknown[] {
  if (Array.isArray(data)) return data
  if (data && typeof data === 'object') {
    const preferred = ['data', 'items', 'results', 'records', 'rows', 'events', 'list']
    const obj = data as Record<string, unknown>
    for (const key of preferred) {
      if (Array.isArray(obj[key]) && (obj[key] as unknown[]).length > 0) return obj[key] as unknown[]
    }
    for (const value of Object.values(obj)) {
      if (Array.isArray(value) && value.length > 0) return value
    }
  }
  return []
}

async function refreshHttpDataset(url: string) {
  const res = await fetch(url, {
    headers: {
      Accept: 'application/json, text/plain, */*',
      'User-Agent': 'dataChef-refresh/0.1',
    },
    signal: AbortSignal.timeout(12_000),
  })

  if (!res.ok) {
    throw new Error(`Source returned ${res.status} ${res.statusText}`)
  }

  const text = await res.text()
  let data: unknown
  try {
    data = JSON.parse(text)
  } catch {
    const lines = text.trim().split('\n').filter(Boolean)
    data = lines.slice(0, 500).map(line => JSON.parse(line))
  }

  const records = extractArray(data) as Record<string, unknown>[]
  return {
    schema: inferSchema(records),
    sampleRows: records.slice(0, 5),
    totalRows: records.length,
  }
}

export async function POST(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const { id } = await params
  const ds = getDataset(id)
  if (!ds) {
    return NextResponse.json({ error: 'Dataset not found' }, { status: 404 })
  }

  try {
    if (ds.connectorId) {
      const connector = getConnector(ds.connectorId)
      if (connector) {
        const rows = await loadRowsFromConnector(ds.connectorId, {
          rowLimit: 500,
          resource: ds.resource,
        })
        const schema = inferSchema(rows)
        const sampleRows = rows.slice(0, 5)
        updateDatasetSchema(id, schema, sampleRows, rows.length)
        return NextResponse.json({
          schema,
          sampleRows,
          totalRows: rows.length,
          source: connector.type,
        })
      }
    }

    if (!ds.url?.startsWith('http')) {
      return NextResponse.json({
        error: 'Live schema refresh is only supported for HTTP and connector-backed sources.',
        cannotRefresh: true,
      }, { status: 422 })
    }

    const refreshed = await refreshHttpDataset(ds.url)
    updateDatasetSchema(id, refreshed.schema, refreshed.sampleRows, refreshed.totalRows)
    return NextResponse.json(refreshed)
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error)
    return NextResponse.json({ error: message }, { status: 502 })
  }
}
