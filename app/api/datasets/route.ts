/**
 * GET  /api/datasets  — list all datasets
 * POST /api/datasets  — create a new dataset (from wizard)
 */

import { NextRequest, NextResponse } from 'next/server'
import { getDatasets, addDataset, type DatasetRecord } from '@/lib/datasets'

export const dynamic = 'force-dynamic'

export function GET() {
  return NextResponse.json(getDatasets())
}

export async function POST(req: NextRequest) {
  const body = await req.json() as Partial<DatasetRecord> & { name: string; source: string; format: string }

  if (!body.name?.trim()) {
    return NextResponse.json({ error: 'name is required' }, { status: 400 })
  }

  const ds = addDataset({
    name:          body.name.trim(),
    source:        body.source ?? 'http',
    url:           body.url,
    auth:          body.auth,
    format:        body.format ?? 'JSON',
    records:       body.totalRows != null
      ? body.totalRows >= 1_000_000
        ? `${(body.totalRows / 1_000_000).toFixed(1)}M`
        : body.totalRows >= 1_000
        ? `${(body.totalRows / 1_000).toFixed(0)}K`
        : String(body.totalRows)
      : '—',
    recordsRaw:    body.totalRows ?? 0,
    schemaVersion: 'v1',
    lastIngested:  'just now',
    size:          '—',
    status:        'active',
    description:   body.description ?? `${body.source?.toUpperCase() ?? 'HTTP'} dataset added via wizard`,
    connection:    body.connectorId
      ? String(body.connection ?? body.connectorId)
      : body.url
      ? `HTTP (${new URL(body.url).hostname})`
      : body.source ?? 'unknown',
    connectorId:   body.connectorId ? String(body.connectorId) : undefined,
    resource:      body.resource ? String(body.resource) : undefined,
    liveType:      null,
    queryDataset:  null,
    schema:        body.schema ?? null,
    sampleRows:    body.sampleRows ?? null,
    totalRows:     body.totalRows ?? null,
  })

  return NextResponse.json(ds, { status: 201 })
}
