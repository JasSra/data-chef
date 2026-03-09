/**
 * GET  /api/datasets  — list all datasets
 * POST /api/datasets  — create a new dataset (from wizard)
 */

import { NextRequest, NextResponse } from 'next/server'
import { getDatasets, addDataset, materializeDataset, type DatasetRecord } from '@/lib/datasets'
import { ensureConnectorSchedulerStarted } from '@/lib/connector-sync'
import { getDatasets as listDatasets } from '@/lib/datasets'
import { updateConnectorDatasets } from '@/lib/connectors'

export const dynamic = 'force-dynamic'

export function GET() {
  ensureConnectorSchedulerStarted()
  return NextResponse.json(getDatasets())
}

export async function POST(req: NextRequest) {
  ensureConnectorSchedulerStarted()
  const body = await req.json() as Partial<DatasetRecord> & { name: string; source: string; format: string }

  if (!body.name?.trim()) {
    return NextResponse.json({ error: 'name is required' }, { status: 400 })
  }

  const hasRows = Array.isArray(body.sampleRows) || body.totalRows != null || Array.isArray(body.schema)
  const ds = hasRows
    ? materializeDataset({
        existingDatasetId: body.id ? String(body.id) : undefined,
        name: body.name.trim(),
        source: body.source ?? 'http',
        url: body.url,
        auth: body.auth,
        format: body.format ?? 'JSON',
        description: body.description ?? `${body.source?.toUpperCase() ?? 'HTTP'} dataset added via wizard`,
        connection: body.connectorId
          ? String(body.connection ?? body.connectorId)
          : body.url
          ? `HTTP (${new URL(body.url).hostname})`
          : body.source ?? 'unknown',
        connectorId: body.connectorId ? String(body.connectorId) : undefined,
        resource: body.resource ? String(body.resource) : undefined,
        sourceRef: body.sourceRef ?? (body.connectorId ? {
          sourceType: 'connector',
          sourceId: String(body.connectorId),
          resource: body.resource ? String(body.resource) : undefined,
        } : null),
        materialization: body.materialization ?? null,
        schema: body.schema ?? [],
        sampleRows: body.sampleRows ?? [],
        totalRows: body.totalRows ?? 0,
      })
    : addDataset({
        name:          body.name.trim(),
        source:        body.source ?? 'http',
        url:           body.url,
        auth:          body.auth,
        format:        body.format ?? 'JSON',
        records:       '—',
        recordsRaw:    0,
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
        schema:        null,
        sampleRows:    null,
        totalRows:     null,
        sourceRef: body.sourceRef ?? (body.connectorId ? {
          sourceType: 'connector',
          sourceId: String(body.connectorId),
          resource: body.resource ? String(body.resource) : undefined,
        } : null),
        materialization: body.materialization ?? null,
      })

  if (ds.connectorId) {
    const linkedNames = listDatasets()
      .filter(dataset => dataset.connectorId === ds.connectorId)
      .map(dataset => dataset.name)
    updateConnectorDatasets(ds.connectorId, linkedNames)
  }

  return NextResponse.json(ds, { status: 201 })
}
