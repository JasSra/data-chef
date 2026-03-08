/**
 * GET  /api/connectors  — list all connectors with computed display fields
 * POST /api/connectors  — create a new connector
 */

import { NextRequest, NextResponse } from 'next/server'
import {
  getConnectors, addConnector, relativeTime, fmtRecords, getSparkValues,
  setAppInsightsCreds, setConnectorRuntimeConfig, ConnectorRecord,
} from '@/lib/connectors'
import type { ConnectorId } from '@/components/ConnectorWizard'
import { seedDefaultQueries } from '@/lib/saved-queries'

export const dynamic = 'force-dynamic'

function toResponse(c: ConnectorRecord) {
  const lastSync = c.lastSyncAt
    ? relativeTime(c.lastSyncAt)
    : c.status === 'connected' ? 'live' : 'never'
  const recordsSynced = c.recordsRaw > 0
    ? fmtRecords(c.recordsRaw) + ' total'
    : '—'
  return {
    id:           c.id,
    name:         c.name,
    type:         c.type,
    status:       c.status,
    authMethod:   c.authMethod,
    endpoint:     c.endpoint,
    description:  c.description,
    datasets:     c.datasets,
    syncInterval: c.syncInterval,
    latencyMs:    c.latencyMs,
    lastSync:     lastSync,
    recordsSynced: recordsSynced,
    sparkValues:  getSparkValues(c.syncHistory),
  }
}

export async function GET() {
  return NextResponse.json(getConnectors().map(toResponse))
}

export async function POST(req: NextRequest) {
  let body: Record<string, unknown>
  try { body = await req.json() } catch { return NextResponse.json({ error: 'Bad Request' }, { status: 400 }) }

  const rec = addConnector({
    name:         String(body.name         ?? 'New Connector'),
    type:         (body.type as ConnectorId) ?? 'http',
    status:       'connected',
    authMethod:   String(body.authMethod   ?? 'None'),
    endpoint:     String(body.endpoint     ?? ''),
    description:  String(body.description  ?? ''),
    datasets:     Array.isArray(body.datasets) ? body.datasets as string[] : [],
    syncInterval: String(body.syncInterval ?? 'on-demand'),
    latencyMs:    0,
    lastSyncAt:   Date.now(),
    recordsRaw:   0,
  })

  if (body.runtimeConfig && typeof body.runtimeConfig === 'object') {
    setConnectorRuntimeConfig(rec.id, body.runtimeConfig as Record<string, unknown>)
  }

  // Store App Insights credentials server-side (never returned to client)
  if (body.type === 'appinsights' && body.aiCredentials) {
    const c = body.aiCredentials as { mode?: string; appId?: string; workspaceId?: string; tenantId: string; clientId: string; clientSecret: string }
    setAppInsightsCreds(rec.id, {
      mode:         (c.mode === 'workspace' ? 'workspace' : 'appinsights'),
      appId:        String(c.appId        ?? ''),
      workspaceId:  String(c.workspaceId  ?? ''),
      tenantId:     String(c.tenantId     ?? ''),
      clientId:     String(c.clientId     ?? ''),
      clientSecret: String(c.clientSecret ?? ''),
    })
    seedDefaultQueries(rec.id)
  }

  return NextResponse.json(toResponse(rec), { status: 201 })
}
