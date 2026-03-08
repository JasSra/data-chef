/**
 * POST /api/appinsights/query
 *
 * Server-side proxy: fetches Azure AD token, executes KQL against the
 * App Insights REST API, and returns normalised { columns, rows, rowCount, durationMs }.
 *
 * Body: { connectorId: string, kql: string, timespan?: string }
 * Response: { columns, rows, rowCount, durationMs, error? }
 */

import { NextRequest, NextResponse } from 'next/server'
import { getAppInsightsCreds } from '@/lib/connectors'
import { executeKQL, executeKQLWorkspace } from '@/lib/appinsights'

export const dynamic = 'force-dynamic'

export async function POST(req: NextRequest) {
  let body: Record<string, unknown>
  try {
    body = await req.json() as Record<string, unknown>
  } catch {
    return NextResponse.json({ error: 'Bad Request' }, { status: 400 })
  }

  const connectorId = String(body.connectorId ?? '')
  const kql         = String(body.kql         ?? body.query ?? '')
  const timespan    = body.timespan ? String(body.timespan) : undefined

  if (!connectorId || !kql.trim()) {
    return NextResponse.json(
      { error: 'connectorId and kql are required' },
      { status: 400 },
    )
  }

  const creds = getAppInsightsCreds(connectorId)
  if (!creds) {
    return NextResponse.json(
      { error: `No credentials found for connector "${connectorId}". Re-create the connector to re-enter credentials.` },
      { status: 404 },
    )
  }

  const result = creds.mode === 'workspace'
    ? await executeKQLWorkspace(creds.workspaceId, creds.tenantId, creds.clientId, creds.clientSecret, kql, timespan)
    : await executeKQL(creds.appId, creds.tenantId, creds.clientId, creds.clientSecret, kql, timespan)

  return NextResponse.json(result)
}
