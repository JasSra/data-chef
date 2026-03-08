/**
 * GET    /api/appinsights/saved-queries?connectorId=xxx   — list saved queries
 * POST   /api/appinsights/saved-queries                   — create a saved query
 * DELETE /api/appinsights/saved-queries?connectorId=xxx&queryId=yyy — delete
 */

import { NextRequest, NextResponse } from 'next/server'
import { getSavedQueries, addSavedQuery, deleteSavedQuery } from '@/lib/saved-queries'

export const dynamic = 'force-dynamic'

export function GET(req: NextRequest) {
  const connectorId = req.nextUrl.searchParams.get('connectorId') ?? ''
  if (!connectorId) {
    return NextResponse.json({ error: 'connectorId required' }, { status: 400 })
  }
  return NextResponse.json(getSavedQueries(connectorId))
}

export async function POST(req: NextRequest) {
  let body: { connectorId?: string; name?: string; kql?: string }
  try {
    body = await req.json() as { connectorId?: string; name?: string; kql?: string }
  } catch {
    return NextResponse.json({ error: 'Bad Request' }, { status: 400 })
  }

  const { connectorId = '', name = '', kql = '' } = body
  if (!connectorId || !name.trim() || !kql.trim()) {
    return NextResponse.json(
      { error: 'connectorId, name, and kql are required' },
      { status: 400 },
    )
  }

  const q = addSavedQuery(connectorId, name.trim(), kql.trim())
  return NextResponse.json(q, { status: 201 })
}

export async function DELETE(req: NextRequest) {
  const connectorId = req.nextUrl.searchParams.get('connectorId') ?? ''
  const queryId     = req.nextUrl.searchParams.get('queryId')     ?? ''
  if (!connectorId || !queryId) {
    return NextResponse.json(
      { error: 'connectorId and queryId required' },
      { status: 400 },
    )
  }
  const ok = deleteSavedQuery(connectorId, queryId)
  return NextResponse.json({ ok })
}
