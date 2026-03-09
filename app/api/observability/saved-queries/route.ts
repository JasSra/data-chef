import { NextRequest, NextResponse } from 'next/server'
import { addSavedQuery, deleteSavedQuery, getSavedQueries } from '@/lib/saved-queries'

export const dynamic = 'force-dynamic'

export function GET(req: NextRequest) {
  const connectorId = req.nextUrl.searchParams.get('connectorId') ?? ''
  if (!connectorId) return NextResponse.json({ error: 'connectorId required' }, { status: 400 })
  return NextResponse.json(getSavedQueries(connectorId))
}

export async function POST(req: NextRequest) {
  let body: { connectorId?: string; name?: string; kql?: string }
  try {
    body = await req.json() as { connectorId?: string; name?: string; kql?: string }
  } catch {
    return NextResponse.json({ error: 'Bad Request' }, { status: 400 })
  }

  const connectorId = String(body.connectorId ?? '')
  const name = String(body.name ?? '')
  const kql = String(body.kql ?? '')
  if (!connectorId || !name.trim() || !kql.trim()) {
    return NextResponse.json({ error: 'connectorId, name, and kql are required' }, { status: 400 })
  }

  return NextResponse.json(addSavedQuery(connectorId, name.trim(), kql.trim()), { status: 201 })
}

export async function DELETE(req: NextRequest) {
  const connectorId = req.nextUrl.searchParams.get('connectorId') ?? ''
  const queryId = req.nextUrl.searchParams.get('queryId') ?? ''
  if (!connectorId || !queryId) {
    return NextResponse.json({ error: 'connectorId and queryId required' }, { status: 400 })
  }
  return NextResponse.json({ ok: deleteSavedQuery(connectorId, queryId) })
}
