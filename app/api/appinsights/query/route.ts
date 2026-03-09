import { NextRequest, NextResponse } from 'next/server'

import { executeLiveObservabilityQuery } from '@/lib/observability'

export const dynamic = 'force-dynamic'

export async function POST(req: NextRequest) {
  let body: Record<string, unknown>
  try {
    body = await req.json() as Record<string, unknown>
  } catch {
    return NextResponse.json({ error: 'Bad Request' }, { status: 400 })
  }

  const connectorId = String(body.connectorId ?? '')
  const kql = String(body.kql ?? body.query ?? '')
  const timespan = body.timespan ? String(body.timespan) : undefined

  if (!connectorId || !kql.trim()) {
    return NextResponse.json(
      { error: 'connectorId and kql are required' },
      { status: 400 },
    )
  }

  const result = await executeLiveObservabilityQuery(connectorId, kql, { timespan })
  return NextResponse.json({
    ...result,
    kqlTranslated: result.translatedQuery,
    bytesScanned: 0,
  }, { status: result.error ? 400 : 200 })
}
