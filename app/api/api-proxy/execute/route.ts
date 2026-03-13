/**
 * POST /api/api-proxy/execute — execute an ApiQL query through the server-side proxy.
 */

import { NextRequest, NextResponse } from 'next/server'
import { parseApiQL } from '@/lib/api-query-parser'
import { executeApiQuery } from '@/lib/api-query-executor'

export const dynamic = 'force-dynamic'

export async function POST(req: NextRequest) {
  let body: Record<string, unknown>
  try { body = await req.json() } catch {
    return NextResponse.json({ error: 'Bad Request' }, { status: 400 })
  }

  const query = String(body.query ?? '').trim()
  if (!query) return NextResponse.json({ error: 'query is required' }, { status: 400 })

  // Parse the query
  const parseResult = parseApiQL(query)
  if (parseResult.error || !parseResult.ast) {
    return NextResponse.json({
      error: parseResult.error ?? 'Parse error',
      errorPos: parseResult.errorPos,
    }, { status: 400 })
  }

  // Execute
  try {
    const result = await executeApiQuery(parseResult.ast)
    return NextResponse.json(result)
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e)
    return NextResponse.json({ error: msg }, { status: 500 })
  }
}
