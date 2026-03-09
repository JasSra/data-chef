import { NextRequest, NextResponse } from 'next/server'
import { searchDocuments } from '@/lib/search'

export const dynamic = 'force-dynamic'

export function GET(req: NextRequest) {
  const query = req.nextUrl.searchParams.get('q') ?? ''
  const limit = Math.min(50, Math.max(1, Number(req.nextUrl.searchParams.get('limit') ?? 20)))
  return NextResponse.json({
    query,
    groups: searchDocuments(query, limit),
  })
}
