import { NextRequest, NextResponse } from 'next/server'
import { getDiscoveryCandidate, setDiscoveryCandidateStatus } from '@/lib/network-discovery'

export const dynamic = 'force-dynamic'

export function GET(_req: NextRequest, context: { params: Promise<{ id: string }> }) {
  return context.params.then(({ id }) => {
    const candidate = getDiscoveryCandidate(id)
    if (!candidate) {
      return NextResponse.json({ error: 'Discovery candidate not found' }, { status: 404 })
    }
    return NextResponse.json(candidate)
  })
}

export async function PATCH(req: NextRequest, context: { params: Promise<{ id: string }> }) {
  const { id } = await context.params
  const body = await req.json().catch(() => ({})) as { status?: string }
  const status = body.status === 'dismissed' ? 'dismissed' : body.status === 'new' ? 'new' : null
  if (!status) {
    return NextResponse.json({ error: 'status must be "new" or "dismissed"' }, { status: 400 })
  }

  const candidate = setDiscoveryCandidateStatus(id, status)
  if (!candidate) {
    return NextResponse.json({ error: 'Discovery candidate not found' }, { status: 404 })
  }

  return NextResponse.json(candidate)
}
