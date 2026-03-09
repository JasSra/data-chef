import { NextResponse } from 'next/server'
import { buildDiscoveryDraft, getDiscoveryCandidate } from '@/lib/network-discovery'

export const dynamic = 'force-dynamic'

export async function POST(_req: Request, context: { params: Promise<{ id: string }> }) {
  const { id } = await context.params
  const candidate = getDiscoveryCandidate(id)
  if (!candidate) {
    return NextResponse.json({ error: 'Discovery candidate not found' }, { status: 404 })
  }
  if (candidate.status === 'added') {
    return NextResponse.json({ error: 'Discovery candidate already converted' }, { status: 409 })
  }
  return NextResponse.json(buildDiscoveryDraft(candidate))
}
