import { NextRequest, NextResponse } from 'next/server'
import { ensureNetworkDiscoverySchedulerStarted, getGroupedDiscoveryCandidates } from '@/lib/network-discovery'

export const dynamic = 'force-dynamic'

export function GET(req: NextRequest) {
  ensureNetworkDiscoverySchedulerStarted()
  const includeDismissed = req.nextUrl.searchParams.get('includeDismissed') === 'true'
  const includeAdded = req.nextUrl.searchParams.get('includeAdded') === 'true'
  return NextResponse.json(getGroupedDiscoveryCandidates({ includeDismissed, includeAdded }))
}
