import { NextRequest, NextResponse } from 'next/server'
import { ensureNetworkDiscoverySchedulerStarted, getDiscoveryOverview, runNetworkDiscoveryScan } from '@/lib/network-discovery'

export const dynamic = 'force-dynamic'

export function GET(req: NextRequest) {
  ensureNetworkDiscoverySchedulerStarted()
  const includeDismissed = req.nextUrl.searchParams.get('includeDismissed') === 'true'
  const includeAdded = req.nextUrl.searchParams.get('includeAdded') === 'true'
  return NextResponse.json(getDiscoveryOverview({ includeDismissed, includeAdded }))
}

export async function POST() {
  ensureNetworkDiscoverySchedulerStarted()
  const result = await runNetworkDiscoveryScan({ force: true })
  return NextResponse.json({
    ...getDiscoveryOverview({ includeDismissed: true }),
    scan: result,
  })
}
