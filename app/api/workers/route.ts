import { NextResponse } from 'next/server'
import { getWorkerState } from '@/lib/pipelines'
import { ensureConnectorSchedulerStarted } from '@/lib/connector-sync'
import { ensureNetworkDiscoverySchedulerStarted } from '@/lib/network-discovery'

export const dynamic = 'force-dynamic'

export function GET() {
  ensureConnectorSchedulerStarted()
  ensureNetworkDiscoverySchedulerStarted()
  return NextResponse.json(getWorkerState())
}
