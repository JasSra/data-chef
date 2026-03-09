import { NextRequest, NextResponse } from 'next/server'
import { getAppSettings, resetAppSettings, rotateAppSecret, saveAppSettings } from '@/lib/app-settings'
import { clearConnectors } from '@/lib/connectors'
import { clearDatasets } from '@/lib/datasets'
import { clearDiscoveryCandidates } from '@/lib/network-discovery'
import { clearPipelines } from '@/lib/pipelines'
import { clearSavedQueries } from '@/lib/saved-queries'

export const dynamic = 'force-dynamic'

export function GET() {
  return NextResponse.json(getAppSettings())
}

export async function PUT(req: NextRequest) {
  const body = await req.json()
  return NextResponse.json(saveAppSettings(body))
}

export async function POST(req: NextRequest) {
  const body = await req.json()
  const action = String(body.action ?? '')

  if (action === 'rotate-key') {
    const key = String(body.key ?? '')
    if (key !== 'ingestKey' && key !== 'queryKey' && key !== 'webhookSecret') {
      return NextResponse.json({ error: 'Invalid key' }, { status: 400 })
    }
    return NextResponse.json(rotateAppSecret(key))
  }

  if (action === 'purge-data') {
    clearDatasets()
    return NextResponse.json({ ok: true })
  }

  if (action === 'delete-workspace') {
    clearDatasets()
    clearConnectors()
    clearDiscoveryCandidates()
    clearPipelines()
    clearSavedQueries()
    return NextResponse.json(resetAppSettings())
  }

  return NextResponse.json({ error: 'Unsupported action' }, { status: 400 })
}
