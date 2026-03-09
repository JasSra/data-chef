import { NextRequest, NextResponse } from 'next/server'
import { getConnector, getConnectorRuntimeConfig, setConnectorRuntimeConfig } from '@/lib/connectors'
import { fetchRedisCatalog, type RedisCatalogKind } from '@/lib/redis'

export const dynamic = 'force-dynamic'

export async function GET(req: NextRequest) {
  const connectorId = req.nextUrl.searchParams.get('connectorId') ?? ''
  const catalog = (req.nextUrl.searchParams.get('catalog') ?? 'commands').toLowerCase() as RedisCatalogKind
  const pattern = req.nextUrl.searchParams.get('pattern') ?? undefined
  const prefix = req.nextUrl.searchParams.get('prefix') ?? undefined
  const limit = Number(req.nextUrl.searchParams.get('limit') ?? 50)

  if (!connectorId) return NextResponse.json({ error: 'connectorId is required' }, { status: 400 })

  const connector = getConnector(connectorId)
  if (!connector) return NextResponse.json({ error: `Unknown connector "${connectorId}"` }, { status: 404 })
  if (connector.type !== 'redis') return NextResponse.json({ error: `Connector "${connector.name}" is not a Redis connector` }, { status: 400 })

  const runtimeConfig = getConnectorRuntimeConfig(connectorId)
  if (!runtimeConfig) {
    return NextResponse.json({ error: `No runtime config found for connector "${connector.name}"` }, { status: 404 })
  }

  try {
    const result = await fetchRedisCatalog(runtimeConfig, { catalog, pattern, prefix, limit })
    if (result.capabilities) {
      setConnectorRuntimeConfig(connectorId, {
        ...runtimeConfig,
        capabilitySnapshot: { ...result.capabilities },
      })
    }
    return NextResponse.json(result)
  } catch (error) {
    return NextResponse.json({ error: error instanceof Error ? error.message : String(error) }, { status: 400 })
  }
}
