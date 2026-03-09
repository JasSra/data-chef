import { NextRequest, NextResponse } from 'next/server'
import { getConnector, getConnectorRuntimeConfig, setConnectorRuntimeConfig } from '@/lib/connectors'
import { executeRedisQuery, fetchRedisCatalog, probeRedisCapabilities, type RedisCatalogKind, type RedisQueryMode, type RedisValueType } from '@/lib/redis'

export const dynamic = 'force-dynamic'

function mergeCapabilities(connectorId: string, capabilities: object) {
  const config = getConnectorRuntimeConfig(connectorId) ?? {}
  setConnectorRuntimeConfig(connectorId, { ...config, capabilitySnapshot: { ...capabilities } })
}

export async function POST(req: NextRequest) {
  let body: Record<string, unknown>
  try {
    body = await req.json() as Record<string, unknown>
  } catch {
    return NextResponse.json({ error: 'Bad Request' }, { status: 400 })
  }

  const connectorId = String(body.connectorId ?? '')
  const mode = (String(body.mode ?? 'command').toLowerCase() as RedisQueryMode)
  const query = String(body.query ?? '')
  const valueType = body.valueType ? String(body.valueType).toLowerCase() as RedisValueType : undefined
  const rowLimit = Number(body.rowLimit ?? 100)
  const catalog = body.catalog ? String(body.catalog).toLowerCase() as RedisCatalogKind : undefined

  if (!connectorId) return NextResponse.json({ error: 'connectorId is required' }, { status: 400 })
  const connector = getConnector(connectorId)
  if (!connector) return NextResponse.json({ error: `Unknown connector "${connectorId}"` }, { status: 404 })
  if (connector.type !== 'redis') return NextResponse.json({ error: `Connector "${connector.name}" is not a Redis connector` }, { status: 400 })

  const runtimeConfig = getConnectorRuntimeConfig(connectorId)
  if (!runtimeConfig) {
    return NextResponse.json({ error: `No runtime config found for connector "${connector.name}"` }, { status: 404 })
  }

  try {
    const result = mode === 'catalog'
      ? await fetchRedisCatalog(runtimeConfig, {
          catalog: catalog ?? 'capabilities',
          pattern: body.resource ? String(body.resource) : undefined,
          prefix: body.prefix ? String(body.prefix) : undefined,
          limit: rowLimit,
        })
      : await executeRedisQuery(runtimeConfig, { mode, query, valueType, rowLimit })

    if (result.capabilities) mergeCapabilities(connectorId, result.capabilities)
    return NextResponse.json(result)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    return NextResponse.json({
      error: message,
      columns: [],
      rows: [],
      rowCount: 0,
      totalRows: 0,
      durationMs: 0,
      redisMode: mode,
    }, { status: 400 })
  }
}

export async function GET(req: NextRequest) {
  const connectorId = req.nextUrl.searchParams.get('connectorId') ?? ''
  if (!connectorId) return NextResponse.json({ error: 'connectorId is required' }, { status: 400 })
  const connector = getConnector(connectorId)
  if (!connector) return NextResponse.json({ error: `Unknown connector "${connectorId}"` }, { status: 404 })
  if (connector.type !== 'redis') return NextResponse.json({ error: `Connector "${connector.name}" is not a Redis connector` }, { status: 400 })
  const runtimeConfig = getConnectorRuntimeConfig(connectorId)
  if (!runtimeConfig) return NextResponse.json({ error: `No runtime config found for connector "${connector.name}"` }, { status: 404 })

  try {
    const capabilities = await probeRedisCapabilities(runtimeConfig)
    mergeCapabilities(connectorId, capabilities)
    return NextResponse.json(capabilities)
  } catch (error) {
    return NextResponse.json({ error: error instanceof Error ? error.message : String(error) }, { status: 400 })
  }
}
