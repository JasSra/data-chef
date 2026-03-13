import 'server-only'
import { type NextRequest, NextResponse } from 'next/server'

export const runtime = 'nodejs'

function resolveConnector(id: string) {
  const { getConnector, getConnectorRuntimeConfig } = require('@/lib/connectors') as typeof import('@/lib/connectors')
  const connector = getConnector(id)
  if (!connector) return null
  const runtimeConfig = getConnectorRuntimeConfig(id)
  if (!runtimeConfig) return null
  return { type: connector.type, runtimeConfig }
}

interface PublishBody {
  connectorId: string
  topic?: string
  queue?: string
  exchange?: string
  routingKey?: string
  payload: string
  qos?: 0 | 1 | 2
  retain?: boolean
  persistent?: boolean
}

export async function POST(req: NextRequest) {
  const body = await req.json() as PublishBody
  const { connectorId, payload, topic, queue, exchange, routingKey, qos = 0, retain = false, persistent = true } = body

  if (!connectorId) return NextResponse.json({ error: 'connectorId required' }, { status: 400 })
  if (payload == null) return NextResponse.json({ error: 'payload required' }, { status: 400 })

  const connector = resolveConnector(connectorId)
  if (!connector) return NextResponse.json({ error: 'Connector not found' }, { status: 404 })

  const { type, runtimeConfig } = connector as { type: string; runtimeConfig: Record<string, unknown> }
  const t0 = performance.now()

  try {
    // ── MQTT ─────────────────────────────────────────────────────────────────
    if (type === 'mqtt') {
      if (!topic) return NextResponse.json({ error: 'topic required for MQTT publish' }, { status: 400 })

      const { connect } = await import('mqtt')
      const cfg = runtimeConfig

      function buildUrl(): string {
        if (cfg.connectionMode === 'connectionString' && cfg.connectionString) return String(cfg.connectionString)
        const protocol = String(cfg.protocol ?? (cfg.tls ? 'mqtts' : 'mqtt'))
        return `${protocol}://${cfg.host ?? 'localhost'}:${cfg.port ?? 1883}`
      }

      await new Promise<void>((resolve, reject) => {
        const client = connect(buildUrl(), {
          clientId: `datachef-pub-${Math.random().toString(16).slice(2, 8)}`,
          username: cfg.username as string | undefined,
          password: cfg.password as string | undefined,
          connectTimeout: 8_000,
          reconnectPeriod: 0,
        })

        const timeout = setTimeout(() => {
          client.end(true)
          reject(new Error('Connection timed out'))
        }, 12_000)

        client.on('connect', () => {
          client.publish(topic, payload, { qos: qos as 0 | 1 | 2, retain }, (err) => {
            clearTimeout(timeout)
            client.end(true)
            if (err) reject(err)
            else resolve()
          })
        })

        client.on('error', (err) => {
          clearTimeout(timeout)
          client.end(true)
          reject(err)
        })
      })

      return NextResponse.json({ ok: true, durationMs: Math.round(performance.now() - t0) })
    }

    // ── RabbitMQ ──────────────────────────────────────────────────────────────
    if (type === 'rabbitmq') {
      const target = queue ?? routingKey ?? ''
      if (!target && !exchange) return NextResponse.json({ error: 'queue or exchange required' }, { status: 400 })

      const amqp = await import('amqplib')
      const cfg = runtimeConfig

      function buildAmqpUrl(): string {
        if (cfg.connectionMode === 'connectionString' && cfg.connectionString) return String(cfg.connectionString)
        const scheme = cfg.tls ? 'amqps' : 'amqp'
        const vhost = encodeURIComponent(String(cfg.vhost ?? '/'))
        return `${scheme}://${cfg.username ?? 'guest'}:${encodeURIComponent(String(cfg.password ?? 'guest'))}@${cfg.host ?? 'localhost'}:${cfg.port ?? 5672}/${vhost}`
      }

      const conn = await amqp.connect(buildAmqpUrl())
      const ch = await conn.createChannel()
      try {
        const msgBuf = Buffer.from(payload)
        const props = {
          persistent,
          contentType: payload.trimStart().startsWith('{') || payload.trimStart().startsWith('[') ? 'application/json' : 'text/plain',
          timestamp: Math.floor(Date.now() / 1000),
        }
        if (exchange) {
          ch.publish(exchange, routingKey ?? '', msgBuf, props)
        } else {
          await ch.assertQueue(target, { durable: true })
          ch.sendToQueue(target, msgBuf, props)
        }
        await ch.close()
      } finally {
        await conn.close()
      }

      return NextResponse.json({ ok: true, durationMs: Math.round(performance.now() - t0) })
    }

    return NextResponse.json({ error: `Publish not supported for connector type: ${type}` }, { status: 400 })
  } catch (e) {
    return NextResponse.json({ error: e instanceof Error ? e.message : String(e) }, { status: 500 })
  }
}
