import 'server-only'

export type MqttQueryMode = 'subscribe' | 'catalog'
export type MqttCatalogKind = 'topics' | 'subscriptions' | 'status'

export interface MqttConnectionConfig {
  connectionMode?: 'fields' | 'connectionString'
  connectionString?: string
  host?: string
  port?: number
  clientId?: string
  username?: string
  password?: string
  tls?: boolean
  keepalive?: number
  protocol?: 'mqtt' | 'mqtts' | 'ws' | 'wss'
  defaultTopic?: string
}

export interface MqttQueryResult {
  columns: string[]
  rows: string[][]
  rowCount: number
  totalRows: number
  durationMs: number
  mqttMode: MqttQueryMode
  error?: string
}

function normaliseCfg(input: Record<string, unknown>): MqttConnectionConfig {
  return {
    connectionMode: (input.connectionMode as MqttConnectionConfig['connectionMode']) ?? 'fields',
    connectionString: input.connectionString as string | undefined,
    host: (input.host as string | undefined) ?? 'localhost',
    port: Number(input.port ?? 1883),
    clientId: (input.clientId as string | undefined) ?? `datachef-${Math.random().toString(16).slice(2, 8)}`,
    username: input.username as string | undefined,
    password: input.password as string | undefined,
    tls: Boolean(input.tls ?? false),
    keepalive: Number(input.keepalive ?? 60),
    protocol: (input.protocol as MqttConnectionConfig['protocol']) ?? (input.tls ? 'mqtts' : 'mqtt'),
    defaultTopic: (input.defaultTopic as string | undefined) ?? '#',
  }
}

function buildMqttUrl(cfg: MqttConnectionConfig): string {
  if (cfg.connectionMode === 'connectionString' && cfg.connectionString) {
    return cfg.connectionString
  }
  const protocol = cfg.protocol ?? (cfg.tls ? 'mqtts' : 'mqtt')
  return `${protocol}://${cfg.host ?? 'localhost'}:${cfg.port ?? 1883}`
}

export async function probeMqttConnection(input: Record<string, unknown>): Promise<{
  ok: boolean; broker?: string; error?: string
}> {
  const cfg = normaliseCfg(input)
  const { connect } = await import('mqtt')
  return new Promise(resolve => {
    const timeout = setTimeout(() => {
      client.end(true)
      resolve({ ok: false, error: 'Connection timed out' })
    }, 8_000)

    const client = connect(buildMqttUrl(cfg), {
      clientId: cfg.clientId,
      username: cfg.username,
      password: cfg.password,
      keepalive: cfg.keepalive ?? 60,
      connectTimeout: 7_000,
      reconnectPeriod: 0,
    })

    client.on('connect', (connack) => {
      clearTimeout(timeout)
      client.end(true)
      resolve({ ok: true, broker: buildMqttUrl(cfg) })
    })

    client.on('error', (err) => {
      clearTimeout(timeout)
      client.end(true)
      resolve({ ok: false, error: err.message })
    })
  })
}

// Subscribe to a topic for a short window and collect messages
export async function subscribeMqttTopic(
  input: Record<string, unknown>,
  options: { topic?: string; windowMs?: number; limit?: number },
): Promise<MqttQueryResult> {
  const t0 = performance.now()
  const cfg = normaliseCfg(input)
  const topic = options.topic ?? cfg.defaultTopic ?? '#'
  const windowMs = Math.min(options.windowMs ?? 5_000, 15_000)
  const limit = options.limit ?? 200

  const { connect } = await import('mqtt')
  const messages: Array<{ topic: string; qos: number; retained: boolean; payload: string; ts: string }> = []

  return new Promise(resolve => {
    const done = (error?: string) => {
      try { client.end(true) } catch {}
      const columns = ['topic', 'qos', 'retained', 'ts', 'payload']
      const rows = messages.slice(0, limit).map(m =>
        [m.topic, String(m.qos), String(m.retained), m.ts, m.payload]
      )
      resolve({
        columns, rows, rowCount: rows.length, totalRows: messages.length,
        durationMs: Math.round(performance.now() - t0),
        mqttMode: 'subscribe',
        error,
      })
    }

    const deadline = setTimeout(() => done(), windowMs)

    const client = connect(buildMqttUrl(cfg), {
      clientId: cfg.clientId,
      username: cfg.username,
      password: cfg.password,
      keepalive: cfg.keepalive ?? 60,
      connectTimeout: 7_000,
      reconnectPeriod: 0,
    })

    client.on('connect', () => {
      client.subscribe(topic, { qos: 1 })
    })

    client.on('message', (t, payload, packet) => {
      messages.push({
        topic: t,
        qos: packet.qos,
        retained: packet.retain,
        payload: payload.toString(),
        ts: new Date().toISOString(),
      })
      if (messages.length >= limit) {
        clearTimeout(deadline)
        done()
      }
    })

    client.on('error', (err) => {
      clearTimeout(deadline)
      done(err.message)
    })
  })
}

export async function fetchMqttCatalog(
  input: Record<string, unknown>,
  options: { catalog: MqttCatalogKind; limit?: number },
): Promise<MqttQueryResult> {
  const t0 = performance.now()
  const cfg = normaliseCfg(input)

  if (options.catalog === 'status') {
    const probe = await probeMqttConnection(input)
    return {
      columns: ['key', 'value'],
      rows: [
        ['broker', buildMqttUrl(cfg)],
        ['status', probe.ok ? 'reachable' : 'unreachable'],
        ['error', probe.error ?? ''],
        ['default_topic', cfg.defaultTopic ?? '#'],
        ['tls', String(cfg.tls ?? false)],
        ['keepalive', String(cfg.keepalive ?? 60)],
      ],
      rowCount: 6, totalRows: 6,
      durationMs: Math.round(performance.now() - t0),
      mqttMode: 'catalog',
    }
  }

  // For 'topics' and 'subscriptions' — sample by subscribing to # briefly
  if (options.catalog === 'topics' || options.catalog === 'subscriptions') {
    const sample = await subscribeMqttTopic(input, { topic: '#', windowMs: 4_000, limit: options.limit ?? 200 })
    // Deduplicate topic names
    const seen = new Set<string>()
    const unique = sample.rows.filter(r => { const t = r[0]; if (seen.has(t)) return false; seen.add(t); return true })
    return {
      columns: ['topic', 'first_message', 'last_qos', 'retained'],
      rows: unique.map(r => [r[0], r[4], r[1], r[2]]),
      rowCount: unique.length, totalRows: unique.length,
      durationMs: Math.round(performance.now() - t0),
      mqttMode: 'catalog',
    }
  }

  return { columns: [], rows: [], rowCount: 0, totalRows: 0, durationMs: Math.round(performance.now() - t0), mqttMode: 'catalog', error: 'Unknown catalog type' }
}

export async function executeMqttQuery(
  input: Record<string, unknown>,
  options: { mode: MqttQueryMode; query?: string; catalog?: MqttCatalogKind; limit?: number; windowMs?: number },
): Promise<MqttQueryResult> {
  if (options.mode === 'catalog') {
    return fetchMqttCatalog(input, { catalog: options.catalog ?? 'topics', limit: options.limit })
  }
  return subscribeMqttTopic(input, { topic: options.query, windowMs: options.windowMs, limit: options.limit })
}
