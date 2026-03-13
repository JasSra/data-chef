import 'server-only'

export type RabbitQueryMode = 'catalog' | 'browse' | 'publish'
export type RabbitCatalogKind = 'overview' | 'queues' | 'exchanges' | 'bindings' | 'vhosts' | 'connections'

export interface RabbitConnectionConfig {
  connectionMode?: 'fields' | 'connectionString' | 'management'
  connectionString?: string
  host?: string
  port?: number
  managementPort?: number
  vhost?: string
  username?: string
  password?: string
  tls?: boolean
  managementTls?: boolean
  prefetchCount?: number
}

export interface RabbitQueryResult {
  columns: string[]
  rows: string[][]
  rowCount: number
  totalRows: number
  durationMs: number
  rabbitMode: RabbitQueryMode
  error?: string
}

function formatValue(v: unknown): string {
  if (v === null || v === undefined) return ''
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

function normaliseCfg(input: Record<string, unknown>): RabbitConnectionConfig {
  return {
    connectionMode: (input.connectionMode as RabbitConnectionConfig['connectionMode']) ?? 'fields',
    connectionString: input.connectionString as string | undefined,
    host: (input.host as string | undefined) ?? 'localhost',
    port: Number(input.port ?? 5672),
    managementPort: Number(input.managementPort ?? 15672),
    vhost: (input.vhost as string | undefined) ?? '/',
    username: (input.username as string | undefined) ?? 'guest',
    password: (input.password as string | undefined) ?? 'guest',
    tls: Boolean(input.tls ?? false),
    managementTls: Boolean(input.managementTls ?? false),
    prefetchCount: Number(input.prefetchCount ?? 50),
  }
}

// Build management API base URL
function mgmtBase(cfg: RabbitConnectionConfig): string {
  const scheme = cfg.managementTls ? 'https' : 'http'
  return `${scheme}://${cfg.host ?? 'localhost'}:${cfg.managementPort ?? 15672}/api`
}

async function mgmtGet(cfg: RabbitConnectionConfig, path: string): Promise<unknown> {
  const creds = Buffer.from(`${cfg.username ?? 'guest'}:${cfg.password ?? 'guest'}`).toString('base64')
  const base = mgmtBase(cfg)
  const res = await fetch(`${base}${path}`, {
    headers: { Authorization: `Basic ${creds}`, Accept: 'application/json' },
    signal: AbortSignal.timeout(10_000),
  })
  if (!res.ok) throw new Error(`RabbitMQ management API ${path}: ${res.status} ${res.statusText}`)
  return res.json()
}

function objectsToTable(items: Array<Record<string, unknown>>, pick?: string[]): {
  columns: string[]; rows: string[][]
} {
  if (!items.length) return { columns: [], rows: [] }
  const allKeys = pick ?? Object.keys(items[0])
  const rows = items.map(item => allKeys.map(k => formatValue(item[k])))
  return { columns: allKeys, rows }
}

export async function probeRabbitConnection(input: Record<string, unknown>): Promise<{
  ok: boolean; version?: string; cluster?: string; error?: string
}> {
  const cfg = normaliseCfg(input)
  try {
    const overview = await mgmtGet(cfg, '/overview') as Record<string, unknown>
    return {
      ok: true,
      version: overview.rabbitmq_version as string,
      cluster: overview.cluster_name as string,
    }
  } catch (e) {
    // Fall back to amqplib AMQP probe
    try {
      const amqp = await import('amqplib')
      const url = buildAmqpUrl(cfg)
      const conn = await amqp.connect(url)
      await conn.close()
      return { ok: true }
    } catch (e2) {
      return { ok: false, error: e instanceof Error ? e.message : String(e2) }
    }
  }
}

function buildAmqpUrl(cfg: RabbitConnectionConfig): string {
  if (cfg.connectionMode === 'connectionString' && cfg.connectionString) {
    return cfg.connectionString
  }
  const scheme = cfg.tls ? 'amqps' : 'amqp'
  const vhost = encodeURIComponent(cfg.vhost ?? '/')
  return `${scheme}://${cfg.username ?? 'guest'}:${encodeURIComponent(cfg.password ?? 'guest')}@${cfg.host ?? 'localhost'}:${cfg.port ?? 5672}/${vhost}`
}

export async function fetchRabbitCatalog(
  input: Record<string, unknown>,
  options: { catalog: RabbitCatalogKind; limit?: number },
): Promise<RabbitQueryResult> {
  const t0 = performance.now()
  const cfg = normaliseCfg(input)
  const limit = options.limit ?? 500
  const vhostEnc = encodeURIComponent(cfg.vhost ?? '/')

  const pathMap: Record<RabbitCatalogKind, string> = {
    overview: '/overview',
    queues: `/queues/${vhostEnc}`,
    exchanges: `/exchanges/${vhostEnc}`,
    bindings: `/bindings/${vhostEnc}`,
    vhosts: '/vhosts',
    connections: '/connections',
  }

  const pickMap: Record<RabbitCatalogKind, string[]> = {
    overview: [],
    queues: ['name', 'vhost', 'durable', 'auto_delete', 'messages', 'messages_ready', 'messages_unacknowledged', 'consumers', 'memory', 'state'],
    exchanges: ['name', 'vhost', 'type', 'durable', 'auto_delete', 'internal'],
    bindings: ['source', 'vhost', 'destination', 'destination_type', 'routing_key'],
    vhosts: ['name', 'description', 'tracing', 'messages', 'messages_ready'],
    connections: ['name', 'vhost', 'user', 'protocol', 'host', 'port', 'state', 'channel_max'],
  }

  try {
    let data = await mgmtGet(cfg, pathMap[options.catalog])

    if (options.catalog === 'overview') {
      // Flatten the overview object into key/value pairs
      const flat = Object.entries(data as Record<string, unknown>)
        .filter(([, v]) => typeof v !== 'object' || v === null)
        .map(([k, v]) => ({ key: k, value: formatValue(v) }))
      const { columns, rows } = objectsToTable(flat, ['key', 'value'])
      return { columns, rows, rowCount: rows.length, totalRows: rows.length, durationMs: Math.round(performance.now() - t0), rabbitMode: 'catalog' }
    }

    const items = (data as Array<Record<string, unknown>>).slice(0, limit)
    const { columns, rows } = objectsToTable(items, pickMap[options.catalog].length ? pickMap[options.catalog] : undefined)
    return { columns, rows, rowCount: rows.length, totalRows: (data as unknown[]).length, durationMs: Math.round(performance.now() - t0), rabbitMode: 'catalog' }
  } catch (e) {
    return { columns: [], rows: [], rowCount: 0, totalRows: 0, durationMs: Math.round(performance.now() - t0), rabbitMode: 'catalog', error: e instanceof Error ? e.message : String(e) }
  }
}

export async function browseRabbitQueue(
  input: Record<string, unknown>,
  options: { queue: string; count?: number; ackMode?: 'ack_requeue_true' | 'reject_requeue_true' },
): Promise<RabbitQueryResult> {
  const t0 = performance.now()
  const cfg = normaliseCfg(input)
  const vhostEnc = encodeURIComponent(cfg.vhost ?? '/')
  const count = Math.min(options.count ?? 50, 250)

  try {
    const creds = Buffer.from(`${cfg.username ?? 'guest'}:${cfg.password ?? 'guest'}`).toString('base64')
    const base = mgmtBase(cfg)
    const res = await fetch(`${base}/queues/${vhostEnc}/${encodeURIComponent(options.queue)}/get`, {
      method: 'POST',
      headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ count, ackmode: options.ackMode ?? 'ack_requeue_true', encoding: 'auto', truncate: 50000 }),
      signal: AbortSignal.timeout(15_000),
    })
    if (!res.ok) throw new Error(`Get messages: ${res.status} ${res.statusText}`)
    const messages = await res.json() as Array<Record<string, unknown>>
    const items = messages.map(m => ({
      routing_key: m.routing_key,
      exchange: m.exchange,
      redelivered: m.redelivered,
      payload_bytes: m.payload_bytes,
      message_count: m.message_count,
      payload: m.payload,
      content_type: (m.properties as Record<string, unknown>)?.content_type,
      delivery_mode: (m.properties as Record<string, unknown>)?.delivery_mode,
    }))
    const columns = ['routing_key', 'exchange', 'redelivered', 'payload_bytes', 'message_count', 'content_type', 'delivery_mode', 'payload']
    const rows = items.map(item => columns.map(col => formatValue((item as Record<string, unknown>)[col])))
    return { columns, rows, rowCount: rows.length, totalRows: rows.length, durationMs: Math.round(performance.now() - t0), rabbitMode: 'browse' }
  } catch (e) {
    return { columns: [], rows: [], rowCount: 0, totalRows: 0, durationMs: Math.round(performance.now() - t0), rabbitMode: 'browse', error: e instanceof Error ? e.message : String(e) }
  }
}

export async function executeRabbitQuery(
  input: Record<string, unknown>,
  options: { mode: RabbitQueryMode; query?: string; queue?: string; catalog?: RabbitCatalogKind; limit?: number },
): Promise<RabbitQueryResult> {
  if (options.mode === 'catalog') {
    return fetchRabbitCatalog(input, { catalog: options.catalog ?? 'queues', limit: options.limit })
  }
  if (options.mode === 'browse' && options.queue) {
    return browseRabbitQueue(input, { queue: options.queue, count: options.limit })
  }
  const t0 = performance.now()
  return { columns: [], rows: [], rowCount: 0, totalRows: 0, durationMs: Math.round(performance.now() - t0), rabbitMode: options.mode, error: 'Specify a queue to browse or use catalog mode' }
}
