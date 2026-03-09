import 'server-only'

import Redis from 'ioredis'

export type RedisQueryMode = 'command' | 'search' | 'json' | 'timeseries' | 'stream' | 'catalog'
export type RedisValueType = 'auto' | 'string' | 'hash' | 'list' | 'set' | 'zset' | 'json' | 'timeseries' | 'stream' | 'search'
export type RedisCatalogKind = 'commands' | 'capabilities' | 'keyspaces' | 'keys' | 'indexes' | 'streams'

export interface RedisConnectionConfig {
  connectionMode?: 'fields' | 'connectionString'
  connectionString?: string
  host?: string
  port?: number
  username?: string
  password?: string
  database?: number
  tls?: boolean
  keyPrefix?: string
}

export interface RedisCapabilities {
  serverKind: 'redis' | 'redis-stack'
  redisVersion: string
  modules: string[]
  supportsSearch: boolean
  supportsJson: boolean
  supportsTimeSeries: boolean
  supportsBloom: boolean
  supportsGraph: boolean
  supportsStreams: boolean
  dbCount: number | null
}

export interface RedisCommandSpec {
  command: string
  summary: string
  group: string
  safety: 'read' | 'blocked'
  mode: RedisQueryMode | 'any'
  module?: 'search' | 'json' | 'timeseries'
  args?: string
  example: string
}

export interface RedisQueryResult {
  columns: string[]
  rows: string[][]
  rowCount: number
  totalRows: number
  durationMs: number
  redisMode: RedisQueryMode
  valueType?: RedisValueType
  capabilities?: RedisCapabilities
  catalogMeta?: Record<string, unknown>
  error?: string
}

type RedisResourceSpec = {
  mode?: RedisQueryMode
  valueType?: RedisValueType
  key?: string
  keyPattern?: string
  index?: string
  query?: string
  stream?: string
  group?: string
}

const BLOCKED_PREFIXES = [
  'append', 'bitop', 'blmove', 'blmpop', 'blpop', 'brpop', 'bzmpop', 'bzpop',
  'copy', 'decr', 'del', 'eval', 'evalsha', 'expire', 'expireat', 'flush',
  'geoadd', 'getdel', 'getex', 'hdel', 'hincr', 'hmset', 'hset', 'hsetnx',
  'incr', 'linsert', 'lmove', 'lmpop', 'lpop', 'lpush', 'lrem', 'lset', 'ltrim',
  'memory purge', 'migrate', 'move', 'mset', 'persist', 'pexpire', 'pexpireat',
  'pfadd', 'psetex', 'publish', 'rename', 'renamenx', 'restore', 'rpop',
  'rpush', 'sadd', 'script', 'sdiffstore', 'set', 'setbit', 'setex', 'setnx',
  'shutdown', 'smove', 'sort_ro', 'spop', 'srem', 'sunionstore', 'swapdb',
  'unlink', 'xack', 'xadd', 'xautoclaim', 'xclaim', 'xdel', 'xgroup create',
  'xgroup createconsumer', 'xgroup delconsumer', 'xgroup destroy', 'xtrim',
  'zadd', 'zincrby', 'zinterstore', 'zmpop', 'zpop', 'zrangestore', 'zrem',
  'zremrange', 'zunionstore',
  'acl', 'bgrewriteaof', 'bgsave', 'client kill', 'client pause', 'cluster',
  'config rewrite', 'config set', 'failover', 'latency reset', 'module load',
  'module unload', 'replicaof', 'slaveof',
]

const BASE_COMMAND_SPECS: RedisCommandSpec[] = [
  { command: 'SCAN', summary: 'Iterate keys safely', group: 'keys', safety: 'read', mode: 'command', args: 'cursor [MATCH pattern] [COUNT n]', example: 'SCAN 0 MATCH user:* COUNT 100' },
  { command: 'TYPE', summary: 'Inspect a key type', group: 'keys', safety: 'read', mode: 'command', args: 'key', example: 'TYPE user:42' },
  { command: 'EXISTS', summary: 'Check whether keys exist', group: 'keys', safety: 'read', mode: 'command', args: 'key [key ...]', example: 'EXISTS user:42' },
  { command: 'TTL', summary: 'Read key TTL in seconds', group: 'keys', safety: 'read', mode: 'command', args: 'key', example: 'TTL user:42' },
  { command: 'PTTL', summary: 'Read key TTL in milliseconds', group: 'keys', safety: 'read', mode: 'command', args: 'key', example: 'PTTL user:42' },
  { command: 'RANDOMKEY', summary: 'Fetch one random key', group: 'keys', safety: 'read', mode: 'command', example: 'RANDOMKEY' },
  { command: 'GET', summary: 'Read a string value', group: 'string', safety: 'read', mode: 'command', args: 'key', example: 'GET user:42:name' },
  { command: 'MGET', summary: 'Read multiple string values', group: 'string', safety: 'read', mode: 'command', args: 'key [key ...]', example: 'MGET user:1:name user:2:name' },
  { command: 'HGETALL', summary: 'Read all hash fields', group: 'hash', safety: 'read', mode: 'command', args: 'key', example: 'HGETALL user:42' },
  { command: 'HMGET', summary: 'Read selected hash fields', group: 'hash', safety: 'read', mode: 'command', args: 'key field [field ...]', example: 'HMGET user:42 name email' },
  { command: 'LRANGE', summary: 'Read a list range', group: 'list', safety: 'read', mode: 'command', args: 'key start stop', example: 'LRANGE queue:emails 0 50' },
  { command: 'SMEMBERS', summary: 'Read all set members', group: 'set', safety: 'read', mode: 'command', args: 'key', example: 'SMEMBERS team:42:members' },
  { command: 'ZRANGE', summary: 'Read sorted-set members', group: 'zset', safety: 'read', mode: 'command', args: 'key start stop [WITHSCORES]', example: 'ZRANGE leaderboard 0 25 WITHSCORES' },
  { command: 'XRANGE', summary: 'Read stream entries', group: 'stream', safety: 'read', mode: 'stream', args: 'key start end [COUNT n]', example: 'XRANGE orders:stream - + COUNT 50' },
  { command: 'XREAD', summary: 'Read from one or more streams', group: 'stream', safety: 'read', mode: 'stream', args: '[COUNT n] STREAMS key id', example: 'XREAD COUNT 50 STREAMS orders:stream 0-0' },
  { command: 'XINFO STREAM', summary: 'Inspect stream metadata', group: 'stream', safety: 'read', mode: 'stream', args: 'key', example: 'XINFO STREAM orders:stream' },
  { command: 'XINFO GROUPS', summary: 'Inspect stream groups', group: 'stream', safety: 'read', mode: 'stream', args: 'key', example: 'XINFO GROUPS orders:stream' },
  { command: 'INFO', summary: 'Inspect server info', group: 'server', safety: 'read', mode: 'command', args: '[section]', example: 'INFO server' },
  { command: 'ROLE', summary: 'Inspect replication role', group: 'server', safety: 'read', mode: 'command', example: 'ROLE' },
  { command: 'MEMORY STATS', summary: 'Inspect memory statistics', group: 'server', safety: 'read', mode: 'command', example: 'MEMORY STATS' },
  { command: 'LATENCY LATEST', summary: 'Inspect latency events', group: 'server', safety: 'read', mode: 'command', example: 'LATENCY LATEST' },
  { command: 'PUBSUB CHANNELS', summary: 'Inspect pub/sub channels', group: 'pubsub', safety: 'read', mode: 'command', example: 'PUBSUB CHANNELS *' },
  { command: 'PUBSUB NUMSUB', summary: 'Inspect pub/sub subscriber counts', group: 'pubsub', safety: 'read', mode: 'command', example: 'PUBSUB NUMSUB jobs updates' },
  { command: 'FT.SEARCH', summary: 'Run a RediSearch query', group: 'search', safety: 'read', mode: 'search', module: 'search', args: 'index query [LIMIT offset num]', example: 'FT.SEARCH idx:users "@email:*@example.com" LIMIT 0 25' },
  { command: 'FT._LIST', summary: 'List RediSearch indexes', group: 'search', safety: 'read', mode: 'search', module: 'search', example: 'FT._LIST' },
  { command: 'JSON.GET', summary: 'Read a JSON document', group: 'json', safety: 'read', mode: 'json', module: 'json', args: 'key [path]', example: 'JSON.GET user:42 $' },
  { command: 'JSON.MGET', summary: 'Read multiple JSON documents', group: 'json', safety: 'read', mode: 'json', module: 'json', args: 'key [key ...] path', example: 'JSON.MGET user:1 user:2 $' },
  { command: 'TS.RANGE', summary: 'Read a time series range', group: 'timeseries', safety: 'read', mode: 'timeseries', module: 'timeseries', args: 'key fromTimestamp toTimestamp', example: 'TS.RANGE cpu:host1 - +' },
  { command: 'TS.MRANGE', summary: 'Read multiple time series by filter', group: 'timeseries', safety: 'read', mode: 'timeseries', module: 'timeseries', args: 'fromTimestamp toTimestamp FILTER label=value', example: 'TS.MRANGE - + FILTER env=prod' },
]

export function getRedisCommandSpecs(capabilities?: RedisCapabilities | null): RedisCommandSpec[] {
  return BASE_COMMAND_SPECS.filter(spec => {
    if (!capabilities || !spec.module) return true
    if (spec.module === 'search') return capabilities.supportsSearch
    if (spec.module === 'json') return capabilities.supportsJson
    if (spec.module === 'timeseries') return capabilities.supportsTimeSeries
    return true
  })
}

export function normaliseRedisConfig(input: Record<string, unknown>): RedisConnectionConfig {
  const connectionMode = input.connectionMode === 'connectionString' ? 'connectionString' : 'fields'
  if (connectionMode === 'connectionString') {
    const raw = String(input.connectionString ?? '').trim()
    if (!raw) throw new Error('Redis connection string is required')
    let parsed: URL
    try {
      parsed = new URL(raw)
    } catch {
      throw new Error('Invalid Redis connection string')
    }
    if (parsed.protocol !== 'redis:' && parsed.protocol !== 'rediss:') {
      throw new Error('Redis connection string must start with redis:// or rediss://')
    }
    const dbFromPath = parsed.pathname.replace(/^\//, '')
    const dbFromQuery = parsed.searchParams.get('db')
    const tlsParam = parsed.searchParams.get('tls')
    return {
      connectionMode,
      connectionString: raw,
      host: parsed.hostname,
      port: parsed.port ? Number(parsed.port) : 6379,
      username: decodeURIComponent(parsed.username || ''),
      password: decodeURIComponent(parsed.password || ''),
      database: dbFromQuery != null
        ? Number(dbFromQuery)
        : dbFromPath && /^\d+$/.test(dbFromPath)
        ? Number(dbFromPath)
        : 0,
      tls: parsed.protocol === 'rediss:' || tlsParam === 'true',
    }
  }

  return {
    connectionMode,
    connectionString: String(input.connectionString ?? ''),
    host: String(input.host ?? '127.0.0.1'),
    port: Number(input.port ?? 6379),
    username: String(input.username ?? ''),
    password: String(input.password ?? ''),
    database: Number(input.database ?? 0),
    tls: Boolean(input.tls),
  }
}

function makeRedisClient(config: RedisConnectionConfig): Redis {
  if (!config.host) throw new Error('Redis host is required')
  return new Redis({
    host: config.host,
    port: config.port ?? 6379,
    username: config.username || undefined,
    password: config.password || undefined,
    db: config.database ?? 0,
    tls: config.tls ? {} : undefined,
    lazyConnect: true,
    maxRetriesPerRequest: 1,
    enableReadyCheck: true,
  })
}

export async function withRedisClient<T>(
  input: Record<string, unknown>,
  fn: (client: Redis, config: RedisConnectionConfig) => Promise<T>,
): Promise<T> {
  const config = normaliseRedisConfig(input)
  const client = makeRedisClient(config)
  try {
    await client.connect()
    return await fn(client, config)
  } finally {
    client.disconnect()
  }
}

function infoSection(text: string, section: string): string[] {
  const start = text.indexOf(`# ${section}`)
  if (start < 0) return []
  const next = text.indexOf('\n# ', start + 1)
  const block = next < 0 ? text.slice(start) : text.slice(start, next)
  return block.split('\n').map(line => line.trim()).filter(Boolean).filter(line => !line.startsWith('#'))
}

function parseFlatPairs(values: unknown[]): Record<string, string> {
  const obj: Record<string, string> = {}
  for (let i = 0; i < values.length; i += 2) {
    obj[String(values[i])] = values[i + 1] == null ? '' : String(values[i + 1])
  }
  return obj
}

function flattenObject(value: unknown, prefix = ''): Record<string, unknown> {
  if (value == null || typeof value !== 'object' || Array.isArray(value)) {
    return prefix ? { [prefix]: value } : { value }
  }
  const row: Record<string, unknown> = {}
  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    const path = prefix ? `${prefix}.${key}` : key
    if (child && typeof child === 'object' && !Array.isArray(child)) {
      Object.assign(row, flattenObject(child, path))
    } else {
      row[path] = Array.isArray(child) ? JSON.stringify(child) : child
    }
  }
  return row
}

function stringifyCell(value: unknown): string {
  if (value === null || value === undefined) return '∅'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function rowsFromObjects(items: Array<Record<string, unknown>>) {
  const columns = Array.from(new Set(items.flatMap(item => Object.keys(item))))
  const rows = items.map(item => columns.map(column => stringifyCell(item[column])))
  return { columns, rows }
}

function tokeniseRedisQuery(query: string): string[] {
  const tokens: string[] = []
  const pattern = /"((?:\\"|[^"])*)"|'((?:\\'|[^'])*)'|`([^`]*)`|([^\s]+)/g
  for (const match of query.matchAll(pattern)) {
    const token = match[1] ?? match[2] ?? match[3] ?? match[4]
    tokens.push(token.replace(/\\"/g, '"').replace(/\\'/g, "'"))
  }
  return tokens
}

function isBlockedRedisCommand(tokens: string[]): boolean {
  const low = tokens.map(token => token.toLowerCase())
  const joined = low.slice(0, 2).join(' ')
  return BLOCKED_PREFIXES.some(prefix => joined === prefix || low[0] === prefix)
}

async function tryCommand<T>(fn: () => Promise<T>, fallback: T): Promise<T> {
  try {
    return await fn()
  } catch {
    return fallback
  }
}

async function detectCapabilities(client: Redis): Promise<RedisCapabilities> {
  const info = await tryCommand(() => client.info(), '')
  const modulesRaw = await tryCommand(() => client.call('MODULE', 'LIST') as Promise<unknown[]>, [])
  const modules = Array.isArray(modulesRaw)
    ? modulesRaw.flatMap(entry => {
        if (!Array.isArray(entry)) return []
        const pairs = parseFlatPairs(entry)
        return pairs.name ? [pairs.name.toLowerCase()] : []
      })
    : []
  const serverLines = infoSection(info, 'Server')
  const keyspaceLines = infoSection(info, 'Keyspace')
  const version = serverLines.find(line => line.startsWith('redis_version:'))?.split(':')[1] ?? ''
  const dbCountRaw = await tryCommand(() => client.config('GET', 'databases') as Promise<string[]>, [])
  const dbCount = Array.isArray(dbCountRaw) && dbCountRaw[1] ? Number(dbCountRaw[1]) : keyspaceLines.length || null
  const has = (name: string) => modules.includes(name)
  return {
    serverKind: has('search') || has('rejson') || has('timeseries') ? 'redis-stack' : 'redis',
    redisVersion: version,
    modules,
    supportsSearch: has('search'),
    supportsJson: has('rejson'),
    supportsTimeSeries: has('timeseries'),
    supportsBloom: has('bf'),
    supportsGraph: has('graph'),
    supportsStreams: true,
    dbCount: Number.isFinite(dbCount) ? dbCount : null,
  }
}

export async function probeRedisCapabilities(input: Record<string, unknown>): Promise<RedisCapabilities> {
  return withRedisClient(input, detectCapabilities)
}

async function scanKeys(client: Redis, pattern: string, limit: number): Promise<string[]> {
  const keys: string[] = []
  let cursor = '0'
  do {
    const reply = await client.scan(cursor, 'MATCH', pattern, 'COUNT', Math.min(Math.max(limit * 2, 50), 500))
    cursor = reply[0]
    keys.push(...reply[1])
  } while (cursor !== '0' && keys.length < limit)
  return keys.slice(0, limit)
}

async function readKeyRow(client: Redis, key: string, explicitType: RedisValueType): Promise<Record<string, unknown>[]> {
  const detectedType = explicitType !== 'auto'
    ? explicitType
    : (() => null)()
  const rawType = detectedType ?? ((await client.type(key)) as RedisValueType)
  const ttl = await tryCommand(() => client.ttl(key), -1)

  if (rawType === 'string') {
    return [{ key, value: await client.get(key), ttl, type: 'string' }]
  }
  if (rawType === 'hash') {
    const value = await client.hgetall(key)
    return [{ key, ttl, type: 'hash', ...value }]
  }
  if (rawType === 'list') {
    const values = await client.lrange(key, 0, 49)
    return values.map((value, index) => ({ key, index, value, ttl, type: 'list' }))
  }
  if (rawType === 'set') {
    const values = await client.smembers(key)
    return values.slice(0, 100).map(value => ({ key, value, ttl, type: 'set' }))
  }
  if (rawType === 'zset') {
    const values = await client.zrange(key, 0, 49, 'WITHSCORES')
    const rows: Record<string, unknown>[] = []
    for (let i = 0; i < values.length; i += 2) {
      rows.push({ key, member: values[i], score: Number(values[i + 1] ?? 0), ttl, type: 'zset' })
    }
    return rows
  }
  if (rawType === 'stream') {
    const values = await client.xrange(key, '-', '+', 'COUNT', 50) as Array<[string, string[]]>
    return values.map(([id, fields]) => ({ key, id, ttl, type: 'stream', ...parseFlatPairs(fields) }))
  }
  if (rawType === 'json') {
    const raw = await client.call('JSON.GET', key, '$') as string | null
    const parsed = raw ? JSON.parse(raw) : null
    const value = Array.isArray(parsed) ? parsed[0] : parsed
    return [{ key, ttl, type: 'json', ...flattenObject(value) }]
  }
  if (rawType === 'timeseries') {
    const values = await client.call('TS.RANGE', key, '-', '+') as Array<[number, string]>
    return values.slice(0, 100).map(([timestamp, value]) => ({ key, timestamp, value: Number(value), ttl, type: 'timeseries' }))
  }
  return [{ key, ttl, type: rawType, value: await client.dump(key) }]
}

function parseRedisResource(resource?: string, fallback?: Record<string, unknown>): RedisResourceSpec {
  const text = String(resource ?? '').trim()
  if (text.startsWith('{')) {
    try {
      return JSON.parse(text) as RedisResourceSpec
    } catch {
      return { keyPattern: text }
    }
  }
  return {
    mode: typeof fallback?.defaultQueryMode === 'string' ? fallback.defaultQueryMode as RedisQueryMode : undefined,
    valueType: typeof fallback?.defaultValueType === 'string' ? fallback.defaultValueType as RedisValueType : undefined,
    keyPattern: text || String(fallback?.defaultKeyPattern ?? '*'),
    index: typeof fallback?.defaultSearchIndex === 'string' ? String(fallback.defaultSearchIndex) : undefined,
  }
}

export async function sampleRedisRowsFromConfig(
  input: Record<string, unknown>,
  resource: string | undefined,
  rowLimit: number,
): Promise<Record<string, unknown>[]> {
  return withRedisClient(input, async client => {
    const spec = parseRedisResource(resource, input)
    const mode = spec.mode ?? 'command'
    if (mode === 'search') {
      const index = spec.index || String(input.defaultSearchIndex ?? '')
      if (!index) throw new Error('Redis search sampling requires an index')
      const raw = await client.call('FT.SEARCH', index, spec.query ?? '*', 'LIMIT', '0', String(Math.max(1, rowLimit))) as unknown[]
      const hits = parseFtSearchResult(raw)
      return hits
    }
    const keyPattern = spec.key || spec.keyPattern || String(input.defaultKeyPattern ?? '*')
    const keys = keyPattern.includes('*') || keyPattern.includes('?')
      ? await scanKeys(client, keyPattern, rowLimit)
      : [keyPattern]
    const rows: Record<string, unknown>[] = []
    for (const key of keys) {
      rows.push(...await readKeyRow(client, key, spec.valueType ?? 'auto'))
      if (rows.length >= rowLimit) break
    }
    return rows.slice(0, rowLimit)
  })
}

function parseFtSearchResult(raw: unknown): Record<string, unknown>[] {
  if (!Array.isArray(raw) || raw.length === 0) return []
  const rows: Record<string, unknown>[] = []
  for (let i = 1; i < raw.length; i += 2) {
    const id = raw[i]
    const payload = raw[i + 1]
    if (Array.isArray(payload)) {
      rows.push({ _id: String(id), ...parseFlatPairs(payload) })
      continue
    }
    if (payload && typeof payload === 'object') {
      rows.push({ _id: String(id), ...(payload as Record<string, unknown>) })
      continue
    }
    rows.push({ _id: String(id), value: payload })
  }
  return rows
}

function parseXInfoGroups(raw: unknown): Record<string, unknown>[] {
  if (!Array.isArray(raw)) return []
  return raw.map(entry => Array.isArray(entry) ? parseFlatPairs(entry) : {})
}

function parseCommandResult(command: string, raw: unknown): Array<Record<string, unknown>> {
  const upper = command.toUpperCase()
  if (upper === 'SCAN' && Array.isArray(raw)) {
    const cursor = raw[0]
    const keys = Array.isArray(raw[1]) ? raw[1] : []
    return (keys as unknown[]).map(key => ({ cursor, key: String(key) }))
  }
  if (upper === 'HGETALL' && Array.isArray(raw)) {
    return [{ ...parseFlatPairs(raw) }]
  }
  if (upper === 'ZRANGE' && Array.isArray(raw)) {
    if (raw.length > 0 && raw.length % 2 === 0) {
      const rows: Record<string, unknown>[] = []
      for (let i = 0; i < raw.length; i += 2) rows.push({ member: String(raw[i]), score: Number(raw[i + 1] ?? 0) })
      return rows
    }
    return (raw as unknown[]).map(value => ({ value }))
  }
  if (upper === 'XRANGE' && Array.isArray(raw)) {
    return (raw as Array<[string, string[]]>).map(([id, fields]) => ({ id, ...parseFlatPairs(fields) }))
  }
  if (upper === 'XINFO' && Array.isArray(raw)) {
    return raw.length > 0 && Array.isArray(raw[0])
      ? parseXInfoGroups(raw)
      : [{ ...parseFlatPairs(raw) }]
  }
  if (upper === 'JSON.GET') {
    const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw
    const payload = Array.isArray(parsed) ? parsed[0] : parsed
    return [{ ...flattenObject(payload) }]
  }
  if (upper.startsWith('FT.')) {
    return parseFtSearchResult(raw)
  }
  if (upper.startsWith('TS.') && Array.isArray(raw)) {
    return (raw as unknown[]).flatMap(entry => {
      if (!Array.isArray(entry)) return []
      if (entry.length >= 3 && Array.isArray(entry[2])) {
        return (entry[2] as Array<[number, string]>).map(([timestamp, value]) => ({
          key: String(entry[0]),
          timestamp,
          value: Number(value),
        }))
      }
      if (entry.length === 2) {
        return [{ timestamp: Number(entry[0]), value: Number(entry[1]) }]
      }
      return []
    })
  }
  if (Array.isArray(raw)) {
    if (raw.length > 0 && raw.every(item => typeof item !== 'object')) {
      return raw.map((value, index) => ({ index, value }))
    }
    return raw as Array<Record<string, unknown>>
  }
  if (raw && typeof raw === 'object') return [raw as Record<string, unknown>]
  return [{ value: raw }]
}

export async function executeRedisQuery(
  input: Record<string, unknown>,
  options: {
    mode?: RedisQueryMode
    query: string
    valueType?: RedisValueType
    rowLimit?: number
  },
): Promise<RedisQueryResult> {
  const started = performance.now()
  return withRedisClient(input, async client => {
    const capabilities = await detectCapabilities(client)
    const mode = options.mode ?? 'command'
    const rowLimit = Math.max(1, Math.min(1000, options.rowLimit ?? 100))
    const tokens = tokeniseRedisQuery(options.query)
    if (tokens.length === 0) throw new Error('Empty Redis query')
    if (isBlockedRedisCommand(tokens)) {
      throw new Error(`Blocked Redis command: ${tokens.slice(0, 2).join(' ').toUpperCase()}`)
    }
    const command = tokens[0].toUpperCase()
    if ((mode === 'search' || command.startsWith('FT.')) && !capabilities.supportsSearch) {
      throw new Error('RediSearch is not available on this server')
    }
    if ((mode === 'json' || command.startsWith('JSON.')) && !capabilities.supportsJson) {
      throw new Error('RedisJSON is not available on this server')
    }
    if ((mode === 'timeseries' || command.startsWith('TS.')) && !capabilities.supportsTimeSeries) {
      throw new Error('RedisTimeSeries is not available on this server')
    }
    const raw = await client.call(tokens[0], ...tokens.slice(1))
    const items = parseCommandResult(command, raw).slice(0, rowLimit)
    const { columns, rows } = rowsFromObjects(items)
    return {
      columns,
      rows,
      rowCount: rows.length,
      totalRows: rows.length,
      durationMs: Math.round(performance.now() - started),
      redisMode: mode,
      valueType: options.valueType,
      capabilities,
    }
  })
}

export async function fetchRedisCatalog(
  input: Record<string, unknown>,
  options: {
    catalog: RedisCatalogKind
    pattern?: string
    prefix?: string
    limit?: number
  },
): Promise<RedisQueryResult> {
  const started = performance.now()
  return withRedisClient(input, async client => {
    const capabilities = await detectCapabilities(client)
    const limit = Math.max(1, Math.min(200, options.limit ?? 50))
    let items: Record<string, unknown>[] = []
    let meta: Record<string, unknown> = {}

    if (options.catalog === 'commands') {
      items = getRedisCommandSpecs(capabilities)
        .filter(spec => !options.prefix || spec.command.toLowerCase().startsWith(options.prefix.toLowerCase()))
        .slice(0, limit)
        .map(spec => ({ ...spec }))
      meta = { count: items.length }
    } else if (options.catalog === 'capabilities') {
      items = [
        { key: 'serverKind', value: capabilities.serverKind },
        { key: 'redisVersion', value: capabilities.redisVersion },
        { key: 'modules', value: capabilities.modules.join(', ') || 'none' },
        { key: 'supportsSearch', value: capabilities.supportsSearch },
        { key: 'supportsJson', value: capabilities.supportsJson },
        { key: 'supportsTimeSeries', value: capabilities.supportsTimeSeries },
        { key: 'dbCount', value: capabilities.dbCount ?? 'unknown' },
      ]
      meta = { capabilities }
    } else if (options.catalog === 'keyspaces') {
      const info = await client.info('keyspace')
      items = infoSection(info, 'Keyspace').map(line => {
        const [db, rest] = line.split(':', 2)
        return { db, stats: rest }
      })
    } else if (options.catalog === 'keys') {
      const keys = await scanKeys(client, options.pattern || '*', limit)
      items = await Promise.all(keys.map(async key => ({ key, type: await client.type(key), ttl: await tryCommand(() => client.ttl(key), -1) })))
      meta = { pattern: options.pattern || '*' }
    } else if (options.catalog === 'indexes') {
      if (!capabilities.supportsSearch) throw new Error('RediSearch is not available on this server')
      const raw = await client.call('FT._LIST') as unknown[]
      items = (Array.isArray(raw) ? raw : []).slice(0, limit).map(index => ({ index }))
    } else if (options.catalog === 'streams') {
      const keys = await scanKeys(client, options.pattern || '*', Math.min(limit * 2, 200))
      const streamKeys: Record<string, unknown>[] = []
      for (const key of keys) {
        if (await client.type(key) !== 'stream') continue
        const groups = await tryCommand(() => client.call('XINFO', 'GROUPS', key) as Promise<unknown>, [])
        streamKeys.push({ key, groups: parseXInfoGroups(groups).length })
        if (streamKeys.length >= limit) break
      }
      items = streamKeys
    }

    const { columns, rows } = rowsFromObjects(items)
    return {
      columns,
      rows,
      rowCount: rows.length,
      totalRows: rows.length,
      durationMs: Math.round(performance.now() - started),
      redisMode: 'catalog',
      capabilities,
      catalogMeta: meta,
    }
  })
}
