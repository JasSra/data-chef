import os from 'node:os'

type EventRow = {
  id: number
  user_id: number
  event_type: string
  amount: number
  country: string
  device: string
  ts: string
}

const EVENT_TYPES = ['purchase', 'refund', 'signup', 'click', 'view', 'error']
const COUNTRIES = ['US', 'GB', 'DE', 'FR', 'JP', 'AU', 'CA', 'BR', 'IN', 'MX']
const DEVICES = ['mobile', 'desktop', 'tablet']

function h(n: number): number { return ((n >>> 0) * 2654435761) >>> 0 }

function fmtBytes(n: number): string {
  if (n >= 1024 ** 3) return `${(n / 1024 ** 3).toFixed(2)} GB`
  if (n >= 1024 ** 2) return `${(n / 1024 ** 2).toFixed(2)} MB`
  if (n >= 1024) return `${(n / 1024).toFixed(2)} KB`
  return `${n} B`
}

function snapshotMemory() {
  const m = process.memoryUsage()
  return {
    rss: m.rss,
    heapUsed: m.heapUsed,
    heapTotal: m.heapTotal,
    external: m.external,
  }
}

function diffMemory(before: ReturnType<typeof snapshotMemory>, after: ReturnType<typeof snapshotMemory>) {
  return {
    rss: after.rss - before.rss,
    heapUsed: after.heapUsed - before.heapUsed,
    heapTotal: after.heapTotal - before.heapTotal,
    external: after.external - before.external,
  }
}

function maybeGc() {
  if (global.gc) global.gc()
}

function generateEvents(count: number): EventRow[] {
  const BASE = 1_700_000_000_000
  return Array.from({ length: count }, (_, i) => {
    const a = h(i + 1), b = h(a), c = h(b), d = h(c)
    return {
      id: i + 1,
      user_id: (a % 10_000) + 1,
      event_type: EVENT_TYPES[b % EVENT_TYPES.length],
      amount: Math.round((c % 10_000) / 100 * 100) / 100,
      country: COUNTRIES[d % COUNTRIES.length],
      device: DEVICES[h(d) % DEVICES.length],
      ts: new Date(BASE + (a % 31_536_000) * 1000).toISOString().split('T')[0],
    }
  })
}

const QUERIES = [
  {
    name: 'Revenue by country and device',
    sql: `SELECT country, device, COUNT(*) AS orders, SUM(amount) AS revenue
FROM events
WHERE event_type = 'purchase'
GROUP BY country, device
HAVING orders > 100
ORDER BY revenue DESC
LIMIT 50`,
  },
  {
    name: 'Top users by spend',
    sql: `SELECT user_id, COUNT(*) AS txns, SUM(amount) AS total_spend, AVG(amount) AS avg_ticket
FROM events
WHERE event_type IN ('purchase', 'refund')
GROUP BY user_id
HAVING txns > 3
ORDER BY total_spend DESC
LIMIT 100`,
  },
  {
    name: 'Daily conversion mix',
    sql: `SELECT ts, event_type, COUNT(*) AS cnt, AVG(amount) AS avg_amount
FROM events
WHERE event_type IN ('purchase', 'signup', 'refund', 'error')
GROUP BY ts, event_type
HAVING cnt > 20
ORDER BY ts DESC
LIMIT 200`,
  },
  {
    name: 'Error and refund hotspots',
    sql: `SELECT country, device, event_type, COUNT(*) AS incidents, MAX(amount) AS max_amount
FROM events
WHERE event_type IN ('error', 'refund')
GROUP BY country, device, event_type
HAVING incidents > 10
ORDER BY incidents DESC
LIMIT 100`,
  },
  {
    name: 'Country summary excluding view/click',
    sql: `SELECT country, COUNT(*) AS total_events, SUM(amount) AS gross_amount, MIN(amount) AS min_amount, MAX(amount) AS max_amount
FROM events
WHERE event_type NOT IN ('view', 'click')
GROUP BY country
HAVING total_events > 1000
ORDER BY gross_amount DESC
LIMIT 20`,
  },
]

async function main() {
  const { executeSQL } = await import(new URL('../lib/mini-sql.ts', import.meta.url).href)
  const raw = process.argv[2]
  if (!raw) throw new Error('Usage: node --experimental-strip-types --expose-gc scripts/benchmark-runtime.ts <rowCount>')
  const rowCount = Number(raw.replace(/_/g, ''))
  if (!Number.isFinite(rowCount) || rowCount <= 0) throw new Error(`Invalid row count: ${raw}`)

  const totalRam = os.totalmem()
  maybeGc()
  const before = snapshotMemory()
  const loadStart = performance.now()
  const data = generateEvents(rowCount)
  const loadMs = performance.now() - loadStart
  maybeGc()
  const afterLoad = snapshotMemory()
  const loadDelta = diffMemory(before, afterLoad)

  const queryResults = QUERIES.map(q => {
    const start = performance.now()
    const result = executeSQL(q.sql, data as unknown as Record<string, unknown>[])
    const ms = performance.now() - start
    return {
      name: q.name,
      ms,
      rowCount: result.rowCount,
      error: result.error ?? null,
    }
  })

  const output = {
    rowCount,
    hostRamBytes: totalRam,
    hostRam: fmtBytes(totalRam),
    loadMs: Math.round(loadMs),
    loadMemory: {
      rssBytes: loadDelta.rss,
      heapUsedBytes: loadDelta.heapUsed,
      heapTotalBytes: loadDelta.heapTotal,
      rss: fmtBytes(loadDelta.rss),
      heapUsed: fmtBytes(loadDelta.heapUsed),
      heapTotal: fmtBytes(loadDelta.heapTotal),
      approxHeapBytesPerRow: loadDelta.heapUsed / rowCount,
      approxRssBytesPerRow: loadDelta.rss / rowCount,
    },
    queryResults: queryResults.map(q => ({
      ...q,
      ms: Math.round(q.ms),
    })),
  }

  console.log(JSON.stringify(output, null, 2))
}

main().catch(err => {
  console.error(err instanceof Error ? err.stack ?? err.message : String(err))
  process.exit(1)
})
