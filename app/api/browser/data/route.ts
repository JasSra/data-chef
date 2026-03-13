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

function fmt(v: unknown): string {
  if (v === null || v === undefined) return ''
  if (v instanceof Date) return v.toISOString()
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

interface DataRequestBody {
  connectorId: string
  table?: string
  schema?: string
  db?: string      // database override
  query?: string   // custom SQL / raw Redis command
  queue?: string   // RabbitMQ queue name
  topic?: string   // MQTT topic
  // Redis
  redisMode?: string
  valueType?: string
  keyPattern?: string
  redisIndex?: string
  redisDb?: number
  page?: number
  pageSize?: number
  windowMs?: number // MQTT subscribe window
}

export async function POST(req: NextRequest) {
  const body = await req.json() as DataRequestBody
  const { connectorId, table, schema, db, query, queue, topic, page = 1, pageSize = 100, windowMs = 5000 } = body

  if (!connectorId) return NextResponse.json({ error: 'connectorId required' }, { status: 400 })

  const connector = await resolveConnector(connectorId)
  if (!connector) return NextResponse.json({ error: 'Connector not found' }, { status: 404 })

  const { type, runtimeConfig } = connector as { type: string; runtimeConfig: Record<string, unknown> }
  const offset = (page - 1) * pageSize

  try {
    // ── MSSQL ─────────────────────────────────────────────────────────────
    if (type === 'mssql') {
      const { executeMssqlQuery } = await import('@/lib/mssql')
      const cfg = db ? { ...runtimeConfig, database: db } : runtimeConfig
      const targetSchema = schema ?? (cfg.schema as string) ?? 'dbo'
      const sql = query
        ?? `SELECT * FROM [${targetSchema}].[${table}] ORDER BY (SELECT NULL) OFFSET ${offset} ROWS FETCH NEXT ${pageSize} ROWS ONLY`
      const result = await executeMssqlQuery(cfg, { query: sql, rowLimit: pageSize })

      let totalRows = result.rowCount
      if (!query && table) {
        try {
          const countResult = await executeMssqlQuery(cfg, {
            query: `SELECT COUNT(*) AS n FROM [${targetSchema}].[${table}]`,
          })
          totalRows = Number(countResult.rows[0]?.[0] ?? result.rowCount)
        } catch { /* ignore count error */ }
      }
      return NextResponse.json({ ...result, totalRows })
    }

    // ── PostgreSQL ────────────────────────────────────────────────────────
    if (type === 'postgresql') {
      const { Pool } = await import('pg')
      const cfg = runtimeConfig
      const pool = new Pool({
        host: (cfg.host as string) ?? 'localhost',
        port: Number(cfg.port ?? 5432),
        database: db ?? (cfg.database as string) ?? 'postgres',
        user: (cfg.dbUser as string) ?? 'postgres',
        password: cfg.dbPass as string | undefined,
        ssl: cfg.ssl ? { rejectUnauthorized: false } : undefined,
        max: 2,
        connectionTimeoutMillis: 15_000,
      })
      const t0 = performance.now()
      try {
        const targetSchema = schema ?? 'public'
        const sql = query ?? `SELECT * FROM "${targetSchema}"."${table}" LIMIT ${pageSize} OFFSET ${offset}`
        const countSql = (!query && table)
          ? `SELECT COUNT(*) AS n FROM "${targetSchema}"."${table}"`
          : null
        const [res, countRes] = await Promise.all([
          pool.query(sql),
          countSql ? pool.query(countSql) : Promise.resolve(null),
        ])
        const columns = res.fields.map(f => f.name)
        const rows = res.rows.map(row => columns.map(col => fmt(row[col])))
        const totalRows = countRes ? Number(countRes.rows[0]?.n ?? rows.length) : rows.length
        return NextResponse.json({ columns, rows, rowCount: rows.length, totalRows, durationMs: Math.round(performance.now() - t0) })
      } finally {
        await pool.end()
      }
    }

    // ── MySQL ─────────────────────────────────────────────────────────────
    if (type === 'mysql') {
      const mysql = await import('mysql2/promise')
      const cfg = runtimeConfig
      const conn = await mysql.createConnection({
        host: (cfg.host as string) ?? 'localhost',
        port: Number(cfg.port ?? 3306),
        database: db ?? (cfg.database as string) ?? undefined,
        user: (cfg.dbUser as string) ?? 'root',
        password: cfg.dbPass as string | undefined,
      })
      const t0 = performance.now()
      try {
        const sql = query ?? `SELECT * FROM \`${table}\` LIMIT ${pageSize} OFFSET ${offset}`
        const [rows, fields] = await conn.query(sql)
        const columns = (fields as import('mysql2').FieldPacket[]).map(f => f.name)
        const strRows = (rows as import('mysql2').RowDataPacket[]).map(row =>
          columns.map(col => fmt(row[col]))
        )
        return NextResponse.json({ columns, rows: strRows, rowCount: strRows.length, totalRows: strRows.length, durationMs: Math.round(performance.now() - t0) })
      } finally {
        await conn.end()
      }
    }

    // ── RabbitMQ ──────────────────────────────────────────────────────────
    if (type === 'rabbitmq') {
      const { browseRabbitQueue } = await import('@/lib/rabbitmq')
      const result = await browseRabbitQueue(runtimeConfig, {
        queue: queue ?? table ?? '',
        count: pageSize,
      })
      return NextResponse.json(result)
    }

    // ── MQTT ──────────────────────────────────────────────────────────────
    if (type === 'mqtt') {
      const { subscribeMqttTopic } = await import('@/lib/mqtt')
      const result = await subscribeMqttTopic(runtimeConfig, {
        topic: topic ?? table ?? '#',
        windowMs,
        limit: pageSize,
      })
      return NextResponse.json(result)
    }

    // ── Redis ─────────────────────────────────────────────────────────────
    if (type === 'redis') {
      const { executeRedisQuery } = await import('@/lib/redis')
      const { redisMode, valueType, keyPattern, redisIndex, redisDb } = body
      const cfg = redisDb != null ? { ...runtimeConfig, database: redisDb } : runtimeConfig
      const effectiveQuery = query
        ?? (keyPattern ? `SCAN 0 MATCH ${keyPattern} COUNT ${pageSize}` : 'SCAN 0 COUNT 100')
      const result = await executeRedisQuery(cfg, {
        mode: redisMode as Parameters<typeof executeRedisQuery>[1]['mode'] ?? 'command',
        query: effectiveQuery,
        valueType: valueType as Parameters<typeof executeRedisQuery>[1]['valueType'],
        rowLimit: pageSize,
      })
      return NextResponse.json(result)
    }

    return NextResponse.json({ error: `Data browsing not supported for type: ${type}` }, { status: 400 })
  } catch (e) {
    return NextResponse.json({ error: e instanceof Error ? e.message : String(e) }, { status: 500 })
  }
}
