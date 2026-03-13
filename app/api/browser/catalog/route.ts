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

export async function GET(req: NextRequest) {
  const sp = req.nextUrl.searchParams
  const connectorId = sp.get('connectorId') ?? ''
  const kind = sp.get('kind') ?? 'tables'
  const schema = sp.get('schema') ?? undefined
  const table = sp.get('table') ?? undefined
  const db = sp.get('db') ?? undefined          // database override
  const pattern = sp.get('pattern') ?? undefined // redis key pattern
  const limit = Math.min(Number(sp.get('limit') ?? 500), 2000)

  if (!connectorId) return NextResponse.json({ error: 'connectorId required' }, { status: 400 })

  const connector = await resolveConnector(connectorId)
  if (!connector) return NextResponse.json({ error: 'Connector not found' }, { status: 404 })

  const { type, runtimeConfig } = connector as { type: string; runtimeConfig: Record<string, unknown> }
  const t0 = performance.now()

  try {
    // ── MSSQL ─────────────────────────────────────────────────────────────
    if (type === 'mssql') {
      const { fetchMssqlCatalog } = await import('@/lib/mssql')
      const cfg = db ? { ...runtimeConfig, database: db } : runtimeConfig
      const result = await fetchMssqlCatalog(cfg, {
        catalog: kind as Parameters<typeof fetchMssqlCatalog>[1]['catalog'],
        schema, table, limit,
      })
      return NextResponse.json(result)
    }

    // ── RabbitMQ ──────────────────────────────────────────────────────────
    if (type === 'rabbitmq') {
      const { fetchRabbitCatalog } = await import('@/lib/rabbitmq')
      const result = await fetchRabbitCatalog(runtimeConfig, {
        catalog: kind as Parameters<typeof fetchRabbitCatalog>[1]['catalog'],
        limit,
      })
      return NextResponse.json(result)
    }

    // ── MQTT ──────────────────────────────────────────────────────────────
    if (type === 'mqtt') {
      const { fetchMqttCatalog } = await import('@/lib/mqtt')
      const result = await fetchMqttCatalog(runtimeConfig, {
        catalog: kind as Parameters<typeof fetchMqttCatalog>[1]['catalog'],
        limit,
      })
      return NextResponse.json(result)
    }

    // ── Redis ─────────────────────────────────────────────────────────────
    if (type === 'redis') {
      const { fetchRedisCatalog } = await import('@/lib/redis')
      const cfg = db != null ? { ...runtimeConfig, database: Number(db) } : runtimeConfig
      const result = await fetchRedisCatalog(cfg, {
        catalog: kind as Parameters<typeof fetchRedisCatalog>[1]['catalog'],
        pattern,
        limit,
      })
      return NextResponse.json(result)
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
        max: 1,
        connectionTimeoutMillis: 10_000,
      })
      try {
        const targetSchema = schema ?? 'public'
        type PgQuery = { sql: string; params: unknown[] }
        const qs: Record<string, PgQuery> = {
          schemas: { sql: `SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog','information_schema','pg_toast') ORDER BY schema_name`, params: [] },
          databases: { sql: `SELECT datname AS database_name, pg_encoding_to_char(encoding) AS encoding FROM pg_database WHERE datistemplate = false ORDER BY datname`, params: [] },
          tables: { sql: `SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY table_name LIMIT $2`, params: [targetSchema, limit] },
          views: { sql: `SELECT table_schema, table_name FROM information_schema.views WHERE table_schema = $1 ORDER BY table_name LIMIT $2`, params: [targetSchema, limit] },
          columns: { sql: `SELECT column_name, data_type, character_maximum_length, is_nullable, column_default FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`, params: [targetSchema, table ?? ''] },
          procedures: { sql: `SELECT routine_name, routine_type, data_type FROM information_schema.routines WHERE routine_schema = $1 ORDER BY routine_name LIMIT $2`, params: [targetSchema, limit] },
          indexes: { sql: `SELECT schemaname, tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = $1 ORDER BY tablename, indexname LIMIT $2`, params: [targetSchema, limit] },
          primarykeys: { sql: `SELECT kcu.column_name FROM information_schema.key_column_usage kcu JOIN information_schema.table_constraints tc ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name WHERE tc.constraint_type = 'PRIMARY KEY' AND kcu.table_schema = $1 AND kcu.table_name = $2 ORDER BY kcu.ordinal_position`, params: [targetSchema, table ?? ''] },
        }
        const q = qs[kind]
        if (!q) return NextResponse.json({ error: `Unknown kind: ${kind}` }, { status: 400 })
        const res = await pool.query(q.sql, q.params)
        const columns = res.fields.map(f => f.name)
        const rows = res.rows.map(row => columns.map(col => fmt(row[col])))
        return NextResponse.json({ columns, rows, rowCount: rows.length, totalRows: rows.length, durationMs: Math.round(performance.now() - t0) })
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
      try {
        const targetSchema = schema ?? db ?? (cfg.database as string) ?? 'mysql'
        let sql = ''
        let params: unknown[] = []
        if (kind === 'schemas' || kind === 'databases') {
          sql = `SELECT SCHEMA_NAME AS schema_name FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME`
          params = []
        } else if (kind === 'tables') {
          sql = `SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME LIMIT ?`
          params = [targetSchema, limit]
        } else if (kind === 'views') {
          sql = `SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME LIMIT ?`
          params = [targetSchema, limit]
        } else if (kind === 'columns') {
          sql = `SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_DEFAULT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`
          params = [targetSchema, table ?? '']
        } else if (kind === 'indexes') {
          sql = `SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, COLUMN_NAME, INDEX_TYPE FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, INDEX_NAME LIMIT ?`
          params = [targetSchema, limit]
        } else if (kind === 'procedures') {
          sql = `SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ? ORDER BY ROUTINE_NAME LIMIT ?`
          params = [targetSchema, limit]
        } else if (kind === 'primarykeys') {
          sql = `SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`
          params = [targetSchema, table ?? '']
        } else {
          return NextResponse.json({ error: `Unknown kind: ${kind}` }, { status: 400 })
        }
        const [rows, fields] = await conn.query(sql, params)
        const fieldArray = fields as import('mysql2').FieldPacket[]
        const columns = fieldArray.map((f: import('mysql2').FieldPacket) => f.name)
        const rowArray = rows as import('mysql2').RowDataPacket[]
        const strRows = rowArray.map(row => columns.map(col => fmt(row[col])))
        return NextResponse.json({ columns, rows: strRows, rowCount: strRows.length, totalRows: strRows.length, durationMs: Math.round(performance.now() - t0) })
      } finally {
        await conn.end()
      }
    }

    return NextResponse.json({ error: `Catalog not supported for type: ${type}` }, { status: 400 })
  } catch (e) {
    return NextResponse.json({ error: e instanceof Error ? e.message : String(e) }, { status: 500 })
  }
}
