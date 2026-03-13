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

interface ExecuteBody {
  connectorId: string
  db?: string
  query: string
}

export async function POST(req: NextRequest) {
  const body = await req.json() as ExecuteBody
  const { connectorId, db, query } = body

  if (!connectorId) return NextResponse.json({ error: 'connectorId required' }, { status: 400 })
  if (!query?.trim()) return NextResponse.json({ error: 'query required' }, { status: 400 })

  const connector = await resolveConnector(connectorId)
  if (!connector) return NextResponse.json({ error: 'Connector not found' }, { status: 404 })

  const { type, runtimeConfig } = connector as { type: string; runtimeConfig: Record<string, unknown> }
  const t0 = performance.now()

  try {
    if (type === 'mssql') {
      const { executeMssqlQuery } = await import('@/lib/mssql')
      const cfg = db ? { ...runtimeConfig, database: db } : runtimeConfig
      const result = await executeMssqlQuery(cfg, { query })
      return NextResponse.json({ ...result, durationMs: Math.round(performance.now() - t0) })
    }

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
        connectionTimeoutMillis: 15_000,
      })
      try {
        const res = await pool.query(query)
        const rowsAffected = res.rowCount ?? 0
        return NextResponse.json({ rowsAffected, durationMs: Math.round(performance.now() - t0) })
      } finally {
        await pool.end()
      }
    }

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
        const [result] = await conn.query(query)
        const rowsAffected = (result as { affectedRows?: number }).affectedRows ?? 0
        return NextResponse.json({ rowsAffected, durationMs: Math.round(performance.now() - t0) })
      } finally {
        await conn.end()
      }
    }

    return NextResponse.json({ error: `Execute not supported for type: ${type}` }, { status: 400 })
  } catch (e) {
    return NextResponse.json({ error: e instanceof Error ? e.message : String(e) }, { status: 500 })
  }
}
