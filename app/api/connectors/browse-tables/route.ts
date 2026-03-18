/**
 * POST /api/connectors/browse-tables
 * 
 * Fetches available tables from a database connector for multi-selection in dataset wizard
 */

import { NextRequest, NextResponse } from 'next/server'
import { getConnectorRuntimeConfig } from '@/lib/connectors'
import { fetchMssqlCatalog } from '@/lib/mssql'

export const dynamic = 'force-dynamic'

export async function POST(req: NextRequest) {
  try {
    const body = await req.json() as { connectorId: string; connectorType: string }
    const { connectorId, connectorType } = body

    if (!connectorId) {
      return NextResponse.json({ error: 'Missing connectorId' }, { status: 400 })
    }

    const config = getConnectorRuntimeConfig(connectorId)
    if (!config) {
      return NextResponse.json({ error: 'Connector not found' }, { status: 404 })
    }

    let tables: string[] = []

    switch (connectorType) {
      case 'mssql': {
        const result = await fetchMssqlCatalog(config, { catalog: 'tables' })
        if (result.error) {
          return NextResponse.json({ error: result.error }, { status: 500 })
        }
        // Extract table names from rows (TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE)
        tables = result.rows.map(row => row[1]) // TABLE_NAME is second column
        break
      }

      case 'postgresql':
      case 'mysql': {
        // For PostgreSQL and MySQL, we need to import their respective modules
        if (connectorType === 'postgresql') {
          const { Client } = await import('pg')
          const host = String(config.host ?? 'localhost')
          const port = Number(config.port ?? 5432)
          const database = String(config.database ?? 'postgres')
          const user = String(config.dbUser ?? 'postgres')
          const password = String(config.dbPass ?? '')

          const client = new Client({ host, port, database, user, password, ssl: config.ssl ? { rejectUnauthorized: false } : false })
          try {
            await client.connect()
            const res = await client.query("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema') ORDER BY tablename")
            tables = res.rows.map((row: { tablename: string }) => row.tablename)
          } finally {
            await client.end()
          }
        } else if (connectorType === 'mysql') {
          const mysql = await import('mysql2/promise')
          const connection = await mysql.createConnection({
            host: String(config.host ?? 'localhost'),
            port: Number(config.port ?? 3306),
            database: String(config.database ?? ''),
            user: String(config.dbUser ?? 'root'),
            password: String(config.dbPass ?? ''),
            ssl: config.ssl ? {} : undefined,
          })
          try {
            const [rows] = await connection.query('SHOW TABLES') as [Array<Record<string, string>>, unknown]
            tables = rows.map(row => Object.values(row)[0])
          } finally {
            await connection.end()
          }
        }
        break
      }

      default:
        return NextResponse.json({ error: `Connector type '${connectorType}' does not support table browsing` }, { status: 400 })
    }

    return NextResponse.json({ tables })
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    return NextResponse.json({ error: message }, { status: 500 })
  }
}
