import { NextRequest, NextResponse } from 'next/server'
import sql from 'mssql'
import { Client as PgClient } from 'pg'
import mysql from 'mysql2/promise'

export async function POST(req: NextRequest) {
  try {
    const body = await req.json()
    const { type, host, port, dbUser, dbPass, ssl, sslMode, instanceName, encrypt, trustServerCertificate } = body

    if (!type || !host) {
      return NextResponse.json({ error: 'Missing required fields: type, host' }, { status: 400 })
    }

    let databases: string[] = []

    switch (type) {
      case 'mssql': {
        const config: sql.config = {
          server: host,
          port: port ? parseInt(port, 10) : 1433,
          user: dbUser || 'sa',
          password: dbPass || '',
          database: 'master',
          options: {
            encrypt: encrypt !== false,
            trustServerCertificate: trustServerCertificate === true,
            instanceName: instanceName || undefined,
          },
          connectionTimeout: 10000,
          requestTimeout: 10000,
        }

        const pool = await sql.connect(config)
        try {
          const result = await pool.request().query(`
            SELECT name 
            FROM sys.databases 
            WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
            ORDER BY name
          `)
          databases = result.recordset.map((row: any) => row.name)
        } finally {
          await pool.close()
        }
        break
      }

      case 'postgresql': {
        const client = new PgClient({
          host,
          port: port ? parseInt(port, 10) : 5432,
          user: dbUser,
          password: dbPass,
          database: 'postgres',
          ssl: ssl === false ? false : (sslMode === 'disable' ? false : { rejectUnauthorized: sslMode === 'verify-full' }),
          connectionTimeoutMillis: 10000,
          query_timeout: 10000,
        })

        await client.connect()
        try {
          const result = await client.query(`
            SELECT datname 
            FROM pg_database 
            WHERE datistemplate = false 
              AND datname NOT IN ('postgres')
            ORDER BY datname
          `)
          databases = result.rows.map((row: any) => row.datname)
        } finally {
          await client.end()
        }
        break
      }

      case 'mysql': {
        const connection = await mysql.createConnection({
          host,
          port: port ? parseInt(port, 10) : 3306,
          user: dbUser,
          password: dbPass,
          ssl: ssl === false ? undefined : (sslMode === 'disable' ? undefined : { rejectUnauthorized: sslMode === 'verify-full' }),
          connectTimeout: 10000,
        })

        try {
          const [rows] = await connection.query(`
            SELECT SCHEMA_NAME as name
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            ORDER BY SCHEMA_NAME
          `)
          databases = (rows as any[]).map(row => row.name)
        } finally {
          await connection.end()
        }
        break
      }

      default:
        return NextResponse.json({ error: `Unsupported database type: ${type}` }, { status: 400 })
    }

    return NextResponse.json({ databases })
  } catch (error: any) {
    console.error('Failed to fetch databases:', error)
    return NextResponse.json(
      { error: error.message || 'Failed to fetch databases' },
      { status: 500 }
    )
  }
}
