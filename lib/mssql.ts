import 'server-only'

export type MssqlQueryMode = 'query' | 'catalog'
export type MssqlCatalogKind = 'databases' | 'schemas' | 'tables' | 'views' | 'columns' | 'procedures' | 'indexes' | 'primarykeys'

export interface MssqlConnectionConfig {
  connectionMode?: 'fields' | 'connectionString'
  connectionString?: string
  host?: string
  port?: number
  database?: string
  dbUser?: string
  dbPass?: string
  instanceName?: string
  encrypt?: boolean
  trustServerCertificate?: boolean
  schema?: string
}

export interface MssqlQueryResult {
  columns: string[]
  rows: string[][]
  rowCount: number
  totalRows: number
  durationMs: number
  mssqlMode: MssqlQueryMode
  error?: string
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return ''
  if (value instanceof Date) return value.toISOString()
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

function normaliseCfg(input: Record<string, unknown>): MssqlConnectionConfig {
  return {
    connectionMode: (input.connectionMode as MssqlConnectionConfig['connectionMode']) ?? 'fields',
    connectionString: input.connectionString as string | undefined,
    host: (input.host as string | undefined) ?? 'localhost',
    port: Number(input.port ?? 1433),
    database: (input.database as string | undefined) ?? 'master',
    dbUser: (input.dbUser as string | undefined) ?? 'sa',
    dbPass: input.dbPass as string | undefined,
    instanceName: input.instanceName as string | undefined,
    encrypt: Boolean(input.encrypt ?? true),
    trustServerCertificate: Boolean(input.trustServerCertificate ?? false),
    schema: (input.schema as string | undefined) ?? 'dbo',
  }
}

async function getMssqlPool(config: MssqlConnectionConfig) {
  const sql = await import('mssql')
  if (config.connectionMode === 'connectionString' && config.connectionString) {
    return await sql.connect(config.connectionString)
  }
  return await sql.connect({
    server: config.host ?? 'localhost',
    port: config.port ?? 1433,
    database: config.database ?? 'master',
    user: config.dbUser ?? 'sa',
    password: config.dbPass ?? '',
    options: {
      instanceName: config.instanceName,
      encrypt: config.encrypt ?? true,
      trustServerCertificate: config.trustServerCertificate ?? false,
    },
  })
}

export async function probeMssqlConnection(input: Record<string, unknown>): Promise<{
  ok: boolean; serverVersion?: string; database?: string; error?: string
}> {
  const cfg = normaliseCfg(input)
  let pool: Awaited<ReturnType<typeof getMssqlPool>> | null = null
  try {
    pool = await getMssqlPool(cfg)
    const res = await pool.query<{ v: string; db: string }>(
      'SELECT @@VERSION AS v, DB_NAME() AS db',
    )
    const row = res.recordset[0]
    return { ok: true, serverVersion: row?.v?.split('\n')[0], database: row?.db }
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : String(e) }
  } finally {
    await pool?.close()
  }
}

export async function executeMssqlQuery(
  input: Record<string, unknown>,
  options: { query: string; rowLimit?: number },
): Promise<MssqlQueryResult> {
  const t0 = performance.now()
  const cfg = normaliseCfg(input)
  let pool: Awaited<ReturnType<typeof getMssqlPool>> | null = null
  try {
    pool = await getMssqlPool(cfg)
    const result = await pool.query(options.query)
    const rs = result.recordset ?? []
    const columns = rs.length > 0 ? Object.keys(rs[0]) : []
    const rows = rs
      .slice(0, options.rowLimit ?? 500)
      .map(row => columns.map(col => formatValue(row[col])))
    return {
      columns, rows,
      rowCount: rows.length,
      totalRows: result.rowsAffected?.[0] ?? rows.length,
      durationMs: Math.round(performance.now() - t0),
      mssqlMode: 'query',
    }
  } catch (e) {
    return {
      columns: [], rows: [], rowCount: 0, totalRows: 0,
      durationMs: Math.round(performance.now() - t0),
      mssqlMode: 'query',
      error: e instanceof Error ? e.message : String(e),
    }
  } finally {
    await pool?.close()
  }
}

export async function fetchMssqlCatalog(
  input: Record<string, unknown>,
  options: { catalog: MssqlCatalogKind; schema?: string; table?: string; limit?: number },
): Promise<MssqlQueryResult> {
  const t0 = performance.now()
  const cfg = normaliseCfg(input)
  const schema = options.schema ?? cfg.schema ?? 'dbo'
  const limit = options.limit ?? 500
  let pool: Awaited<ReturnType<typeof getMssqlPool>> | null = null

  const queries: Record<MssqlCatalogKind, string> = {
    databases: 'SELECT name AS database_name, create_date, compatibility_level, collation_name FROM sys.databases ORDER BY name',
    schemas: `SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY schema_name`,
    tables: `SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${schema}' ORDER BY TABLE_NAME`,
    views: `SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '${schema}' ORDER BY TABLE_NAME`,
    columns: options.table
      ? `SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME = '${options.table}' ORDER BY ORDINAL_POSITION`
      : `SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '${schema}' ORDER BY TABLE_NAME, ORDINAL_POSITION`,
    procedures: `SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE, CREATED, LAST_ALTERED FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = '${schema}' ORDER BY ROUTINE_NAME`,
    indexes: `SELECT t.name AS table_name, i.name AS index_name, i.type_desc, i.is_unique, i.is_primary_key FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id WHERE t.schema_id = SCHEMA_ID('${schema}') ORDER BY t.name, i.name`,
    primarykeys: options.table
      ? `SELECT kcu.COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND tc.TABLE_NAME = kcu.TABLE_NAME WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND kcu.TABLE_SCHEMA = '${schema}' AND kcu.TABLE_NAME = '${options.table}' ORDER BY kcu.ORDINAL_POSITION`
      : `SELECT '' AS COLUMN_NAME WHERE 1=0`,
  }

  try {
    pool = await getMssqlPool(cfg)
    const q = queries[options.catalog] + ` OFFSET 0 ROWS FETCH NEXT ${limit} ROWS ONLY`.replace(/OFFSET.*/, '')
    // Some queries don't support FETCH NEXT, use TOP instead
    const safeQ = queries[options.catalog].replace(/^SELECT /, `SELECT TOP ${limit} `)
    const result = await pool.query(safeQ)
    const rs = result.recordset ?? []
    const columns = rs.length > 0 ? Object.keys(rs[0]) : []
    const rows = rs.map(row => columns.map(col => formatValue(row[col])))
    return {
      columns, rows,
      rowCount: rows.length, totalRows: rows.length,
      durationMs: Math.round(performance.now() - t0),
      mssqlMode: 'catalog',
    }
  } catch (e) {
    return {
      columns: [], rows: [], rowCount: 0, totalRows: 0,
      durationMs: Math.round(performance.now() - t0),
      mssqlMode: 'catalog',
      error: e instanceof Error ? e.message : String(e),
    }
  } finally {
    await pool?.close()
  }
}

export async function sampleMssqlRowsFromConfig(
  input: Record<string, unknown>,
  resource: string | undefined,
  rowLimit: number,
): Promise<Array<Record<string, unknown>>> {
  const cfg = normaliseCfg(input)
  const schema = cfg.schema ?? 'dbo'
  let pool: Awaited<ReturnType<typeof getMssqlPool>> | null = null
  try {
    pool = await getMssqlPool(cfg)
    const src = resource?.trim()
      ? (resource.includes(' ') ? `(${resource}) AS _sub` : `[${schema}].[${resource}]`)
      : `[${schema}].[dbo_sample]`
    const result = await pool.query(`SELECT TOP ${rowLimit} * FROM ${src}`)
    return result.recordset ?? []
  } finally {
    await pool?.close()
  }
}
