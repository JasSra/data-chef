import path from 'node:path'
import type { SourceReference } from '@/lib/datasets'
import { loadSourceRows } from '@/lib/runtime-data'

const alasql = eval('require')(path.join(process.cwd(), 'node_modules/alasql/dist/alasql.js'))

type Row = Record<string, unknown>

export interface FederatedSourceBinding extends SourceReference {
  alias: string
  queryHint?: string
  rowLimit?: number
}

export interface FederatedExecutionOptions {
  defaultRowLimit?: number
  maxSources?: number
  maxTotalRows?: number
  timespan?: string
}

export interface FederatedQueryResult {
  columns: string[]
  rows: string[][]
  rowCount: number
  totalRows: number
  warnings: string[]
  truncated: boolean
}

function validateAlias(alias: string) {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(alias)
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return '∅'
  if (typeof value === 'number') return String(Math.round(value * 1000) / 1000)
  if (typeof value === 'boolean') return value ? 'true' : 'false'
  if (Array.isArray(value) || (typeof value === 'object' && value !== null)) return JSON.stringify(value)
  return String(value)
}

function createDbName() {
  return `federated_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`
}

function cleanupDatabase(name: string, aliases: string[]) {
  try {
    alasql(`DROP DATABASE ${name}`)
  } catch {
    // noop
  }
  for (const alias of aliases) {
    if (alasql.tables?.[alias]) delete alasql.tables[alias]
  }
}

export async function executeFederatedSql(
  sql: string,
  sources: FederatedSourceBinding[],
  options: FederatedExecutionOptions = {},
): Promise<FederatedQueryResult> {
  const warnings: string[] = []
  const defaultRowLimit = Math.max(1, options.defaultRowLimit ?? 500)
  const maxSources = Math.max(1, options.maxSources ?? 6)
  const maxTotalRows = Math.max(defaultRowLimit, options.maxTotalRows ?? 5_000)

  if (!sources.length) throw new Error('At least one source binding is required')
  if (sources.length > maxSources) throw new Error(`Federated query supports at most ${maxSources} sources`)

  const aliases = new Set<string>()
  const tables = new Map<string, Row[]>()
  let totalLoadedRows = 0
  let truncated = false

  for (const source of sources) {
    if (!validateAlias(source.alias)) {
      throw new Error(`Invalid alias "${source.alias}". Use letters, numbers, and underscores only.`)
    }
    if (aliases.has(source.alias)) throw new Error(`Duplicate alias "${source.alias}"`)
    aliases.add(source.alias)

    const rowLimit = Math.max(1, Math.min(source.rowLimit ?? defaultRowLimit, maxTotalRows))
    const rows = await loadSourceRows(source, { rowLimit, timespan: options.timespan })
    tables.set(source.alias, rows)
    totalLoadedRows += rows.length
    if (rows.length >= rowLimit) {
      warnings.push(`Alias ${source.alias} loaded only the first ${rowLimit} rows`)
      truncated = true
    }
    if (totalLoadedRows > maxTotalRows) {
      warnings.push(`Combined source rows exceeded ${maxTotalRows}; later sources may be truncated`)
      truncated = true
    }
  }

  const databaseName = createDbName()
  try {
    alasql(`CREATE DATABASE ${databaseName}`)
    alasql(`USE ${databaseName}`)
    for (const [alias, rows] of tables.entries()) {
      alasql(`DROP TABLE IF EXISTS ${alias}`)
      alasql(`CREATE TABLE ${alias}`)
      alasql.tables[alias].data = rows
    }
    const raw = alasql(sql)
    const resultRows = Array.isArray(raw) ? raw : []
    const columns = resultRows[0] ? Object.keys(resultRows[0]) : []
    return {
      columns,
      rows: resultRows.map(row => columns.map(column => formatValue((row as Row)[column]))),
      rowCount: resultRows.length,
      totalRows: resultRows.length,
      warnings,
      truncated,
    }
  } catch (error: unknown) {
    throw new Error(error instanceof Error ? error.message : String(error))
  } finally {
    cleanupDatabase(databaseName, Array.from(aliases))
  }
}
