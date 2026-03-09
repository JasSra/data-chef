import { executeSQL } from '@/lib/mini-sql'
import { inferType, loadSourceRaw, loadSourceRows, parseSchemaText } from '@/lib/runtime-data'
import type { SourceType } from '@/lib/datasets'

export type RuntimeRow = Record<string, unknown>
export type RuntimeQueryLang = 'sql' | 'jsonpath' | 'jmespath' | 'kql'

export interface RuntimeStepInput {
  id: string
  op: string
  label: string
  config: Record<string, unknown>
}

export interface StepExecutionResult {
  rows: RuntimeRow[]
  removed: number
  logs: string[]
}

function pathKey(path: string): string {
  return path.replace(/^\$\./, '')
}

function coerceValue(value: unknown, targetType: string): unknown {
  if (value === null || value === undefined) return value
  switch (targetType) {
    case 'string':
      return String(value)
    case 'integer': {
      const n = Number(value)
      return Number.isFinite(n) ? Math.trunc(n) : null
    }
    case 'float': {
      const n = Number(value)
      return Number.isFinite(n) ? n : null
    }
    case 'boolean':
      if (typeof value === 'boolean') return value
      if (typeof value === 'string') {
        const normalized = value.trim().toLowerCase()
        if (['true', '1', 'yes', 'y'].includes(normalized)) return true
        if (['false', '0', 'no', 'n'].includes(normalized)) return false
      }
      return Boolean(value)
    case 'date': {
      const d = new Date(String(value))
      return Number.isNaN(d.getTime()) ? null : d.toISOString().slice(0, 10)
    }
    case 'timestamp': {
      const d = new Date(String(value))
      return Number.isNaN(d.getTime()) ? null : d.toISOString()
    }
    case 'json':
      if (typeof value === 'string') {
        try { return JSON.parse(value) } catch { return null }
      }
      return value
    default:
      return value
  }
}

function flattenObject(prefix: string, value: RuntimeRow, out: RuntimeRow) {
  for (const [key, nested] of Object.entries(value)) {
    out[`${prefix}_${key}`] = nested
  }
}

function valueAtPath(obj: unknown, path: string): unknown {
  const key = pathKey(path)
  if (!key) return obj
  return key.split('.').reduce<unknown>((acc, part) => {
    if (acc && typeof acc === 'object' && part in (acc as Record<string, unknown>)) {
      return (acc as Record<string, unknown>)[part]
    }
    return undefined
  }, obj)
}

function applyUrlTemplate(url: string, value: string): string {
  if (url.includes('{{value}}')) return url.replaceAll('{{value}}', encodeURIComponent(value))
  const sep = url.includes('?') ? '&' : '?'
  return `${url}${sep}value=${encodeURIComponent(value)}`
}

function projectEnrichFields(payload: unknown, fieldsText: string): RuntimeRow {
  const fields = fieldsText.split(',').map(f => f.trim()).filter(Boolean)
  if (fields.length === 0) {
    if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
      return Object.fromEntries(Object.entries(payload as RuntimeRow).map(([k, v]) => [`enrich_${k}`, v]))
    }
    return { enrich_value: payload }
  }
  const out: RuntimeRow = {}
  for (const field of fields) {
    out[`enrich_${field.replace(/\./g, '_')}`] = valueAtPath(payload, field)
  }
  return out
}

export function previewCell(v: unknown): string {
  if (v === null || v === undefined) return '∅'
  if (Array.isArray(v)) return `[${v.length}]`
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

export async function loadPipelineSourceRows(
  sourceType: SourceType,
  sourceId: string,
  resource?: string,
  rowLimit = 200,
): Promise<RuntimeRow[]> {
  try {
    return await loadSourceRows({ sourceType, sourceId, resource }, { rowLimit })
  } catch {
    const raw = await loadSourceRaw({ sourceType, sourceId, resource }, { rowLimit })
    return raw.map(item => item && typeof item === 'object' ? item as RuntimeRow : { value: item })
  }
}

export function kqlToSQL(kql: string): string {
  const pipes = kql.replace(/\/\/[^\n]*/g, '').split('|').map(s => s.trim()).filter(Boolean)
  if (!pipes.length) throw new Error('Empty KQL query')

  const table = pipes[0]
  let select = '*', where = '', groupBy = '', orderBy = '', limit = ''

  for (let i = 1; i < pipes.length; i++) {
    const op = pipes[i]
    if (/^where\s+/i.test(op)) {
      where = op.replace(/^where\s+/i, '')
        .replace(/==/g, '=').replace(/!=/g, '<>')
        .replace(/\band\b/gi, 'AND').replace(/\bor\b/gi, 'OR').replace(/\bnot\b/gi, 'NOT')
    } else if (/^project\s+/i.test(op)) {
      select = op.replace(/^project\s+/i, '')
    } else if (/^summarize\s+count\(\)\s+by\s+/i.test(op)) {
      const field = op.replace(/^summarize\s+count\(\)\s+by\s+/i, '').trim()
      select = `${field}, COUNT(*) AS count_`
      groupBy = field
    } else if (/^(order|sort)\s+by\s+/i.test(op)) {
      orderBy = op.replace(/^(order|sort)\s+by\s+/i, '')
        .replace(/\basc\b/gi, 'ASC').replace(/\bdesc\b/gi, 'DESC')
    } else if (/^(limit|take|top)\s+\d+/i.test(op)) {
      limit = op.replace(/^(limit|take|top)\s+/i, '')
    }
  }

  let sql = `SELECT ${select} FROM ${table}`
  if (where) sql += ` WHERE ${where}`
  if (groupBy) sql += ` GROUP BY ${groupBy}`
  if (orderBy) sql += ` ORDER BY ${orderBy}`
  if (limit) sql += ` LIMIT ${limit}`
  return sql
}

function rowFromColumns(columns: string[], values: string[]): RuntimeRow {
  const row: RuntimeRow = {}
  columns.forEach((col, i) => { row[col] = values[i] })
  return row
}

export async function runQueryOverRows(
  lang: RuntimeQueryLang,
  queryText: string,
  rows: RuntimeRow[],
): Promise<RuntimeRow[]> {
  const text = queryText.trim()
  if (!text) return rows

  if (lang === 'sql' || lang === 'kql') {
    const result = executeSQL(lang === 'kql' ? kqlToSQL(text) : text, rows)
    if (result.error) throw new Error(result.error)
    return result.rows.map(r => rowFromColumns(result.columns, r))
  }

  if (lang === 'jsonpath') {
    const { JSONPath } = await import('jsonpath-plus')
    const raw = JSONPath({ path: text, json: rows })
    return Array.isArray(raw)
      ? raw.map(item => (item && typeof item === 'object' ? item as RuntimeRow : { value: item }))
      : [{ value: raw }]
  }

  const jmespath = await import('jmespath')
  const raw = jmespath.search(rows, text)
  return Array.isArray(raw)
    ? raw.map(item => (item && typeof item === 'object' ? item as RuntimeRow : { value: item }))
    : [{ value: raw }]
}

export async function executePipelineStep(step: RuntimeStepInput, rows: RuntimeRow[]): Promise<StepExecutionResult> {
  switch (step.op) {
    case 'extract':
    case 'write':
      return { rows, removed: 0, logs: [`${step.label}: source ready`, `${rows.length} rows available`] }

    case 'validate': {
      const expected = parseSchemaText(String(step.config.schemaText ?? ''))
      if (!expected.size) return { rows, removed: 0, logs: ['No schema assertions configured'] }
      const next = rows.filter(row => {
        for (const [field, type] of expected.entries()) {
          const value = row[field]
          if (value === undefined || value === null) {
            if (step.config.validateMode === 'strict') return false
            continue
          }
          if (inferType(value) !== type) return false
        }
        return true
      })
      return {
        rows: next,
        removed: rows.length - next.length,
        logs: [
          `Validated ${rows.length} rows against ${expected.size} fields`,
          `${next.length} rows passed${rows.length !== next.length ? `, ${rows.length - next.length} removed` : ''}`,
        ],
      }
    }

    case 'map': {
      const mappings = (step.config.mappings as { from: string; to: string }[]) ?? []
      if (!mappings.length) return { rows, removed: 0, logs: ['No field mappings configured'] }
      const next = rows.map(r => {
        const out: RuntimeRow = { ...r }
        for (const m of mappings) {
          const from = (m.from ?? '').replace(/^\$\./, '')
          const to = (m.to ?? '').replace(/^\$\./, '')
          if (!from || !to || !(from in out)) continue
          out[to] = out[from]
          if (to !== from) delete out[from]
        }
        return out
      })
      return { rows: next, removed: 0, logs: [`Applied ${mappings.length} mappings`] }
    }

    case 'coerce': {
      const field = pathKey(String(step.config.coerceField ?? ''))
      const targetType = String(step.config.coerceType ?? 'string').toLowerCase()
      if (!field) return { rows, removed: 0, logs: ['No field configured for coercion'] }
      const next = rows.map(row => ({
        ...row,
        [field]: coerceValue(row[field], targetType),
      }))
      return { rows: next, removed: 0, logs: [`Coerced ${field} to ${targetType}`] }
    }

    case 'flatten': {
      const field = pathKey(String(step.config.flattenField ?? ''))
      const mode = String(step.config.flattenMode ?? 'object').toLowerCase()
      if (!field) return { rows, removed: 0, logs: ['No field configured for flatten'] }
      if (mode === 'array') {
        const next: RuntimeRow[] = []
        for (const row of rows) {
          const arr = row[field]
          if (!Array.isArray(arr) || arr.length === 0) {
            next.push(row)
            continue
          }
          for (const item of arr) {
            next.push({ ...row, [field]: item })
          }
        }
        return {
          rows: next,
          removed: Math.max(0, rows.length - next.length),
          logs: [`Flattened array field ${field}`, `${next.length} output rows generated`],
        }
      }

      const next = rows.map(row => {
        const nested = row[field]
        if (!nested || typeof nested !== 'object' || Array.isArray(nested)) return row
        const out: RuntimeRow = { ...row }
        flattenObject(field, nested as RuntimeRow, out)
        delete out[field]
        return out
      })
      return { rows: next, removed: 0, logs: [`Flattened object field ${field}`] }
    }

    case 'enrich': {
      const lookupUrl = String(step.config.lookupUrl ?? '').trim()
      const joinKey = pathKey(String(step.config.joinKey ?? ''))
      if (!lookupUrl) return { rows, removed: 0, logs: ['No lookup URL configured'] }
      if (!joinKey) return { rows, removed: 0, logs: ['No join key configured'] }

      const cache = new Map<string, RuntimeRow>()
      let misses = 0
      const next = await Promise.all(rows.map(async row => {
        const raw = row[joinKey]
        if (raw === undefined || raw === null || raw === '') return row
        const key = String(raw)
        let enrichRow = cache.get(key)
        if (!enrichRow) {
          const res = await fetch(applyUrlTemplate(lookupUrl, key), {
            headers: { Accept: 'application/json, text/plain, */*', 'User-Agent': 'dataChef-pipeline/0.1' },
            signal: AbortSignal.timeout(10_000),
          })
          if (!res.ok) {
            misses += 1
            cache.set(key, {})
            return row
          }
          const payload = await res.json()
          enrichRow = projectEnrichFields(payload, String(step.config.enrichFields ?? ''))
          cache.set(key, enrichRow)
        }
        return { ...row, ...enrichRow }
      }))
      return {
        rows: next,
        removed: 0,
        logs: [
          `Enriched ${rows.length} rows via ${cache.size} unique lookups`,
          misses > 0 ? `${misses} lookup requests failed or returned non-OK` : 'All lookups completed',
        ],
      }
    }

    case 'dedupe': {
      const key = String(step.config.dedupeKey ?? '').replace(/^\$\./, '')
      if (!key) return { rows, removed: 0, logs: ['No dedupe key configured'] }
      const seen = new Set<string>()
      const next = rows.filter(row => {
        const sig = JSON.stringify(row[key] ?? null)
        if (seen.has(sig)) return false
        seen.add(sig)
        return true
      })
      return {
        rows: next,
        removed: rows.length - next.length,
        logs: [`Deduped on ${key}`, `${rows.length - next.length} duplicates removed`],
      }
    }

    case 'query': {
      const next = await runQueryOverRows(
        (String(step.config.queryType ?? 'sql').toLowerCase() as RuntimeQueryLang),
        String(step.config.queryText ?? ''),
        rows,
      )
      return {
        rows: next,
        removed: Math.max(0, rows.length - next.length),
        logs: [`Query returned ${next.length} rows from ${rows.length}`],
      }
    }

    case 'condition': {
      const field = String(step.config.conditionField ?? '').replace(/^\$\./, '')
      const op = String(step.config.conditionOp ?? '==')
      const val = String(step.config.conditionValue ?? '')
      if (!field) return { rows, removed: 0, logs: ['No condition field configured'] }
      const next = rows.filter(r => {
        const raw = r[field]
        const rv = raw == null ? '' : String(raw)
        switch (op) {
          case '==': return rv === val
          case '!=': return rv !== val
          case '>': return Number(rv) > Number(val)
          case '>=': return Number(rv) >= Number(val)
          case '<': return Number(rv) < Number(val)
          case '<=': return Number(rv) <= Number(val)
          case 'contains': return rv.includes(val)
          case 'startsWith': return rv.startsWith(val)
          case 'exists': return raw !== undefined && raw !== null
          case 'isNull': return raw === undefined || raw === null
          default: return true
        }
      })
      return {
        rows: next,
        removed: rows.length - next.length,
        logs: [`Condition ${field} ${op} ${val}`, `${next.length} rows matched`],
      }
    }

    default:
      return { rows, removed: 0, logs: [`Unsupported op "${step.op}" - passing through`] }
  }
}
