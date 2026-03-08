/**
 * POST /api/pipelines/preview
 *
 * Returns up to 10 sample rows after executing pipeline steps against a real
 * bounded sample from the selected dataset.
 */

import { NextRequest, NextResponse } from 'next/server'
import { executeSQL } from '@/lib/mini-sql'
import {
  inferType,
  loadDatasetRaw,
  loadDatasetRows,
  parseSchemaText,
} from '@/lib/runtime-data'
import { getDatasets } from '@/lib/datasets'

type Row = Record<string, unknown>
type QueryLang = 'sql' | 'jsonpath' | 'jmespath' | 'kql'

interface StepInput { op: string; config: Record<string, unknown> }

function previewCell(v: unknown): string {
  if (v === null || v === undefined) return '∅'
  if (Array.isArray(v)) return `[${v.length}]`
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

function kqlToSQL(kql: string): string {
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

function rowFromColumns(columns: string[], values: string[]): Row {
  const row: Row = {}
  columns.forEach((col, i) => { row[col] = values[i] })
  return row
}

async function runQueryStep(
  lang: QueryLang,
  queryText: string,
  rows: Row[],
): Promise<Row[]> {
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
      ? raw.map(item => (item && typeof item === 'object' ? item as Row : { value: item }))
      : [{ value: raw }]
  }

  const jmespath = await import('jmespath')
  const raw = jmespath.search(rows, text)
  return Array.isArray(raw)
    ? raw.map(item => (item && typeof item === 'object' ? item as Row : { value: item }))
    : [{ value: raw }]
}

function applyValidateStep(step: StepInput, rows: Row[]): { rows: Row[]; removed: number } {
  const expected = parseSchemaText(String(step.config.schemaText ?? ''))
  if (!expected.size) return { rows, removed: 0 }

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
  return { rows: next, removed: rows.length - next.length }
}

function applyMapStep(step: StepInput, rows: Row[]): { rows: Row[]; removed: number } {
  const mappings = (step.config.mappings as { from: string; to: string }[]) ?? []
  if (!mappings.length) return { rows, removed: 0 }
  return {
    rows: rows.map(r => {
      const out: Row = { ...r }
      for (const m of mappings) {
        const from = (m.from ?? '').replace(/^\$\./, '')
        const to = (m.to ?? '').replace(/^\$\./, '')
        if (!from || !to || !(from in out)) continue
        out[to] = out[from]
        if (to !== from) delete out[from]
      }
      return out
    }),
    removed: 0,
  }
}

function applyDedupeStep(step: StepInput, rows: Row[]): { rows: Row[]; removed: number } {
  const key = String(step.config.dedupeKey ?? '').replace(/^\$\./, '')
  if (!key) return { rows, removed: 0 }
  const seen = new Set<string>()
  const next = rows.filter(row => {
    const sig = JSON.stringify(row[key] ?? null)
    if (seen.has(sig)) return false
    seen.add(sig)
    return true
  })
  return { rows: next, removed: rows.length - next.length }
}

function applyConditionStep(step: StepInput, rows: Row[]): { rows: Row[]; removed: number } {
  const field = String(step.config.conditionField ?? '').replace(/^\$\./, '')
  const op = String(step.config.conditionOp ?? '==')
  const val = String(step.config.conditionValue ?? '')
  if (!field) return { rows, removed: 0 }

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
  return { rows: next, removed: rows.length - next.length }
}

async function applyStep(step: StepInput, rows: Row[]): Promise<{ rows: Row[]; removed: number }> {
  switch (step.op) {
    case 'extract':
    case 'write':
      return { rows, removed: 0 }
    case 'validate':
      return applyValidateStep(step, rows)
    case 'map':
      return applyMapStep(step, rows)
    case 'enrich':
      return { rows, removed: 0 }
    case 'dedupe':
      return applyDedupeStep(step, rows)
    case 'query': {
      const next = await runQueryStep(
        (String(step.config.queryType ?? 'sql').toLowerCase() as QueryLang),
        String(step.config.queryText ?? ''),
        rows,
      )
      return { rows: next, removed: Math.max(0, rows.length - next.length) }
    }
    case 'condition':
      return applyConditionStep(step, rows)
    default:
      return { rows, removed: 0 }
  }
}

function generateMockRowsFromSchema(datasetId: string, count: number): Row[] | null {
  const ds = getDatasets().find(d => d.id === datasetId || d.name === datasetId)
  if (!ds?.schema?.length) return null

  const seed = (i: number, field: string) => {
    let h = 0
    for (const c of `${datasetId}${field}${i}`) h = (h * 31 + c.charCodeAt(0)) >>> 0
    return h
  }

  return Array.from({ length: count }, (_, i) => {
    const row: Row = {}
    for (const { field, type, nullable, example } of ds.schema!) {
      if (nullable && seed(i, field + 'null') % 7 === 0) { row[field] = null; continue }
      const h = seed(i, field)
      switch (type) {
        case 'integer':   row[field] = (h % 10000) + 1; break
        case 'float':     row[field] = Math.round((h % 100000) / 100) / 10; break
        case 'boolean':   row[field] = h % 2 === 0; break
        case 'timestamp': row[field] = new Date(Date.now() - (h % 30) * 86400000).toISOString(); break
        case 'date':      row[field] = new Date(Date.now() - (h % 365) * 86400000).toISOString().slice(0, 10); break
        case 'array':     row[field] = Array.from({ length: (h % 4) + 1 }, (_, j) => `item_${j + 1}`); break
        case 'object':    {
          // Use example keys if parseable, else generic
          const exKeys = example.replace(/[{}]/g, '').split(',').map(s => s.trim().split(':')[0].trim()).filter(Boolean)
          row[field] = Object.fromEntries((exKeys.length ? exKeys : ['id', 'value']).map(k => [k, `${k}_${h % 100}`]))
          break
        }
        default: {
          // Use the example value as a template, varying the suffix
          const base = example.replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 16) || field
          row[field] = `${base}_${String(h).slice(-4)}`
        }
      }
    }
    return row
  })
}

export async function POST(req: NextRequest) {
  const { dataset, stepIndex, steps, rowLimit = 50, cachedRows } = await req.json() as {
    dataset: string
    stepIndex: number
    steps: StepInput[]
    rowLimit?: number
    cachedRows?: Row[]
  }

  const cap = Math.min(Math.max(rowLimit, 10), 500)
  let rows: Row[]
  let sourceRows: Row[] | undefined  // returned to client for caching

  if (cachedRows && Array.isArray(cachedRows) && cachedRows.length > 0) {
    // Client provided cached source rows — skip the expensive fetch
    rows = cachedRows
  } else {
    // Fresh fetch — slice to cap, then send back as sourceRows so client can cache
    try {
      rows = await loadDatasetRows(dataset, { rowLimit: cap })
    } catch {
      try {
        const raw = await loadDatasetRaw(dataset, { rowLimit: cap })
        rows = raw.map(item => item && typeof item === 'object' ? item as Row : { value: item })
      } catch {
        // Fall back to schema-based mock rows for datasets with no live loader
        rows = generateMockRowsFromSchema(dataset, cap) ?? []
      }
    }
    rows = rows.slice(0, cap)
    sourceRows = rows
  }

  let totalRemoved = 0
  for (let i = 0; i <= stepIndex && i < steps.length; i++) {
    const result = await applyStep(steps[i], rows)
    rows = result.rows
    totalRemoved += result.removed
  }

  const columns = rows[0] ? Object.keys(rows[0]) : []
  const preview = rows.map(r => columns.map(col => previewCell(r[col])))
  return NextResponse.json({ columns, rows: preview, rowCount: rows.length, removed: totalRemoved, sourceRows })
}
