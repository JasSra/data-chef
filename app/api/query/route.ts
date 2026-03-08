/**
 * POST /api/query
 *
 * Server-side query execution. The client sends only the query text;
 * all data lives in server module memory (Rick & Morty cache + 500K events).
 *
 * Body: { sql: string, lang: 'sql'|'jsonpath'|'jmespath'|'kql', dataset: string }
 * Response: { columns, rows, rowCount, totalRows, durationMs, bytesScanned, kqlTranslated?, error? }
 */

import { NextRequest, NextResponse } from 'next/server'
import { executeSQL } from '@/lib/mini-sql'
import { EVENTS_BYTES } from '@/lib/synthetic-data'
import {
  executeDatasetQuery,
  loadDatasetRows,
  loadDatasetRaw,
  bytesForRows,
} from '@/lib/runtime-data'

type Row = Record<string, unknown>

function bytesFor(dataset: string, rows: number): number {
  if (dataset === 'events') return EVENTS_BYTES
  return bytesForRows(rows)
}

/* ── KQL → SQL (minimal transpiler, mirrors client version) ─────────────────── */
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
        .replace(/contains\("([^"]+)"\)/gi, "LIKE '%$1%'")
    } else if (/^project\s+/i.test(op)) {
      select = op.replace(/^project\s+/i, '')
    } else if (/^summarize\s+count\(\)\s+by\s+/i.test(op)) {
      const field = op.replace(/^summarize\s+count\(\)\s+by\s+/i, '').trim()
      select = `${field}, COUNT(*) AS count_`; groupBy = field
    } else if (/^summarize\s+sum\((\w+)\)\s+by\s+/i.test(op)) {
      const m = op.match(/^summarize\s+sum\((\w+)\)\s+by\s+(.+)/i)!
      select = `${m[2].trim()}, SUM(${m[1]}) AS sum_${m[1]}`; groupBy = m[2].trim()
    } else if (/^summarize\s+avg\((\w+)\)\s+by\s+/i.test(op)) {
      const m = op.match(/^summarize\s+avg\((\w+)\)\s+by\s+(.+)/i)!
      select = `${m[2].trim()}, AVG(${m[1]}) AS avg_${m[1]}`; groupBy = m[2].trim()
    } else if (/^(order|sort)\s+by\s+/i.test(op)) {
      orderBy = op.replace(/^(order|sort)\s+by\s+/i, '')
        .replace(/\basc\b/gi, 'ASC').replace(/\bdesc\b/gi, 'DESC')
    } else if (/^(limit|take|top)\s+\d+/i.test(op)) {
      limit = op.replace(/^(limit|take|top)\s+/i, '')
    }
  }

  let sql = `SELECT ${select} FROM ${table}`
  if (where)   sql += ` WHERE ${where}`
  if (groupBy) sql += ` GROUP BY ${groupBy}`
  if (orderBy) sql += ` ORDER BY ${orderBy}`
  if (limit)   sql += ` LIMIT ${limit}`
  return sql
}

/* ── Normalise any array result to { columns, rows } ────────────────────────── */
function normalise(raw: unknown): { columns: string[]; rows: string[][] } {
  const arr = Array.isArray(raw) ? raw : (raw !== null && raw !== undefined ? [raw] : [])
  if (!arr.length) return { columns: ['(empty)'], rows: [] }

  const first = arr[0]
  if (typeof first === 'object' && first !== null) {
    const columns = Object.keys(first as object)
    const rows = arr.map(item =>
      columns.map(c => {
        const v = (item as Row)[c]
        if (v === null || v === undefined) return '∅'
        if (Array.isArray(v))              return `[${v.length}]`
        if (typeof v === 'object')         return JSON.stringify(v)
        return String(v)
      })
    )
    return { columns, rows }
  }
  return { columns: ['value'], rows: arr.map(v => [String(v)]) }
}

/* ── POST handler ─────────────────────────────────────────────────────────── */
export async function POST(req: NextRequest) {
  const t0 = performance.now()
  try {
    const body = await req.json()
    const { lang = 'sql', dataset = 'rick-morty-characters' } = body
    const rawQuery: string = body.sql ?? body.query ?? ''

    if (!rawQuery.trim()) {
      return NextResponse.json({ error: 'Empty query' }, { status: 400 })
    }

    /* ── SQL / KQL ── */
    if (lang === 'sql' || lang === 'kql') {
      let sql = rawQuery
      let kqlTranslated: string | undefined
      if (lang === 'kql') {
        sql = kqlToSQL(rawQuery)
        kqlTranslated = sql
      }
      if (lang === 'sql') {
        const pushedDown = await executeDatasetQuery(dataset, sql)
        if (pushedDown) {
          const durationMs = Math.round(performance.now() - t0)
          return NextResponse.json({
            columns:      pushedDown.columns,
            rows:         pushedDown.rows,
            rowCount:     pushedDown.rowCount,
            totalRows:    pushedDown.totalRows,
            durationMs,
            bytesScanned: bytesFor(dataset, pushedDown.rowCount),
            pushedDown:   true,
          })
        }
      }
      const data = await loadDatasetRows(dataset, { rowLimit: 5_000 })
      const result = executeSQL(sql, data)
      const durationMs = Math.round(performance.now() - t0)
      return NextResponse.json({
        columns:      result.columns,
        rows:         result.rows,
        rowCount:     result.rowCount,
        totalRows:    data.length,
        durationMs,
        bytesScanned: bytesFor(dataset, data.length),
        kqlTranslated,
        error:        result.error,
      })
    }

    /* ── JSONPath ── */
    if (lang === 'jsonpath') {
      const { JSONPath } = await import('jsonpath-plus')
      const chars = await loadDatasetRaw(dataset, { rowLimit: 5_000 })
      const path = rawQuery.trim().replace(/^#[^\n]*\n?/gm, '').trim()
      const raw = JSONPath({ path, json: chars })
      const { columns, rows } = normalise(raw)
      const durationMs = Math.round(performance.now() - t0)
      return NextResponse.json({
        columns, rows, rowCount: rows.length,
        totalRows: chars.length, durationMs,
        bytesScanned: bytesFor(dataset, chars.length),
      })
    }

    /* ── JMESPath ── */
    if (lang === 'jmespath') {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const jmespath = (await import('jmespath')) as any
      const chars = await loadDatasetRaw(dataset, { rowLimit: 5_000 })
      const expr = rawQuery.trim().replace(/^#[^\n]*\n?/gm, '').trim()
      const raw  = jmespath.search(chars, expr)
      const { columns, rows } = normalise(raw)
      const durationMs = Math.round(performance.now() - t0)
      return NextResponse.json({
        columns, rows, rowCount: rows.length,
        totalRows: chars.length, durationMs,
        bytesScanned: bytesFor(dataset, chars.length),
      })
    }

    return NextResponse.json({ error: `Unknown lang: ${lang}` }, { status: 400 })

  } catch (e: unknown) {
    return NextResponse.json({
      error: e instanceof Error ? e.message : String(e),
      columns: [], rows: [], rowCount: 0,
      durationMs: Math.round(performance.now() - t0),
    })
  }
}
