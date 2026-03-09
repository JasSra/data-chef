/**
 * POST /api/query
 *
 * Unified query execution across datasets and live connectors.
 */

import { NextRequest, NextResponse } from 'next/server'
import { executeSQL } from '@/lib/mini-sql'
import { EVENTS_BYTES } from '@/lib/synthetic-data'
import {
  executeSourceQuery,
  loadSourceRows,
  loadSourceRaw,
  bytesForRows,
  bytesForSource,
} from '@/lib/runtime-data'
import type { SourceReference } from '@/lib/datasets'
import { getConnector, getConnectorRuntimeConfig } from '@/lib/connectors'
import { executeLiveObservabilityQuery, isObservabilityConnectorType } from '@/lib/observability'
import {
  executeRedisQuery,
  fetchRedisCatalog,
  type RedisCatalogKind,
  type RedisQueryMode,
  type RedisValueType,
} from '@/lib/redis'

type Row = Record<string, unknown>

function bytesForDataset(dataset: string, rows: number): number {
  if (dataset === 'events') return EVENTS_BYTES
  return bytesForRows(rows)
}

function normalizeSource(body: Record<string, unknown>): SourceReference {
  const sourceType = String(body.sourceType ?? (body.connectorId ? 'connector' : 'dataset')).toLowerCase()
  const sourceId = String(body.sourceId ?? body.connectorId ?? body.dataset ?? 'rick-morty-characters')
  return {
    sourceType: sourceType === 'connector' ? 'connector' : 'dataset',
    sourceId,
    resource: body.resource ? String(body.resource) : undefined,
  }
}

function bytesForResolvedSource(source: SourceReference, rowCount: number): number {
  return source.sourceType === 'dataset'
    ? bytesForDataset(source.sourceId, rowCount)
    : bytesForSource(source.sourceType, rowCount)
}

function kqlToSQL(kql: string): string {
  const pipes = kql.replace(/\/\/[^\n]*/g, '').split('|').map(s => s.trim()).filter(Boolean)
  if (!pipes.length) throw new Error('Empty KQL query')

  const table = pipes[0]
  let select = '*'
  let where = ''
  let groupBy = ''
  let orderBy = ''
  let limit = ''

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
      select = `${field}, COUNT(*) AS count_`
      groupBy = field
    } else if (/^summarize\s+sum\((\w+)\)\s+by\s+/i.test(op)) {
      const match = op.match(/^summarize\s+sum\((\w+)\)\s+by\s+(.+)/i)
      if (!match) continue
      select = `${match[2].trim()}, SUM(${match[1]}) AS sum_${match[1]}`
      groupBy = match[2].trim()
    } else if (/^summarize\s+avg\((\w+)\)\s+by\s+/i.test(op)) {
      const match = op.match(/^summarize\s+avg\((\w+)\)\s+by\s+(.+)/i)
      if (!match) continue
      select = `${match[2].trim()}, AVG(${match[1]}) AS avg_${match[1]}`
      groupBy = match[2].trim()
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

function normalise(raw: unknown): { columns: string[]; rows: string[][] } {
  const arr = Array.isArray(raw) ? raw : (raw !== null && raw !== undefined ? [raw] : [])
  if (!arr.length) return { columns: ['(empty)'], rows: [] }

  const first = arr[0]
  if (typeof first === 'object' && first !== null) {
    const columns = Object.keys(first as object)
    const rows = arr.map(item =>
      columns.map(column => {
        const value = (item as Row)[column]
        if (value === null || value === undefined) return '∅'
        if (Array.isArray(value)) return `[${value.length}]`
        if (typeof value === 'object') return JSON.stringify(value)
        return String(value)
      }),
    )
    return { columns, rows }
  }

  return { columns: ['value'], rows: arr.map(value => [String(value)]) }
}

export async function POST(req: NextRequest) {
  const t0 = performance.now()

  try {
    const body = await req.json() as Record<string, unknown>
    const source = normalizeSource(body)
    const lang = String(body.lang ?? 'sql').toLowerCase()
    const rawQuery = String(body.sql ?? body.query ?? '')
    const rowLimit = Math.max(1, Math.min(50_000, Number(body.rowLimit) || 5_000))

    if (!rawQuery.trim()) {
      return NextResponse.json({ error: 'Empty query' }, { status: 400 })
    }

    if (source.sourceType === 'connector') {
      const connector = getConnector(source.sourceId)
      if (!connector) {
        return NextResponse.json({ error: `Unknown connector "${source.sourceId}"` }, { status: 404 })
      }

      if (connector.type === 'appinsights' || isObservabilityConnectorType(connector.type)) {
        const result = await executeLiveObservabilityQuery(source.sourceId, rawQuery, {
          timespan: body.timespan ? String(body.timespan) : undefined,
        })
        return NextResponse.json({
          ...result,
          kqlTranslated: result.translatedQuery,
          bytesScanned: 0,
        }, { status: result.error ? 400 : 200 })
      }

      if (connector.type === 'redis') {
        const runtimeConfig = getConnectorRuntimeConfig(source.sourceId)
        if (!runtimeConfig) {
          return NextResponse.json({ error: `No runtime config found for connector "${connector.name}"` }, { status: 404 })
        }

        const mode = String(body.redisMode ?? body.mode ?? 'command').toLowerCase() as RedisQueryMode
        const valueType = body.redisValueType
          ? String(body.redisValueType).toLowerCase() as RedisValueType
          : body.valueType
          ? String(body.valueType).toLowerCase() as RedisValueType
          : undefined
        const catalog = body.catalog ? String(body.catalog).toLowerCase() as RedisCatalogKind : undefined

        const result = mode === 'catalog'
          ? await fetchRedisCatalog(runtimeConfig, {
              catalog: catalog ?? 'capabilities',
              pattern: source.resource,
              limit: rowLimit,
            })
          : await executeRedisQuery(runtimeConfig, {
              mode,
              query: rawQuery,
              valueType,
              rowLimit,
            })

        return NextResponse.json({
          ...result,
          bytesScanned: 0,
        }, { status: result.error ? 400 : 200 })
      }
    }

    if (lang === 'sql' || lang === 'kql') {
      let sql = rawQuery
      let kqlTranslated: string | undefined
      if (lang === 'kql') {
        sql = kqlToSQL(rawQuery)
        kqlTranslated = sql
      }

      if (lang === 'sql') {
        const pushedDown = await executeSourceQuery(source, sql)
        if (pushedDown) {
          return NextResponse.json({
            columns: pushedDown.columns,
            rows: pushedDown.rows,
            rowCount: pushedDown.rowCount,
            totalRows: pushedDown.totalRows,
            durationMs: Math.round(performance.now() - t0),
            bytesScanned: bytesForResolvedSource(source, pushedDown.rowCount),
            pushedDown: true,
          })
        }
      }

      const rows = await loadSourceRows(source, { rowLimit })
      const result = executeSQL(sql, rows)
      return NextResponse.json({
        columns: result.columns,
        rows: result.rows,
        rowCount: result.rowCount,
        totalRows: rows.length,
        durationMs: Math.round(performance.now() - t0),
        bytesScanned: bytesForResolvedSource(source, rows.length),
        kqlTranslated,
        error: result.error,
      })
    }

    if (lang === 'jsonpath') {
      const { JSONPath } = await import('jsonpath-plus')
      const sourceRows = await loadSourceRaw(source, { rowLimit })
      const path = rawQuery.trim().replace(/^#[^\n]*\n?/gm, '').trim()
      const raw = JSONPath({ path, json: sourceRows })
      const { columns, rows } = normalise(raw)
      return NextResponse.json({
        columns,
        rows,
        rowCount: rows.length,
        totalRows: sourceRows.length,
        durationMs: Math.round(performance.now() - t0),
        bytesScanned: bytesForResolvedSource(source, sourceRows.length),
      })
    }

    if (lang === 'jmespath') {
      const jmespath = await import('jmespath')
      const sourceRows = await loadSourceRaw(source, { rowLimit })
      const expr = rawQuery.trim().replace(/^#[^\n]*\n?/gm, '').trim()
      const raw = jmespath.search(sourceRows, expr)
      const { columns, rows } = normalise(raw)
      return NextResponse.json({
        columns,
        rows,
        rowCount: rows.length,
        totalRows: sourceRows.length,
        durationMs: Math.round(performance.now() - t0),
        bytesScanned: bytesForResolvedSource(source, sourceRows.length),
      })
    }

    return NextResponse.json({ error: `Unknown lang: ${lang}` }, { status: 400 })
  } catch (error: unknown) {
    return NextResponse.json({
      error: error instanceof Error ? error.message : String(error),
      columns: [],
      rows: [],
      rowCount: 0,
      durationMs: Math.round(performance.now() - t0),
    }, { status: 400 })
  }
}
