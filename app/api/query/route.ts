/**
 * POST /api/query
 *
 * Unified query execution across datasets, live connectors, federated sources,
 * and recipe-driven query templates.
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
import type { SourceReference, SourceType } from '@/lib/datasets'
import { getConnector, getConnectorRuntimeConfig } from '@/lib/connectors'
import { executeLiveObservabilityQuery, isObservabilityConnectorType } from '@/lib/observability'
import {
  executeRedisQuery,
  fetchRedisCatalog,
  type RedisCatalogKind,
  type RedisQueryMode,
  type RedisValueType,
} from '@/lib/redis'
import { executeFederatedSql, type FederatedSourceBinding } from '@/lib/federated-query'
import { getRecipe, type QueryRecipe, type RecipeVariableDefinition } from '@/lib/query-recipes'
import { inferVariablesFromRecipe } from '@/lib/query-designer'
import { resolveTimeWindow, type TimeWindowPreset } from '@/lib/query-time'

type Row = Record<string, unknown>

interface ExplainableBinding {
  alias: string
  sourceType: SourceType
  sourceId: string
  resource?: string
  rowLimit?: number
}

function bytesForDataset(dataset: string, rows: number): number {
  if (dataset === 'events') return EVENTS_BYTES
  return bytesForRows(rows)
}

function normalizeLegacySource(body: Record<string, unknown>): SourceReference {
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

function normalizeSources(body: Record<string, unknown>, recipe: QueryRecipe | null): FederatedSourceBinding[] {
  const raw = Array.isArray(body.sources) ? body.sources : recipe?.sources
  if (raw && raw.length > 0) {
    return raw.map((entry, index) => {
      const item = entry as Record<string, unknown>
      return {
        alias: String(item.alias ?? `source_${index + 1}`),
        sourceType: (String(item.sourceType ?? 'dataset').toLowerCase() === 'connector' ? 'connector' : 'dataset') as SourceType,
        sourceId: String(item.sourceId ?? item.dataset ?? ''),
        resource: item.resource ? String(item.resource) : undefined,
        queryHint: item.queryHint ? String(item.queryHint) : undefined,
        rowLimit: item.rowLimit ? Number(item.rowLimit) : undefined,
      }
    }).filter(source => source.sourceId)
  }

  const legacySource = normalizeLegacySource(body)
  return [{ alias: 'source_rows', ...legacySource, rowLimit: Number(body.rowLimit) || 5000 }]
}

function resolveVariableDefaults(recipe: QueryRecipe | null) {
  return Object.fromEntries((recipe?.variables ?? []).map(variable => [variable.name, variable.defaultValue ?? '']))
}

function isBuiltinTimeVariable(name: string) {
  return ['startTime', 'endTime', 'timespanIso', 'bucketHint'].includes(name)
}

function coerceVariable(variable: RecipeVariableDefinition, value: unknown): string | number | boolean {
  if (value === undefined || value === null || value === '') {
    if (variable.required) throw new Error(`Missing required variable "${variable.label || variable.name}"`)
    return variable.defaultValue ?? ''
  }
  switch (variable.type) {
    case 'number': {
      const n = Number(value)
      if (!Number.isFinite(n)) throw new Error(`Variable "${variable.name}" must be a number`)
      if (variable.validation?.min !== undefined && n < variable.validation.min) throw new Error(`Variable "${variable.name}" is below minimum`)
      if (variable.validation?.max !== undefined && n > variable.validation.max) throw new Error(`Variable "${variable.name}" is above maximum`)
      return n
    }
    case 'boolean':
      return value === true || String(value).toLowerCase() === 'true'
    case 'enum': {
      const text = String(value)
      if (variable.options?.length && !variable.options.includes(text)) throw new Error(`Variable "${variable.name}" must be one of ${variable.options.join(', ')}`)
      return text
    }
    case 'date':
    case 'datetime':
    case 'string':
    case 'timeWindow':
    default: {
      const text = String(value)
      if (variable.validation?.pattern && !(new RegExp(variable.validation.pattern).test(text))) {
        throw new Error(`Variable "${variable.name}" does not match the required pattern`)
      }
      return text
    }
  }
}

function renderLiteral(value: string | number | boolean, lang: string): string {
  if (typeof value === 'number') return String(value)
  if (typeof value === 'boolean') {
    if (lang === 'sql') return value ? 'TRUE' : 'FALSE'
    return value ? 'true' : 'false'
  }
  if (lang === 'kql') return `"${value.replace(/"/g, '\\"')}"`
  return `'${value.replace(/'/g, "''")}'`
}

function renderRaw(value: string | number | boolean): string {
  return String(value)
}

function applyTemplate(text: string, replacements: Record<string, string>) {
  return Object.entries(replacements).reduce((acc, [key, value]) => (
    acc.replace(new RegExp(`\\{\\{\\s*${key}\\s*\\}\\}`, 'g'), value)
  ), text)
}

function explainableBindings(sources: FederatedSourceBinding[]): ExplainableBinding[] {
  return sources.map(source => ({
    alias: source.alias,
    sourceType: source.sourceType,
    sourceId: source.sourceId,
    resource: source.resource,
    rowLimit: source.rowLimit,
  }))
}

export async function POST(req: NextRequest) {
  const t0 = performance.now()

  try {
    const body = await req.json() as Record<string, unknown>
    const recipe = body.recipeId ? getRecipe(String(body.recipeId)) : null
    if (body.recipeId && !recipe) {
      return NextResponse.json({ error: `Unknown recipe "${String(body.recipeId)}"` }, { status: 404 })
    }

    const lang = String(body.lang ?? recipe?.lang ?? 'sql').toLowerCase()
    const rawQuery = String(body.query ?? body.sql ?? recipe?.queryText ?? '')
    if (!rawQuery.trim()) {
      return NextResponse.json({ error: 'Empty query' }, { status: 400 })
    }

    const sources = normalizeSources(body, recipe)
    const inferredRecipe = inferVariablesFromRecipe(rawQuery, sources, recipe)
    const requestedVariables = (body.variables && typeof body.variables === 'object' ? body.variables : {}) as Record<string, unknown>
    const recipeDefaults = {
      ...resolveVariableDefaults(recipe),
      ...Object.fromEntries(inferredRecipe.variables.map(variable => [variable.name, variable.defaultValue ?? ''])),
    }
    const variableDefs = inferredRecipe.variables.filter(variable => !isBuiltinTimeVariable(variable.name))
    const coercedVariables = Object.fromEntries(variableDefs.map(variable => (
      [variable.name, coerceVariable(variable, requestedVariables[variable.name] ?? recipeDefaults[variable.name])]
    )))

    const timeWindowPreset = String(
      body.timeWindow
      ?? requestedVariables.timeWindow
      ?? (recipe?.timeWindowBinding?.enabled ? recipe.timeWindowBinding.defaultPreset : 'last_24h'),
    ) as TimeWindowPreset
    const resolvedTimeWindow = resolveTimeWindow(timeWindowPreset)

    const queryReplacements = Object.fromEntries([
      ...Object.entries(coercedVariables).map(([key, value]) => [key, renderLiteral(value as string | number | boolean, lang)]),
      ['startTime', renderLiteral(resolvedTimeWindow.startTime, lang)],
      ['endTime', renderLiteral(resolvedTimeWindow.endTime, lang)],
      ['timespanIso', renderLiteral(resolvedTimeWindow.timespanIso, lang)],
      ['bucketHint', renderLiteral(resolvedTimeWindow.bucketHint, lang)],
    ])
    const rawReplacements = Object.fromEntries([
      ...Object.entries(coercedVariables).map(([key, value]) => [key, renderRaw(value as string | number | boolean)]),
      ['startTime', resolvedTimeWindow.startTime],
      ['endTime', resolvedTimeWindow.endTime],
      ['timespanIso', resolvedTimeWindow.timespanIso],
      ['bucketHint', resolvedTimeWindow.bucketHint],
    ])

    const renderedQuery = applyTemplate(rawQuery, queryReplacements)
    const resolvedSources = sources.map(source => ({
      ...source,
      resource: source.resource ? applyTemplate(source.resource, rawReplacements) : source.resource,
      queryHint: source.queryHint ? applyTemplate(source.queryHint, rawReplacements) : source.queryHint,
    }))

    if (!renderedQuery.trim()) {
      return NextResponse.json({ error: 'Rendered query is empty after variable substitution' }, { status: 400 })
    }

    const isNewFederatedRequest = Array.isArray(body.sources) || !!body.recipeId

    if (isNewFederatedRequest && lang === 'sql') {
      const federated = await executeFederatedSql(renderedQuery, resolvedSources, {
        defaultRowLimit: 500,
        maxSources: 6,
        maxTotalRows: 5_000,
        timespan: resolvedTimeWindow.timespanIso,
      })
      return NextResponse.json({
        columns: federated.columns,
        rows: federated.rows,
        rowCount: federated.rowCount,
        totalRows: federated.totalRows,
        durationMs: Math.round(performance.now() - t0),
        bytesScanned: resolvedSources.reduce((sum, source) => sum + bytesForResolvedSource(source, source.rowLimit ?? 500), 0),
        renderedQuery,
        executionMode: 'federated',
        warnings: federated.warnings,
        truncated: federated.truncated,
        recipeId: recipe?.id ?? null,
        boundVariables: {
          ...coercedVariables,
          timeWindow: resolvedTimeWindow.preset,
          startTime: resolvedTimeWindow.startTime,
          endTime: resolvedTimeWindow.endTime,
          timespanIso: resolvedTimeWindow.timespanIso,
          bucketHint: resolvedTimeWindow.bucketHint,
        },
        timeWindow: resolvedTimeWindow,
        sourceBindings: explainableBindings(resolvedSources),
      })
    }

    const source = resolvedSources[0] ?? { alias: 'source_rows', ...normalizeLegacySource(body) }

    if (source.sourceType === 'connector') {
      const connector = getConnector(source.sourceId)
      if (!connector) {
        return NextResponse.json({ error: `Unknown connector "${source.sourceId}"` }, { status: 404 })
      }

      if (connector.type === 'appinsights' || isObservabilityConnectorType(connector.type)) {
        const result = await executeLiveObservabilityQuery(source.sourceId, renderedQuery, {
          timespan: resolvedTimeWindow.timespanIso,
        })
        return NextResponse.json({
          ...result,
          kqlTranslated: result.translatedQuery,
          bytesScanned: 0,
          renderedQuery,
          executionMode: 'pushdown',
          warnings: [],
          recipeId: recipe?.id ?? null,
          boundVariables: {
            ...coercedVariables,
            timeWindow: resolvedTimeWindow.preset,
            startTime: resolvedTimeWindow.startTime,
            endTime: resolvedTimeWindow.endTime,
            timespanIso: resolvedTimeWindow.timespanIso,
            bucketHint: resolvedTimeWindow.bucketHint,
          },
          timeWindow: resolvedTimeWindow,
          sourceBindings: explainableBindings([source]),
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
              limit: Number(body.rowLimit) || 500,
            })
          : await executeRedisQuery(runtimeConfig, {
              mode,
              query: renderedQuery,
              valueType,
              rowLimit: Number(body.rowLimit) || 500,
            })

        return NextResponse.json({
          ...result,
          bytesScanned: 0,
          renderedQuery,
          executionMode: 'pushdown',
          warnings: [],
          recipeId: recipe?.id ?? null,
          boundVariables: {
            ...coercedVariables,
            timeWindow: resolvedTimeWindow.preset,
            startTime: resolvedTimeWindow.startTime,
            endTime: resolvedTimeWindow.endTime,
            timespanIso: resolvedTimeWindow.timespanIso,
            bucketHint: resolvedTimeWindow.bucketHint,
          },
          timeWindow: resolvedTimeWindow,
          sourceBindings: explainableBindings([source]),
        }, { status: result.error ? 400 : 200 })
      }
    }

    if (lang === 'sql' || lang === 'kql') {
      let sql = renderedQuery
      let kqlTranslated: string | undefined
      if (lang === 'kql') {
        sql = kqlToSQL(renderedQuery)
        kqlTranslated = sql
      }

      if (!isNewFederatedRequest && lang === 'sql') {
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
            renderedQuery,
            executionMode: 'pushdown',
            warnings: [],
            recipeId: recipe?.id ?? null,
            boundVariables: {
              ...coercedVariables,
              timeWindow: resolvedTimeWindow.preset,
              startTime: resolvedTimeWindow.startTime,
              endTime: resolvedTimeWindow.endTime,
              timespanIso: resolvedTimeWindow.timespanIso,
              bucketHint: resolvedTimeWindow.bucketHint,
            },
            timeWindow: resolvedTimeWindow,
            sourceBindings: explainableBindings([source]),
          })
        }
      }

      const rows = await loadSourceRows(source, { rowLimit: Number(body.rowLimit) || 5_000, timespan: resolvedTimeWindow.timespanIso })
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
        renderedQuery,
        executionMode: 'in_memory',
        warnings: [],
        recipeId: recipe?.id ?? null,
        boundVariables: {
          ...coercedVariables,
          timeWindow: resolvedTimeWindow.preset,
          startTime: resolvedTimeWindow.startTime,
          endTime: resolvedTimeWindow.endTime,
          timespanIso: resolvedTimeWindow.timespanIso,
          bucketHint: resolvedTimeWindow.bucketHint,
        },
        timeWindow: resolvedTimeWindow,
        sourceBindings: explainableBindings([source]),
      })
    }

    if (isNewFederatedRequest) {
      return NextResponse.json({ error: 'Federated multi-source querying is currently SQL-only' }, { status: 400 })
    }

    if (lang === 'jsonpath') {
      const { JSONPath } = await import('jsonpath-plus')
      const sourceRows = await loadSourceRaw(source, { rowLimit: Number(body.rowLimit) || 5_000 })
      const path = renderedQuery.trim().replace(/^#[^\n]*\n?/gm, '').trim()
      const raw = JSONPath({ path, json: sourceRows })
      const { columns, rows } = normalise(raw)
      return NextResponse.json({
        columns,
        rows,
        rowCount: rows.length,
        totalRows: sourceRows.length,
        durationMs: Math.round(performance.now() - t0),
        bytesScanned: bytesForResolvedSource(source, sourceRows.length),
        renderedQuery,
        executionMode: 'in_memory',
        warnings: [],
        recipeId: recipe?.id ?? null,
        boundVariables: coercedVariables,
        timeWindow: resolvedTimeWindow,
        sourceBindings: explainableBindings([source]),
      })
    }

    if (lang === 'jmespath') {
      const jmespath = await import('jmespath')
      const sourceRows = await loadSourceRaw(source, { rowLimit: Number(body.rowLimit) || 5_000 })
      const expr = renderedQuery.trim().replace(/^#[^\n]*\n?/gm, '').trim()
      const raw = jmespath.search(sourceRows, expr)
      const { columns, rows } = normalise(raw)
      return NextResponse.json({
        columns,
        rows,
        rowCount: rows.length,
        totalRows: sourceRows.length,
        durationMs: Math.round(performance.now() - t0),
        bytesScanned: bytesForResolvedSource(source, sourceRows.length),
        renderedQuery,
        executionMode: 'in_memory',
        warnings: [],
        recipeId: recipe?.id ?? null,
        boundVariables: coercedVariables,
        timeWindow: resolvedTimeWindow,
        sourceBindings: explainableBindings([source]),
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
