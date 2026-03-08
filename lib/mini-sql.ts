/**
 * Mini SQL engine — no deps, browser-native.
 *
 * Supports:
 *   SELECT [DISTINCT] [* | col [AS alias], ...agg(col) AS alias]
 *   FROM   <ignored — always queries the `data` array>
 *   WHERE  <conditions with =, !=, <, >, <=, >=, LIKE, IN, NOT IN,
 *            IS NULL, IS NOT NULL, AND, OR, NOT>
 *   GROUP BY  <one or more columns>
 *   HAVING <conditions on aggregated values>
 *   ORDER BY  <column or alias> [ASC | DESC]
 *   LIMIT  <n>
 *
 * Aggregates: COUNT(*), COUNT(col), SUM(col), AVG(col), MIN(col), MAX(col)
 */

/* ── Types ─────────────────────────────────────────────────────────────── */
type Row = Record<string, unknown>

interface SelectCol {
  alias: string           // final column name in result
  field?: string          // source field name (undefined for wildcards)
  agg?: 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX'
  aggArg?: string         // field arg to aggregate ('*' for COUNT(*))
  isStar?: boolean        // SELECT *
}

interface ParsedSQL {
  select: SelectCol[]
  distinct: boolean       // SELECT DISTINCT
  where: string           // raw WHERE clause string
  groupBy: string[]       // one or more column names
  having: string          // raw HAVING clause string (applied after aggregation)
  orderBy: string         // column or alias
  orderDir: 'ASC' | 'DESC'
  limit: number
}

export interface SqlResult {
  columns: string[]
  rows:    string[][]
  rowCount: number
  durationMs: number
  error?: string
}

/* ── Public API ─────────────────────────────────────────────────────────── */
export function executeSQL(sql: string, data: Row[]): SqlResult {
  const t0 = performance.now()
  try {
    const parsed = parse(sql.replace(/\s+/g, ' ').replace(/;\s*$/, '').trim())
    const rows   = run(parsed, data)
    const durationMs = Math.round(performance.now() - t0)
    return { columns: rows.columns, rows: rows.rows, rowCount: rows.rows.length, durationMs }
  } catch (e: unknown) {
    return { columns: [], rows: [], rowCount: 0, durationMs: 0, error: String(e) }
  }
}

/* ── Parser ─────────────────────────────────────────────────────────────── */
function parse(sql: string): ParsedSQL {
  const upper = sql.toUpperCase()

  /* locate clause positions (-1 = absent) */
  const selectIdx  = upper.indexOf('SELECT')
  const fromIdx    = upper.indexOf(' FROM ')
  const whereIdx   = upper.indexOf(' WHERE ')
  const groupIdx   = upper.indexOf(' GROUP BY ')
  const havingIdx  = upper.indexOf(' HAVING ')
  const orderIdx   = upper.indexOf(' ORDER BY ')
  const limitIdx   = upper.indexOf(' LIMIT ')

  if (selectIdx === -1) throw new Error('Missing SELECT')
  if (fromIdx   === -1) throw new Error('Missing FROM')

  /* Helper: first positive index from a list of candidates, or fallback */
  function firstAfter(after: number, candidates: number[], fallback: number): number {
    const valid = candidates.filter(c => c > after)
    return valid.length ? Math.min(...valid) : fallback
  }

  /* DISTINCT */
  const rawSelectStr = sql.slice(selectIdx + 6, fromIdx).trim()
  const distinct     = /^DISTINCT\b/i.test(rawSelectStr)
  const selectStr    = distinct ? rawSelectStr.replace(/^DISTINCT\s+/i, '') : rawSelectStr

  /* WHERE */
  const whereEnd = firstAfter(whereIdx, [groupIdx, havingIdx, orderIdx, limitIdx], sql.length)
  const whereStr = whereIdx !== -1 ? sql.slice(whereIdx + 7, whereEnd).trim() : ''

  /* GROUP BY */
  const groupEnd = firstAfter(groupIdx, [havingIdx, orderIdx, limitIdx], sql.length)
  const groupStr = groupIdx !== -1 ? sql.slice(groupIdx + 9, groupEnd).trim() : ''

  /* HAVING */
  const havingEnd = firstAfter(havingIdx, [orderIdx, limitIdx], sql.length)
  const havingStr = havingIdx !== -1 ? sql.slice(havingIdx + 8, havingEnd).trim() : ''

  /* ORDER BY */
  const orderEnd  = limitIdx > orderIdx ? limitIdx : sql.length
  let orderStr    = ''
  let orderDir: 'ASC' | 'DESC' = 'ASC'
  if (orderIdx !== -1) {
    const raw = sql.slice(orderIdx + 9, orderEnd).trim()
    const m   = raw.match(/^(.*?)\s+(ASC|DESC)\s*$/i)
    if (m) { orderStr = m[1].trim(); orderDir = m[2].toUpperCase() as 'ASC' | 'DESC' }
    else   { orderStr = raw }
  }

  /* LIMIT */
  const limitN = limitIdx !== -1
    ? parseInt(sql.slice(limitIdx + 7).trim(), 10)
    : Infinity

  return {
    select:   parseSelect(selectStr),
    distinct,
    where:    whereStr,
    groupBy:  groupStr ? groupStr.split(/\s*,\s*/) : [],
    having:   havingStr,
    orderBy:  orderStr,
    orderDir,
    limit:    isNaN(limitN) ? Infinity : limitN,
  }
}

/* ── SELECT clause parser ────────────────────────────────────────────────── */
function parseSelect(raw: string): SelectCol[] {
  if (raw.trim() === '*') return [{ alias: '*', isStar: true }]

  return splitTopLevel(raw).map(token => {
    token = token.trim()

    /* alias: "expr AS alias" */
    const asMatch = token.match(/^(.*?)\s+AS\s+(\w+)\s*$/i)
    const expr    = asMatch ? asMatch[1].trim() : token
    const alias   = asMatch ? asMatch[2] : expr

    /* aggregate: COUNT(*), SUM(col), etc. */
    const aggMatch = expr.match(/^(COUNT|SUM|AVG|MIN|MAX)\s*\(\s*(.*?)\s*\)\s*$/i)
    if (aggMatch) {
      return {
        alias,
        agg:    aggMatch[1].toUpperCase() as SelectCol['agg'],
        aggArg: aggMatch[2] === '*' ? '*' : aggMatch[2],
      }
    }

    /* CASE … END */
    if (/^CASE\b/i.test(expr)) return { alias, field: undefined }

    return { alias, field: expr }
  })
}

/* Split a comma-separated list but ignore commas inside parentheses */
function splitTopLevel(s: string): string[] {
  const parts: string[] = []
  let depth = 0, start = 0
  for (let i = 0; i < s.length; i++) {
    if (s[i] === '(') depth++
    else if (s[i] === ')') depth--
    else if (s[i] === ',' && depth === 0) {
      parts.push(s.slice(start, i))
      start = i + 1
    }
  }
  parts.push(s.slice(start))
  return parts
}

/* ── Executor ────────────────────────────────────────────────────────────── */
function run(
  { select, distinct, where, groupBy, having, orderBy, orderDir, limit }: ParsedSQL,
  data: Row[]
): { columns: string[]; rows: string[][] } {

  /* WHERE filter */
  let rows: Row[] = where ? data.filter(r => evalWhere(where, r)) : data

  /* GROUP BY / aggregation */
  const hasAgg = select.some(c => c.agg)
  if (groupBy.length || hasAgg) {
    rows = applyGroupBy(rows, groupBy, select)
  } else {
    /* simple projection */
    const isStar = select.some(c => c.isStar)
    rows = rows.map(r =>
      isStar
        ? { ...r }
        : Object.fromEntries(
            select.map(c => [c.alias, c.field !== undefined ? r[c.field] : null])
          )
    )
  }

  /* HAVING — filter on aggregated rows (evaluated after GROUP BY) */
  if (having) {
    rows = rows.filter(r => evalWhere(having, r))
  }

  /* DISTINCT — deduplicate based on all column values */
  if (distinct) {
    const seen = new Set<string>()
    rows = rows.filter(r => {
      const key = JSON.stringify(r)
      if (seen.has(key)) return false
      seen.add(key); return true
    })
  }

  /* ORDER BY */
  if (orderBy) {
    rows = [...rows].sort((a, b) => {
      const av = a[orderBy] ?? 0
      const bv = b[orderBy] ?? 0
      if (typeof av === 'number' && typeof bv === 'number') {
        return orderDir === 'ASC' ? av - bv : bv - av
      }
      const [as_, bs_] = [String(av), String(bv)]
      return orderDir === 'ASC' ? as_.localeCompare(bs_) : bs_.localeCompare(as_)
    })
  }

  /* LIMIT */
  rows = rows.slice(0, limit === Infinity ? undefined : limit)

  /* Format */
  const columns = rows.length ? Object.keys(rows[0]) : select.map(c => c.alias)
  const formatted = rows.map(r =>
    columns.map(col => {
      const v = r[col]
      if (v === null || v === undefined) return '∅'
      if (typeof v === 'number')         return String(Math.round(v * 1000) / 1000)
      return String(v)
    })
  )

  return { columns, rows: formatted }
}

/* ── GROUP BY ────────────────────────────────────────────────────────────── */
function applyGroupBy(data: Row[], groupByCols: string[], cols: SelectCol[]): Row[] {
  const groups = new Map<string, Row[]>()

  if (groupByCols.length) {
    for (const row of data) {
      // Composite key: join all group-by column values with a null-byte separator
      const key = groupByCols.map(c => String(row[c] ?? '')).join('\x00')
      const g   = groups.get(key) ?? []
      g.push(row)
      groups.set(key, g)
    }
  } else {
    groups.set('__all__', data)
  }

  return Array.from(groups.values()).map(groupRows => {
    const result: Row = {}
    for (const col of cols) {
      if (col.isStar) {
        Object.assign(result, groupRows[0])
        continue
      }
      if (!col.agg) {
        // plain column — take value from first row in the group
        result[col.alias] = col.field !== undefined ? groupRows[0][col.field] : null
        continue
      }
      const vals = col.aggArg === '*'
        ? groupRows.map(() => 1)
        : groupRows
            .map(r => (col.aggArg ? r[col.aggArg] : 0))
            .filter(v => v !== null && v !== undefined)
            .map(Number)

      result[col.alias] = aggregate(col.agg!, vals)
    }
    return result
  })
}

function aggregate(fn: NonNullable<SelectCol['agg']>, vals: number[]): number {
  if (!vals.length) return 0
  switch (fn) {
    case 'COUNT': return vals.length
    case 'SUM':   return vals.reduce((a, b) => a + b, 0)
    case 'AVG':   return vals.reduce((a, b) => a + b, 0) / vals.length
    case 'MIN':   return Math.min(...vals)
    case 'MAX':   return Math.max(...vals)
  }
}

/* ── WHERE evaluator ────────────────────────────────────────────────────── */
function evalWhere(expr: string, row: Row): boolean {
  /* handle OR (lowest precedence) */
  const orParts = splitBoolOp(expr, 'OR')
  if (orParts.length > 1) return orParts.some(p => evalWhere(p.trim(), row))

  /* handle AND */
  const andParts = splitBoolOp(expr, 'AND')
  if (andParts.length > 1) return andParts.every(p => evalWhere(p.trim(), row))

  /* handle NOT */
  const notMatch = expr.match(/^NOT\s+(.+)$/i)
  if (notMatch) return !evalWhere(notMatch[1].trim(), row)

  /* strip parens */
  if (expr.startsWith('(') && expr.endsWith(')')) return evalWhere(expr.slice(1, -1), row)

  return evalCondition(expr, row)
}

/** split on a boolean op word, but not if inside parens or quotes */
function splitBoolOp(expr: string, op: 'AND' | 'OR'): string[] {
  const parts: string[] = []
  let depth = 0, inStr = false, quoteChar = '', last = 0

  for (let i = 0; i < expr.length; i++) {
    const ch = expr[i]
    if (inStr)  { if (ch === quoteChar) inStr = false; continue }
    if (ch === '"' || ch === "'") { inStr = true; quoteChar = ch; continue }
    if (ch === '(') { depth++; continue }
    if (ch === ')') { depth--; continue }

    /* keyword must start exactly at i — check case-insensitive match + word boundaries */
    if (depth === 0 && i + op.length <= expr.length &&
        expr.slice(i, i + op.length).toUpperCase() === op) {
      const prevOk = i === 0 || !/\w/.test(expr[i - 1])
      const nextOk = i + op.length >= expr.length || !/\w/.test(expr[i + op.length])
      if (prevOk && nextOk) {
        parts.push(expr.slice(last, i).trim())
        i += op.length                               // skip past keyword
        while (i < expr.length && expr[i] === ' ') i++ // skip trailing spaces
        last = i
        i--                                          // compensate for loop i++
      }
    }
  }
  parts.push(expr.slice(last).trim())
  return parts.filter(Boolean)
}

function evalCondition(cond: string, row: Row): boolean {
  // col IS [NOT] NULL
  const nullM = cond.match(/^(\w+)\s+(IS\s+NOT\s+NULL|IS\s+NULL)\s*$/i)
  if (nullM) {
    const v = row[nullM[1]]
    return nullM[2].toUpperCase().includes('NOT')
      ? v !== null && v !== undefined
      : v === null || v === undefined
  }

  // col LIKE '%val%'
  const likeM = cond.match(/^(\w+)\s+LIKE\s+'([^']*)'\s*$/i)
  if (likeM) {
    const val = String(row[likeM[1]] ?? '')
    const pat = likeM[2].replace(/%/g, '.*').replace(/_/g, '.')
    return new RegExp(`^${pat}$`, 'i').test(val)
  }

  // col [NOT] IN ('a', 'b', ...)
  const inM = cond.match(/^(\w+)\s+(NOT\s+)?IN\s*\(([^)]+)\)\s*$/i)
  if (inM) {
    const rowVal = String(row[inM[1]] ?? '').toLowerCase()
    const items  = inM[3].split(',').map(s => s.trim().replace(/^['"]|['"]$/g, '').toLowerCase())
    const found  = items.includes(rowVal)
    return inM[2] ? !found : found   // NOT IN flips the result
  }

  // col op value   (=, !=, <>, <, >, <=, >=)
  const compM = cond.match(/^(\w+)\s*(=|!=|<>|<=|>=|<|>)\s*(.+)$/)
  if (!compM) return true  // unknown condition → pass-through

  const [, col, op, rawVal] = compM
  const rowVal = row[col]
  const isStr  = rawVal.startsWith("'") || rawVal.startsWith('"')
  const litVal: unknown = isStr
    ? rawVal.slice(1, -1)
    : isNaN(Number(rawVal)) ? rawVal : Number(rawVal)

  return compare(rowVal, op, litVal)
}

function compare(left: unknown, op: string, right: unknown): boolean {
  /* numeric comparison */
  const ln = Number(left), rn = Number(right)
  if (!isNaN(ln) && !isNaN(rn)) {
    switch (op) {
      case '=':  return ln === rn
      case '!=':
      case '<>': return ln !== rn
      case '<':  return ln < rn
      case '>':  return ln > rn
      case '<=': return ln <= rn
      case '>=': return ln >= rn
    }
  }
  /* string comparison */
  const ls = String(left ?? '').toLowerCase()
  const rs = String(right ?? '').toLowerCase()
  switch (op) {
    case '=':  return ls === rs
    case '!=':
    case '<>': return ls !== rs
    case '<':  return ls  < rs
    case '>':  return ls  > rs
    case '<=': return ls <= rs
    case '>=': return ls >= rs
  }
  return false
}
