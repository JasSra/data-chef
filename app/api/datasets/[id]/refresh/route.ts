/**
 * POST /api/datasets/[id]/refresh
 *
 * Re-fetches schema + sample rows for HTTP datasets server-side (no CORS).
 * Uses the same inference logic as /api/wizard/preview.
 * Updates the server registry and returns the updated dataset.
 */

import { NextRequest, NextResponse } from 'next/server'
import { getDataset, updateDatasetSchema } from '@/lib/datasets'

export const dynamic = 'force-dynamic'

/* ── Type helpers (shared with wizard/preview) ───────────────────────────── */
function inferType(value: unknown): string {
  if (value === null || value === undefined) return 'null'
  if (typeof value === 'boolean') return 'boolean'
  if (typeof value === 'number')  return Number.isInteger(value) ? 'integer' : 'float'
  if (typeof value === 'string') {
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(value)) return 'timestamp'
    if (/^\d{4}-\d{2}-\d{2}$/.test(value))              return 'date'
    return 'string'
  }
  if (Array.isArray(value))      return 'array'
  if (typeof value === 'object') return 'object'
  return 'string'
}

function formatExample(value: unknown): string {
  if (value === null || value === undefined) return 'null'
  if (Array.isArray(value))  return `[${value.length} items]`
  if (typeof value === 'object') {
    const keys = Object.keys(value as object)
    return `{ ${keys.slice(0, 4).join(', ')}${keys.length > 4 ? ', …' : ''} }`
  }
  const s = String(value)
  return s.length > 50 ? s.slice(0, 47) + '…' : s
}

function extractArray(data: unknown): unknown[] {
  if (Array.isArray(data)) return data
  if (data && typeof data === 'object') {
    const preferred = ['data', 'items', 'results', 'records', 'rows', 'events', 'list']
    const obj = data as Record<string, unknown>
    for (const key of preferred) {
      if (Array.isArray(obj[key]) && (obj[key] as unknown[]).length > 0) return obj[key] as unknown[]
    }
    for (const val of Object.values(obj)) {
      if (Array.isArray(val) && val.length > 0) return val
    }
  }
  return []
}

function inferSchema(records: Record<string, unknown>[], sample = 200) {
  const recs  = records.slice(0, sample)
  const count = recs.length
  if (count === 0) return []

  const typeFreq = new Map<string, Map<string, number>>()
  const nullFreq = new Map<string, number>()
  const examples = new Map<string, unknown>()

  for (const rec of recs) {
    for (const [key, val] of Object.entries(rec)) {
      if (!typeFreq.has(key)) { typeFreq.set(key, new Map()); nullFreq.set(key, 0) }
      const t = inferType(val)
      if (t === 'null') {
        nullFreq.set(key, (nullFreq.get(key) ?? 0) + 1)
      } else {
        const m = typeFreq.get(key)!
        m.set(t, (m.get(t) ?? 0) + 1)
        if (!examples.has(key)) examples.set(key, val)
      }
    }
  }

  return Array.from(typeFreq.entries()).map(([field, types]) => {
    let bestType = 'string', bestCount = 0
    for (const [t, c] of types) if (c > bestCount) { bestType = t; bestCount = c }
    const nulls    = nullFreq.get(field) ?? 0
    const nullable = count > 0 && nulls / count > 0.05
    return { field, type: bestType, nullable, example: formatExample(examples.get(field)) }
  })
}

/* ── Route handler ───────────────────────────────────────────────────────── */
export async function POST(
  _req: NextRequest,
  { params }: { params: { id: string } },
) {
  const ds = getDataset(params.id)
  if (!ds) {
    return NextResponse.json({ error: 'Dataset not found' }, { status: 404 })
  }

  if (!ds.url?.startsWith('http')) {
    return NextResponse.json({
      error: 'Live schema refresh is only supported for HTTP sources.',
      cannotRefresh: true,
    }, { status: 422 })
  }

  const headers: Record<string, string> = {
    'Accept':     'application/json, text/plain, */*',
    'User-Agent': 'dataChef-refresh/0.1',
  }

  let res: Response
  try {
    res = await fetch(ds.url, { headers, signal: AbortSignal.timeout(12_000) })
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e)
    return NextResponse.json({ error: `Could not reach URL: ${msg}` }, { status: 502 })
  }

  if (!res.ok) {
    return NextResponse.json({
      error: `Source returned ${res.status} ${res.statusText}`,
    }, { status: 502 })
  }

  const text = await res.text()
  let data: unknown
  try {
    data = JSON.parse(text)
  } catch {
    const lines = text.trim().split('\n').filter(Boolean)
    try {
      data = lines.slice(0, 500).map(l => JSON.parse(l))
    } catch {
      return NextResponse.json({ error: 'Response is not valid JSON or JSONL.' }, { status: 422 })
    }
  }

  const records    = extractArray(data) as Record<string, unknown>[]
  const schema     = inferSchema(records)
  const sampleRows = records.slice(0, 5)
  const totalRows  = records.length

  updateDatasetSchema(params.id, schema, sampleRows, totalRows)

  return NextResponse.json({ schema, sampleRows, totalRows })
}
