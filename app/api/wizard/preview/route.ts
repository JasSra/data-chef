/**
 * POST /api/wizard/preview
 *
 * Accepts source config from the New Dataset wizard.
 * For HTTP sources: fetches the URL server-side (no CORS), parses the response,
 * infers schema field types from actual data, and returns sample rows.
 * For non-HTTP sources: returns a cannotConnect flag (we can't reach those live).
 */

import { NextRequest, NextResponse } from 'next/server'
import {
  buildAuthHeaders,
  extractArray,
  inferSchema,
  loadRowsFromConnector,
} from '@/lib/runtime-data'

export const dynamic = 'force-dynamic'

/* ── Types ───────────────────────────────────────────────────────────────────── */
interface PreviewRequest {
  source:        string
  connectorId?:  string
  resource?:     string
  url?:          string
  auth?:         string
  apiKeyHeader?: string
  apiKeyValue?:  string
  bearerToken?:  string
  basicUser?:    string
  basicPass?:    string
  format?:       string
}

export interface SchemaField {
  field:    string
  type:     string
  nullable: boolean
  example:  string
}

export interface PreviewResponse {
  schema:       SchemaField[]
  sampleRows:   Record<string, unknown>[]
  totalRows:    number
  contentType?: string
  error?:       string
  cannotConnect?: boolean
  message?:     string
}

/* ── Route handler ───────────────────────────────────────────────────────────── */
export async function POST(req: NextRequest) {
  const body = await req.json() as PreviewRequest

  if (body.source === 'conn') {
    if (!body.connectorId) {
      return NextResponse.json<PreviewResponse>({
        schema: [], sampleRows: [], totalRows: 0, error: 'Please choose a saved connector.',
      })
    }
    try {
      const records = await loadRowsFromConnector(body.connectorId, {
        rowLimit: 500,
        resource: body.resource,
      })
      if (records.length === 0) {
        return NextResponse.json<PreviewResponse>({
          schema: [], sampleRows: [], totalRows: 0,
          error: 'Connector returned no records for this resource.',
        })
      }
      const schema = inferSchema(records)
      const sampleRows = records.slice(0, 5)
      return NextResponse.json<PreviewResponse>({
        schema, sampleRows, totalRows: records.length,
      })
    } catch (e: unknown) {
      return NextResponse.json<PreviewResponse>({
        schema: [], sampleRows: [], totalRows: 0,
        error: e instanceof Error ? e.message : String(e),
      })
    }
  }

  /* Non-HTTP sources: we can't reach them live in this demo */
  if (body.source !== 'http') {
    return NextResponse.json<PreviewResponse>({
      schema: [], sampleRows: [], totalRows: 0,
      cannotConnect: true,
      message: `Live preview isn't available for this source type. Credentials will be validated when the dataset is saved.`,
    })
  }

  if (!body.url?.startsWith('http')) {
    return NextResponse.json<PreviewResponse>({ schema: [], sampleRows: [], totalRows: 0, error: 'Please enter a valid URL (http:// or https://).' })
  }

  /* Build request headers from auth config */
  const headers = buildAuthHeaders(body, 'dataChef-preview/0.1')

  /* Fetch from server side (avoids browser CORS restrictions) */
  let res: Response
  try {
    res = await fetch(body.url, { headers, signal: AbortSignal.timeout(12_000) })
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e)
    return NextResponse.json<PreviewResponse>({ schema: [], sampleRows: [], totalRows: 0, error: `Could not reach URL: ${msg}` })
  }

  if (!res.ok) {
    return NextResponse.json<PreviewResponse>({
      schema: [], sampleRows: [], totalRows: 0,
      error: `Server returned ${res.status} ${res.statusText}`,
    })
  }

  const contentType = res.headers.get('content-type') ?? ''
  const text = await res.text()

  /* Parse JSON or JSONL */
  let data: unknown
  try {
    data = JSON.parse(text)
  } catch {
    // Try JSONL (newline-delimited JSON)
    const lines = text.trim().split('\n').filter(Boolean)
    try {
      data = lines.slice(0, 500).map(l => JSON.parse(l))
    } catch {
      return NextResponse.json<PreviewResponse>({
        schema: [], sampleRows: [], totalRows: 0,
        error: 'Response is not valid JSON or JSONL. Try CSV format.',
      })
    }
  }

  const records = extractArray(data) as Record<string, unknown>[]
  if (records.length === 0) {
    return NextResponse.json<PreviewResponse>({
      schema: [], sampleRows: [], totalRows: 0,
      error: 'No records found — the response did not contain a JSON array.',
    })
  }

  const schema     = inferSchema(records)
  const sampleRows = records.slice(0, 5)

  return NextResponse.json<PreviewResponse>({ schema, sampleRows, totalRows: records.length, contentType })
}
