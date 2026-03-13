/**
 * POST /api/api-proxy/fetch-spec — server-side fetch of a Swagger/OpenAPI spec URL.
 * Bypasses CORS by fetching on the server. Validates and normalizes the spec.
 */

import { NextRequest, NextResponse } from 'next/server'
import { validateProxyUrl } from '@/lib/ssrf-guard'
import { normalizeToOpenApiV3, detectSpecVersion } from '@/lib/openapi-normalizer'
import { countEndpoints } from '@/lib/api-services'

export const dynamic = 'force-dynamic'

export async function POST(req: NextRequest) {
  let body: Record<string, unknown>
  try { body = await req.json() } catch {
    return NextResponse.json({ error: 'Bad Request' }, { status: 400 })
  }

  const url = String(body.url ?? '').trim()
  if (!url) return NextResponse.json({ error: 'url is required' }, { status: 400 })

  const allowPrivate = body.allowPrivate === true

  // SSRF protection
  const guard = validateProxyUrl(url, { allowPrivate })
  if (!guard.valid) {
    return NextResponse.json({ error: guard.error }, { status: 400 })
  }

  // Fetch the spec
  let rawText: string
  try {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 30_000)
    const res = await fetch(url, {
      signal: controller.signal,
      headers: { Accept: 'application/json, */*' },
    })
    clearTimeout(timeout)

    if (!res.ok) {
      return NextResponse.json(
        { error: `Failed to fetch spec: ${res.status} ${res.statusText}` },
        { status: 502 },
      )
    }
    rawText = await res.text()
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e)
    return NextResponse.json({ error: `Fetch failed: ${msg}` }, { status: 502 })
  }

  // Parse JSON
  let raw: Record<string, unknown>
  try {
    raw = JSON.parse(rawText)
  } catch {
    return NextResponse.json({ error: 'Response is not valid JSON' }, { status: 422 })
  }

  // Detect version
  const specVersion = detectSpecVersion(raw)
  if (specVersion === 'unknown') {
    return NextResponse.json({ error: 'Not a valid Swagger 2.0 or OpenAPI 3.x spec' }, { status: 422 })
  }

  // Normalize to v3
  let normalized
  try {
    normalized = normalizeToOpenApiV3(raw)
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e)
    return NextResponse.json({ error: `Normalization failed: ${msg}` }, { status: 422 })
  }

  const endpointCount = countEndpoints(normalized.paths as Record<string, Record<string, unknown>>)

  // Derive base URL: prefer spec's own servers[0].url, then fall back to the
  // *origin* of the swagger URL so /swagger/v1/swagger.json → https://host
  let baseUrl = normalized.servers?.[0]?.url ?? ''
  if (!baseUrl) {
    try { baseUrl = new URL(url).origin } catch { baseUrl = '' }
  }
  // Strip any swagger-specific path segments that may have leaked in via basePath
  baseUrl = baseUrl.replace(/\/swagger(\/[^/]+)*\/?$/, '').replace(/\/+$/, '')

  // Determine auth schemes from spec
  const securitySchemes = normalized.components?.securitySchemes ?? {}
  const detectedAuth = Object.values(securitySchemes).map(scheme => ({
    type: scheme.type,
    name: scheme.name,
    in: scheme.in,
    scheme: scheme.scheme,
  }))

  return NextResponse.json({
    valid: true,
    specVersion: specVersion === 'swagger-2.0' ? '2.0' : normalized.openapi,
    title: normalized.info.title,
    description: normalized.info.description ?? '',
    apiVersion: normalized.info.version,
    baseUrl,
    endpointCount,
    tags: (normalized.tags ?? []).map(t => t.name),
    detectedAuth,
    // Return the normalized spec for saving
    spec: normalized,
  })
}
