/**
 * GET  /api/api-services/[id]/spec  — get spec metadata for a service
 * POST /api/api-services/[id]/spec  — re-fetch spec or add a new version
 */

import { NextRequest, NextResponse } from 'next/server'
import {
  getApiService, addApiServiceVersion, updateApiService, saveSpec, loadSpec, countEndpoints,
} from '@/lib/api-services'
import { validateProxyUrl } from '@/lib/ssrf-guard'
import { normalizeToOpenApiV3, detectSpecVersion } from '@/lib/openapi-normalizer'

export const dynamic = 'force-dynamic'

export async function GET(_req: NextRequest, { params }: { params: { id: string } }) {
  const service = getApiService(params.id)
  if (!service) return NextResponse.json({ error: 'Not found' }, { status: 404 })

  // Return the active version spec
  const active = service.versions.find(v => v.version === service.activeVersion)
  if (!active) {
    return NextResponse.json({ versions: service.versions, active: null })
  }

  const spec = loadSpec(params.id, active.specFileName)
  return NextResponse.json({
    versions: service.versions,
    activeVersion: service.activeVersion,
    spec,
  })
}

export async function POST(req: NextRequest, { params }: { params: { id: string } }) {
  const service = getApiService(params.id)
  if (!service) return NextResponse.json({ error: 'Not found' }, { status: 404 })

  let body: Record<string, unknown>
  try { body = await req.json() } catch { body = {} }

  const url = String(body.swaggerUrl ?? service.swaggerUrl).trim()
  const versionLabel = String(body.version ?? '').trim()

  // SSRF check
  const guard = validateProxyUrl(url)
  if (!guard.valid) return NextResponse.json({ error: guard.error }, { status: 400 })

  // Fetch + normalize
  let normalized
  let rawSpecVersion: string
  try {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 30_000)
    const res = await fetch(url, {
      signal: controller.signal,
      headers: { Accept: 'application/json, */*' },
    })
    clearTimeout(timeout)

    if (!res.ok) {
      updateApiService(params.id, { status: 'error', lastError: `Fetch failed: ${res.status}` })
      return NextResponse.json({ error: `Fetch failed: ${res.status}` }, { status: 502 })
    }

    const raw = await res.json()
    const sv = detectSpecVersion(raw)
    if (sv === 'unknown') {
      return NextResponse.json({ error: 'Not a valid Swagger/OpenAPI spec' }, { status: 422 })
    }
    rawSpecVersion = sv === 'swagger-2.0' ? '2.0' : raw.openapi
    normalized = normalizeToOpenApiV3(raw)
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e)
    updateApiService(params.id, { status: 'error', lastError: msg })
    return NextResponse.json({ error: msg }, { status: 502 })
  }

  const version = versionLabel || normalized.info.version || 'latest'
  const endpointCount = countEndpoints(normalized.paths as Record<string, Record<string, unknown>>)

  // Save spec file and register version
  const specFileName = saveSpec(params.id, version, normalized)
  addApiServiceVersion(params.id, {
    version,
    specFileName,
    fetchedAt: Date.now(),
    endpointCount,
    openApiVersion: rawSpecVersion,
  })

  updateApiService(params.id, { status: 'active', lastError: undefined })

  const updated = getApiService(params.id)
  return NextResponse.json(updated)
}
