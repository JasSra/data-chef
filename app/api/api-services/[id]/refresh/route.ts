/**
 * POST /api/api-services/[id]/refresh — re-fetches the Swagger spec and adds a new version.
 */

import { NextRequest, NextResponse } from 'next/server'
import {
  getApiService, addApiServiceVersion, updateApiService, saveSpec, countEndpoints,
} from '@/lib/api-services'
import { validateProxyUrl } from '@/lib/ssrf-guard'
import { normalizeToOpenApiV3, detectSpecVersion } from '@/lib/openapi-normalizer'

export const dynamic = 'force-dynamic'

export async function POST(_req: NextRequest, { params }: { params: { id: string } }) {
  const service = getApiService(params.id)
  if (!service) return NextResponse.json({ error: 'Not found' }, { status: 404 })
  if (!service.swaggerUrl) return NextResponse.json({ error: 'No spec URL stored for this service' }, { status: 400 })

  // SSRF check (skip for services with allowPrivate)
  const guard = validateProxyUrl(service.swaggerUrl, { allowPrivate: service.allowPrivate })
  if (!guard.valid) return NextResponse.json({ error: guard.error }, { status: 400 })

  // Re-fetch spec
  let normalized
  let rawSpecVersion: string
  try {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 30_000)
    const res = await fetch(service.swaggerUrl, {
      signal: controller.signal,
      headers: { Accept: 'application/json, */*' },
    })
    clearTimeout(timeout)

    if (!res.ok) {
      updateApiService(params.id, { status: 'error', lastError: `Fetch failed: ${res.status}` })
      return NextResponse.json({ error: `Failed to fetch spec: ${res.status}` }, { status: 502 })
    }

    const raw = await res.json()
    const sv = detectSpecVersion(raw)
    if (sv === 'unknown') {
      updateApiService(params.id, { status: 'error', lastError: 'Not a valid Swagger/OpenAPI spec' })
      return NextResponse.json({ error: 'Not a valid Swagger/OpenAPI spec' }, { status: 422 })
    }
    rawSpecVersion = sv === 'swagger-2.0' ? '2.0' : raw.openapi
    normalized = normalizeToOpenApiV3(raw)
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e)
    updateApiService(params.id, { status: 'error', lastError: msg })
    return NextResponse.json({ error: `Spec fetch/parse failed: ${msg}` }, { status: 502 })
  }

  const endpointCount = countEndpoints(normalized.paths as Record<string, Record<string, unknown>>)
  const apiVersion = normalized.info.version ?? service.activeVersion

  const specFileName = saveSpec(params.id, apiVersion, normalized)
  addApiServiceVersion(params.id, {
    version: apiVersion,
    specFileName,
    fetchedAt: Date.now(),
    endpointCount,
    openApiVersion: rawSpecVersion,
  })

  updateApiService(params.id, { status: 'active', activeVersion: apiVersion, lastError: undefined })

  const updated = getApiService(params.id)
  return NextResponse.json(updated)
}
