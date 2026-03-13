/**
 * GET  /api/api-services  — list all API services
 * POST /api/api-services  — create a new API service (fetches + stores spec)
 */

import { NextRequest, NextResponse } from 'next/server'
import {
  getApiServices, addApiService, addApiServiceVersion, updateApiService, saveSpec, countEndpoints,
} from '@/lib/api-services'
import type { ApiServiceAuth } from '@/lib/api-services'
import { validateProxyUrl } from '@/lib/ssrf-guard'
import { normalizeToOpenApiV3, detectSpecVersion } from '@/lib/openapi-normalizer'

export const dynamic = 'force-dynamic'

export async function GET() {
  const services = getApiServices()
  return NextResponse.json(services)
}

export async function POST(req: NextRequest) {
  let body: Record<string, unknown>
  try { body = await req.json() } catch {
    return NextResponse.json({ error: 'Bad Request' }, { status: 400 })
  }

  const name = String(body.name ?? '').trim()
  const swaggerUrl = String(body.swaggerUrl ?? '').trim()
  if (!name) return NextResponse.json({ error: 'name is required' }, { status: 400 })
  if (!swaggerUrl) return NextResponse.json({ error: 'swaggerUrl is required' }, { status: 400 })

  const allowPrivate = body.allowPrivate === true

  // SSRF check
  const guard = validateProxyUrl(swaggerUrl, { allowPrivate })
  if (!guard.valid) return NextResponse.json({ error: guard.error }, { status: 400 })

  // Fetch + normalize spec
  let normalized
  let rawSpecVersion: string
  try {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 30_000)
    const res = await fetch(swaggerUrl, {
      signal: controller.signal,
      headers: { Accept: 'application/json, */*' },
    })
    clearTimeout(timeout)

    if (!res.ok) {
      return NextResponse.json({ error: `Failed to fetch spec: ${res.status}` }, { status: 502 })
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
    return NextResponse.json({ error: `Spec fetch/parse failed: ${msg}` }, { status: 502 })
  }

  const baseUrl = String(body.baseUrl ?? normalized.servers?.[0]?.url ?? '').replace(/\/+$/, '')
  const endpointCount = countEndpoints(normalized.paths as Record<string, Record<string, unknown>>)
  const apiVersion = normalized.info.version ?? '1.0'

  // Parse auth
  const auth: ApiServiceAuth = { scheme: 'none' }
  if (body.auth && typeof body.auth === 'object') {
    const a = body.auth as Record<string, unknown>
    auth.scheme = (a.scheme as ApiServiceAuth['scheme']) ?? 'none'
    if (auth.scheme === 'api_key') {
      auth.apiKeyName = String(a.apiKeyName ?? '')
      auth.apiKeyLocation = (a.apiKeyLocation as ApiServiceAuth['apiKeyLocation']) ?? 'query'
      auth.apiKeyValue = String(a.apiKeyValue ?? '')
    } else if (auth.scheme === 'bearer') {
      auth.bearerToken = String(a.bearerToken ?? '')
    } else if (auth.scheme === 'basic') {
      auth.basicUsername = String(a.basicUsername ?? '')
      auth.basicPassword = String(a.basicPassword ?? '')
    } else if (auth.scheme === 'oauth2') {
      auth.oauth2TokenUrl = String(a.oauth2TokenUrl ?? '')
      auth.oauth2ClientId = String(a.oauth2ClientId ?? '')
      auth.oauth2ClientSecret = String(a.oauth2ClientSecret ?? '')
      auth.oauth2Scopes = Array.isArray(a.oauth2Scopes) ? a.oauth2Scopes.map(String) : []
    }
  }

  const tags = (normalized.tags ?? []).map(t => t.name)

  // Create the service record
  const rec = addApiService({
    name,
    description: String(body.description ?? normalized.info.description ?? ''),
    baseUrl,
    swaggerUrl,
    auth,
    tags,
    allowPrivate,
  })

  // Save the spec file and register the version
  const specFileName = saveSpec(rec.id, apiVersion, normalized)
  addApiServiceVersion(rec.id, {
    version: apiVersion,
    specFileName,
    fetchedAt: Date.now(),
    endpointCount,
    openApiVersion: rawSpecVersion,
  })

  // Update status
  updateApiService(rec.id, { status: 'active', activeVersion: apiVersion })

  // Return fresh record
  const updated = getApiServices().find(s => s.id === rec.id)
  return NextResponse.json(updated, { status: 201 })
}
