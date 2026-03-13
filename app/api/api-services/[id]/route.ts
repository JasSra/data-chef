/**
 * GET    /api/api-services/[id] — get single service
 * PATCH  /api/api-services/[id] — update service metadata
 * DELETE /api/api-services/[id] — remove service
 */

import { NextRequest, NextResponse } from 'next/server'
import { getApiService, updateApiServiceFull, deleteApiService } from '@/lib/api-services'
import type { ApiServiceAuth } from '@/lib/api-services'

export const dynamic = 'force-dynamic'


export async function GET(_req: NextRequest, { params }: { params: { id: string } }) {
  const service = getApiService(params.id)
  if (!service) return NextResponse.json({ error: 'Not found' }, { status: 404 })
  return NextResponse.json(service)
}

export async function PATCH(req: NextRequest, { params }: { params: { id: string } }) {
  let body: Record<string, unknown>
  try { body = await req.json() } catch {
    return NextResponse.json({ error: 'Bad Request' }, { status: 400 })
  }

  const changes: Parameters<typeof updateApiServiceFull>[1] = {}
  if (typeof body.name === 'string') changes.name = body.name
  if (typeof body.description === 'string') changes.description = body.description
  if (typeof body.baseUrl === 'string') changes.baseUrl = body.baseUrl
  if (typeof body.swaggerUrl === 'string') changes.swaggerUrl = body.swaggerUrl
  if (typeof body.status === 'string') changes.status = body.status as 'active' | 'error' | 'pending'
  if (typeof body.activeVersion === 'string') changes.activeVersion = body.activeVersion
  if (Array.isArray(body.tags)) changes.tags = body.tags.map(String)
  if (Array.isArray(body.excludedEndpoints)) changes.excludedEndpoints = body.excludedEndpoints.map(String)
  if (body.customHeaders && typeof body.customHeaders === 'object' && !Array.isArray(body.customHeaders)) {
    changes.customHeaders = Object.fromEntries(
      Object.entries(body.customHeaders as Record<string, unknown>).map(([k, v]) => [k, String(v)])
    )
  }

  // Parse auth if provided
  let auth: ApiServiceAuth | undefined
  if (body.auth && typeof body.auth === 'object') {
    const a = body.auth as Record<string, unknown>
    auth = { scheme: (a.scheme as ApiServiceAuth['scheme']) ?? 'none' }
    if (auth.scheme === 'api_key') {
      auth.apiKeyName = String(a.apiKeyName ?? '')
      auth.apiKeyLocation = (a.apiKeyLocation as ApiServiceAuth['apiKeyLocation']) ?? 'query'
      auth.apiKeyValue = String(a.apiKeyValue ?? '')
    } else if (auth.scheme === 'bearer') {
      auth.bearerToken = String(a.bearerToken ?? '')
    } else if (auth.scheme === 'basic') {
      auth.basicUsername = String(a.basicUsername ?? '')
      auth.basicPassword = String(a.basicPassword ?? '')
    }
  }

  // Single atomic write — avoids two-write race condition on Windows
  const updated = updateApiServiceFull(params.id, changes, auth)
  if (!updated) return NextResponse.json({ error: 'Not found' }, { status: 404 })

  return NextResponse.json(updated)
}

export async function DELETE(_req: NextRequest, { params }: { params: { id: string } }) {
  const deleted = deleteApiService(params.id)
  if (!deleted) return NextResponse.json({ error: 'Not found' }, { status: 404 })
  return NextResponse.json({ ok: true })
}
