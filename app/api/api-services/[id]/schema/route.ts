/**
 * GET /api/api-services/[id]/schema — schema registry for autocomplete
 */

import { NextRequest, NextResponse } from 'next/server'
import { getApiService, loadSpec } from '@/lib/api-services'
import { extractSchemaRegistry } from '@/lib/api-schema-registry'
import type { OpenApiSpec } from '@/lib/openapi-normalizer'

export const dynamic = 'force-dynamic'

export async function GET(_req: NextRequest, { params }: { params: { id: string } }) {
  const service = getApiService(params.id)
  if (!service) return NextResponse.json({ error: 'Not found' }, { status: 404 })

  const active = service.versions.find(v => v.version === service.activeVersion)
  if (!active) return NextResponse.json({ error: 'No active spec version' }, { status: 404 })

  const spec = loadSpec(params.id, active.specFileName)
  if (!spec) return NextResponse.json({ error: 'Spec file not found' }, { status: 404 })

  // ?full=1 bypasses exclusions (used by endpoint manager UI)
  const full = _req.nextUrl.searchParams.get('full') === '1'
  const exclusions = full ? undefined : service.excludedEndpoints
  const registry = extractSchemaRegistry(spec as unknown as OpenApiSpec, params.id, active.version, exclusions)
  return NextResponse.json(registry)
}
