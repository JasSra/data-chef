import { NextResponse } from 'next/server'
import { PIPELINE_MAP, buildPipelineResponse } from '@/lib/pipelines'

export function GET(_req: Request, { params }: { params: { id: string } }) {
  const pipeline = PIPELINE_MAP.get(params.id)
  if (!pipeline) return NextResponse.json({ error: 'Not found' }, { status: 404 })
  return NextResponse.json(buildPipelineResponse(pipeline))
}
