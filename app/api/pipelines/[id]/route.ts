import { NextResponse } from 'next/server'
import { getPipeline, buildPipelineResponse } from '@/lib/pipelines'

export function GET(_req: Request, { params }: { params: { id: string } }) {
  const pipeline = getPipeline(params.id)
  if (!pipeline) return NextResponse.json({ error: 'Not found' }, { status: 404 })
  return NextResponse.json(buildPipelineResponse(pipeline))
}
