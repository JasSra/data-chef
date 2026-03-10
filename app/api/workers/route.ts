import { NextResponse } from 'next/server'
import { getWorkerState } from '@/lib/pipelines'
import { bootstrapWorkers } from '@/lib/bootstrap'

export const dynamic = 'force-dynamic'

export function GET() {
  bootstrapWorkers()
  return NextResponse.json(getWorkerState())
}
