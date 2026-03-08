import { NextResponse } from 'next/server'
import { getWorkerState } from '@/lib/pipelines'

export const dynamic = 'force-dynamic'

export function GET() {
  return NextResponse.json(getWorkerState())
}
