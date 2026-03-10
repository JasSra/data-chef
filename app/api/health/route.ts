import { NextResponse } from 'next/server'
import { getWorkerState } from '@/lib/pipelines'
import { bootstrapWorkers } from '@/lib/bootstrap'

export const dynamic = 'force-dynamic'

/**
 * Health check endpoint for Docker and load balancers.
 * Ensures workers are running and returns system status.
 */
export function GET() {
  // Ensure workers are started (idempotent)
  bootstrapWorkers()
  
  const workerState = getWorkerState()
  
  return NextResponse.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    workers: workerState,
    uptime: process.uptime(),
  }, {
    status: 200,
    headers: {
      'Cache-Control': 'no-store, no-cache, must-revalidate',
    },
  })
}
