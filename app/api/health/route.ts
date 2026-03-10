import { NextResponse } from 'next/server'
import { getWorkerState } from '@/lib/pipelines'
import { bootstrapWorkers } from '@/lib/bootstrap'
import { existsSync, statSync, readdirSync } from 'fs'
import { join } from 'path'

export const dynamic = 'force-dynamic'

/**
 * Comprehensive health check endpoint for Docker and load balancers.
 * Returns server status, worker state, memory usage, data storage status.
 */
export function GET() {
  // Ensure workers are started (idempotent)
  bootstrapWorkers()
  
  const workerState = getWorkerState()
  const startTime = Date.now()
  
  // Gather system metrics
  const memoryUsage = process.memoryUsage()
  const uptime = process.uptime()
  
  // Check data directory status
  const dataDir = join(process.cwd(), '.datachef')
  let dataStatus = {
    exists: false,
    writable: false,
    size: 0,
    files: 0,
    tenants: 0,
  }
  
  try {
    if (existsSync(dataDir)) {
      dataStatus.exists = true
      
      // Check if writable by attempting to stat
      try {
        const stats = statSync(dataDir)
        dataStatus.writable = stats.isDirectory()
        
        // Count files recursively
        const countFiles = (dir: string): number => {
          let count = 0
          try {
            const items = readdirSync(dir, { withFileTypes: true })
            for (const item of items) {
              if (item.isDirectory()) {
                if (item.name.startsWith('tenant_')) {
                  dataStatus.tenants++
                }
                count += countFiles(join(dir, item.name))
              } else {
                count++
              }
            }
          } catch {
            // Skip inaccessible directories
          }
          return count
        }
        
        dataStatus.files = countFiles(dataDir)
      } catch {
        dataStatus.writable = false
      }
    }
  } catch (err) {
    // Data directory check failed - continue with defaults
  }
  
  // Check required environment
  const hasSecretKey = !!process.env.CONNECTOR_SECRET_KEY
  const otelEnabled = process.env.OTEL_ENABLED === 'true'
  
  // Determine overall health status
  const isHealthy = dataStatus.exists && dataStatus.writable && hasSecretKey
  const status = isHealthy ? 'healthy' : 'degraded'
  const httpStatus = isHealthy ? 200 : 503
  
  const warnings: string[] = []
  if (!hasSecretKey) {
    warnings.push('CONNECTOR_SECRET_KEY not set - encrypted credentials will fail')
  }
  if (!dataStatus.exists) {
    warnings.push('Data directory .datachef does not exist')
  }
  if (!dataStatus.writable) {
    warnings.push('Data directory is not writable')
  }
  
  const responseTime = Date.now() - startTime
  
  return NextResponse.json({
    status,
    timestamp: new Date().toISOString(),
    version: process.env.npm_package_version || '0.1.0',
    
    system: {
      uptime: Math.floor(uptime),
      uptimeFormatted: formatUptime(uptime),
      nodeVersion: process.version,
      platform: process.platform,
      arch: process.arch,
      pid: process.pid,
    },
    
    memory: {
      heapUsed: Math.round(memoryUsage.heapUsed / 1024 / 1024),
      heapTotal: Math.round(memoryUsage.heapTotal / 1024 / 1024),
      rss: Math.round(memoryUsage.rss / 1024 / 1024),
      external: Math.round(memoryUsage.external / 1024 / 1024),
      unit: 'MB',
    },
    
    data: dataStatus,
    
    workers: workerState,
    
    environment: {
      nodeEnv: process.env.NODE_ENV || 'development',
      hasSecretKey,
      otelEnabled,
    },
    
    warnings: warnings.length > 0 ? warnings : undefined,
    
    responseTime: `${responseTime}ms`,
  }, {
    status: httpStatus,
    headers: {
      'Cache-Control': 'no-store, no-cache, must-revalidate',
      'X-Health-Status': status,
    },
  })
}

function formatUptime(seconds: number): string {
  const days = Math.floor(seconds / 86400)
  const hours = Math.floor((seconds % 86400) / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  const secs = Math.floor(seconds % 60)
  
  const parts = []
  if (days > 0) parts.push(`${days}d`)
  if (hours > 0) parts.push(`${hours}h`)
  if (minutes > 0) parts.push(`${minutes}m`)
  if (secs > 0 || parts.length === 0) parts.push(`${secs}s`)
  
  return parts.join(' ')
}
