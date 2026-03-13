/**
 * API Performance Tracker — per-request and aggregated throughput metrics.
 */

import 'server-only'

/* ── Types ─────────────────────────────────────────────────────────── */

export interface RequestMetrics {
  url: string
  method: string
  statusCode: number
  latencyMs: number
  ttfbMs: number
  responseSizeBytes: number
  cached: boolean
  timestamp: number
}

export interface AggregatedMetrics {
  serviceId: string
  totalRequests: number
  avgLatencyMs: number
  p50LatencyMs: number
  p95LatencyMs: number
  p99LatencyMs: number
  errorRate: number
  totalBytesTransferred: number
  requestsPerMinute: number
  cacheHitRate: number
}

export interface BenchmarkResult {
  runs: number
  concurrency: number
  totalDurationMs: number
  avgLatencyMs: number
  p50LatencyMs: number
  p95LatencyMs: number
  p99LatencyMs: number
  minLatencyMs: number
  maxLatencyMs: number
  requestsPerSecond: number
  errorCount: number
  successCount: number
  totalBytesTransferred: number
}

/* ── In-memory metrics store ───────────────────────────────────────── */

const metricsStore = new Map<string, RequestMetrics[]>()
const MAX_METRICS_PER_SERVICE = 500

export function recordMetrics(serviceId: string, metrics: RequestMetrics): void {
  const existing = metricsStore.get(serviceId) ?? []
  existing.push(metrics)

  // Keep only the latest N
  if (existing.length > MAX_METRICS_PER_SERVICE) {
    existing.splice(0, existing.length - MAX_METRICS_PER_SERVICE)
  }

  metricsStore.set(serviceId, existing)
}

export function getAggregatedMetrics(serviceId: string): AggregatedMetrics {
  const metrics = metricsStore.get(serviceId) ?? []

  if (metrics.length === 0) {
    return {
      serviceId,
      totalRequests: 0,
      avgLatencyMs: 0,
      p50LatencyMs: 0,
      p95LatencyMs: 0,
      p99LatencyMs: 0,
      errorRate: 0,
      totalBytesTransferred: 0,
      requestsPerMinute: 0,
      cacheHitRate: 0,
    }
  }

  const latencies = metrics.map(m => m.latencyMs).sort((a, b) => a - b)
  const errors = metrics.filter(m => m.statusCode >= 400).length
  const cached = metrics.filter(m => m.cached).length
  const totalBytes = metrics.reduce((sum, m) => sum + m.responseSizeBytes, 0)

  // Calculate time window for req/min
  const oldest = metrics[0].timestamp
  const newest = metrics[metrics.length - 1].timestamp
  const windowMs = Math.max(newest - oldest, 60_000)

  return {
    serviceId,
    totalRequests: metrics.length,
    avgLatencyMs: Math.round(latencies.reduce((a, b) => a + b, 0) / latencies.length),
    p50LatencyMs: percentile(latencies, 50),
    p95LatencyMs: percentile(latencies, 95),
    p99LatencyMs: percentile(latencies, 99),
    errorRate: errors / metrics.length,
    totalBytesTransferred: totalBytes,
    requestsPerMinute: Math.round((metrics.length / windowMs) * 60_000),
    cacheHitRate: cached / metrics.length,
  }
}

export function computeBenchmarkResult(
  latencies: number[],
  errors: number,
  totalBytes: number,
  totalDurationMs: number,
  runs: number,
  concurrency: number,
): BenchmarkResult {
  const sorted = [...latencies].sort((a, b) => a - b)

  return {
    runs,
    concurrency,
    totalDurationMs: Math.round(totalDurationMs),
    avgLatencyMs: Math.round(sorted.reduce((a, b) => a + b, 0) / sorted.length),
    p50LatencyMs: percentile(sorted, 50),
    p95LatencyMs: percentile(sorted, 95),
    p99LatencyMs: percentile(sorted, 99),
    minLatencyMs: sorted[0] ?? 0,
    maxLatencyMs: sorted[sorted.length - 1] ?? 0,
    requestsPerSecond: Math.round((runs / totalDurationMs) * 1000 * 10) / 10,
    errorCount: errors,
    successCount: runs - errors,
    totalBytesTransferred: totalBytes,
  }
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0
  const idx = Math.ceil((p / 100) * sorted.length) - 1
  return Math.round(sorted[Math.max(0, idx)])
}

export function clearServiceMetrics(serviceId: string): void {
  metricsStore.delete(serviceId)
}
