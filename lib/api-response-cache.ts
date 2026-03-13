/**
 * API Response Cache — in-memory LRU cache with TTL for proxied API responses.
 */

import 'server-only'

import { createHash } from 'node:crypto'

/* ── Types ─────────────────────────────────────────────────────────── */

interface CacheEntry {
  key: string
  data: unknown
  headers: Record<string, string>
  status: number
  storedAt: number
  ttlMs: number
  sizeBytes: number
}

/* ── LRU Cache ─────────────────────────────────────────────────────── */

const MAX_ENTRIES = 100
const MAX_SIZE_BYTES = 50 * 1024 * 1024 // 50MB
const DEFAULT_TTL_MS = 60_000 // 60s for GET
const MUTATION_TTL_MS = 0 // No caching for mutations

const cache = new Map<string, CacheEntry>()
let totalSize = 0

export function makeCacheKey(
  serviceId: string,
  method: string,
  url: string,
  params: Record<string, unknown>,
  body?: string,
): string {
  const sortedParams = JSON.stringify(params, Object.keys(params).sort())
  const payload = `${serviceId}:${method}:${url}:${sortedParams}:${body ?? ''}`
  return createHash('sha256').update(payload).digest('hex')
}

export function getCached(key: string): CacheEntry | null {
  const entry = cache.get(key)
  if (!entry) return null

  // Check TTL
  if (Date.now() - entry.storedAt > entry.ttlMs) {
    cache.delete(key)
    totalSize -= entry.sizeBytes
    return null
  }

  // Move to end (most recently accessed)
  cache.delete(key)
  cache.set(key, entry)
  return entry
}

export function setCached(
  key: string,
  data: unknown,
  headers: Record<string, string>,
  status: number,
  method: string,
  upstreamCacheControl?: string,
): void {
  // Don't cache mutations by default
  if (method !== 'GET' && !upstreamCacheControl) return

  const json = JSON.stringify(data)
  const sizeBytes = json.length * 2 // rough estimate

  // Determine TTL
  let ttlMs = method === 'GET' ? DEFAULT_TTL_MS : MUTATION_TTL_MS
  if (upstreamCacheControl) {
    const maxAgeMatch = upstreamCacheControl.match(/max-age=(\d+)/)
    if (maxAgeMatch) ttlMs = parseInt(maxAgeMatch[1], 10) * 1000
    if (upstreamCacheControl.includes('no-cache') || upstreamCacheControl.includes('no-store')) return
  }

  if (ttlMs <= 0) return

  // Evict if needed
  while (cache.size >= MAX_ENTRIES || totalSize + sizeBytes > MAX_SIZE_BYTES) {
    const oldest = cache.keys().next().value
    if (!oldest) break
    const oldEntry = cache.get(oldest)
    if (oldEntry) totalSize -= oldEntry.sizeBytes
    cache.delete(oldest)
  }

  const entry: CacheEntry = {
    key,
    data,
    headers,
    status,
    storedAt: Date.now(),
    ttlMs,
    sizeBytes,
  }

  cache.set(key, entry)
  totalSize += sizeBytes
}

export function clearServiceCache(serviceId: string): number {
  let cleared = 0
  for (const [key, entry] of cache.entries()) {
    if (key.startsWith(serviceId) || entry.key.includes(serviceId)) {
      totalSize -= entry.sizeBytes
      cache.delete(key)
      cleared++
    }
  }
  return cleared
}

export function clearAllCache(): void {
  cache.clear()
  totalSize = 0
}

export function getCacheStats(): { entries: number; sizeBytes: number; maxEntries: number; maxSizeBytes: number } {
  return { entries: cache.size, sizeBytes: totalSize, maxEntries: MAX_ENTRIES, maxSizeBytes: MAX_SIZE_BYTES }
}
