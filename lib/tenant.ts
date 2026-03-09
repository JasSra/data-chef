import 'server-only'

import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'fs'
import { join } from 'path'
import { headers } from 'next/headers'

export type TenantStatus = 'active' | 'disabled'

export interface TenantContext {
  tenantId: string
  slug: string
  hostnames: string[]
  status: TenantStatus
  region: string
  timezone: string
  dataResidencyLockedAt?: number | null
}

interface TenantRegistryFile {
  tenants: TenantContext[]
}

const STORE_ROOT = join(process.cwd(), '.datachef')
const PLATFORM_DIR = join(STORE_ROOT, 'platform')
const TENANT_REGISTRY_FILE = join(PLATFORM_DIR, 'tenants.json')
const DEFAULT_TENANT_ID = 'tenant_local'
const DEFAULT_HOSTS = ['localhost', '127.0.0.1']

function ensureDir(path: string) {
  if (!existsSync(path)) mkdirSync(path, { recursive: true })
}

function seedRegistry(): TenantRegistryFile {
  return {
    tenants: [
      {
        tenantId: DEFAULT_TENANT_ID,
        slug: 'local',
        hostnames: [...DEFAULT_HOSTS],
        status: 'active',
        region: 'ap-southeast-2',
        timezone: 'Australia/Brisbane',
        dataResidencyLockedAt: null,
      },
    ],
  }
}

function readRegistry(): TenantRegistryFile {
  ensureDir(PLATFORM_DIR)
  if (!existsSync(TENANT_REGISTRY_FILE)) {
    const seeded = seedRegistry()
    writeFileSync(TENANT_REGISTRY_FILE, JSON.stringify(seeded, null, 2), 'utf8')
    return seeded
  }

  try {
    const raw = readFileSync(TENANT_REGISTRY_FILE, 'utf8')
    const parsed = JSON.parse(raw) as Partial<TenantRegistryFile>
    const tenants = Array.isArray(parsed.tenants) && parsed.tenants.length > 0 ? parsed.tenants : seedRegistry().tenants
    return { tenants }
  } catch {
    const seeded = seedRegistry()
    writeFileSync(TENANT_REGISTRY_FILE, JSON.stringify(seeded, null, 2), 'utf8')
    return seeded
  }
}

function writeRegistry(registry: TenantRegistryFile): void {
  ensureDir(PLATFORM_DIR)
  const tmpPath = `${TENANT_REGISTRY_FILE}.${process.pid}.${Math.random().toString(36).slice(2, 8)}.tmp`
  writeFileSync(tmpPath, JSON.stringify(registry, null, 2), 'utf8')
  renameSync(tmpPath, TENANT_REGISTRY_FILE)
}

function normalizeHost(value: string): string {
  return value.trim().toLowerCase().replace(/:\d+$/, '')
}

function getRequestHost(): string | null {
  try {
    const store = headers()
    const forwardedHost = store.get('x-forwarded-host')
    const host = forwardedHost || store.get('host') || ''
    const normalized = normalizeHost(host)
    return normalized || null
  } catch {
    return null
  }
}

export function getDefaultTenantId(): string {
  return DEFAULT_TENANT_ID
}

export function getTenantStoreDir(tenantId?: string): string {
  return join(STORE_ROOT, 'tenants', tenantId || getCurrentTenantContext().tenantId)
}

export function listTenants(): TenantContext[] {
  return readRegistry().tenants.map(tenant => ({ ...tenant, hostnames: [...tenant.hostnames] }))
}

export function resolveTenantContext(host?: string | null): TenantContext {
  const registry = readRegistry()
  const normalizedHost = normalizeHost(host || getRequestHost() || '')
  const byHost = normalizedHost
    ? registry.tenants.find(tenant => tenant.hostnames.some(candidate => normalizeHost(candidate) === normalizedHost))
    : null
  const resolved = byHost ?? registry.tenants[0] ?? seedRegistry().tenants[0]
  return { ...resolved, hostnames: [...resolved.hostnames] }
}

export function getCurrentTenantContext(): TenantContext {
  return resolveTenantContext()
}

export function saveTenantContext(input: TenantContext): TenantContext {
  const registry = readRegistry()
  const index = registry.tenants.findIndex(tenant => tenant.tenantId === input.tenantId)
  const next = {
    ...input,
    hostnames: input.hostnames.map(normalizeHost).filter(Boolean),
  }
  if (index === -1) {
    registry.tenants.push(next)
  } else {
    registry.tenants[index] = next
  }
  writeRegistry(registry)
  return { ...next, hostnames: [...next.hostnames] }
}
