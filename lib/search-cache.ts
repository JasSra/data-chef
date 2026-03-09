import { getCurrentTenantContext } from '@/lib/tenant'

const versions = new Map<string, number>()

export function getSearchVersion(tenantId?: string): number {
  const resolvedTenantId = tenantId ?? getCurrentTenantContext().tenantId
  return versions.get(resolvedTenantId) ?? 0
}

export function invalidateSearchIndex(tenantId?: string): void {
  const resolvedTenantId = tenantId ?? getCurrentTenantContext().tenantId
  versions.set(resolvedTenantId, getSearchVersion(resolvedTenantId) + 1)
}
