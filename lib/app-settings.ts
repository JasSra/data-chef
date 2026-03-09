import 'server-only'

import { randomBytes } from 'crypto'
import { existsSync, readFileSync } from 'fs'
import { join } from 'path'
import type { AppSettings, BrandingSettings } from '@/lib/app-settings-schema'
import { readJsonFile, writeJsonFile } from '@/lib/json-store'
import { invalidateSearchIndex } from '@/lib/search-cache'
import { getCurrentTenantContext, saveTenantContext } from '@/lib/tenant'

const SETTINGS_FILE = 'settings.json'
const LEGACY_SETTINGS_PATH = join(process.cwd(), '.data', 'app-settings.json')

function generateSecret(prefix: string, size = 24): string {
  return `${prefix}_${randomBytes(size).toString('base64url')}`
}

function defaultBranding(): BrandingSettings {
  return {
    productName: 'dataChef',
    logoMode: 'both',
    logoUrl: '',
    faviconUrl: '',
    primaryColor: '#22d3ee',
    accentColor: '#2563eb',
    surfaceStyle: 'default',
    supportUrl: 'https://www.threatco.io',
    websiteUrl: 'https://www.threatco.io',
    aboutHeadline: 'Data operations workspace',
    aboutBody: 'Ingest, query, and transform operational data from APIs, databases, and observability systems.',
    parentCompanyLabel: 'ThreatCo',
  }
}

function buildDefaultSettings(): AppSettings {
  const tenant = getCurrentTenantContext()
  const now = Date.now()
  return {
    setupCompleted: false,
    tenant,
    workspace: {
      workspaceName: 'acme-labs',
      companyName: 'Acme Labs',
      region: tenant.region,
      timezone: tenant.timezone,
    },
    owner: {
      name: 'Jane Doe',
      email: 'owner@acme-labs.io',
      role: 'Owner',
    },
    queryEngine: {
      maxRows: 5000,
      timeoutSeconds: 30,
      defaultDataset: '',
      autoExecuteOnOpen: false,
    },
    apiKeys: {
      ingestKey: generateSecret('dc_live_ingest'),
      queryKey: generateSecret('dc_live_query'),
      webhookSecret: generateSecret('whsec'),
    },
    notifications: {
      pipelineFailure: true,
      pipelineSuccess: false,
      emailEnabled: true,
      slackEnabled: false,
      emailAddress: 'owner@acme-labs.io',
      slackChannel: '#data-alerts',
    },
    networkDiscovery: {
      enabled: false,
      scanOnSetup: true,
      backgroundRefreshEnabled: true,
      refreshIntervalMinutes: 60,
      subnetMode: 'local-subnet',
      lastScanAt: null,
    },
    branding: defaultBranding(),
    createdAt: now,
    updatedAt: now,
  }
}

function readLegacySettings(): Partial<AppSettings> | null {
  if (!existsSync(LEGACY_SETTINGS_PATH)) return null
  try {
    const raw = readFileSync(LEGACY_SETTINGS_PATH, 'utf8')
    return JSON.parse(raw) as Partial<AppSettings>
  } catch {
    return null
  }
}

function normaliseSettings(input?: Partial<AppSettings>): AppSettings {
  const tenant = getCurrentTenantContext()
  const defaults = buildDefaultSettings()
  const mergedTenant = {
    ...tenant,
    ...input?.tenant,
    tenantId: input?.tenant?.tenantId ?? tenant.tenantId,
    slug: input?.tenant?.slug ?? tenant.slug,
    hostnames: input?.tenant?.hostnames ?? tenant.hostnames,
    status: input?.tenant?.status ?? tenant.status,
    region: input?.tenant?.region ?? input?.workspace?.region ?? tenant.region,
    timezone: input?.tenant?.timezone ?? input?.workspace?.timezone ?? tenant.timezone,
    dataResidencyLockedAt: input?.tenant?.dataResidencyLockedAt ?? tenant.dataResidencyLockedAt ?? null,
  }

  const merged: AppSettings = {
    ...defaults,
    ...input,
    tenant: mergedTenant,
    workspace: {
      ...defaults.workspace,
      ...input?.workspace,
      region: input?.workspace?.region ?? mergedTenant.region,
      timezone: input?.workspace?.timezone ?? mergedTenant.timezone,
    },
    owner: {
      ...defaults.owner,
      ...input?.owner,
    },
    queryEngine: {
      ...defaults.queryEngine,
      ...input?.queryEngine,
    },
    apiKeys: {
      ...defaults.apiKeys,
      ...input?.apiKeys,
    },
    notifications: {
      ...defaults.notifications,
      ...input?.notifications,
    },
    networkDiscovery: {
      ...defaults.networkDiscovery,
      ...input?.networkDiscovery,
    },
    branding: {
      ...defaults.branding,
      ...input?.branding,
    },
    createdAt: input?.createdAt ?? defaults.createdAt,
    updatedAt: Date.now(),
  }

  if (!merged.notifications.emailAddress) {
    merged.notifications.emailAddress = merged.owner.email
  }

  merged.tenant.region = merged.workspace.region
  merged.tenant.timezone = merged.workspace.timezone
  saveTenantContext(merged.tenant)

  return merged
}

const cache = new Map<string, AppSettings>()

function writeSettings(settings: AppSettings): AppSettings {
  writeJsonFile(SETTINGS_FILE, settings)
  cache.set(settings.tenant.tenantId, settings)
  invalidateSearchIndex(settings.tenant.tenantId)
  return settings
}

export function getAppSettings(): AppSettings {
  const tenant = getCurrentTenantContext()
  const cached = cache.get(tenant.tenantId)
  if (cached) return cached

  const legacy = readLegacySettings()
  const stored = readJsonFile<Partial<AppSettings> | null>(SETTINGS_FILE, legacy)
  const next = normaliseSettings(stored ?? undefined)
  writeSettings(next)
  return next
}

export function saveAppSettings(patch: Partial<AppSettings>): AppSettings {
  const current = getAppSettings()
  const next = normaliseSettings({
    ...current,
    ...patch,
    tenant: { ...current.tenant, ...patch.tenant },
    workspace: { ...current.workspace, ...patch.workspace },
    owner: { ...current.owner, ...patch.owner },
    queryEngine: { ...current.queryEngine, ...patch.queryEngine },
    apiKeys: { ...current.apiKeys, ...patch.apiKeys },
    notifications: { ...current.notifications, ...patch.notifications },
    networkDiscovery: { ...current.networkDiscovery, ...patch.networkDiscovery },
    branding: { ...current.branding, ...patch.branding },
  })

  if (!next.notifications.emailAddress) {
    next.notifications.emailAddress = next.owner.email
  }

  return writeSettings(next)
}

export function rotateAppSecret(key: keyof AppSettings['apiKeys']): AppSettings {
  const prefix = key === 'webhookSecret' ? 'whsec' : key === 'ingestKey' ? 'dc_live_ingest' : 'dc_live_query'
  return saveAppSettings({
    apiKeys: {
      ...getAppSettings().apiKeys,
      [key]: generateSecret(prefix),
    },
  })
}

export function resetAppSettings(): AppSettings {
  const reset = normaliseSettings(buildDefaultSettings())
  cache.delete(reset.tenant.tenantId)
  return writeSettings(reset)
}
