import 'server-only'

import { randomBytes } from 'crypto'
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs'
import { dirname, join } from 'path'
import type { AppSettings } from '@/lib/app-settings-schema'

const SETTINGS_PATH = join(process.cwd(), '.data', 'app-settings.json')

function generateSecret(prefix: string, size = 24): string {
  return `${prefix}_${randomBytes(size).toString('base64url')}`
}

function buildDefaultSettings(): AppSettings {
  const now = Date.now()
  return {
    setupCompleted: false,
    workspace: {
      workspaceName: 'acme-labs',
      companyName: 'Acme Labs',
      region: 'ap-southeast-2',
      timezone: 'Australia/Brisbane',
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
    createdAt: now,
    updatedAt: now,
  }
}

function ensureDir(path: string) {
  const dir = dirname(path)
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true })
}

function normaliseSettings(input?: Partial<AppSettings>): AppSettings {
  const defaults = buildDefaultSettings()
  const merged: AppSettings = {
    ...defaults,
    ...input,
    workspace: {
      ...defaults.workspace,
      ...input?.workspace,
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
    createdAt: input?.createdAt ?? defaults.createdAt,
    updatedAt: Date.now(),
  }

  if (!merged.notifications.emailAddress) {
    merged.notifications.emailAddress = merged.owner.email
  }

  return merged
}

let cache: AppSettings | null = null

function writeSettings(settings: AppSettings): AppSettings {
  ensureDir(SETTINGS_PATH)
  writeFileSync(SETTINGS_PATH, JSON.stringify(settings, null, 2), 'utf8')
  cache = settings
  return settings
}

export function getAppSettings(): AppSettings {
  if (cache) return cache
  if (!existsSync(SETTINGS_PATH)) {
    return writeSettings(buildDefaultSettings())
  }

  try {
    const raw = readFileSync(SETTINGS_PATH, 'utf8')
    cache = normaliseSettings(JSON.parse(raw) as Partial<AppSettings>)
    return cache
  } catch {
    return writeSettings(buildDefaultSettings())
  }
}

export function saveAppSettings(patch: Partial<AppSettings>): AppSettings {
  const current = getAppSettings()
  const next = normaliseSettings({
    ...current,
    ...patch,
    workspace: { ...current.workspace, ...patch.workspace },
    owner: { ...current.owner, ...patch.owner },
    queryEngine: { ...current.queryEngine, ...patch.queryEngine },
    apiKeys: { ...current.apiKeys, ...patch.apiKeys },
    notifications: { ...current.notifications, ...patch.notifications },
    networkDiscovery: { ...current.networkDiscovery, ...patch.networkDiscovery },
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
  return writeSettings(buildDefaultSettings())
}
