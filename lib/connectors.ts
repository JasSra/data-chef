/**
 * Server-side connector registry.
 * Persisted to a workspace-local JSON file so state survives reloads
 * and is visible across route workers in Next.js dev.
 */

import type { ConnectorId } from '@/components/ConnectorWizard'
import { readJsonFile, writeJsonFile } from '@/lib/json-store'
import { decryptSecret, encryptSecret, isEncryptedValue } from '@/lib/secret-crypto'
import { invalidateSearchIndex } from '@/lib/search-cache'

export interface SyncRecord {
  ts:         number   // epoch ms
  records:    number
  durationMs: number
  ok:         boolean
}

export interface ConnectorRecord {
  id:           string
  name:         string
  type:         ConnectorId
  status:       'connected' | 'disconnected'
  authMethod:   string
  endpoint:     string
  description:  string
  datasets:     string[]
  syncInterval: string
  latencyMs:    number
  lastSyncAt:   number | null
  recordsRaw:   number
  syncHistory:  SyncRecord[]   // up to 20 most-recent syncs
  createdAt:    number
  lastRunAt?:   number | null
  lastError?:   string | null
}

export interface ConnectorRuntimeConfig {
  [key: string]: unknown
}

export interface ConnectorTransferRecord {
  id?: string
  name: string
  type: ConnectorId
  status?: 'connected' | 'disconnected'
  authMethod?: string
  endpoint?: string
  description?: string
  datasets?: string[]
  syncInterval?: string
  latencyMs?: number
  lastSyncAt?: number | null
  recordsRaw?: number
  syncHistory?: SyncRecord[]
  createdAt?: number
  lastRunAt?: number | null
  lastError?: string | null
  runtimeConfig?: ConnectorRuntimeConfig
  aiCredentials?: AppInsightsCredentials
  observabilityCredentials?: ObservabilityCredentials
  azureB2cCredentials?: AzureB2CCredentials
  azureEntraIdCredentials?: AzureEntraIdCredentials
  githubCredentials?: GitHubCredentials
  azureDevOpsCredentials?: AzureDevOpsCredentials
}

/* ── Display helpers ─────────────────────────────────────────────── */
export function relativeTime(ts: number | null): string {
  if (!ts) return 'never'
  const d = Date.now() - ts
  if (d < 60_000)     return 'just now'
  if (d < 3_600_000)  return `${Math.round(d / 60_000)}m ago`
  if (d < 86_400_000) return `${Math.round(d / 3_600_000)}h ago`
  return `${Math.round(d / 86_400_000)}d ago`
}

export function fmtRecords(n: number): string {
  if (n === 0)        return '—'
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000)     return `${(n / 1_000).toFixed(0)}K`
  return n.toLocaleString()
}

/** Returns last `count` record-count values from sync history, padded with 0s. */
export function getSparkValues(history: SyncRecord[], count = 12): number[] {
  const vals = history.slice(-count).map(r => r.records)
  while (vals.length < count) vals.unshift(0)
  return vals
}

/* ── Seeded history ──────────────────────────────────────────────── */
const NOW = Date.now()
const MIN = 60_000
const HR  = 3_600_000
const DAY = 86_400_000

function seedHistory(
  count: number, interval: number, recordsBase: number,
  variance: number, latencyMs: number, lastAt: number,
): SyncRecord[] {
  return Array.from({ length: count }, (_, i) => ({
    ts:         lastAt - (count - 1 - i) * interval,
    records:    Math.round(recordsBase * (1 + Math.sin(i * 1.4) * variance)),
    durationMs: Math.round(latencyMs   * (1 + Math.sin(i * 2.1) * 0.2)),
    ok:         true,
  }))
}

const SEED: ConnectorRecord[] = []

/* ── App Insights credential vault (server-side only, never serialised) ── */
export interface AppInsightsCredentials {
  authMode: 'api_key' | 'entra_client_secret'
  appId: string
  apiKey: string
  connectionString?: string
  mode?: 'appinsights' | 'workspace'
  workspaceId?: string
  tenantId?: string
  clientId?: string
  clientSecret?: string
}

export interface AzureB2CCredentials {
  tenantId: string
  clientId: string
  authMode: 'client_secret' | 'client_certificate'
  clientSecret: string
  certificatePem: string
  privateKeyPem: string
  thumbprint: string
  cloud: 'global'
}

export interface AzureEntraIdCredentials extends AzureB2CCredentials {}

export interface AzureDevOpsProjectSelection {
  id: string
  name: string
  description?: string
  visibility?: string
}

export interface AzureDevOpsRepoSelection {
  projectId: string
  projectName: string
  repositoryId: string
  repositoryName: string
  fullName: string
  defaultBranch?: string
}

export interface GitHubRepoSelection {
  owner: string
  repo: string
  fullName: string
  private: boolean
  ownerType: 'User' | 'Organization'
}

export type GitHubResource = 'repos' | 'pullRequests' | 'issues'

export type GitHubCredentials =
  | {
      mode: 'pat'
      token: string
      username?: string
    }
  | {
      mode: 'oauth'
      clientId: string
      clientSecret: string
      accessToken: string
      refreshToken: string
      expiresAt: number
      scope: string
      tokenType: string
      accountLogin: string
    }
  | {
      mode: 'app'
      appId: string
      clientId: string
      clientSecret: string
      privateKey: string
      installationId: number
      accountLogin?: string
    }

export type AzureDevOpsResource =
  | 'repositories'
  | 'commits'
  | 'pullRequests'
  | 'branches'
  | 'workItems'
  | 'pipelines'
  | 'pipelineRuns'

export type AzureDevOpsCredentials =
  | {
      mode: 'pat'
      organization: string
      pat: string
      username?: string
    }
  | {
      mode: 'entra'
      tenantId: string
      clientId: string
      clientSecret: string
      organization: string
      accessToken: string
      refreshToken: string
      expiresAt: number
      scope: string
      tokenType: string
      accountName?: string
    }

export type ObservabilityCredentials =
  | {
      provider: 'appinsights'
      authMode: 'api_key' | 'entra_client_secret'
      appId: string
      apiKey: string
      connectionString?: string
      mode?: 'appinsights' | 'workspace'
      workspaceId?: string
      tenantId?: string
      clientId?: string
      clientSecret?: string
    }
  | {
      provider: 'azuremonitor'
      workspaceId: string
      tenantId: string
      clientId: string
      clientSecret: string
    }
  | {
      provider: 'elasticsearch'
      endpoint: string
      authType: 'basic' | 'apikey'
      username: string
      password: string
      apiKey: string
      indexPattern: string
    }
  | {
      provider: 'datadog'
      site: string
      apiKey: string
      applicationKey: string
      source: 'logs' | 'events'
      defaultQuery: string
    }

interface ConnectorStateFile {
  connectors: ConnectorRecord[]
  runtimeConfigById: Record<string, ConnectorRuntimeConfig>
  aiCredsById: Record<string, AppInsightsCredentials>
  observabilityCredsById: Record<string, ObservabilityCredentials>
  azureB2cCredsById: Record<string, AzureB2CCredentials>
  azureEntraIdCredsById: Record<string, AzureEntraIdCredentials>
  githubCredsById: Record<string, GitHubCredentials>
  azureDevOpsCredsById: Record<string, AzureDevOpsCredentials>
}

const CONNECTOR_STORE_FILE = 'connectors.json'

function seedState(): ConnectorStateFile {
  return {
    connectors: [...SEED],
    runtimeConfigById: {},
    aiCredsById: {},
    observabilityCredsById: {},
    azureB2cCredsById: {},
    azureEntraIdCredsById: {},
    githubCredsById: {},
    azureDevOpsCredsById: {},
  }
}

function cloneState(state: ConnectorStateFile): ConnectorStateFile {
  return {
    connectors: state.connectors.map(connector => ({
      ...connector,
      datasets: [...connector.datasets],
      syncHistory: connector.syncHistory.map(entry => ({ ...entry })),
    })),
    runtimeConfigById: Object.fromEntries(
      Object.entries(state.runtimeConfigById).map(([id, config]) => [id, { ...config }]),
    ),
    aiCredsById: Object.fromEntries(
      Object.entries(state.aiCredsById).map(([id, creds]) => [id, { ...creds }]),
    ),
    observabilityCredsById: Object.fromEntries(
      Object.entries(state.observabilityCredsById ?? {}).map(([id, creds]) => [id, { ...creds }]),
    ),
    azureB2cCredsById: Object.fromEntries(
      Object.entries(state.azureB2cCredsById ?? {}).map(([id, creds]) => [id, { ...creds }]),
    ),
    azureEntraIdCredsById: Object.fromEntries(
      Object.entries(state.azureEntraIdCredsById ?? {}).map(([id, creds]) => [id, { ...creds }]),
    ),
    githubCredsById: Object.fromEntries(
      Object.entries(state.githubCredsById ?? {}).map(([id, creds]) => [id, { ...creds }]),
    ),
    azureDevOpsCredsById: Object.fromEntries(
      Object.entries(state.azureDevOpsCredsById ?? {}).map(([id, creds]) => [id, { ...creds }]),
    ),
  }
}

function readState(): ConnectorStateFile {
  const state = readJsonFile<ConnectorStateFile>(CONNECTOR_STORE_FILE, seedState())
  return cloneState(state)
}

function writeState(state: ConnectorStateFile): void {
  writeJsonFile(CONNECTOR_STORE_FILE, state)
  invalidateSearchIndex()
}

function makeImportedId(baseId?: string): string {
  const state = readState()
  const candidate = (baseId && baseId.trim()) || `c${Date.now().toString(36)}`
  if (!state.connectors.some(c => c.id === candidate)) return candidate
  return `${candidate}-${Math.random().toString(36).slice(2, 6)}`
}

function maybeDecrypt(value: unknown): string {
  if (typeof value === 'string') return value
  if (isEncryptedValue(value)) return decryptSecret(value)
  return ''
}

function encodeGitHubCreds(creds: GitHubCredentials): GitHubCredentials {
  switch (creds.mode) {
    case 'pat':
      return {
        ...creds,
        token: encryptSecret(creds.token) as unknown as string,
      }
    case 'oauth':
      return {
        ...creds,
        clientSecret: encryptSecret(creds.clientSecret) as unknown as string,
        accessToken: encryptSecret(creds.accessToken) as unknown as string,
        refreshToken: creds.refreshToken ? (encryptSecret(creds.refreshToken) as unknown as string) : '',
      }
    case 'app':
      return {
        ...creds,
        clientSecret: encryptSecret(creds.clientSecret) as unknown as string,
        privateKey: encryptSecret(creds.privateKey) as unknown as string,
      }
  }
}

function decodeGitHubCreds(creds: GitHubCredentials): GitHubCredentials {
  switch (creds.mode) {
    case 'pat':
      return {
        ...creds,
        token: maybeDecrypt(creds.token),
      }
    case 'oauth':
      return {
        ...creds,
        clientSecret: maybeDecrypt(creds.clientSecret),
        accessToken: maybeDecrypt(creds.accessToken),
        refreshToken: creds.refreshToken ? maybeDecrypt(creds.refreshToken) : '',
      }
    case 'app':
      return {
        ...creds,
        clientSecret: maybeDecrypt(creds.clientSecret),
        privateKey: maybeDecrypt(creds.privateKey),
      }
  }
}

function encodeAzureDevOpsCreds(creds: AzureDevOpsCredentials): AzureDevOpsCredentials {
  switch (creds.mode) {
    case 'pat':
      return {
        ...creds,
        pat: encryptSecret(creds.pat) as unknown as string,
      }
    case 'entra':
      return {
        ...creds,
        clientSecret: encryptSecret(creds.clientSecret) as unknown as string,
        accessToken: encryptSecret(creds.accessToken) as unknown as string,
        refreshToken: creds.refreshToken ? (encryptSecret(creds.refreshToken) as unknown as string) : '',
      }
  }
}

function decodeAzureDevOpsCreds(creds: AzureDevOpsCredentials): AzureDevOpsCredentials {
  switch (creds.mode) {
    case 'pat':
      return {
        ...creds,
        pat: maybeDecrypt(creds.pat),
      }
    case 'entra':
      return {
        ...creds,
        clientSecret: maybeDecrypt(creds.clientSecret),
        accessToken: maybeDecrypt(creds.accessToken),
        refreshToken: creds.refreshToken ? maybeDecrypt(creds.refreshToken) : '',
      }
  }
}

export function setAppInsightsCreds(id: string, creds: AppInsightsCredentials): void {
  const state = readState()
  state.aiCredsById[id] = { ...creds }
  writeState(state)
}
export function getAppInsightsCreds(id: string): AppInsightsCredentials | null {
  const state = readState()
  return state.aiCredsById[id] ? { ...state.aiCredsById[id] } : null
}

export function setObservabilityCreds(id: string, creds: ObservabilityCredentials): void {
  const state = readState()
  state.observabilityCredsById[id] = { ...creds }
  if (creds.provider === 'appinsights') {
    state.aiCredsById[id] = {
      authMode: creds.authMode,
      appId: creds.appId,
      apiKey: creds.apiKey,
      connectionString: creds.connectionString,
      mode: creds.mode,
      workspaceId: creds.workspaceId,
      tenantId: creds.tenantId,
      clientId: creds.clientId,
      clientSecret: creds.clientSecret,
    }
  }
  writeState(state)
}

export function getObservabilityCreds(id: string): ObservabilityCredentials | null {
  const state = readState()
  const direct = state.observabilityCredsById[id]
  if (direct) return { ...direct }

  const ai = state.aiCredsById[id]
  if (!ai) return null
  return {
    provider: 'appinsights',
    authMode: ai.authMode ?? 'entra_client_secret',
    appId: ai.appId,
    apiKey: ai.apiKey ?? '',
    connectionString: ai.connectionString,
    mode: ai.mode,
    workspaceId: ai.workspaceId,
    tenantId: ai.tenantId,
    clientId: ai.clientId,
    clientSecret: ai.clientSecret,
  }
}

export function setAzureB2CCreds(id: string, creds: AzureB2CCredentials): void {
  const state = readState()
  state.azureB2cCredsById[id] = { ...creds }
  writeState(state)
}

export function getAzureB2CCreds(id: string): AzureB2CCredentials | null {
  const state = readState()
  return state.azureB2cCredsById[id] ? { ...state.azureB2cCredsById[id] } : null
}

export function setAzureEntraIdCreds(id: string, creds: AzureEntraIdCredentials): void {
  const state = readState()
  state.azureEntraIdCredsById[id] = { ...creds }
  writeState(state)
}

export function getAzureEntraIdCreds(id: string): AzureEntraIdCredentials | null {
  const state = readState()
  return state.azureEntraIdCredsById[id] ? { ...state.azureEntraIdCredsById[id] } : null
}

export function setGitHubCreds(id: string, creds: GitHubCredentials): void {
  const state = readState()
  state.githubCredsById[id] = encodeGitHubCreds(creds)
  writeState(state)
}

export function getGitHubCreds(id: string): GitHubCredentials | null {
  const state = readState()
  const creds = state.githubCredsById[id]
  return creds ? decodeGitHubCreds(creds) : null
}

export function setAzureDevOpsCreds(id: string, creds: AzureDevOpsCredentials): void {
  const state = readState()
  state.azureDevOpsCredsById[id] = encodeAzureDevOpsCreds(creds)
  writeState(state)
}

export function getAzureDevOpsCreds(id: string): AzureDevOpsCredentials | null {
  const state = readState()
  const creds = state.azureDevOpsCredsById[id]
  return creds ? decodeAzureDevOpsCreds(creds) : null
}

export function setConnectorRuntimeConfig(id: string, config: ConnectorRuntimeConfig): void {
  const state = readState()
  state.runtimeConfigById[id] = { ...config }
  writeState(state)
}

export function getConnectorRuntimeConfig(id: string): ConnectorRuntimeConfig | null {
  const state = readState()
  return state.runtimeConfigById[id] ? { ...state.runtimeConfigById[id] } : null
}

/* ── Store ───────────────────────────────────────────────────────── */
export function getConnectors(): ConnectorRecord[]      { return readState().connectors }
export function getConnector(id: string): ConnectorRecord | null {
  return readState().connectors.find(c => c.id === id) ?? null
}

export function addConnector(
  data: Omit<ConnectorRecord, 'id' | 'syncHistory' | 'createdAt'>,
): ConnectorRecord {
  const state = readState()
  const rec: ConnectorRecord = {
    ...data,
    id:          `c${Date.now().toString(36)}`,
    syncHistory: [],
    createdAt:   Date.now(),
    lastRunAt:   data.lastRunAt ?? null,
    lastError:   data.lastError ?? null,
  }
  state.connectors.unshift(rec)
  writeState(state)
  return rec
}

export function updateConnector(
  id: string,
  changes: Partial<Omit<ConnectorRecord, 'id' | 'syncHistory' | 'createdAt'>> & {
    syncHistory?: SyncRecord[]
    createdAt?: number
  },
): ConnectorRecord | null {
  const state = readState()
  const index = state.connectors.findIndex(connector => connector.id === id)
  if (index === -1) return null

  const current = state.connectors[index]
  state.connectors[index] = {
    ...current,
    ...changes,
    id: current.id,
    createdAt: current.createdAt,
    syncHistory: Array.isArray(changes.syncHistory)
      ? changes.syncHistory.map(entry => ({ ...entry }))
      : current.syncHistory.map(entry => ({ ...entry })),
    datasets: Array.isArray(changes.datasets)
      ? changes.datasets.map(String)
      : [...current.datasets],
  }

  writeState(state)
  return { ...state.connectors[index], datasets: [...state.connectors[index].datasets], syncHistory: state.connectors[index].syncHistory.map(entry => ({ ...entry })) }
}

export function exportConnector(id: string): ConnectorTransferRecord | null {
  const state = readState()
  const connector = state.connectors.find(item => item.id === id) ?? null
  if (!connector) return null
  return {
    ...connector,
    datasets: [...connector.datasets],
    syncHistory: connector.syncHistory.map(entry => ({ ...entry })),
    runtimeConfig: state.runtimeConfigById[id] ? { ...state.runtimeConfigById[id] } : undefined,
    aiCredentials: state.aiCredsById[id] ? { ...state.aiCredsById[id] } : undefined,
    observabilityCredentials: state.observabilityCredsById[id] ? { ...state.observabilityCredsById[id] } : undefined,
    azureB2cCredentials: state.azureB2cCredsById[id] ? { ...state.azureB2cCredsById[id] } : undefined,
    azureEntraIdCredentials: state.azureEntraIdCredsById[id] ? { ...state.azureEntraIdCredsById[id] } : undefined,
    githubCredentials: state.githubCredsById[id] ? decodeGitHubCreds(state.githubCredsById[id]) : undefined,
    azureDevOpsCredentials: state.azureDevOpsCredsById[id] ? decodeAzureDevOpsCreds(state.azureDevOpsCredsById[id]) : undefined,
  }
}

export function exportConnectors(ids?: string[]): ConnectorTransferRecord[] {
  const targetIds = ids && ids.length > 0 ? new Set(ids) : null
  return readState().connectors
    .filter(connector => (targetIds ? targetIds.has(connector.id) : true))
    .map(connector => exportConnector(connector.id))
    .filter((connector): connector is ConnectorTransferRecord => connector !== null)
}

export function importConnectors(records: ConnectorTransferRecord[]): ConnectorRecord[] {
  const state = readState()
  const imported: ConnectorRecord[] = []

  for (const record of records) {
    const id = makeImportedId(record.id)
    const connector: ConnectorRecord = {
      id,
      name: record.name || 'Imported Connector',
      type: record.type,
      status: record.status === 'disconnected' ? 'disconnected' : 'connected',
      authMethod: record.authMethod ?? 'None',
      endpoint: record.endpoint ?? '',
      description: record.description ?? '',
      datasets: Array.isArray(record.datasets) ? record.datasets.map(String) : [],
      syncInterval: record.syncInterval ?? 'on-demand',
      latencyMs: Number.isFinite(record.latencyMs) ? Number(record.latencyMs) : 0,
      lastSyncAt: typeof record.lastSyncAt === 'number' ? record.lastSyncAt : null,
      recordsRaw: Number.isFinite(record.recordsRaw) ? Number(record.recordsRaw) : 0,
      syncHistory: Array.isArray(record.syncHistory)
        ? record.syncHistory
            .filter(entry => entry && typeof entry.ts === 'number')
            .map(entry => ({
              ts: entry.ts,
              records: Number.isFinite(entry.records) ? Number(entry.records) : 0,
              durationMs: Number.isFinite(entry.durationMs) ? Number(entry.durationMs) : 0,
              ok: entry.ok !== false,
            }))
            .slice(-20)
        : [],
      createdAt: typeof record.createdAt === 'number' ? record.createdAt : Date.now(),
      lastRunAt: typeof record.lastRunAt === 'number' ? record.lastRunAt : null,
      lastError: typeof record.lastError === 'string' ? record.lastError : null,
    }

    state.connectors.unshift(connector)

    if (record.runtimeConfig && typeof record.runtimeConfig === 'object') {
      state.runtimeConfigById[id] = { ...record.runtimeConfig }
    }

    if (record.type === 'appinsights' && record.aiCredentials) {
      state.aiCredsById[id] = {
        authMode: record.aiCredentials.authMode === 'api_key' ? 'api_key' : 'entra_client_secret',
        appId: record.aiCredentials.appId ?? '',
        apiKey: record.aiCredentials.apiKey ?? '',
        connectionString: record.aiCredentials.connectionString ?? '',
        mode: record.aiCredentials.mode === 'workspace' ? 'workspace' : 'appinsights',
        workspaceId: record.aiCredentials.workspaceId ?? '',
        tenantId: record.aiCredentials.tenantId ?? '',
        clientId: record.aiCredentials.clientId ?? '',
        clientSecret: record.aiCredentials.clientSecret ?? '',
      }
    }

    if (record.observabilityCredentials) {
      state.observabilityCredsById[id] = { ...record.observabilityCredentials }
    }

    if (record.type === 'azureb2c' && record.azureB2cCredentials) {
      state.azureB2cCredsById[id] = {
        tenantId: record.azureB2cCredentials.tenantId ?? '',
        clientId: record.azureB2cCredentials.clientId ?? '',
        authMode: record.azureB2cCredentials.authMode === 'client_certificate'
          ? 'client_certificate'
          : 'client_secret',
        clientSecret: record.azureB2cCredentials.clientSecret ?? '',
        certificatePem: record.azureB2cCredentials.certificatePem ?? '',
        privateKeyPem: record.azureB2cCredentials.privateKeyPem ?? '',
        thumbprint: record.azureB2cCredentials.thumbprint ?? '',
        cloud: 'global',
      }
    }

    if (record.type === 'azureentraid' && record.azureEntraIdCredentials) {
      state.azureEntraIdCredsById[id] = {
        tenantId: record.azureEntraIdCredentials.tenantId ?? '',
        clientId: record.azureEntraIdCredentials.clientId ?? '',
        authMode: record.azureEntraIdCredentials.authMode === 'client_certificate'
          ? 'client_certificate'
          : 'client_secret',
        clientSecret: record.azureEntraIdCredentials.clientSecret ?? '',
        certificatePem: record.azureEntraIdCredentials.certificatePem ?? '',
        privateKeyPem: record.azureEntraIdCredentials.privateKeyPem ?? '',
        thumbprint: record.azureEntraIdCredentials.thumbprint ?? '',
        cloud: 'global',
      }
    }

    if (record.type === 'github' && record.githubCredentials) {
      state.githubCredsById[id] = encodeGitHubCreds(record.githubCredentials)
    }

    if (record.type === 'azuredevops' && record.azureDevOpsCredentials) {
      state.azureDevOpsCredsById[id] = encodeAzureDevOpsCreds(record.azureDevOpsCredentials)
    }

    imported.push(connector)
  }

  writeState(state)
  return imported
}

export function updateConnectorSync(
  id: string, records: number, durationMs: number, ok: boolean,
): void {
  const state = readState()
  const c = state.connectors.find(r => r.id === id)
  if (!c) return
  c.lastRunAt = Date.now()
  c.lastError = null
  c.syncHistory = [...c.syncHistory.slice(-19), { ts: Date.now(), records, durationMs, ok }]
  if (ok) {
    c.lastSyncAt = Date.now()
    c.latencyMs  = durationMs
    if (records > 0) c.recordsRaw = records
    c.status     = 'connected'
  }
  writeState(state)
}

export function updateConnectorFailure(id: string, error: string): void {
  const state = readState()
  const c = state.connectors.find(r => r.id === id)
  if (!c) return
  c.lastRunAt = Date.now()
  c.lastError = error
  writeState(state)
}

export function updateConnectorDatasets(id: string, datasets: string[]): void {
  const state = readState()
  const c = state.connectors.find(r => r.id === id)
  if (!c) return
  c.datasets = [...datasets]
  writeState(state)
}

export function clearConnectors(): void {
  writeState(seedState())
}
