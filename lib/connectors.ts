/**
 * Server-side connector registry.
 * Module-level state persists across requests in the same Node.js process.
 */

import type { ConnectorId } from '@/components/ConnectorWizard'

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
}

export interface ConnectorRuntimeConfig {
  [key: string]: unknown
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

const SEED: ConnectorRecord[] = [
  {
    id: 'c0', name: 'Rick & Morty API', type: 'http', status: 'connected',
    authMethod: 'None (public)',
    endpoint: 'https://rickandmortyapi.com/api/character/',
    description: 'Live read-only REST API — all 826 characters, paginated, cached server-side',
    datasets: ['rick-morty-characters'], syncInterval: 'on-demand',
    latencyMs: 180, lastSyncAt: NOW - 5 * MIN, recordsRaw: 826,
    syncHistory: seedHistory(12, 2 * HR,  826,     0,    180, NOW - 5 * MIN),
    createdAt:   NOW - 30 * DAY,
  },
  {
    id: 'c1', name: 'Stripe Webhooks', type: 'webhook', status: 'connected',
    authMethod: 'HMAC-SHA256',
    endpoint: 'https://api.datachef.io/ingest/webhook/acme-labs',
    description: 'Inbound Stripe payment and billing events via signed webhooks',
    datasets: ['billing-events'], syncInterval: 'real-time',
    latencyMs: 12, lastSyncAt: NOW - 2 * MIN, recordsRaw: 4_100_000,
    syncHistory: seedHistory(12, 30 * MIN, 3200,   0.25, 12,  NOW - 2 * MIN),
    createdAt:   NOW - 20 * DAY,
  },
  {
    id: 'c2', name: 'S3 Data Lake', type: 's3', status: 'connected',
    authMethod: 'AWS IAM Role',
    endpoint: 's3://acme-datalake-prod/logs/',
    description: 'Production log archive in S3-compatible storage (Parquet + JSONL)',
    datasets: ['server-logs'], syncInterval: 'every 15min',
    latencyMs: 340, lastSyncAt: NOW - 1 * HR, recordsRaw: 12_100_000,
    syncHistory: seedHistory(12, 15 * MIN, 95000,  0.3,  340, NOW - 1 * HR),
    createdAt:   NOW - 15 * DAY,
  },
  {
    id: 'c3', name: 'Commerce API', type: 'http', status: 'connected',
    authMethod: 'OAuth 2.0',
    endpoint: 'https://api.commerce.io/v2/products',
    description: 'Product catalog REST API with OAuth2 and incremental cursor-based sync',
    datasets: ['product-catalog'], syncInterval: 'every 6h',
    latencyMs: 220, lastSyncAt: NOW - 3 * HR, recordsRaw: 89_000,
    syncHistory: seedHistory(12, 6 * HR,   820,    0.1,  220, NOW - 3 * HR),
    createdAt:   NOW - 10 * DAY,
  },
  {
    id: 'c4', name: 'PostgreSQL (prod)', type: 'postgresql', status: 'connected',
    authMethod: 'TLS + password',
    endpoint: 'pg-prod.internal:5432/appdb',
    description: 'Primary application database — user tables with CDC-based incremental sync',
    datasets: ['user-profiles'], syncInterval: 'every 30min',
    latencyMs: 8, lastSyncAt: NOW - 20 * MIN, recordsRaw: 150_000,
    syncHistory: seedHistory(12, 30 * MIN, 2400,   0.2,  8,   NOW - 20 * MIN),
    createdAt:   NOW - 8 * DAY,
  },
  {
    id: 'c5', name: 'Legacy FTP', type: 'sftp', status: 'disconnected',
    authMethod: 'Basic auth',
    endpoint: 'ftp://legacy.supplier.net/exports/',
    description: 'Supplier data export over FTP — pending migration to SFTP',
    datasets: [], syncInterval: 'daily',
    latencyMs: 0, lastSyncAt: NOW - 3 * DAY, recordsRaw: 0,
    syncHistory: seedHistory(8, DAY, 4200, 0.4, 1800, NOW - 3 * DAY),
    createdAt:   NOW - 25 * DAY,
  },
]

/* ── App Insights credential vault (server-side only, never serialised) ── */
export interface AppInsightsCredentials {
  mode:         'appinsights' | 'workspace'  // which API endpoint to use
  appId:        string   // App Insights Application ID (appinsights mode)
  workspaceId:  string   // Log Analytics Workspace ID (workspace mode)
  tenantId:     string
  clientId:     string
  clientSecret: string
}

const _aiCreds = new Map<string, AppInsightsCredentials>()
const _runtimeConfig = new Map<string, ConnectorRuntimeConfig>()

export function setAppInsightsCreds(id: string, creds: AppInsightsCredentials): void {
  _aiCreds.set(id, creds)
}
export function getAppInsightsCreds(id: string): AppInsightsCredentials | null {
  return _aiCreds.get(id) ?? null
}

export function setConnectorRuntimeConfig(id: string, config: ConnectorRuntimeConfig): void {
  _runtimeConfig.set(id, config)
}

export function getConnectorRuntimeConfig(id: string): ConnectorRuntimeConfig | null {
  return _runtimeConfig.get(id) ?? null
}

/* ── Store ───────────────────────────────────────────────────────── */
let _store: ConnectorRecord[] = [...SEED]

export function getConnectors(): ConnectorRecord[]      { return [..._store] }
export function getConnector(id: string): ConnectorRecord | null {
  return _store.find(c => c.id === id) ?? null
}

export function addConnector(
  data: Omit<ConnectorRecord, 'id' | 'syncHistory' | 'createdAt'>,
): ConnectorRecord {
  const rec: ConnectorRecord = {
    ...data,
    id:          `c${Date.now().toString(36)}`,
    syncHistory: [],
    createdAt:   Date.now(),
  }
  _store.unshift(rec)
  return rec
}

export function updateConnectorSync(
  id: string, records: number, durationMs: number, ok: boolean,
): void {
  const c = _store.find(r => r.id === id)
  if (!c) return
  c.syncHistory = [...c.syncHistory.slice(-19), { ts: Date.now(), records, durationMs, ok }]
  if (ok) {
    c.lastSyncAt = Date.now()
    c.latencyMs  = durationMs
    if (records > 0) c.recordsRaw = records
    c.status     = 'connected'
  }
}
