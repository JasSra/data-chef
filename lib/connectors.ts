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

const SEED: ConnectorRecord[] = []

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
