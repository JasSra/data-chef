/**
 * Server-side Azure Application Insights / Azure Monitor helper.
 *
 * Supports two query paths:
 *   1. App Insights REST API v1  (legacy, works with all resource types)
 *      POST https://api.applicationinsights.io/v1/apps/{appId}/query
 *
 *   2. Azure Monitor Log Analytics API  (recommended for workspace-based resources)
 *      POST https://api.loganalytics.azure.com/v1/workspaces/{workspaceId}/query
 *
 * Both use the same OAuth2 client_credentials flow but with different resource scopes.
 * Token cache persists across requests within the same Node.js process.
 */

interface TokenEntry {
  token:     string
  expiresAt: number  // epoch ms
}

// Keyed by `${scope}:${tenantId}:${clientId}` so each scope is cached independently.
const _tokenCache = new Map<string, TokenEntry>()

export interface AiQueryResult {
  columns:    string[]
  rows:       string[][]
  rowCount:   number
  durationMs: number
  error?:     string
}

async function getToken(
  tenantId:     string,
  clientId:     string,
  clientSecret: string,
  scope:        string,
): Promise<string> {
  const cacheKey = `${scope}:${tenantId}:${clientId}`
  const cached   = _tokenCache.get(cacheKey)
  if (cached && cached.expiresAt - Date.now() > 120_000) return cached.token

  const res = await fetch(
    `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
    {
      method:  'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body:    new URLSearchParams({ grant_type: 'client_credentials', client_id: clientId, client_secret: clientSecret, scope }).toString(),
      signal:  AbortSignal.timeout(10_000),
    },
  )
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`Azure AD token error ${res.status}: ${text.slice(0, 300)}`)
  }
  const data = await res.json() as { access_token: string; expires_in: number }
  const entry: TokenEntry = { token: data.access_token, expiresAt: Date.now() + data.expires_in * 1000 }
  _tokenCache.set(cacheKey, entry)
  return entry.token
}

/** Kept for backwards compat — used by the connector test route */
export async function getAzureToken(tenantId: string, clientId: string, clientSecret: string): Promise<string> {
  return getToken(tenantId, clientId, clientSecret, 'https://api.applicationinsights.io/.default')
}

function parseTable(data: { tables?: Array<{ columns: Array<{ name: string }>; rows: unknown[][] }> }, durationMs: number): AiQueryResult {
  const table = data.tables?.[0]
  if (!table) return { columns: [], rows: [], rowCount: 0, durationMs, error: 'No table in response' }
  const columns = table.columns.map(c => c.name)
  const rows    = table.rows.map(r => r.map(cell => cell === null || cell === undefined ? '∅' : String(cell)))
  return { columns, rows, rowCount: rows.length, durationMs }
}

/**
 * App Insights REST API v1 (legacy path).
 * Requires the App Insights Application ID (not the workspace ID).
 * Role needed: Monitoring Reader on the App Insights resource.
 */
export async function executeKQL(
  appId:        string,
  tenantId:     string,
  clientId:     string,
  clientSecret: string,
  kql:          string,
  timespan?:    string,
): Promise<AiQueryResult> {
  const t0    = performance.now()
  const token = await getToken(tenantId, clientId, clientSecret, 'https://api.applicationinsights.io/.default')

  const reqBody: Record<string, unknown> = { query: kql }
  if (timespan) reqBody.timespan = timespan

  const res = await fetch(`https://api.applicationinsights.io/v1/apps/${appId}/query`, {
    method:  'POST',
    headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
    body:    JSON.stringify(reqBody),
    signal:  AbortSignal.timeout(30_000),
  })
  const durationMs = Math.round(performance.now() - t0)
  if (!res.ok) {
    const text = await res.text()
    return { columns: [], rows: [], rowCount: 0, durationMs, error: `App Insights API ${res.status}: ${text.slice(0, 400)}` }
  }
  return parseTable(await res.json(), durationMs)
}

/**
 * Azure Monitor Log Analytics API (recommended / newer path).
 * Works with workspace-based App Insights resources (all resources created since ~2021).
 * Requires the Log Analytics Workspace ID (found in the workspace → Overview page).
 * Role needed: Log Analytics Reader on the workspace (or Monitoring Reader on App Insights).
 *
 * Advantages over the legacy path:
 *   - Single endpoint for all Azure Monitor data (App Insights, VMs, containers, etc.)
 *   - Not affected by the api.applicationinsights.io API-key retirement (March 2026)
 *   - Supports cross-workspace queries
 */
export async function executeKQLWorkspace(
  workspaceId:  string,
  tenantId:     string,
  clientId:     string,
  clientSecret: string,
  kql:          string,
  timespan?:    string,
): Promise<AiQueryResult> {
  const t0    = performance.now()
  const token = await getToken(tenantId, clientId, clientSecret, 'https://api.loganalytics.io/.default')

  const reqBody: Record<string, unknown> = { query: kql }
  if (timespan) reqBody.timespan = timespan

  const res = await fetch(`https://api.loganalytics.azure.com/v1/workspaces/${workspaceId}/query`, {
    method:  'POST',
    headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
    body:    JSON.stringify(reqBody),
    signal:  AbortSignal.timeout(30_000),
  })
  const durationMs = Math.round(performance.now() - t0)
  if (!res.ok) {
    const text = await res.text()
    return { columns: [], rows: [], rowCount: 0, durationMs, error: `Log Analytics API ${res.status}: ${text.slice(0, 400)}` }
  }
  return parseTable(await res.json(), durationMs)
}
