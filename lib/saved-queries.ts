import { readJsonFile, writeJsonFile } from '@/lib/json-store'
import { invalidateSearchIndex } from '@/lib/search-cache'

export interface SavedAiQuery {
  id: string
  name: string
  kql: string
  createdAt: number
}

interface SavedQueryStateFile {
  byConnectorId: Record<string, SavedAiQuery[]>
}

const STORE_FILE = 'saved-queries.json'

function readState(): SavedQueryStateFile {
  const state = readJsonFile<SavedQueryStateFile>(STORE_FILE, { byConnectorId: {} })
  return {
    byConnectorId: Object.fromEntries(
      Object.entries(state.byConnectorId ?? {}).map(([connectorId, queries]) => [
        connectorId,
        Array.isArray(queries) ? queries.map(query => ({ ...query })) : [],
      ]),
    ),
  }
}

function writeState(state: SavedQueryStateFile): void {
  writeJsonFile(STORE_FILE, state)
  invalidateSearchIndex()
}

export function getSavedQueries(connectorId: string): SavedAiQuery[] {
  return readState().byConnectorId[connectorId] ?? []
}

export function listAllSavedQueries(): Array<SavedAiQuery & { connectorId: string }> {
  const state = readState()
  return Object.entries(state.byConnectorId).flatMap(([connectorId, queries]) =>
    queries.map(query => ({ ...query, connectorId })),
  )
}

export function addSavedQuery(connectorId: string, name: string, kql: string): SavedAiQuery {
  const state = readState()
  const query: SavedAiQuery = {
    id: `sq_${Date.now().toString(36)}`,
    name,
    kql,
    createdAt: Date.now(),
  }
  const existing = state.byConnectorId[connectorId] ?? []
  state.byConnectorId[connectorId] = [...existing, query]
  writeState(state)
  return query
}

export function deleteSavedQuery(connectorId: string, queryId: string): boolean {
  const state = readState()
  const existing = state.byConnectorId[connectorId]
  if (!existing) return false
  const next = existing.filter(query => query.id !== queryId)
  state.byConnectorId[connectorId] = next
  writeState(state)
  return next.length !== existing.length
}

export function seedDefaultQueries(
  connectorId: string,
  provider: 'appinsights' | 'azuremonitor' | 'elasticsearch' | 'datadog' = 'appinsights',
): void {
  const state = readState()
  if ((state.byConnectorId[connectorId] ?? []).length > 0) return

  const now = Date.now()
  const defaults: Record<typeof provider, SavedAiQuery[]> = {
    appinsights: [
      { id: 'ai_d1', name: 'Exception summary (24h)', kql: `exceptions\n| where timestamp > ago(24h)\n| summarize count() by type\n| order by count_ desc\n| limit 20`, createdAt: now },
      { id: 'ai_d2', name: 'Request failure rate', kql: `requests\n| where timestamp > ago(24h)\n| summarize total=count(), failed=countif(success==false) by bin(timestamp, 1h)\n| extend failRate = todouble(failed)/todouble(total)*100\n| order by timestamp asc`, createdAt: now },
      { id: 'ai_d3', name: 'Slow dependencies (p95)', kql: `dependencies\n| where timestamp > ago(24h)\n| summarize p95=percentile(duration, 95), total=count() by name\n| order by p95 desc\n| limit 20`, createdAt: now },
      { id: 'ai_d4', name: 'Custom events top 10', kql: `customEvents\n| where timestamp > ago(24h)\n| summarize count() by name\n| order by count_ desc\n| limit 10`, createdAt: now },
    ],
    azuremonitor: [
      { id: 'az_d1', name: 'Request trend (24h)', kql: `AppRequests\n| where TimeGenerated > ago(24h)\n| summarize count() by bin(TimeGenerated, 1h)\n| order by TimeGenerated asc`, createdAt: now },
      { id: 'az_d2', name: 'Exceptions by problem', kql: `AppExceptions\n| where TimeGenerated > ago(24h)\n| summarize count() by ProblemId\n| order by count_ desc\n| limit 20`, createdAt: now },
      { id: 'az_d3', name: 'Slow dependencies', kql: `AppDependencies\n| where TimeGenerated > ago(24h)\n| summarize count() by Target\n| order by count_ desc\n| limit 20`, createdAt: now },
      { id: 'az_d4', name: 'Availability results', kql: `AppAvailabilityResults\n| where TimeGenerated > ago(24h)\n| summarize count() by Name\n| order by count_ desc\n| limit 20`, createdAt: now },
    ],
    elasticsearch: [
      { id: 'es_d1', name: 'Error logs (24h)', kql: `logs\n| where log.level:error and @timestamp > ago(24h)\n| limit 100`, createdAt: now },
      { id: 'es_d2', name: 'Top services by count', kql: `logs\n| summarize count() by service.name\n| order by count_ desc\n| limit 20`, createdAt: now },
      { id: 'es_d3', name: 'Recent auth failures', kql: `logs\n| where event.category:authentication and event.outcome:failure\n| limit 100`, createdAt: now },
      { id: 'es_d4', name: 'Sorted recent errors', kql: `logs\n| where error.type:* \n| order by @timestamp desc\n| limit 50`, createdAt: now },
    ],
    datadog: [
      { id: 'dd_d1', name: 'Recent error logs', kql: `logs\n| where status:error\n| limit 100`, createdAt: now },
      { id: 'dd_d2', name: 'Top services', kql: `logs\n| summarize count() by service\n| order by count_ desc\n| limit 20`, createdAt: now },
      { id: 'dd_d3', name: 'Warnings ordered by time', kql: `logs\n| where status:warn\n| order by timestamp desc\n| limit 50`, createdAt: now },
      { id: 'dd_d4', name: 'Gateway errors', kql: `logs\n| where service:gateway and status:error\n| limit 100`, createdAt: now },
    ],
  }

  state.byConnectorId[connectorId] = defaults[provider]
  writeState(state)
}

export function clearSavedQueries(): void {
  writeState({ byConnectorId: {} })
}
