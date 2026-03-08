/**
 * Server-side saved-query registry, keyed by connector ID.
 * Module-level state persists across requests.
 */

export interface SavedAiQuery {
  id:        string
  name:      string
  kql:       string
  createdAt: number
}

const _store = new Map<string, SavedAiQuery[]>()

export function getSavedQueries(connectorId: string): SavedAiQuery[] {
  return _store.get(connectorId) ?? []
}

export function addSavedQuery(connectorId: string, name: string, kql: string): SavedAiQuery {
  const q: SavedAiQuery = {
    id:        `sq_${Date.now().toString(36)}`,
    name,
    kql,
    createdAt: Date.now(),
  }
  const existing = _store.get(connectorId) ?? []
  _store.set(connectorId, [...existing, q])
  return q
}

export function deleteSavedQuery(connectorId: string, queryId: string): boolean {
  const existing = _store.get(connectorId)
  if (!existing) return false
  const next = existing.filter(q => q.id !== queryId)
  _store.set(connectorId, next)
  return next.length !== existing.length
}

export function seedDefaultQueries(connectorId: string): void {
  if ((_store.get(connectorId) ?? []).length > 0) return
  _store.set(connectorId, [
    {
      id: 'ai_d1', name: 'Exception summary (24h)',
      kql: `exceptions\n| where timestamp > ago(24h)\n| summarize count() by type\n| order by count_ desc\n| limit 20`,
      createdAt: Date.now(),
    },
    {
      id: 'ai_d2', name: 'Request failure rate',
      kql: `requests\n| where timestamp > ago(24h)\n| summarize total=count(), failed=countif(success==false) by bin(timestamp, 1h)\n| extend failRate = todouble(failed)/todouble(total)*100\n| order by timestamp asc`,
      createdAt: Date.now(),
    },
    {
      id: 'ai_d3', name: 'Slow dependencies (p95)',
      kql: `dependencies\n| where timestamp > ago(24h)\n| summarize p95=percentile(duration, 95), total=count() by name\n| order by p95 desc\n| limit 20`,
      createdAt: Date.now(),
    },
    {
      id: 'ai_d4', name: 'Custom events top 10',
      kql: `customEvents\n| where timestamp > ago(24h)\n| summarize count() by name\n| order by count_ desc\n| limit 10`,
      createdAt: Date.now(),
    },
  ])
}
