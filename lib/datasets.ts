/**
 * Server-side dataset registry.
 * Module-level store persists across requests in the same Node.js process.
 */

export interface SchemaField {
  field:    string
  type:     string
  nullable: boolean
  example:  string
}

export interface DatasetRecord {
  id:           string
  name:         string
  source:       string          // 'http' | 'pg' | 'mysql' | 's3' | 'file' | 'conn' | 'webhook' | 'memory'
  url?:         string          // for HTTP sources
  auth?:        string          // auth type used
  format:       string
  records:      string          // display string e.g. "826" or "2.4M"
  recordsRaw:   number
  schemaVersion:string
  lastIngested: string
  size:         string
  status:       'active' | 'draft' | 'failed'
  description:  string
  connection:   string
  connectorId?: string
  resource?:    string
  liveType:     'rm-api' | 'server-api' | null
  queryDataset: string | null
  schema:       SchemaField[] | null   // inferred from real data
  sampleRows:   Record<string, unknown>[] | null
  totalRows:    number | null
  createdAt:    number
}

/* ── Seed data ───────────────────────────────────────────────────────────── */
const NOW = Date.now()
const DAY = 86_400_000

const SEED: DatasetRecord[] = []

/* ── Store ───────────────────────────────────────────────────────────────── */
let _store: DatasetRecord[] = [...SEED]

export function getDatasets(): DatasetRecord[] {
  return [..._store]
}

export function getDataset(id: string): DatasetRecord | null {
  return _store.find(d => d.id === id) ?? null
}

export function addDataset(data: Omit<DatasetRecord, 'id' | 'createdAt'>): DatasetRecord {
  const ds: DatasetRecord = {
    ...data,
    id:        `ds_${Date.now().toString(36)}`,
    createdAt: Date.now(),
  }
  _store.push(ds)
  return ds
}

export function getDatasetByUrl(url: string): DatasetRecord | null {
  return _store.find(d => d.url === url) ?? null
}

export function updateDatasetSchema(
  id: string,
  schema: SchemaField[],
  sampleRows: Record<string, unknown>[],
  totalRows: number,
): void {
  const ds = _store.find(d => d.id === id)
  if (ds) {
    ds.schema = schema
    ds.sampleRows = sampleRows
    ds.totalRows = totalRows
    ds.lastIngested = 'just now'
    ds.records = totalRows >= 1_000_000
      ? `${(totalRows / 1_000_000).toFixed(1)}M`
      : totalRows >= 1_000
      ? `${(totalRows / 1_000).toFixed(0)}K`
      : String(totalRows)
  }
}
