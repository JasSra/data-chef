/**
 * Server-side dataset registry.
 * Persisted to a workspace-local JSON file so state survives reloads
 * and stays shared across route workers in Next.js dev.
 */

import { readJsonFile, writeJsonFile } from '@/lib/json-store'

export interface SchemaField {
  field:    string
  type:     string
  nullable: boolean
  example:  string
}

export type SourceType = 'dataset' | 'connector'
export type DatasetMaterializationKind = 'connector' | 'pipeline' | 'manual' | 'file'
export type DatasetRefreshMode = 'manual' | 'scheduled'

export interface SourceReference {
  sourceType: SourceType
  sourceId: string
  resource?: string
}

export interface DatasetMaterialization {
  kind: DatasetMaterializationKind
  sourceType?: SourceType
  sourceId?: string
  resource?: string
  refreshMode?: DatasetRefreshMode
  refreshIntervalMinutes?: number | null
  owningPipelineId?: string | null
  lastMaterializedAt?: number | null
  lastRunStatus?: 'succeeded' | 'failed' | null
  lastRunError?: string | null
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
  sourceRef?:   SourceReference | null
  materialization?: DatasetMaterialization | null
  createdAt:    number
}

/* ── Seed data ───────────────────────────────────────────────────────────── */
const NOW = Date.now()
const DAY = 86_400_000

const SEED: DatasetRecord[] = []

/* ── Store ───────────────────────────────────────────────────────────────── */
const DATASET_STORE_FILE = 'datasets.json'

interface DatasetStateFile {
  datasets: DatasetRecord[]
}

function seedState(): DatasetStateFile {
  return { datasets: [...SEED] }
}

function readState(): DatasetStateFile {
  const state = readJsonFile<DatasetStateFile>(DATASET_STORE_FILE, seedState())
  return {
    datasets: state.datasets.map(dataset => normalizeDatasetRecord({
      ...dataset,
      schema: dataset.schema ? dataset.schema.map(field => ({ ...field })) : null,
      sampleRows: dataset.sampleRows ? dataset.sampleRows.map(row => ({ ...row })) : null,
    })),
  }
}

function writeState(state: DatasetStateFile): void {
  writeJsonFile(DATASET_STORE_FILE, state)
}

export function getDatasets(): DatasetRecord[] {
  return readState().datasets
}

export function getDataset(id: string): DatasetRecord | null {
  return readState().datasets.find(d => d.id === id) ?? null
}

export function addDataset(data: Omit<DatasetRecord, 'id' | 'createdAt'>): DatasetRecord {
  const state = readState()
  const ds = normalizeDatasetRecord({
    ...data,
    id:        `ds_${Date.now().toString(36)}`,
    createdAt: Date.now(),
  })
  state.datasets.push(ds)
  writeState(state)
  return ds
}

export function clearDatasets(): void {
  writeState({ datasets: [] })
}

export function getDatasetByUrl(url: string): DatasetRecord | null {
  return readState().datasets.find(d => d.url === url) ?? null
}

export function updateDatasetSchema(
  id: string,
  schema: SchemaField[],
  sampleRows: Record<string, unknown>[],
  totalRows: number,
): void {
  const state = readState()
  const ds = state.datasets.find(d => d.id === id)
  if (ds) {
    ds.schema = schema
    ds.sampleRows = sampleRows
    ds.totalRows = totalRows
    ds.lastIngested = 'just now'
    const baseMaterialization = ds.materialization ?? inferMaterialization(ds)
    if (!baseMaterialization) {
      ds.materialization = null
    } else {
      ds.materialization = {
      ...baseMaterialization,
      lastMaterializedAt: Date.now(),
      lastRunStatus: 'succeeded',
      lastRunError: null,
      }
    }
    ds.records = totalRows >= 1_000_000
      ? `${(totalRows / 1_000_000).toFixed(1)}M`
      : totalRows >= 1_000
      ? `${(totalRows / 1_000).toFixed(0)}K`
      : String(totalRows)
    ds.recordsRaw = totalRows
    writeState(state)
  }
}

function inferSourceRef(dataset: Partial<DatasetRecord>): SourceReference | null {
  if (dataset.sourceRef?.sourceType && dataset.sourceRef.sourceId) {
    return { ...dataset.sourceRef }
  }
  if (dataset.connectorId) {
    return {
      sourceType: 'connector',
      sourceId: dataset.connectorId,
      resource: dataset.resource,
    }
  }
  return null
}

function inferMaterialization(dataset: Partial<DatasetRecord>): DatasetMaterialization | null {
  if (dataset.materialization) {
    return { ...dataset.materialization }
  }

  const sourceRef = inferSourceRef(dataset)
  if (sourceRef?.sourceType === 'connector') {
    return {
      kind: 'connector',
      sourceType: sourceRef.sourceType,
      sourceId: sourceRef.sourceId,
      resource: sourceRef.resource,
      refreshMode: 'manual',
      lastMaterializedAt: null,
      lastRunStatus: null,
      lastRunError: null,
    }
  }

  if (dataset.source === 'file') {
    return {
      kind: 'file',
      refreshMode: 'manual',
      lastMaterializedAt: null,
      lastRunStatus: null,
      lastRunError: null,
    }
  }

  return {
    kind: 'manual',
    refreshMode: 'manual',
    lastMaterializedAt: null,
    lastRunStatus: null,
    lastRunError: null,
  }
}

function normalizeDatasetRecord(dataset: DatasetRecord): DatasetRecord {
  const sourceRef = inferSourceRef(dataset)
  const materialization = inferMaterialization(dataset)
  return {
    ...dataset,
    sourceRef,
    materialization,
  }
}

function formatRecords(totalRows: number): string {
  if (totalRows >= 1_000_000) return `${(totalRows / 1_000_000).toFixed(1)}M`
  if (totalRows >= 1_000) return `${(totalRows / 1_000).toFixed(0)}K`
  return String(totalRows)
}

export interface MaterializeDatasetInput {
  existingDatasetId?: string
  name: string
  source: string
  format?: string
  description?: string
  connection?: string
  url?: string
  auth?: string
  connectorId?: string
  resource?: string
  sourceRef?: SourceReference | null
  materialization?: DatasetMaterialization | null
  schema: SchemaField[]
  sampleRows: Record<string, unknown>[]
  totalRows: number
}

export function materializeDataset(input: MaterializeDatasetInput): DatasetRecord {
  const state = readState()
  const dataset = input.existingDatasetId
    ? state.datasets.find(entry => entry.id === input.existingDatasetId)
    : undefined

  const sourceRef = input.sourceRef ?? inferSourceRef({
    connectorId: input.connectorId,
    resource: input.resource,
  })
  const baseMaterialization = input.materialization ?? inferMaterialization({
    source: input.source,
    connectorId: input.connectorId,
    sourceRef,
  }) ?? {
    kind: 'manual' as const,
    refreshMode: 'manual' as const,
    lastMaterializedAt: null,
    lastRunStatus: null,
    lastRunError: null,
  }
  const materialization: DatasetMaterialization = {
    ...baseMaterialization,
    sourceType: input.materialization?.sourceType ?? sourceRef?.sourceType,
    sourceId: input.materialization?.sourceId ?? sourceRef?.sourceId,
    resource: input.materialization?.resource ?? sourceRef?.resource,
    lastMaterializedAt: Date.now(),
    lastRunStatus: 'succeeded' as const,
    lastRunError: null,
  }

  if (dataset) {
    dataset.name = input.name.trim()
    dataset.source = input.source
    dataset.format = input.format ?? dataset.format
    dataset.description = input.description ?? dataset.description
    dataset.connection = input.connection ?? dataset.connection
    dataset.url = input.url ?? dataset.url
    dataset.auth = input.auth ?? dataset.auth
    dataset.connectorId = input.connectorId ?? dataset.connectorId
    dataset.resource = input.resource ?? dataset.resource
    dataset.sourceRef = sourceRef
    dataset.materialization = materialization
    dataset.schema = input.schema
    dataset.sampleRows = input.sampleRows
    dataset.totalRows = input.totalRows
    dataset.records = formatRecords(input.totalRows)
    dataset.recordsRaw = input.totalRows
    dataset.lastIngested = 'just now'
    writeState(state)
    return dataset
  }

  const created = normalizeDatasetRecord({
    id: `ds_${Date.now().toString(36)}`,
    name: input.name.trim(),
    source: input.source,
    url: input.url,
    auth: input.auth,
    format: input.format ?? 'JSON',
    records: formatRecords(input.totalRows),
    recordsRaw: input.totalRows,
    schemaVersion: 'v1',
    lastIngested: 'just now',
    size: '—',
    status: 'active',
    description: input.description ?? `${input.source.toUpperCase()} dataset`,
    connection: input.connection ?? input.source,
    connectorId: input.connectorId,
    resource: input.resource,
    liveType: null,
    queryDataset: null,
    schema: input.schema,
    sampleRows: input.sampleRows,
    totalRows: input.totalRows,
    sourceRef,
    materialization,
    createdAt: Date.now(),
  })
  state.datasets.push(created)
  writeState(state)
  return created
}
