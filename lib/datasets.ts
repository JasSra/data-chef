/**
 * Server-side dataset registry.
 * Persisted to a workspace-local JSON file so state survives reloads
 * and stays shared across route workers in Next.js dev.
 */

import { readJsonFile, writeJsonFile } from '@/lib/json-store'
import { invalidateSearchIndex } from '@/lib/search-cache'

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

const DEMO_B2C_ROWS: Record<string, unknown>[] = [
  {
    id: '00095109-0c69-4079-be07-70511a206eef',
    displayName: 'unknown',
    givenName: 'berenice',
    surname: 'lynch',
    mail: null,
    userPrincipalName: '00095109-0c69-4079-be07-70511a206eef@bneuatb2c.onmicrosoft.com',
    accountEnabled: true,
    createdDateTime: '2025-07-24T06:17:10Z',
    identities: [
      { signInType: 'userName', issuer: 'bneuatb2c.onmicrosoft.com', issuerAssignedId: '65nxdi_at_s9f3ebis.mailosaur.net' },
      { signInType: 'emailAddress', issuer: 'bneuatb2c.onmicrosoft.com', issuerAssignedId: '65nxdi@s9f3ebis.mailosaur.net' },
      { signInType: 'userPrincipalName', issuer: 'bneuatb2c.onmicrosoft.com', issuerAssignedId: '00095109-0c69-4079-be07-70511a206eef@bneuatb2c.onmicrosoft.com' },
    ],
  },
  {
    id: '00611f9b-181e-43ce-ace2-3361548e48f1',
    displayName: 'unknown',
    givenName: 'Kevin',
    surname: 'Connolly',
    mail: null,
    userPrincipalName: '00611f9b-181e-43ce-ace2-3361548e48f1@bneuatb2c.onmicrosoft.com',
    accountEnabled: true,
    createdDateTime: '2025-07-07T21:20:46Z',
    identities: [
      { signInType: 'userName', issuer: 'bneuatb2c.onmicrosoft.com', issuerAssignedId: 'kevin.connolly_at_brisbane.qld.gov.au' },
      { signInType: 'emailAddress', issuer: 'bneuatb2c.onmicrosoft.com', issuerAssignedId: 'kevin.connolly@brisbane.qld.gov.au' },
      { signInType: 'userPrincipalName', issuer: 'bneuatb2c.onmicrosoft.com', issuerAssignedId: '00611f9b-181e-43ce-ace2-3361548e48f1@bneuatb2c.onmicrosoft.com' },
    ],
  },
  {
    id: '19db7bf2-fac8-47aa-b0b6-3f174333e0eb',
    displayName: 'unknown',
    givenName: 'claws',
    surname: 'struck',
    mail: null,
    userPrincipalName: '19db7bf2-fac8-47aa-b0b6-3f174333e0eb@bneuatb2c.onmicrosoft.com',
    accountEnabled: true,
    createdDateTime: '2025-06-13T22:05:40Z',
    identities: [
      { signInType: 'userName', issuer: 'bneuatb2c.onmicrosoft.com', issuerAssignedId: 'claws-struck_at_yo6ycezb.mailosaur.net' },
      { signInType: 'emailAddress', issuer: 'bneuatb2c.onmicrosoft.com', issuerAssignedId: 'claws-struck@yo6ycezb.mailosaur.net' },
      { signInType: 'userPrincipalName', issuer: 'bneuatb2c.onmicrosoft.com', issuerAssignedId: '19db7bf2-fac8-47aa-b0b6-3f174333e0eb@bneuatb2c.onmicrosoft.com' },
    ],
  },
  {
    id: '1a29976e-4b37-4262-ba69-26b99f90b36a',
    displayName: 'unknown',
    givenName: 'allan',
    surname: 'boehm',
    mail: null,
    userPrincipalName: '1a29976e-4b37-4262-ba69-26b99f90b36a@bneuatb2c.onmicrosoft.com',
    accountEnabled: true,
    createdDateTime: '2025-07-28T10:54:16Z',
    identities: [
      { signInType: 'userName', issuer: 'bneuatb2c.onmicrosoft.com', issuerAssignedId: '0251w_at_s9f3ebis.mailosaur.net' },
      { signInType: 'emailAddress', issuer: 'bneuatb2c.onmicrosoft.com', issuerAssignedId: '0251w@s9f3ebis.mailosaur.net' },
      { signInType: 'userPrincipalName', issuer: 'bneuatb2c.onmicrosoft.com', issuerAssignedId: '1a29976e-4b37-4262-ba69-26b99f90b36a@bneuatb2c.onmicrosoft.com' },
    ],
  },
  {
    id: '00d8e062-7b59-485d-a188-3a36108f8153',
    displayName: 'Ken Zhou',
    givenName: 'Ken',
    surname: 'Zhou',
    mail: 'ken.zhou@example.com',
    userPrincipalName: '00d8e062-7b59-485d-a188-3a36108f8153@bneuatb2c.onmicrosoft.com',
    accountEnabled: false,
    createdDateTime: '2026-01-04T03:20:00Z',
    identities: [
      { signInType: 'emailAddress', issuer: 'bneuatb2c.onmicrosoft.com', issuerAssignedId: 'ken.zhou@example.com' },
      { signInType: 'userPrincipalName', issuer: 'bneuatb2c.onmicrosoft.com', issuerAssignedId: '00d8e062-7b59-485d-a188-3a36108f8153@bneuatb2c.onmicrosoft.com' },
    ],
  },
]

const DEMO_NGINX_ROWS: Record<string, unknown>[] = [
  { ts: '2026-03-01T10:02:10Z', method: 'GET', path: '/products/sku-001', status: 200, latencyMs: 118, bytes: 5412, country: 'AU', device: 'desktop', orderId: null, userId: 'u-1001', revenue: 0 },
  { ts: '2026-03-01T10:02:24Z', method: 'POST', path: '/checkout', status: 200, latencyMs: 642, bytes: 981, country: 'AU', device: 'mobile', orderId: 'ord-5001', userId: 'u-1001', revenue: 149.95 },
  { ts: '2026-03-01T10:03:02Z', method: 'GET', path: '/cart', status: 499, latencyMs: 3201, bytes: 0, country: 'US', device: 'mobile', orderId: null, userId: 'u-2042', revenue: 0 },
  { ts: '2026-03-01T10:03:19Z', method: 'GET', path: '/api/recommendations', status: 502, latencyMs: 881, bytes: 214, country: 'SG', device: 'desktop', orderId: null, userId: 'u-9911', revenue: 0 },
  { ts: '2026-03-01T10:04:11Z', method: 'GET', path: '/products/sku-204', status: 200, latencyMs: 96, bytes: 4761, country: 'GB', device: 'desktop', orderId: null, userId: 'u-7712', revenue: 0 },
  { ts: '2026-03-01T10:04:48Z', method: 'POST', path: '/checkout', status: 500, latencyMs: 1333, bytes: 312, country: 'AU', device: 'mobile', orderId: 'ord-5002', userId: 'u-7712', revenue: 249.0 },
]

const SEED: DatasetRecord[] = [
  {
    id: 'demo_b2c_users',
    name: 'Demo B2C Users',
    source: 'memory',
    format: 'json',
    records: formatRecords(DEMO_B2C_ROWS.length),
    recordsRaw: DEMO_B2C_ROWS.length,
    schemaVersion: 'v1',
    lastIngested: 'seeded',
    size: '—',
    status: 'active',
    description: 'Seeded demo dataset for identity flattening, filtering, enrichment, and branching examples.',
    connection: 'Built-in demo',
    liveType: null,
    queryDataset: null,
    schema: [
      { field: 'id', type: 'string', nullable: false, example: '00095109-0c69-4079-be07-70511a206eef' },
      { field: 'displayName', type: 'string', nullable: false, example: 'unknown' },
      { field: 'givenName', type: 'string', nullable: true, example: 'berenice' },
      { field: 'surname', type: 'string', nullable: true, example: 'lynch' },
      { field: 'mail', type: 'string', nullable: true, example: 'ken.zhou@example.com' },
      { field: 'userPrincipalName', type: 'string', nullable: false, example: '...@bneuatb2c.onmicrosoft.com' },
      { field: 'accountEnabled', type: 'boolean', nullable: false, example: 'true' },
      { field: 'createdDateTime', type: 'timestamp', nullable: false, example: '2025-07-24T06:17:10Z' },
      { field: 'identities', type: 'array', nullable: false, example: '[3 items]' },
    ],
    sampleRows: DEMO_B2C_ROWS,
    totalRows: DEMO_B2C_ROWS.length,
    sourceRef: null,
    materialization: {
      kind: 'manual',
      refreshMode: 'manual',
      lastMaterializedAt: null,
      lastRunStatus: null,
      lastRunError: null,
    },
    createdAt: NOW,
  },
  {
    id: 'demo_nginx_ecommerce_logs',
    name: 'Demo NGINX Ecommerce Logs',
    source: 'memory',
    format: 'json',
    records: formatRecords(DEMO_NGINX_ROWS.length),
    recordsRaw: DEMO_NGINX_ROWS.length,
    schemaVersion: 'v1',
    lastIngested: 'seeded',
    size: '—',
    status: 'active',
    description: 'Seeded ecommerce access logs for latency, error, revenue, and conversion-style pipeline demos.',
    connection: 'Built-in demo',
    liveType: null,
    queryDataset: null,
    schema: [
      { field: 'ts', type: 'timestamp', nullable: false, example: '2026-03-01T10:02:10Z' },
      { field: 'method', type: 'string', nullable: false, example: 'GET' },
      { field: 'path', type: 'string', nullable: false, example: '/checkout' },
      { field: 'status', type: 'integer', nullable: false, example: '500' },
      { field: 'latencyMs', type: 'integer', nullable: false, example: '1333' },
      { field: 'bytes', type: 'integer', nullable: false, example: '5412' },
      { field: 'country', type: 'string', nullable: false, example: 'AU' },
      { field: 'device', type: 'string', nullable: false, example: 'mobile' },
      { field: 'orderId', type: 'string', nullable: true, example: 'ord-5001' },
      { field: 'userId', type: 'string', nullable: false, example: 'u-1001' },
      { field: 'revenue', type: 'float', nullable: false, example: '149.95' },
    ],
    sampleRows: DEMO_NGINX_ROWS,
    totalRows: DEMO_NGINX_ROWS.length,
    sourceRef: null,
    materialization: {
      kind: 'manual',
      refreshMode: 'manual',
      lastMaterializedAt: null,
      lastRunStatus: null,
      lastRunError: null,
    },
    createdAt: NOW,
  },
]

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
  const merged = new Map<string, DatasetRecord>()
  for (const dataset of SEED) merged.set(dataset.id, normalizeDatasetRecord({
    ...dataset,
    schema: dataset.schema ? dataset.schema.map(field => ({ ...field })) : null,
    sampleRows: dataset.sampleRows ? dataset.sampleRows.map(row => ({ ...row })) : null,
  }))
  for (const dataset of state.datasets) {
    merged.set(dataset.id, normalizeDatasetRecord({
      ...dataset,
      schema: dataset.schema ? dataset.schema.map(field => ({ ...field })) : null,
      sampleRows: dataset.sampleRows ? dataset.sampleRows.map(row => ({ ...row })) : null,
    }))
  }
  return {
    datasets: Array.from(merged.values()),
  }
}

function writeState(state: DatasetStateFile): void {
  writeJsonFile(DATASET_STORE_FILE, state)
  invalidateSearchIndex()
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
