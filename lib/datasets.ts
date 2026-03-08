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

const SEED: DatasetRecord[] = [
  {
    id: 'rm', name: 'rick-morty-characters', source: 'http',
    url: 'https://rickandmortyapi.com/api/character/',
    format: 'JSON', records: '826', recordsRaw: 826,
    schemaVersion: 'v1', lastIngested: 'live', size: '~84 KB',
    status: 'active',
    description: 'Rick & Morty API — all 826 characters fetched live from rickandmortyapi.com',
    connection: 'HTTP (rickandmortyapi.com)',
    liveType: 'rm-api', queryDataset: 'rick-morty-characters',
    schema: [
      { field: 'id',       type: 'integer',   nullable: false, example: '1' },
      { field: 'name',     type: 'string',    nullable: false, example: 'Rick Sanchez' },
      { field: 'status',   type: 'string',    nullable: false, example: 'Alive' },
      { field: 'species',  type: 'string',    nullable: false, example: 'Human' },
      { field: 'type',     type: 'string',    nullable: true,  example: '(none)' },
      { field: 'gender',   type: 'string',    nullable: false, example: 'Male' },
      { field: 'origin',   type: 'object',    nullable: false, example: '{ name, url }' },
      { field: 'location', type: 'object',    nullable: false, example: '{ name, url }' },
      { field: 'image',    type: 'string',    nullable: false, example: 'https://rickandmorty…' },
      { field: 'episode',  type: 'array',     nullable: false, example: '[51 items]' },
      { field: 'url',      type: 'string',    nullable: false, example: 'https://rickandmorty…' },
      { field: 'created',  type: 'timestamp', nullable: false, example: '2017-11-04T18:48:46…' },
    ],
    sampleRows: null, totalRows: 826,
    createdAt: NOW - 30 * DAY,
  },
  {
    id: 'events', name: 'synthetic-events', source: 'memory',
    format: 'JSON', records: '500K', recordsRaw: 500_000,
    schemaVersion: 'v1', lastIngested: 'live (memory)', size: '~30 MB',
    status: 'active',
    description: '500K deterministic synthetic billing events — server-side in-memory cache',
    connection: 'In-memory (server-side)',
    liveType: 'server-api', queryDataset: 'events',
    schema: [
      { field: 'id',         type: 'integer', nullable: false, example: '1' },
      { field: 'user_id',    type: 'integer', nullable: false, example: '4821' },
      { field: 'event_type', type: 'string',  nullable: false, example: 'purchase' },
      { field: 'amount',     type: 'float',   nullable: true,  example: '49.99' },
      { field: 'country',    type: 'string',  nullable: false, example: 'US' },
      { field: 'device',     type: 'string',  nullable: false, example: 'mobile' },
      { field: 'ts',         type: 'date',    nullable: false, example: '2026-01-15' },
    ],
    sampleRows: null, totalRows: 500_000,
    createdAt: NOW - 25 * DAY,
  },
  {
    id: '1', name: 'billing-events', source: 'webhook',
    format: 'JSONL', records: '2.4M', recordsRaw: 2_400_000,
    schemaVersion: 'v3', lastIngested: '2h ago', size: '4.1 GB',
    status: 'active',
    description: 'Stripe webhook events for billing and subscription management',
    connection: 'Stripe Webhooks',
    liveType: null, queryDataset: null,
    schema: [
      { field: 'event_id',    type: 'string',    nullable: false, example: 'evt_1NkU2n' },
      { field: 'type',        type: 'string',    nullable: false, example: 'invoice_paid' },
      { field: 'amount',      type: 'integer',   nullable: false, example: '2500' },
      { field: 'currency',    type: 'string',    nullable: false, example: 'usd' },
      { field: 'customer_id', type: 'string',    nullable: false, example: 'cus_9s6XKz' },
      { field: 'customer',    type: 'object',    nullable: true,  example: '{ id, email, name }' },
      { field: 'metadata',    type: 'object',    nullable: true,  example: '{ ip_address, tags }' },
      { field: 'timestamp',   type: 'timestamp', nullable: false, example: '2026-03-06T08:23:11Z' },
    ],
    sampleRows: null, totalRows: 2_400_000,
    createdAt: NOW - 20 * DAY,
  },
  {
    id: '2', name: 'user-profiles', source: 'pg',
    format: 'JSON', records: '150K', recordsRaw: 150_000,
    schemaVersion: 'v1', lastIngested: '1d ago', size: '340 MB',
    status: 'active',
    description: 'Registered user records exported from the auth service',
    connection: 'PostgreSQL (prod)',
    liveType: null, queryDataset: null,
    schema: [
      { field: 'id',         type: 'string',    nullable: false, example: 'usr_a1b2c3' },
      { field: 'email',      type: 'string',    nullable: false, example: 'jane@acme.io' },
      { field: 'name',       type: 'string',    nullable: false, example: 'Jane Doe' },
      { field: 'plan',       type: 'string',    nullable: false, example: 'pro' },
      { field: 'country',    type: 'string',    nullable: true,  example: 'US' },
      { field: 'verified',   type: 'boolean',   nullable: false, example: 'true' },
      { field: 'created_at', type: 'timestamp', nullable: false, example: '2025-01-10T09:00:00Z' },
      { field: 'last_login', type: 'timestamp', nullable: true,  example: '2026-03-07T14:22:00Z' },
    ],
    sampleRows: null, totalRows: 150_000,
    createdAt: NOW - 15 * DAY,
  },
  {
    id: '3', name: 'product-catalog', source: 'http',
    url: 'https://api.commerce.io/v2/products',
    format: 'JSON-LD', records: '89K', recordsRaw: 89_000,
    schemaVersion: 'v2', lastIngested: '3d ago', size: '210 MB',
    status: 'active',
    description: 'Product metadata with semantic markup for e-commerce',
    connection: 'HTTP (api.commerce.io)',
    liveType: null, queryDataset: null,
    schema: [
      { field: 'id',          type: 'string',  nullable: false, example: 'prod_x9k2' },
      { field: 'sku',         type: 'string',  nullable: false, example: 'SKU-00421' },
      { field: 'name',        type: 'string',  nullable: false, example: 'Wireless Headphones' },
      { field: 'category',    type: 'string',  nullable: false, example: 'Electronics' },
      { field: 'price',       type: 'float',   nullable: false, example: '149.99' },
      { field: 'currency',    type: 'string',  nullable: false, example: 'USD' },
      { field: 'inventory',   type: 'integer', nullable: false, example: '342' },
      { field: 'description', type: 'string',  nullable: true,  example: 'Premium noise-cancelling…' },
      { field: 'tags',        type: 'array',   nullable: true,  example: '[3 items]' },
    ],
    sampleRows: null, totalRows: 89_000,
    createdAt: NOW - 10 * DAY,
  },
  {
    id: '4', name: 'server-logs', source: 's3',
    format: 'JSONL', records: '12.1M', recordsRaw: 12_100_000,
    schemaVersion: 'v1', lastIngested: '15m ago', size: '22.4 GB',
    status: 'active',
    description: 'Application server access and error logs (structured JSON)',
    connection: 'S3 (acme-logs-prod)',
    liveType: null, queryDataset: null,
    schema: [
      { field: 'ts',          type: 'timestamp', nullable: false, example: '2026-03-08T12:44:01Z' },
      { field: 'level',       type: 'string',    nullable: false, example: 'INFO' },
      { field: 'host',        type: 'string',    nullable: false, example: 'api-prod-03' },
      { field: 'path',        type: 'string',    nullable: false, example: '/v1/users' },
      { field: 'status_code', type: 'integer',   nullable: false, example: '200' },
      { field: 'duration_ms', type: 'integer',   nullable: false, example: '34' },
      { field: 'bytes',       type: 'integer',   nullable: false, example: '1024' },
      { field: 'user_agent',  type: 'string',    nullable: true,  example: 'Mozilla/5.0…' },
    ],
    sampleRows: null, totalRows: 12_100_000,
    createdAt: NOW - 5 * DAY,
  },
  {
    id: '5', name: 'api-events', source: 'http',
    format: 'JSONL', records: '4.7M', recordsRaw: 4_700_000,
    schemaVersion: 'v2', lastIngested: '30m ago', size: '8.9 GB',
    status: 'active',
    description: 'API gateway request/response telemetry events',
    connection: 'HTTP SSE (gateway.internal)',
    liveType: null, queryDataset: null,
    schema: [
      { field: 'ts',             type: 'timestamp', nullable: false, example: '2026-03-08T12:30:00Z' },
      { field: 'method',         type: 'string',    nullable: false, example: 'GET' },
      { field: 'path',           type: 'string',    nullable: false, example: '/api/v2/events' },
      { field: 'status',         type: 'integer',   nullable: false, example: '200' },
      { field: 'latency_ms',     type: 'integer',   nullable: false, example: '48' },
      { field: 'user_id',        type: 'string',    nullable: true,  example: 'usr_a1b2' },
      { field: 'ip',             type: 'string',    nullable: false, example: '203.0.113.1' },
      { field: 'response_bytes', type: 'integer',   nullable: false, example: '2048' },
    ],
    sampleRows: null, totalRows: 4_700_000,
    createdAt: NOW - 2 * DAY,
  },
]

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
