/**
 * Server-side pipeline registry.
 * Module-level state persists across requests in the same Node.js process.
 */

/* ── Step / pipeline types ──────────────────────────────────────────────────── */
export interface StepDef {
  label:      string
  durationMs: number
  rowsIn?:    number
  rowsOut?:   number
  logLines?:  string[]
  isError?:   boolean
  errorMsg?:  string
}

export interface UiStep {
  id:     string
  op:     string
  label:  string
  icon:   string
  status: 'ok' | 'error' | 'skip'
  config: string
}

export interface PipelineDef {
  id:             string
  name:           string
  description:    string
  status:         'active' | 'draft'
  avgDuration:    string
  dataset:        string
  steps:          StepDef[]        // server-side execution steps
  uiSteps:        UiStep[]         // client-side DAG nodes
  quarantineStep: UiStep | null
}

export interface RunRecord {
  id:             string
  status:         'succeeded' | 'failed'
  durationMs:     number
  startedAt:      number           // epoch ms
  stepsCompleted: number
}

/* ── Pipeline definitions ────────────────────────────────────────────────────── */
export const PIPELINES: PipelineDef[] = [
  {
    id: 'p1', name: 'billing-normalize-v1',
    description: 'Ingest, coerce, and deduplicate Stripe billing events into Parquet',
    status: 'active', avgDuration: '1m 34s', dataset: 'billing-events',
    steps: [
      { label: 'HTTP Fetch — Stripe Webhooks',     durationMs: 340, rowsIn: 0,      rowsOut: 24_381, logLines: ['Connecting to Stripe webhook stream…', '24 381 events received (JSONL)'] },
      { label: 'Schema Check (v3 strict)',          durationMs: 190, rowsIn: 24_381, rowsOut: 24_228, logLines: ['Validating against billing.schema.v3', '153 records failed → quarantine'] },
      { label: 'Coerce Types',                      durationMs: 260, rowsIn: 24_228, rowsOut: 24_228, logLines: ['$.amount → decimal(18,2)', '$.timestamp → ISO-8601 date-time'] },
      { label: 'Deduplicate (event_id, 7d window)', durationMs: 580, rowsIn: 24_228, rowsOut: 23_904, logLines: ['324 duplicates removed', 'Window: 2024-01-08 → 2024-01-15'] },
      { label: 'Write Parquet → s3://acme-billing', durationMs: 430, rowsIn: 23_904, rowsOut: 23_904, logLines: ['Partition by event_date', 'Wrote 3 part files (128 MB total)'] },
    ],
    uiSteps: [
      { id: 's1', op: 'extract',  label: 'HTTP Fetch',    icon: 'fetch',    status: 'ok', config: 'source: Stripe Webhooks · mode: JSONL' },
      { id: 's2', op: 'validate', label: 'Schema Check',  icon: 'validate', status: 'ok', config: 'schema: v3 · mode: strict' },
      { id: 's3', op: 'coerce',   label: 'Coerce Types',  icon: 'coerce',   status: 'ok', config: '$.amount → decimal(18,2)' },
      { id: 's4', op: 'dedupe',   label: 'Deduplicate',   icon: 'dedupe',   status: 'ok', config: 'key: $.event_id · window: 7d' },
      { id: 's5', op: 'write',    label: 'Write Parquet', icon: 'write',    status: 'ok', config: 'format: parquet · partitionBy: event_date' },
    ],
    quarantineStep: { id: 'sq', op: 'quarantine', label: 'Quarantine', icon: 'quarantine', status: 'ok', config: 'invalid records → /quarantine/billing/' },
  },
  {
    id: 'p2', name: 'user-enrichment',
    description: 'Enrich user profiles with geolocation and segment tags via HTTP lookup',
    status: 'active', avgDuration: '45s', dataset: 'user-profiles',
    steps: [
      { label: 'PostgreSQL Read (incremental)',  durationMs: 420, rowsIn: 0,     rowsOut: 8_751, logLines: ['Incremental read since 2024-01-14T18:00Z', '8 751 rows loaded'] },
      { label: 'Geo Lookup — geo.acme.io',       durationMs: 680, rowsIn: 8_751, rowsOut: 8_690, logLines: ['61 IPs unresolvable → quarantine', 'Avg latency: 72ms/batch'] },
      { label: 'Segment Tag Mapping',            durationMs: 160, rowsIn: 8_690, rowsOut: 8_690, logLines: ['Applied 12 segment rules', '$.plan → segment_tag via dict v7'] },
      { label: 'Write JSONL → S3',               durationMs: 290, rowsIn: 8_690, rowsOut: 8_690, logLines: ['s3://acme-profiles/enriched/', 'Wrote 8 690 records (4.2 MB)'] },
    ],
    uiSteps: [
      { id: 's1', op: 'extract',  label: 'PostgreSQL Read', icon: 'fetch',    status: 'ok', config: 'table: users · incremental: true' },
      { id: 's2', op: 'enrich',   label: 'Geo Lookup',      icon: 'validate', status: 'ok', config: 'HTTP: geo.acme.io/v1/lookup' },
      { id: 's3', op: 'map',      label: 'Tag Mapping',     icon: 'coerce',   status: 'ok', config: '$.plan → segment_tag via dict' },
      { id: 's4', op: 'write',    label: 'Write JSON',      icon: 'write',    status: 'ok', config: 'format: jsonl · dest: S3' },
    ],
    quarantineStep: { id: 'sq', op: 'quarantine', label: 'Quarantine', icon: 'quarantine', status: 'ok', config: 'unresolvable IPs → /quarantine/users/' },
  },
  {
    id: 'p3', name: 'log-aggregation',
    description: 'Aggregate server logs into hourly summaries and error reports',
    status: 'active', avgDuration: '3m 12s', dataset: 'server-logs',
    steps: [
      { label: 'S3 Read — acme-logs-prod (*.jsonl)', durationMs: 300, rowsIn: 0,       rowsOut: 192_440, logLines: ['Scanning s3://acme-logs-prod/', 'Found 192 440 log lines across 8 files'] },
      {
        label: 'Schema Check (v1 strict)',
        durationMs: 240, rowsIn: 192_440, rowsOut: 0,
        isError: true,
        errorMsg: 'Schema validation failed: field "request_id" missing in 14 312 records (7.4%). Threshold: 5%. Pipeline halted — records routed to /quarantine/logs/',
        logLines: ['Checking against server-log.schema.v1', '14 312 records missing required field "request_id"'],
      },
    ],
    uiSteps: [
      { id: 's1', op: 'extract',  label: 'S3 Read',      icon: 'fetch',    status: 'ok',    config: 'bucket: acme-logs-prod · glob: *.jsonl' },
      { id: 's2', op: 'validate', label: 'Schema Check', icon: 'validate', status: 'error', config: 'schema: v1 · strict — FAILED' },
      { id: 's3', op: 'flatten',  label: 'Flatten',      icon: 'coerce',   status: 'skip',  config: 'skipped — upstream failure' },
      { id: 's4', op: 'write',    label: 'Write',        icon: 'write',    status: 'skip',  config: 'skipped — upstream failure' },
    ],
    quarantineStep: { id: 'sq', op: 'quarantine', label: 'Quarantine', icon: 'quarantine', status: 'ok', config: '' },
  },
  {
    id: 'p4', name: 'product-sync',
    description: 'Sync product catalog from external API with JSON-LD normalization',
    status: 'draft', avgDuration: '—', dataset: 'product-catalog',
    steps: [
      { label: 'HTTP Fetch — api.commerce.io/products (OAuth)', durationMs: 460, rowsIn: 0,     rowsOut: 4_821, logLines: ['OAuth 2.0 token acquired', '4 821 products fetched (paginated)'] },
      { label: 'JSON-LD Framing (schema.org/Product)',          durationMs: 310, rowsIn: 4_821, rowsOut: 4_821, logLines: ['Applied @context: schema.org', 'Framed 4 821 Product entities'] },
      { label: 'Write Parquet → s3://acme-catalog',             durationMs: 390, rowsIn: 4_821, rowsOut: 4_821, logLines: ['format: parquet · snappy compression', 'Wrote 1 part file (2.1 MB)'] },
    ],
    uiSteps: [
      { id: 's1', op: 'extract', label: 'HTTP Fetch',    icon: 'fetch',  status: 'ok', config: 'GET api.commerce.io/products · OAuth 2.0' },
      { id: 's2', op: 'map',     label: 'JSON-LD Frame', icon: 'coerce', status: 'ok', config: 'context: schema.org · frame: Product' },
      { id: 's3', op: 'write',   label: 'Write Parquet', icon: 'write',  status: 'ok', config: 'format: parquet · dest: S3' },
    ],
    quarantineStep: null,
  },
]

export const PIPELINE_MAP = new Map(PIPELINES.map(p => [p.id, p]))

export function addPipeline(def: Omit<PipelineDef, 'id'> & { id?: string }): PipelineDef {
  const pipeline: PipelineDef = { ...def, id: def.id ?? `p${Date.now().toString(36)}` }
  PIPELINES.push(pipeline)
  PIPELINE_MAP.set(pipeline.id, pipeline)
  _runHistory.set(pipeline.id, [])
  _runsToday.set(pipeline.id, 0)
  return pipeline
}

export function updatePipeline(id: string, patch: Partial<Omit<PipelineDef, 'id'>>): PipelineDef | null {
  const existing = PIPELINE_MAP.get(id)
  if (!existing) return null
  Object.assign(existing, patch)
  return existing
}

/* ── Run history (module-level, persists across requests) ────────────────────── */
const MAX_HISTORY = 20

/** Seed realistic-looking historical runs, oldest first */
function seedRuns(
  id: string, count: number, intervalMs: number,
  durationMs: number, stepsCompleted: number, lastFailed = false,
): RunRecord[] {
  return Array.from({ length: count }, (_, i) => {
    const isLast = i === count - 1
    return {
      id:             `${id}-seed-${i}`,
      status:         lastFailed && isLast ? 'failed' : 'succeeded',
      durationMs:     Math.max(100, durationMs + Math.round(Math.sin(i * 1.7) * durationMs * 0.12)),
      startedAt:      Date.now() - (count - i) * intervalMs,
      stepsCompleted: lastFailed && isLast ? 1 : stepsCompleted,
    }
  })
}

// Oldest runs first; at(-1) is the most recent
const _runHistory = new Map<string, RunRecord[]>([
  ['p1', seedRuns('p1', 12, 7_200_000,  94_000, 5)],
  ['p2', seedRuns('p2', 4,  21_600_000, 45_000, 4)],
  ['p3', seedRuns('p3', 20, 1_800_000, 192_000, 4, true)],
  ['p4', []],
])

// Separate today's run counter (pre-seeded to match seed data)
const _runsToday = new Map<string, number>([
  ['p1', 12], ['p2', 4], ['p3', 48], ['p4', 0],
])

export function getRunHistory(id: string): RunRecord[] {
  return _runHistory.get(id) ?? []
}

export function recordRun(
  id: string,
  status: 'succeeded' | 'failed',
  durationMs: number,
  stepsCompleted: number,
): void {
  const run: RunRecord = {
    id: `${id}-${Date.now()}`,
    status, durationMs, stepsCompleted,
    startedAt: Date.now(),
  }
  const history = [...getRunHistory(id), run]
  if (history.length > MAX_HISTORY) history.splice(0, history.length - MAX_HISTORY)
  _runHistory.set(id, history)
  _runsToday.set(id, (_runsToday.get(id) ?? 0) + 1)
}

/* ── Worker pool ─────────────────────────────────────────────────────────────── */
const MAX_WORKERS = 5
let _activeRuns = 0

export function workerStart(): void { _activeRuns++ }
export function workerEnd():   void { _activeRuns = Math.max(0, _activeRuns - 1) }

export function getWorkerState() {
  return {
    active: _activeRuns,
    total:  MAX_WORKERS,
    pct:    Math.min(100, Math.round((_activeRuns / MAX_WORKERS) * 100)),
  }
}

/* ── API response builder ────────────────────────────────────────────────────── */
export function buildPipelineResponse(p: PipelineDef) {
  const history  = getRunHistory(p.id)
  const last     = history.at(-1)
  return {
    id:             p.id,
    name:           p.name,
    description:    p.description,
    status:         p.status,
    avgDuration:    p.avgDuration,
    dataset:        p.dataset,
    uiSteps:        p.uiSteps,
    quarantineStep: p.quarantineStep,
    lastRunAt:      last?.startedAt ?? null,
    lastRunStatus:  last?.status ?? (p.status === 'draft' ? 'draft' : null),
    runsToday:      _runsToday.get(p.id) ?? 0,
    recentRuns:     history.map(r => ({ status: r.status, durationMs: r.durationMs })),
  }
}
