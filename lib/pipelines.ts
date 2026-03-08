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

export interface RuntimeStep {
  id: string
  op: string
  label: string
  config: Record<string, unknown>
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
  runtimeSteps?:  RuntimeStep[]    // runtime step definitions with real config
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
export const PIPELINES: PipelineDef[] = []

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
const _runHistory = new Map<string, RunRecord[]>()

// Separate today's run counter (pre-seeded to match seed data)
const _runsToday = new Map<string, number>()

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
