/**
 * Server-side pipeline registry.
 * Persisted to a workspace-local JSON file so pipeline definitions survive reloads
 * and stay shared across route workers in Next.js dev.
 */

import type { SourceType } from '@/lib/datasets'
import { readJsonFile, writeJsonFile } from '@/lib/json-store'
import { getDataset } from '@/lib/datasets'
import { getConnector } from '@/lib/connectors'
import { invalidateSearchIndex } from '@/lib/search-cache'
import { getCurrentTenantContext } from '@/lib/tenant'

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

export interface PipelineSourceRef {
  sourceType: SourceType
  sourceId: string
  resource?: string
}

export interface PipelineOutputTarget {
  mode: 'none' | 'dataset'
  datasetId?: string
  datasetName?: string
  refreshMode?: 'manual' | 'scheduled'
  refreshIntervalMinutes?: number | null
}

export interface PipelineDef {
  id:             string
  name:           string
  description:    string
  notes?:         string
  status:         'active' | 'draft'
  avgDuration:    string
  dataset:        string
  sourceType?:    SourceType
  sourceId?:      string
  resource?:      string
  outputTarget?:  PipelineOutputTarget | null
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

export interface StepRunMetric {
  stepIndex: number
  stepId: string
  label: string
  op: string
  rowsIn: number
  rowsOut: number
  removed: number
  failed: boolean
  failedRows: number
  durationMs: number
  throughputPerSec: number
}

export interface PipelineRunSummary {
  rowsIn: number
  rowsOut: number
  removed: number
  failedRows: number
  failedSteps: number
  totalSteps: number
  durationMs: number
  throughputPerSec: number
  errorRate: number
  healthScore: number
}

export interface LatestRunResult {
  pipelineId: string
  status: 'succeeded' | 'failed'
  startedAt: number
  durationMs: number
  columns: string[]
  rows: string[][]
  rowCount: number
  summary: PipelineRunSummary
  stepMetrics: StepRunMetric[]
  error?: string | null
}

export interface PipelineTemplate {
  version: 1
  name: string
  description: string
  notes: string
  status: 'active' | 'draft'
  source: {
    sourceType: SourceType
    sourceName: string
    resource?: string
  }
  outputTarget: PipelineOutputTarget | null
  steps: RuntimeStep[]
}

interface PipelineStateFile {
  pipelines: PipelineDef[]
  runHistory: Record<string, RunRecord[]>
  runsToday: Record<string, number>
  deletedIds?: string[]
}

interface TenantPipelineRuntime {
  latestRunResults: Map<string, LatestRunResult>
  activeRuns: number
}

const PIPELINE_STORE_FILE = 'pipelines.json'
const runtimeByTenant = new Map<string, TenantPipelineRuntime>()

function getRuntime(): TenantPipelineRuntime {
  const tenantId = getCurrentTenantContext().tenantId
  const existing = runtimeByTenant.get(tenantId)
  if (existing) return existing
  const created: TenantPipelineRuntime = {
    latestRunResults: new Map<string, LatestRunResult>(),
    activeRuns: 0,
  }
  runtimeByTenant.set(tenantId, created)
  return created
}

function seedState(): PipelineStateFile {
  return {
    pipelines: [
      {
        id: 'demo_pipeline_b2c_identity_gold',
        name: 'Demo · B2C Identity Expansion Gold',
        description: 'Seeded walkthrough for flattening nested B2C identities into a relational identity table.',
        notes: `This demo is designed to teach the pipeline builder end to end.\n\nWalkthrough:\n1. Select the source node to confirm the demo dataset binding.\n2. Click each node in order and watch how preview evolves.\n3. Use the output node to inspect the final result.\n4. Run the pipeline to keep the latest output in memory for the run page.\n\nWhat it demonstrates:\n- strict validation\n- row filtering via condition\n- timestamp coercion\n- flatten array + flatten object\n- SQL projection over transformed rows\n- dedupe and enrich\n- final output shaping`,
        status: 'draft',
        avgDuration: '—',
        dataset: 'demo_b2c_users',
        sourceType: 'dataset',
        sourceId: 'demo_b2c_users',
        resource: '',
        outputTarget: { mode: 'dataset', datasetName: 'demo-b2c-identity-gold', refreshMode: 'manual' },
        steps: [],
        uiSteps: [],
        runtimeSteps: [
          { id: 's1', op: 'validate', label: 'Validate B2C shape', config: { validateMode: 'strict', quarantine: true, schemaText: 'id: string\ndisplayName: string\ngivenName: string\nsurname: string\nmail: string\nuserPrincipalName: string\naccountEnabled: boolean\ncreatedDateTime: timestamp\nidentities: array' } },
          { id: 's2', op: 'condition', label: 'Keep enabled accounts', config: { conditionField: '$.accountEnabled', conditionOp: '==', conditionValue: 'true', trueBranch: 'Enabled', falseBranch: 'Disabled' } },
          { id: 's3', op: 'coerce', label: 'Normalize created timestamp', config: { coerceField: '$.createdDateTime', coerceType: 'timestamp' } },
          { id: 's4', op: 'flatten', label: 'Unwind identities array', config: { flattenField: '$.identities', flattenMode: 'array' } },
          { id: 's5', op: 'flatten', label: 'Expand identity object', config: { flattenField: '$.identities', flattenMode: 'object' } },
          { id: 's6', op: 'query', label: 'Project identity grain', config: { queryDataset: '', queryType: 'sql', queryText: "SELECT id, displayName, givenName, surname, userPrincipalName, createdDateTime, identities_signInType AS signInType, identities_issuerAssignedId AS signInId, identities_issuer AS issuer\nFROM upstream\nWHERE identities_signInType IN ('emailAddress', 'userName', 'userPrincipalName')\nORDER BY createdDateTime DESC\nLIMIT 250" } },
          { id: 's7', op: 'dedupe', label: 'Deduplicate sign-in IDs', config: { dedupeKey: '$.signInId', dedupeWindow: 'all' } },
          { id: 's8', op: 'enrich', label: 'Classify sign-in metadata', config: { lookupUrl: 'http://localhost:3333/api/pipelines/demo-enrich', joinKey: '$.signInId', enrichFields: 'normalized,domain,tenant,isMailosaur,isSynthetic,signInKindGuess,hasGuidPrefix' } },
          { id: 's9', op: 'map', label: 'Rename final columns', config: { mappings: [
            { from: '$.signInId', to: 'identityKey', transform: '' },
            { from: '$.signInType', to: 'identityType', transform: '' },
            { from: '$.enrich_domain', to: 'identityDomain', transform: '' },
            { from: '$.enrich_tenant', to: 'tenantSlug', transform: '' },
            { from: '$.enrich_isSynthetic', to: 'isSynthetic', transform: '' },
          ] } },
          { id: 's10', op: 'write', label: 'Write gold dataset', config: { createDataset: true, newDatasetName: 'demo-b2c-identity-gold', destFormat: 'jsonl' } },
        ],
        quarantineStep: null,
      },
      {
        id: 'demo_pipeline_nginx_checkout_triage',
        name: 'Demo · NGINX Checkout Triage',
        description: 'Seeded walkthrough for filtering ecommerce logs down to operationally relevant checkout failures.',
        notes: `This example is intentionally simpler than the identity pipeline.\n\nUse it to learn:\n- SQL filtering and ordering\n- branch/condition as a triage gate\n- dedupe of repeated order events\n- final output preview and runtime metrics`,
        status: 'draft',
        avgDuration: '—',
        dataset: 'demo_nginx_ecommerce_logs',
        sourceType: 'dataset',
        sourceId: 'demo_nginx_ecommerce_logs',
        resource: '',
        outputTarget: { mode: 'dataset', datasetName: 'demo-nginx-checkout-triage', refreshMode: 'manual' },
        steps: [],
        uiSteps: [],
        runtimeSteps: [
          { id: 'n1', op: 'query', label: 'Keep checkout and API traffic', config: { queryDataset: '', queryType: 'sql', queryText: "SELECT ts, method, path, status, latencyMs, country, device, orderId, userId, revenue\nFROM upstream\nWHERE path LIKE '%checkout%' OR path LIKE '%api%'\nORDER BY latencyMs DESC\nLIMIT 100" } },
          { id: 'n2', op: 'condition', label: 'Keep failing requests', config: { conditionField: '$.status', conditionOp: '>=', conditionValue: '500', trueBranch: 'Critical', falseBranch: 'Drop' } },
          { id: 'n3', op: 'dedupe', label: 'Deduplicate order IDs', config: { dedupeKey: '$.orderId', dedupeWindow: 'all' } },
          { id: 'n4', op: 'write', label: 'Write triage dataset', config: { createDataset: true, newDatasetName: 'demo-nginx-checkout-triage', destFormat: 'jsonl' } },
        ],
        quarantineStep: null,
      },
    ],
    runHistory: {},
    runsToday: {},
    deletedIds: [],
  }
}

function normalizeRunRecord(run: RunRecord): RunRecord {
  return {
    id: run.id,
    status: run.status,
    durationMs: run.durationMs,
    startedAt: run.startedAt,
    stepsCompleted: run.stepsCompleted,
  }
}

function readState(): PipelineStateFile {
  const state = readJsonFile<PipelineStateFile>(PIPELINE_STORE_FILE, seedState())
  const deletedIds = new Set(state.deletedIds ?? [])
  const mergedPipelines = new Map<string, PipelineDef>()
  for (const pipeline of seedState().pipelines) {
    if (deletedIds.has(pipeline.id)) continue
    mergedPipelines.set(pipeline.id, normalizePipeline(pipeline))
  }
  for (const pipeline of state.pipelines) {
    if (deletedIds.has(pipeline.id)) continue
    mergedPipelines.set(pipeline.id, normalizePipeline({
      ...pipeline,
      steps: pipeline.steps.map(step => ({ ...step, logLines: step.logLines ? [...step.logLines] : undefined })),
      uiSteps: pipeline.uiSteps.map(step => ({ ...step })),
      runtimeSteps: pipeline.runtimeSteps?.map(step => ({ ...step, config: { ...step.config } })),
      quarantineStep: pipeline.quarantineStep ? { ...pipeline.quarantineStep } : null,
    }))
  }
  return {
    pipelines: Array.from(mergedPipelines.values()),
    runHistory: Object.fromEntries(
      Object.entries(state.runHistory ?? {}).map(([id, history]) => [id, history.map(normalizeRunRecord)]),
    ),
    runsToday: Object.fromEntries(Object.entries(state.runsToday ?? {}).map(([id, count]) => [id, Number(count) || 0])),
    deletedIds: [...deletedIds],
  }
}

function writeState(): void {
  const state = readState()
  writeJsonFile<PipelineStateFile>(PIPELINE_STORE_FILE, state)
}

/* ── Pipeline definitions ────────────────────────────────────────────────────── */
function normalizePipeline(pipeline: PipelineDef): PipelineDef {
  const runtimeSteps = pipeline.runtimeSteps?.map(step => ({ ...step, config: { ...step.config } })) ?? []
  const uiSteps = pipeline.uiSteps?.length
    ? pipeline.uiSteps.map(step => ({ ...step }))
    : runtimeSteps.map(step => ({
        id: step.id,
        op: step.op,
        label: step.label,
        icon: step.op === 'condition' ? 'branch' : step.op === 'map' || step.op === 'enrich' ? 'coerce' : step.op,
        status: 'ok' as const,
        config: step.label,
      }))
  const steps = pipeline.steps?.length
    ? pipeline.steps.map(step => ({ ...step, logLines: step.logLines ? [...step.logLines] : undefined }))
    : runtimeSteps.map(step => ({ label: step.label, durationMs: 350, rowsIn: 0, rowsOut: 0, logLines: [`Running ${step.label}…`, `${step.label} completed`] }))
  return {
    ...pipeline,
    notes: pipeline.notes ?? '',
    sourceType: pipeline.sourceType ?? 'dataset',
    sourceId: pipeline.sourceId ?? pipeline.dataset,
    outputTarget: pipeline.outputTarget ?? null,
    runtimeSteps,
    uiSteps,
    steps,
    quarantineStep: pipeline.quarantineStep ?? (
      runtimeSteps.some(step => step.op === 'validate' && step.config.quarantine)
        ? { id: `${pipeline.id}-q`, op: 'quarantine', label: 'Quarantine', icon: 'quarantine', status: 'ok', config: 'invalid records → /quarantine/' }
        : null
    ),
  }
}

export function addPipeline(def: Omit<PipelineDef, 'id'> & { id?: string }): PipelineDef {
  const state = readState()
  const pipeline = normalizePipeline({ ...def, id: def.id ?? `p${Date.now().toString(36)}` })
  state.pipelines.push(pipeline)
  state.runHistory[pipeline.id] = []
  state.runsToday[pipeline.id] = 0
  writeJsonFile<PipelineStateFile>(PIPELINE_STORE_FILE, state)
  invalidateSearchIndex()
  return pipeline
}

export function updatePipeline(id: string, patch: Partial<Omit<PipelineDef, 'id'>>): PipelineDef | null {
  const state = readState()
  const existing = state.pipelines.find(pipeline => pipeline.id === id)
  if (!existing) return null
  Object.assign(existing, patch)
  const normalized = normalizePipeline(existing)
  Object.assign(existing, normalized)
  writeJsonFile<PipelineStateFile>(PIPELINE_STORE_FILE, state)
  invalidateSearchIndex()
  return existing
}

export function deletePipelinesReferencingSource(sourceType: SourceType, sourceId: string): number {
  const state = readState()
  const before = state.pipelines.length
  const removedIds = [
    state.pipelines
      .filter(pipeline => (pipeline.sourceType ?? 'dataset') === sourceType && (pipeline.sourceId ?? pipeline.dataset) === sourceId)
      .map(pipeline => pipeline.id),
  ].flat()
  if (removedIds.length === 0) return 0

  const removedIdSet = new Set(removedIds)
  state.pipelines = state.pipelines.filter(pipeline => !removedIdSet.has(pipeline.id))
  state.deletedIds = Array.from(new Set([...(state.deletedIds ?? []), ...removedIds]))
  for (const id of removedIds) {
    delete state.runHistory[id]
    delete state.runsToday[id]
    getRuntime().latestRunResults.delete(id)
  }
  writeJsonFile<PipelineStateFile>(PIPELINE_STORE_FILE, state)
  invalidateSearchIndex()
  return before - state.pipelines.length
}

export function deletePipelinesWritingDataset(datasetId: string): number {
  const state = readState()
  const before = state.pipelines.length
  const removedIds = [
    state.pipelines
      .filter(pipeline => pipeline.outputTarget?.mode === 'dataset' && pipeline.outputTarget.datasetId === datasetId)
      .map(pipeline => pipeline.id),
  ].flat()
  if (removedIds.length === 0) return 0

  const removedIdSet = new Set(removedIds)
  state.pipelines = state.pipelines.filter(pipeline => !removedIdSet.has(pipeline.id))
  state.deletedIds = Array.from(new Set([...(state.deletedIds ?? []), ...removedIds]))
  for (const id of removedIds) {
    delete state.runHistory[id]
    delete state.runsToday[id]
    getRuntime().latestRunResults.delete(id)
  }
  writeJsonFile<PipelineStateFile>(PIPELINE_STORE_FILE, state)
  invalidateSearchIndex()
  return before - state.pipelines.length
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

export function getPipelines(): PipelineDef[] {
  return readState().pipelines.map(pipeline => normalizePipeline(pipeline))
}

export function getPipeline(id: string): PipelineDef | null {
  return getPipelines().find(pipeline => pipeline.id === id) ?? null
}

export function getRunHistory(id: string): RunRecord[] {
  return readState().runHistory[id] ?? []
}

export function getLatestRunResult(id: string): LatestRunResult | null {
  return getRuntime().latestRunResults.get(id) ?? null
}

export function setLatestRunResult(id: string, result: LatestRunResult): void {
  getRuntime().latestRunResults.set(id, result)
}

export function recordRun(
  id: string,
  status: 'succeeded' | 'failed',
  durationMs: number,
  stepsCompleted: number,
): void {
  const state = readState()
  const run: RunRecord = {
    id: `${id}-${Date.now()}`,
    status, durationMs, stepsCompleted,
    startedAt: Date.now(),
  }
  const history = [...getRunHistory(id), run]
  if (history.length > MAX_HISTORY) history.splice(0, history.length - MAX_HISTORY)
  state.runHistory[id] = history
  state.runsToday[id] = (state.runsToday[id] ?? 0) + 1
  writeJsonFile<PipelineStateFile>(PIPELINE_STORE_FILE, state)
}

/* ── Worker pool ─────────────────────────────────────────────────────────────── */
const MAX_WORKERS = 5

export function workerStart(): void { getRuntime().activeRuns++ }
export function workerEnd():   void { getRuntime().activeRuns = Math.max(0, getRuntime().activeRuns - 1) }

export function getWorkerState() {
  const activeRuns = getRuntime().activeRuns
  return {
    active: activeRuns,
    total:  MAX_WORKERS,
    pct:    Math.min(100, Math.round((activeRuns / MAX_WORKERS) * 100)),
  }
}

/* ── API response builder ────────────────────────────────────────────────────── */
export function buildPipelineResponse(p: PipelineDef) {
  const normalized = normalizePipeline(p)
  const history  = getRunHistory(p.id)
  const last     = history.length ? history[history.length - 1] : undefined
  return {
    id:             normalized.id,
    name:           normalized.name,
    description:    normalized.description,
    notes:          normalized.notes ?? '',
    status:         normalized.status,
    avgDuration:    normalized.avgDuration,
    dataset:        normalized.dataset,
    sourceType:     normalized.sourceType,
    sourceId:       normalized.sourceId,
    resource:       normalized.resource ?? null,
    outputTarget:   normalized.outputTarget,
    uiSteps:        normalized.uiSteps,
    runtimeSteps:   normalized.runtimeSteps ?? [],
    quarantineStep: normalized.quarantineStep,
    lastRunAt:      last?.startedAt ?? null,
    lastRunStatus:  last?.status ?? (normalized.status === 'draft' ? 'draft' : null),
    runsToday:      readState().runsToday[normalized.id] ?? 0,
    recentRuns:     history.map(r => ({ status: r.status, durationMs: r.durationMs })),
    latestRunResult: getLatestRunResult(normalized.id),
    exportTemplate: exportPipelineTemplate(normalized),
  }
}

export function exportPipelineTemplate(pipeline: PipelineDef): PipelineTemplate {
  const sourceId = pipeline.sourceId ?? pipeline.dataset
  const sourceName = (pipeline.sourceType ?? 'dataset') === 'connector'
    ? (getConnector(sourceId)?.name ?? sourceId)
    : (getDataset(sourceId)?.name ?? sourceId)

  return {
    version: 1,
    name: pipeline.name,
    description: pipeline.description,
    notes: pipeline.notes ?? '',
    status: pipeline.status,
    source: {
      sourceType: pipeline.sourceType ?? 'dataset',
      sourceName,
      resource: pipeline.resource,
    },
    outputTarget: pipeline.outputTarget ?? null,
    steps: (pipeline.runtimeSteps ?? []).map(step => ({
      id: step.id,
      op: step.op,
      label: step.label,
      config: { ...step.config },
    })),
  }
}

export function clearPipelines(): void {
  writeJsonFile<PipelineStateFile>(PIPELINE_STORE_FILE, {
    pipelines: [],
    runHistory: {},
    runsToday: {},
    deletedIds: seedState().pipelines.map(pipeline => pipeline.id),
  })
  const runtime = getRuntime()
  runtime.latestRunResults.clear()
  runtime.activeRuns = 0
  invalidateSearchIndex()
}
