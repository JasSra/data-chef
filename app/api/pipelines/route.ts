import { NextRequest, NextResponse } from 'next/server'
import { PIPELINES, buildPipelineResponse, addPipeline, updatePipeline } from '@/lib/pipelines'
import type { StepDef, UiStep, RuntimeStep } from '@/lib/pipelines'
import type { SourceType } from '@/lib/datasets'

export const dynamic = 'force-dynamic'

export function GET() {
  return NextResponse.json(PIPELINES.map(buildPipelineResponse))
}

const OP_DURATIONS: Record<string, number> = {
  extract: 400, validate: 250, query: 320, map: 180, coerce: 220, flatten: 260, enrich: 600, dedupe: 500, condition: 120, write: 380,
}

function opIcon(op: string): string {
  if (op === 'extract')   return 'fetch'
  if (op === 'validate')  return 'validate'
  if (op === 'query')     return 'query'
  if (op === 'map')       return 'coerce'
  if (op === 'coerce')    return 'coerce'
  if (op === 'flatten')   return 'flatten'
  if (op === 'enrich')    return 'coerce'
  if (op === 'dedupe')    return 'dedupe'
  if (op === 'condition') return 'branch'
  if (op === 'write')     return 'write'
  return op
}

function configSummary(op: string, config: Record<string, unknown>): string {
  switch (op) {
    case 'extract':   return `${config.sourceType ?? 'http'} · ${(config.format as string ?? 'JSON').toUpperCase()}`
    case 'validate':  return `mode: ${config.validateMode ?? 'strict'}${config.quarantine ? ' · quarantine on' : ''}`
    case 'query':     return `${config.queryType ?? 'sql'}${config.queryDataset ? ` on ${config.queryDataset}` : ''}`
    case 'map':       return `${(config.mappings as unknown[])?.filter((m: unknown) => (m as {from:string}).from).length ?? 0} field mappings`
    case 'coerce':    return `${config.coerceField ?? '$.field'} → ${config.coerceType ?? 'string'}`
    case 'flatten':   return `${config.flattenField ?? '$.field'} · ${config.flattenMode ?? 'object'}`
    case 'enrich':    return config.lookupUrl ? `lookup: ${config.lookupUrl}` : 'HTTP lookup'
    case 'dedupe':    return `key: ${config.dedupeKey ?? '—'} · window: ${config.dedupeWindow ?? '7d'}`
    case 'condition': return `${config.conditionField ?? '$.field'} ${config.conditionOp ?? '=='} ${config.conditionValue ?? '…'}`
    case 'write':
      if (config.createDataset) return `→ dataset: ${config.newDatasetName ?? 'new'}`
      if (config.targetDatasetId) return '→ refresh dataset'
      return `${config.destType ?? 'S3'} · ${config.destFormat ?? 'parquet'}`
    default:          return 'configure step'
  }
}

interface BuilderStepBody {
  id: string
  op: string
  label: string
  config: Record<string, unknown>
}

export async function POST(req: NextRequest) {
  const body = await req.json()
  const { id, name, description, dataset, sourceType, sourceId, resource, outputTarget, status, steps } = body as {
    id?: string
    name: string
    description: string
    dataset: string
    sourceType?: SourceType
    sourceId?: string
    resource?: string
    outputTarget?: {
      mode: 'none' | 'dataset'
      datasetId?: string
      datasetName?: string
      refreshMode?: 'manual' | 'scheduled'
      refreshIntervalMinutes?: number | null
    }
    status: 'active' | 'draft'
    steps: BuilderStepBody[]
  }

  const resolvedSourceType = sourceType ?? 'dataset'
  const resolvedSourceId = sourceId ?? dataset ?? ''

  const uiSteps: UiStep[] = steps.map(s => ({
    id:     s.id,
    op:     s.op,
    label:  s.label,
    icon:   opIcon(s.op),
    status: 'ok',
    config: configSummary(s.op, s.config),
  }))

  const stepDefs: StepDef[] = steps.map(s => ({
    label:      s.label,
    durationMs: OP_DURATIONS[s.op] ?? 500,
    rowsIn:     0,
    rowsOut:    0,
    logLines:   [`Running ${s.label}…`, `${s.label} completed`],
  }))
  const runtimeSteps: RuntimeStep[] = steps.map(s => ({
    id: s.id,
    op: s.op,
    label: s.label,
    config: s.config,
  }))

  if (id) {
    const updated = updatePipeline(id, {
      name: name || 'Untitled Pipeline',
      description,
      dataset: resolvedSourceId,
      sourceType: resolvedSourceType,
      sourceId: resolvedSourceId,
      resource,
      outputTarget: outputTarget ?? null,
      status: status || 'draft',
      uiSteps,
      steps: stepDefs,
      runtimeSteps,
      quarantineStep: uiSteps.some(s => s.op === 'validate') && steps.some(s => s.config.quarantine)
        ? { id: `${id}-q`, op: 'quarantine', label: 'Quarantine', icon: 'quarantine', status: 'ok', config: 'invalid records → /quarantine/' }
        : null,
    })
    if (!updated) return NextResponse.json({ error: 'Not found' }, { status: 404 })
    return NextResponse.json(buildPipelineResponse(updated))
  }

  const pipeline = addPipeline({
    name:        name || 'Untitled Pipeline',
    description: description || '',
    dataset:     resolvedSourceId || '',
    sourceType:  resolvedSourceType,
    sourceId:    resolvedSourceId,
    resource,
    outputTarget: outputTarget ?? null,
    status:      status || 'draft',
    avgDuration: '—',
    steps:       stepDefs,
    uiSteps,
    runtimeSteps,
    quarantineStep: uiSteps.some(s => s.op === 'validate') && steps.some(s => s.config.quarantine)
      ? { id: 'q-new', op: 'quarantine', label: 'Quarantine', icon: 'quarantine', status: 'ok', config: 'invalid records → /quarantine/' }
      : null,
  })

  return NextResponse.json(buildPipelineResponse(pipeline), { status: 201 })
}
