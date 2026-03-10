'use client'

import { useState, useRef, useCallback, useEffect } from 'react'
import { useRouter } from 'next/navigation'
import {
  Plus, GitBranch, X, Play, Clock, AlertTriangle, CheckCircle2,
  Circle, Package, Filter, Layers, Database, AlertCircle, RefreshCw,
  Loader2, ChevronRight, Terminal, Zap, ArrowLeft, Pencil,
} from 'lucide-react'
import StatusBadge from '@/components/StatusBadge'

/* ── Types ───────────────────────────────────────────────────────────────────── */
type StepStatus = 'idle' | 'running' | 'done' | 'error' | 'skip'
type RunStatus  = 'idle' | 'running' | 'succeeded' | 'failed'

interface RunState {
  status:      RunStatus
  stepStates:  StepStatus[]
  currentStep: number
  log:         { stepIndex: number; message: string; ts: number }[]
  durationMs:  number
  stepMetrics: StepMetric[]
  latestResult: LatestRunResult | null
}

interface UiStep {
  id:     string
  op:     string
  label:  string
  icon:   string
  status: 'ok' | 'error' | 'skip'
  config: string
}

interface RecentRun { status: 'succeeded' | 'failed'; durationMs?: number }

interface StepMetric {
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

interface RunSummary {
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

interface LatestRunResult {
  pipelineId: string
  status: 'succeeded' | 'failed'
  startedAt: number
  durationMs: number
  columns: string[]
  rows: string[][]
  rowCount: number
  summary: RunSummary
  stepMetrics: StepMetric[]
  error?: string | null
}

interface ClientPipeline {
  id:             string
  name:           string
  description:    string
  notes?:         string
  status:         'active' | 'draft'
  lastRun:        string
  lastRunStatus:  'succeeded' | 'failed' | 'draft'
  avgDuration:    string
  runsToday:      number
  sourceType:     'dataset' | 'connector'
  dataset:        string
  recentRuns:     RecentRun[]
  steps:          UiStep[]
  quarantineStep: UiStep | null
  latestRunResult?: LatestRunResult | null
}

/* ── Utilities ───────────────────────────────────────────────────────────────── */
function fmtMs(ms: number) {
  if (ms >= 60_000) return `${(ms / 60_000).toFixed(1)}m`
  if (ms >= 1_000)  return `${(ms / 1_000).toFixed(1)}s`
  return `${ms}ms`
}

function relTime(epochMs: number): string {
  const diff = Date.now() - epochMs
  if (diff < 60_000)     return `${Math.round(diff / 1_000)}s ago`
  if (diff < 3_600_000)  return `${Math.round(diff / 60_000)}m ago`
  if (diff < 86_400_000) return `${Math.round(diff / 3_600_000)}h ago`
  return `${Math.round(diff / 86_400_000)}d ago`
}

function fmtPct(value: number) {
  return `${(value * 100).toFixed(value < 0.1 ? 1 : 0)}%`
}

function stepPurpose(op: string): string {
  switch (op) {
    case 'extract':
    case 'fetch':
      return 'Loads the source rows into the pipeline runtime.'
    case 'validate':
      return 'Checks the incoming rows against required fields, types, or schema rules.'
    case 'query':
      return 'Filters or reshapes the stream using a query or expression layer.'
    case 'map':
      return 'Renames or remaps fields into the output shape.'
    case 'coerce':
      return 'Normalizes field values into explicit types before later comparisons or writes.'
    case 'flatten':
      return 'Expands nested objects or arrays into a flatter row shape.'
    case 'enrich':
      return 'Adds lookup-derived fields from another source or service.'
    case 'dedupe':
      return 'Removes repeated rows using a business key or window.'
    case 'condition':
      return 'Applies a branch or keep/drop rule based on a field comparison.'
    case 'write':
      return 'Prepares the final shaped output for preview or persistence.'
    case 'quarantine':
      return 'Captures invalid or rejected rows outside the main success path.'
    default:
      return 'Executes a pipeline transformation step.'
  }
}

function initRunState(stepCount: number): RunState {
  return { status: 'idle', stepStates: Array(stepCount).fill('idle'), currentStep: -1, log: [], durationMs: 0, stepMetrics: [], latestResult: null }
}

/* ── Step icon ───────────────────────────────────────────────────────────────── */
function StepIcon({ op, size = 16 }: { op: string; size?: number }) {
  const cls = `w-${size === 12 ? 3 : 4} h-${size === 12 ? 3 : 4}`
  if (op === 'fetch' || op === 'extract') return <Package   className={cls} />
  if (op === 'validate')                  return <Filter    className={cls} />
  if (['coerce','enrich','map','flatten'].includes(op)) return <Layers className={cls} />
  if (op === 'dedupe')                    return <RefreshCw className={cls} />
  if (op === 'write')                     return <Database  className={cls} />
  if (op === 'quarantine')                return <AlertCircle className={cls} />
  return <Circle className={cls} />
}

/* ── DAG node style ──────────────────────────────────────────────────────────── */
function nodeStyle(base: string, run: StepStatus) {
  if (run === 'running') return 'border-indigo-400 bg-indigo-500/15 text-indigo-200 shadow-[0_0_12px_rgba(99,102,241,0.35)]'
  if (run === 'done')    return 'border-emerald-500/60 bg-emerald-500/8 text-emerald-200'
  if (run === 'error')   return 'border-rose-500/70 bg-rose-500/12 text-rose-200 shadow-[0_0_10px_rgba(244,63,94,0.25)]'
  if (run === 'skip')    return 'border-chef-border/40 bg-chef-bg/50 text-chef-muted opacity-50'
  if (base === 'error')  return 'border-rose-500/50 bg-rose-500/8 text-rose-300'
  if (base === 'skip')   return 'border-chef-border bg-chef-bg text-chef-muted opacity-50'
  if (base === 'warn')   return 'border-amber-500/50 bg-amber-500/5 text-amber-300'
  return 'border-indigo-500/30 bg-indigo-500/5 text-indigo-300'
}

/* ── DAG ─────────────────────────────────────────────────────────────────────── */
function PipelineDAG({ pipeline, runState, selectedStepId, onSelectStep }: {
  pipeline: ClientPipeline
  runState: RunState
  selectedStepId: string | null
  onSelectStep: (stepId: string) => void
}) {
  const { steps, quarantineStep } = pipeline
  const NODE_W = 152, NODE_H = 70, H_GAP = 56, V_GAP = 56
  const COLS = 4
  const rows = Math.ceil(steps.length / COLS)
  
  const mainYOffset = quarantineStep ? NODE_H + V_GAP : 0
  const svgW = COLS * NODE_W + (COLS - 1) * H_GAP
  const mainSvgH = rows * NODE_H + (rows - 1) * V_GAP
  const svgH = mainSvgH + mainYOffset

  const getPos = (i: number) => {
    const r = Math.floor(i / COLS)
    const c = r % 2 === 0 ? (i % COLS) : (COLS - 1 - (i % COLS))
    return { x: 20 + c * (NODE_W + H_GAP), y: 20 + mainYOffset + r * (NODE_H + V_GAP) }
  }

  return (
    <div className="relative overflow-x-auto pb-2">
      <div className="relative" style={{ width: svgW + 60, height: svgH + 40 }}>
        <svg className="absolute inset-0 pointer-events-none" width={svgW + 60} height={svgH + 40}>
          <defs>
            {['ok','err','warn','dim'].map(k => (
              <marker key={k} id={`arr-${k}`} markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
                <polygon points="0 0, 8 3, 0 6" fill={k==='ok'?'#6366f1':k==='err'?'#f43f5e':k==='warn'?'#f59e0b':'#334155'} />
              </marker>
            ))}
          </defs>

          {steps.map((step, i) => {
            if (i === steps.length - 1) return null
            const p1 = getPos(i)
            const p2 = getPos(i + 1)
            const r1 = Math.floor(i / COLS)
            const r2 = Math.floor((i + 1) / COLS)

            const rs = runState.stepStates[i]
            const color  = rs === 'done' ? '#22c55e' : rs === 'error' ? '#f43f5e' : rs === 'skip' ? '#334155' : step.status === 'error' ? '#f43f5e' : '#6366f1'
            const marker = rs === 'error' || step.status === 'error' ? 'url(#arr-err)' : rs === 'skip' ? 'url(#arr-dim)' : 'url(#arr-ok)'
            const dash   = rs === 'skip' || step.status === 'skip' ? '5 3' : undefined

            if (r1 === r2) {
              const goingRight = r1 % 2 === 0
              const x1 = goingRight ? p1.x + NODE_W : p1.x
              const x2 = goingRight ? p2.x - 4 : p2.x + NODE_W + 4
              const y  = p1.y + NODE_H / 2
              return <line key={i} x1={x1} y1={y} x2={x2} y2={y} stroke={color} strokeWidth="1.5" markerEnd={marker} strokeDasharray={dash} />
            } else {
              const x  = p1.x + NODE_W / 2
              const y1 = p1.y + NODE_H
              const y2 = p2.y - 4
              return <line key={i} x1={x} y1={y1} x2={x} y2={y2} stroke={color} strokeWidth="1.5" markerEnd={marker} strokeDasharray={dash} />
            }
          })}

          {quarantineStep && steps.length > 1 && (() => {
            const pSrc = getPos(1)
            const pDst = { x: pSrc.x, y: 20 }
            return (
              <path d={`M ${pSrc.x + NODE_W / 2} ${pSrc.y - 4} L ${pDst.x + NODE_W / 2} ${pDst.y + NODE_H + 4}`}
                stroke="#f59e0b" strokeWidth="1.5" strokeDasharray="5 3" fill="none" markerEnd="url(#arr-warn)" />
            )
          })()}
        </svg>

        {steps.map((step, i) => {
          const rs = runState.stepStates[i] ?? 'idle'
          const p = getPos(i)
          return (
            <div
              key={step.id}
              className={`absolute border rounded-xl flex flex-col gap-1.5 px-3 py-2.5 transition-all duration-300 cursor-pointer ${
                selectedStepId === step.id ? 'ring-2 ring-indigo-500/70 ring-offset-1 ring-offset-chef-surface z-10' : ''
              } ${nodeStyle(step.status, rs)}`}
              style={{ left: p.x, top: p.y, width: NODE_W, height: NODE_H }}
              onClick={() => onSelectStep(step.id)}
            >
              <div className="flex items-center gap-2">
                {rs === 'running' ? <Loader2 size={14} className="animate-spin text-indigo-300 shrink-0" /> : <StepIcon op={step.icon} />}
                <span className="text-[11px] font-semibold leading-tight truncate">{step.label}</span>
                {rs === 'done'  && <CheckCircle2  size={11} className="text-emerald-400 ml-auto shrink-0" />}
                {rs === 'error' && <AlertTriangle size={11} className="text-rose-400 ml-auto shrink-0" />}
                {rs === 'idle' && step.status === 'error' && <AlertTriangle size={11} className="text-rose-400 ml-auto shrink-0" />}
              </div>
              <div className="text-[9px] text-chef-muted leading-tight truncate">{step.config}</div>
            </div>
          )
        })}

        {quarantineStep && steps.length > 1 && (() => {
          const pSrc = getPos(1)
          const pDst = { x: pSrc.x, y: 20 }
          return (
            <div className={`absolute border rounded-xl flex flex-col gap-1.5 px-3 py-2.5 border-amber-500/40 bg-amber-500/5 text-amber-300 cursor-pointer ${
              selectedStepId === quarantineStep.id ? 'ring-2 ring-indigo-500/70 ring-offset-1 ring-offset-chef-surface z-10' : ''
            }`}
              style={{ left: pDst.x, top: pDst.y, width: NODE_W, height: NODE_H }}
              onClick={() => onSelectStep(quarantineStep.id)}
              role="button"
              tabIndex={0}>
              <div className="flex items-center gap-2">
                <StepIcon op="quarantine" />
                <span className="text-[11px] font-semibold truncate">{quarantineStep.label}</span>
                <AlertTriangle size={11} className="text-amber-400 ml-auto shrink-0" />
              </div>
              <div className="text-[9px] text-chef-muted leading-tight truncate">{quarantineStep.config}</div>
            </div>
          )
        })()}
      </div>
    </div>
  )
}

/* ── Sparkbar ────────────────────────────────────────────────────────────────── */
function Sparkbar({ runs }: { runs: RecentRun[] }) {
  if (runs.length === 0) {
    return <span className="text-[11px] text-chef-muted italic">No runs yet</span>
  }
  return (
    <>
      {runs.map((r, i) => (
        <div
          key={i}
          className={`h-4 w-2 rounded-sm ${r.status === 'succeeded' ? 'bg-emerald-500' : 'bg-rose-500'} ${
            i === runs.length - 1 ? 'opacity-100' : 'opacity-35'
          }`}
          title={`Run ${i + 1}: ${r.status}`}
        />
      ))}
    </>
  )
}

/* ── Main page ───────────────────────────────────────────────────────────────── */
export default function PipelinesPage() {
  const router = useRouter()

  const [pipelines, setPipelines] = useState<ClientPipeline[]>([])
  const [loading, setLoading]     = useState(true)
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [selectedDagStepId, setSelectedDagStepId] = useState<string | null>(null)
  const [runState, setRunState]     = useState<RunState>(initRunState(0))
  const [showLog, setShowLog]       = useState(false)
  const [isHelpOpen, setIsHelpOpen] = useState(true)
  const [showLegend, setShowLegend] = useState(false)
  const logEndRef                   = useRef<HTMLDivElement>(null)
  const abortRef                    = useRef<AbortController | null>(null)

  // Derive selected pipeline from state (always reflects latest data)
  const selected = pipelines.find(p => p.id === selectedId) ?? null

  // Load all pipeline data from server (definitions + real run history)
  useEffect(() => {
    fetch('/api/pipelines')
      .then(r => r.json())
      .then((data: Array<{
        id: string
        name: string
        description: string
        notes?: string
        status: 'active' | 'draft'
        avgDuration: string
        dataset: string
        sourceType?: 'dataset' | 'connector'
        uiSteps: UiStep[]
        quarantineStep: UiStep | null
        lastRunAt: number | null
        lastRunStatus: string | null
        runsToday: number
        recentRuns: { status: string }[]
        latestRunResult?: LatestRunResult | null
      }>) => {
        setPipelines(data.map(d => ({
          id:             d.id,
          name:           d.name,
          description:    d.description,
          notes:          d.notes ?? '',
          status:         d.status,
          avgDuration:    d.avgDuration,
          sourceType:     d.sourceType ?? 'dataset',
          dataset:        d.dataset,
          steps:          d.uiSteps,
          quarantineStep: d.quarantineStep,
          lastRun:        d.lastRunAt ? relTime(d.lastRunAt) : 'Never',
          lastRunStatus:  (d.lastRunStatus ?? 'draft') as ClientPipeline['lastRunStatus'],
          runsToday:      d.runsToday,
          recentRuns:     d.recentRuns as RecentRun[],
          latestRunResult: d.latestRunResult ?? null,
        })))
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [])

  function selectPipeline(id: string) {
    if (selectedId === id) { setSelectedId(null); return }
    const p = pipelines.find(q => q.id === id)
    if (!p) return
    setSelectedId(id)
    setSelectedDagStepId(p.steps[0]?.id ?? p.quarantineStep?.id ?? null)
    setRunState({
      ...initRunState(p.steps.length),
      latestResult: p.latestRunResult ?? null,
      stepMetrics: p.latestRunResult?.stepMetrics ?? [],
      durationMs: p.latestRunResult?.durationMs ?? 0,
      status: p.latestRunResult?.status ?? 'idle',
    })
    setShowLog(false)
    abortRef.current?.abort()
  }

  const runPipeline = useCallback(async () => {
    if (!selectedId) return
    const currentPipeline = pipelines.find(p => p.id === selectedId)
    if (!currentPipeline || runState.status === 'running') return

    abortRef.current?.abort()
    const ctrl = new AbortController()
    abortRef.current = ctrl
    const capturedId = selectedId

    setRunState({
      status: 'running',
      stepStates: Array(currentPipeline.steps.length).fill('idle'),
      currentStep: -1, log: [], durationMs: 0, stepMetrics: [], latestResult: null,
    })
    setShowLog(true)

    try {
      const res    = await fetch(`/api/pipelines/${capturedId}/run`, { method: 'POST', signal: ctrl.signal })
      const reader = res.body!.getReader()
      const dec    = new TextDecoder()
      let buf      = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buf += dec.decode(value, { stream: true })

        const lines = buf.split('\n')
        buf = lines.pop() ?? ''

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          let event: Record<string, unknown>
          try { event = JSON.parse(line.slice(6)) } catch { continue }

          const type = event.type as string

          if (type === 'step_start') {
            const idx = event.stepIndex as number
            setRunState(prev => {
              const next = [...prev.stepStates]
              for (let i = 0; i < idx; i++) if (next[i] === 'idle') next[i] = 'done'
              next[idx] = 'running'
              return { ...prev, currentStep: idx, stepStates: next }
            })
          }

          if (type === 'log') {
            setRunState(prev => ({
              ...prev,
              log: [...prev.log, { stepIndex: event.stepIndex as number, message: event.message as string, ts: Date.now() }],
            }))
            setTimeout(() => logEndRef.current?.scrollIntoView({ behavior: 'smooth' }), 50)
          }

          if (type === 'step_done') {
            const idx = event.stepIndex as number
            setRunState(prev => {
              const next = [...prev.stepStates]; next[idx] = 'done'
              const metric: StepMetric = {
                stepIndex: idx,
                stepId: String(event.stepId ?? `step_${idx}`),
                label: String(event.label ?? `Step ${idx + 1}`),
                op: String(event.op ?? 'runtime'),
                rowsIn: Number(event.rowsIn ?? 0),
                rowsOut: Number(event.rowsOut ?? 0),
                removed: Number(event.removed ?? 0),
                failed: false,
                failedRows: 0,
                durationMs: Number(event.durationMs ?? 0),
                throughputPerSec: Number(event.throughputPerSec ?? 0),
              }
              const metrics = [...prev.stepMetrics]
              metrics[idx] = metric
              return { ...prev, stepStates: next, stepMetrics: metrics }
            })
          }

          if (type === 'step_error') {
            const idx = event.stepIndex as number
            setRunState(prev => {
              const next = [...prev.stepStates]
              next[idx] = 'error'
              for (let i = idx + 1; i < next.length; i++) next[i] = 'skip'
              return {
                ...prev, stepStates: next,
                stepMetrics: [
                  ...prev.stepMetrics.filter(metric => metric.stepIndex !== idx),
                  {
                    stepIndex: idx,
                    stepId: `step_${idx}`,
                    label: String(event.label ?? `Step ${idx + 1}`),
                    op: 'runtime',
                    rowsIn: 0,
                    rowsOut: 0,
                    removed: 0,
                    failed: true,
                    failedRows: 0,
                    durationMs: 0,
                    throughputPerSec: 0,
                  },
                ].sort((a, b) => a.stepIndex - b.stepIndex),
                log: [...prev.log, { stepIndex: idx, message: `❌ ${event.message as string}`, ts: Date.now() }],
              }
            })
            setTimeout(() => logEndRef.current?.scrollIntoView({ behavior: 'smooth' }), 50)
          }

          if (type === 'result') {
            const latestResult: LatestRunResult = {
              pipelineId: capturedId,
              status: 'succeeded',
              startedAt: Date.now(),
              durationMs: Number((event.summary as RunSummary | undefined)?.durationMs ?? 0),
              columns: (event.columns as string[]) ?? [],
              rows: (event.rows as string[][]) ?? [],
              rowCount: Number(event.rowCount ?? 0),
              summary: event.summary as RunSummary,
              stepMetrics: (event.stepMetrics as StepMetric[]) ?? [],
              error: null,
            }
            setRunState(prev => ({
              ...prev,
              latestResult,
              stepMetrics: latestResult.stepMetrics,
            }))
            setPipelines(prev => prev.map(p => p.id === capturedId ? { ...p, latestRunResult: latestResult } : p))
          }

          if (type === 'done') {
            const status     = event.status as 'succeeded' | 'failed'
            const durationMs = event.durationMs as number
            setRunState(prev => ({
              ...prev, status, durationMs, currentStep: -1,
              stepStates: prev.stepStates.map(s => s === 'running' ? 'done' : s === 'idle' ? 'skip' : s),
              log: [...prev.log, {
                stepIndex: -1,
                message: status === 'succeeded'
                  ? `✅ Pipeline completed in ${fmtMs(durationMs)}`
                  : `❌ Pipeline failed after ${fmtMs(durationMs)}`,
                ts: Date.now(),
              }],
            }))
            // Update pipeline stats with real run result
            setPipelines(prev => prev.map(p => {
              if (p.id !== capturedId) return p
              const newRuns: RecentRun[] = [...p.recentRuns.slice(-19), { status }]
              return {
                ...p,
                lastRun:       'just now',
                lastRunStatus: status,
                runsToday:     p.runsToday + 1,
                recentRuns:    newRuns,
              }
            }))
            setTimeout(() => logEndRef.current?.scrollIntoView({ behavior: 'smooth' }), 50)
          }
        }
      }
    } catch (e: unknown) {
      if ((e as Error).name === 'AbortError') return
      setRunState(prev => ({
        ...prev, status: 'failed',
        log: [...prev.log, { stepIndex: -1, message: `❌ Fetch error: ${String(e)}`, ts: Date.now() }],
      }))
    }
  }, [selectedId, runState.status, pipelines])

  const latestResult = runState.latestResult ?? selected?.latestRunResult ?? null
  const stepMetrics = runState.stepMetrics.length ? runState.stepMetrics : (latestResult?.stepMetrics ?? [])
  const maxStepDuration = stepMetrics.reduce((max, metric) => Math.max(max, metric.durationMs), 1)
  const maxStepVolume = stepMetrics.reduce((max, metric) => Math.max(max, metric.rowsIn, metric.rowsOut), 1)
  const selectedDagStep = selected
    ? [...selected.steps, ...(selected.quarantineStep ? [selected.quarantineStep] : [])].find(step => step.id === selectedDagStepId) ?? selected.steps[0] ?? selected.quarantineStep ?? null
    : null

  return (
    <div className="flex h-full">

      {/* ── Pipeline list — hidden on narrow viewports when detail is open ── */}
      <div className={`flex flex-col transition-all duration-200 border-r border-chef-border ${selected ? 'w-64 shrink-0 hidden lg:flex' : 'flex-1'}`}>
        <div className="px-5 py-4 border-b border-chef-border flex items-center gap-3">
          <GitBranch size={16} className="text-indigo-400 shrink-0" />
          <h2 className="text-sm font-semibold text-chef-text flex-1">All Pipelines</h2>
          <button
            onClick={() => router.push('/pipelines/builder')}
            className="flex items-center gap-1.5 bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-medium px-3 py-1.5 rounded-lg transition-colors"
          >
            <Plus size={13} /> New Pipeline
          </button>
        </div>

        <div className="flex-1 overflow-auto py-2 px-3">
          {loading && Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="w-full rounded-xl border border-chef-border bg-chef-card mb-2 p-4 animate-pulse">
              <div className="flex items-center justify-between gap-2 mb-2">
                <div className="h-3.5 bg-chef-border/50 rounded w-40" />
                <div className="h-4 bg-chef-border/50 rounded w-14" />
              </div>
              <div className="h-2.5 bg-chef-border/30 rounded w-3/4 mb-3" />
              <div className="flex gap-3">
                <div className="h-2 bg-chef-border/30 rounded w-16" />
                <div className="h-2 bg-chef-border/30 rounded w-14" />
              </div>
            </div>
          ))}
          {!loading && pipelines.map(p => (
            <div
              key={p.id}
              role="button"
              tabIndex={0}
              onClick={() => selectPipeline(p.id)}
              onKeyDown={e => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault()
                  selectPipeline(p.id)
                }
              }}
              className={`w-full text-left rounded-xl border mb-2 p-4 transition-all cursor-pointer focus:outline-none focus:ring-2 focus:ring-indigo-500/40 ${
                selectedId === p.id
                  ? 'border-indigo-500/40 bg-indigo-500/5'
                  : 'border-chef-border bg-chef-card hover:border-indigo-500/20 hover:bg-chef-card-hover'
              }`}
            >
              <div className="flex items-center justify-between gap-2">
                <div className="font-mono text-sm font-semibold text-chef-text truncate">{p.name}</div>
                <div className="flex items-center gap-1.5 shrink-0">
                  <StatusBadge status={p.status} />
                  <div
                    role="button"
                    tabIndex={0}
                    onKeyDown={e => {
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault()
                        e.stopPropagation()
                        router.push(`/pipelines/builder?id=${p.id}`)
                      }
                    }}
                    onClick={e => { e.stopPropagation(); router.push(`/pipelines/builder?id=${p.id}`) }}
                    className="p-1 text-chef-muted hover:text-indigo-400 hover:bg-indigo-500/10 rounded transition-colors"
                    title="Edit pipeline"
                  >
                    <Pencil size={11} />
                  </div>
                </div>
              </div>
              <div className="text-[11px] text-chef-muted mt-1.5 leading-tight">{p.description}</div>
              <div className="flex items-center gap-3 mt-3 text-[10px] text-chef-muted">
                <span className="flex items-center gap-1"><Clock size={10} /> {p.lastRun}</span>
                <StatusBadge status={p.lastRunStatus} />
                <span className="ml-auto">{p.steps.length} steps</span>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* ── Detail panel ── */}
      {selected && (
        <div className="flex-1 flex flex-col min-w-0 overflow-hidden animate-slide-in">

          {/* Header */}
          <div className="px-6 py-4 border-b border-chef-border flex items-start gap-4 shrink-0">
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-3 flex-wrap">
                {/* Back button — only visible when list is hidden (narrow screens) */}
                <button
                  onClick={() => setSelectedId(null)}
                  className="lg:hidden flex items-center gap-1 text-xs text-chef-muted hover:text-chef-text transition-colors"
                >
                  <ArrowLeft size={14} /> Back
                </button>
                <span className="font-mono text-base font-bold text-chef-text">{selected.name}</span>
                <StatusBadge status={selected.status} />
                {runState.status === 'running' && (
                  <span className="flex items-center gap-1.5 text-[11px] text-indigo-300 font-mono bg-indigo-500/10 px-2 py-0.5 rounded-full border border-indigo-500/30 animate-pulse">
                    <Loader2 size={10} className="animate-spin" /> running
                  </span>
                )}
                {runState.status === 'succeeded' && (
                  <span className="flex items-center gap-1.5 text-[11px] text-emerald-300 font-mono bg-emerald-500/10 px-2 py-0.5 rounded-full border border-emerald-500/30">
                    <CheckCircle2 size={10} /> done · {fmtMs(runState.durationMs)}
                  </span>
                )}
                {runState.status === 'failed' && (
                  <span className="flex items-center gap-1.5 text-[11px] text-rose-300 font-mono bg-rose-500/10 px-2 py-0.5 rounded-full border border-rose-500/30">
                    <AlertTriangle size={10} /> failed · {fmtMs(runState.durationMs)}
                  </span>
                )}
              </div>
              <div className="text-sm text-chef-muted mt-1">{selected.description}</div>
              <div className="flex flex-wrap items-center gap-4 mt-2 text-[11px] text-chef-muted">
                <span className="flex items-center gap-1.5"><Database size={11} />{selected.sourceType === 'connector' ? `Connector: ${selected.dataset}` : selected.dataset}</span>
                <span className="flex items-center gap-1.5"><Clock size={11} />Last run {selected.lastRun}</span>
                <span className="flex items-center gap-1.5"><GitBranch size={11} />{selected.runsToday} runs today</span>
                <span>avg {selected.avgDuration}</span>
              </div>
            </div>

            <div className="flex items-center gap-2 shrink-0">
              <button
                onClick={() => setIsHelpOpen(!isHelpOpen)}
                className={`p-1.5 rounded-lg transition-colors border ${
                  isHelpOpen
                    ? 'border-indigo-500/40 bg-indigo-500/10 text-indigo-400'
                    : 'border-chef-border bg-chef-card text-chef-muted hover:text-chef-text'
                }`}
                title="Toggle details panel"
              >
                <ChevronRight size={14} className={`transition-transform ${isHelpOpen ? 'rotate-180' : ''}`} />
              </button>
              <button
                onClick={runPipeline}
                disabled={runState.status === 'running'}
                className={`flex items-center gap-1.5 text-xs font-semibold px-3 py-1.5 rounded-lg border transition-colors ${
                  runState.status === 'running'
                    ? 'border-indigo-500/40 bg-indigo-500/10 text-indigo-400 cursor-not-allowed'
                    : 'border-chef-border bg-chef-card hover:border-indigo-500/50 hover:bg-indigo-500/5 text-chef-text hover:text-indigo-300'
                }`}
              >
                {runState.status === 'running'
                  ? <><Loader2 size={12} className="animate-spin" /> Running…</>
                  : <><Play size={12} fill="currentColor" /> Run now</>}
              </button>
              <button
                title="Close"
                onClick={() => { setSelectedId(null); abortRef.current?.abort() }}
                className="p-1.5 text-chef-muted hover:text-chef-text hover:bg-chef-card rounded-lg transition-colors border border-transparent"
              >
                <X size={14} />
              </button>
            </div>
          </div>

          {/* Sparkbar — real run history */}
          <div className="px-6 py-2.5 border-b border-chef-border flex items-center gap-2 shrink-0">
            <span className="text-[9px] text-chef-muted uppercase tracking-widest font-semibold mr-1">Recent runs</span>
            <Sparkbar runs={selected.recentRuns} />
            {selected.recentRuns.length > 0 && (
              <span className="ml-auto text-[10px] text-chef-muted font-mono">{selected.recentRuns.length} runs</span>
            )}
          </div>

          {/* Body Split */}
          <div className="flex-1 flex min-h-0">
            <div className="flex-1 overflow-auto">

            {/* DAG */}
            <div className="px-6 pt-5 pb-4">
              <div className="flex items-center justify-between mb-4">
                <div>
                  <div className="text-sm font-semibold text-chef-text">Pipeline DAG</div>
                  <div className="text-[11px] text-chef-muted mt-0.5">
                    {selected.steps.length} steps · {selected.quarantineStep ? 'quarantine branch enabled' : 'linear pipeline'}
                  </div>
                </div>
                <div className="flex items-center gap-4 text-[10px] text-chef-muted">
                  <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 bg-indigo-500 rounded inline-block" />Normal flow</span>
                  {selected.quarantineStep && <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 bg-amber-500 rounded inline-block" />Quarantine</span>}
                </div>
              </div>
              <div className="bg-chef-bg border border-chef-border rounded-xl p-6 overflow-x-auto">
                <PipelineDAG
                  pipeline={selected}
                  runState={runState}
                  selectedStepId={selectedDagStepId}
                  onSelectStep={setSelectedDagStepId}
                />
              </div>
              <div className="mt-3 text-[11px] text-chef-muted">
                Click a node to inspect what it does and how it is configured.
              </div>
            </div>

            {latestResult && (
              <div className="px-6 pb-4">
                <div className="text-sm font-semibold text-chef-text mb-3">Latest Run Snapshot</div>
                <div className="grid grid-cols-2 lg:grid-cols-5 gap-3">
                  <div className="rounded-xl border border-chef-border bg-chef-card p-3">
                    <div className="text-[10px] uppercase tracking-widest text-chef-muted">Rows</div>
                    <div className="text-lg font-mono text-chef-text mt-1">{latestResult.summary.rowsOut}</div>
                    <div className="text-[11px] text-chef-muted mt-1">from {latestResult.summary.rowsIn} input</div>
                  </div>
                  <div className="rounded-xl border border-chef-border bg-chef-card p-3">
                    <div className="text-[10px] uppercase tracking-widest text-chef-muted">Throughput</div>
                    <div className="text-lg font-mono text-emerald-400 mt-1">{latestResult.summary.throughputPerSec}/s</div>
                    <div className="text-[11px] text-chef-muted mt-1">{fmtMs(latestResult.summary.durationMs)}</div>
                  </div>
                  <div className="rounded-xl border border-chef-border bg-chef-card p-3">
                    <div className="text-[10px] uppercase tracking-widest text-chef-muted">Removed</div>
                    <div className="text-lg font-mono text-amber-300 mt-1">{latestResult.summary.removed}</div>
                    <div className="text-[11px] text-chef-muted mt-1">failed rows {latestResult.summary.failedRows}</div>
                  </div>
                  <div className="rounded-xl border border-chef-border bg-chef-card p-3">
                    <div className="text-[10px] uppercase tracking-widest text-chef-muted">Error Rate</div>
                    <div className="text-lg font-mono text-rose-300 mt-1">{fmtPct(latestResult.summary.errorRate)}</div>
                    <div className="text-[11px] text-chef-muted mt-1">{latestResult.summary.failedSteps} failed steps</div>
                  </div>
                  <div className="rounded-xl border border-chef-border bg-chef-card p-3">
                    <div className="text-[10px] uppercase tracking-widest text-chef-muted">Health</div>
                    <div className="text-lg font-mono text-sky-300 mt-1">{latestResult.summary.healthScore}/100</div>
                    <div className="mt-2 h-1.5 rounded-full bg-chef-bg overflow-hidden">
                      <div className="h-full bg-sky-400" style={{ width: `${latestResult.summary.healthScore}%` }} />
                    </div>
                  </div>
                </div>
              </div>
            )}

            {stepMetrics.length > 0 && (
              <div className="px-6 pb-4">
                <div className="flex items-center justify-between mb-3">
                  <div className="text-sm font-semibold text-chef-text">Step Runtime Graph</div>
                  <button 
                    onClick={() => setShowLegend(!showLegend)}
                    className="text-[10px] text-chef-muted hover:text-chef-text transition-colors"
                  >
                    {showLegend ? 'Hide Legend' : 'Show Legend'}
                  </button>
                </div>
                
                <div className="rounded-xl border border-chef-border bg-chef-card p-5">
                  <div className="h-32 flex items-end gap-2 shrink-0">
                    {stepMetrics.map((metric, i) => {
                      const heightPct = Math.max(5, (metric.durationMs / maxStepDuration) * 100)
                      const isSelected = selectedDagStepId === metric.stepId || (i === 0 && !selectedDagStepId)
                      const color = metric.failed 
                        ? 'bg-rose-500 hover:bg-rose-400' 
                        : isSelected ? 'bg-indigo-500' : 'bg-emerald-500 hover:bg-emerald-400'
                      
                      return (
                        <div 
                          key={metric.stepIndex} 
                          className="flex-1 flex flex-col justify-end group cursor-pointer h-full"
                          onClick={() => setSelectedDagStepId(metric.stepId)}
                          title={`${metric.label}\nDuration: ${fmtMs(metric.durationMs)}\nRows: ${metric.rowsIn} in -> ${metric.rowsOut} out`}
                        >
                          <div 
                            className={`w-full rounded-t-sm transition-all duration-300 ${color} ${isSelected ? 'ring-2 ring-indigo-400 ring-offset-1 ring-offset-chef-card opacity-100' : 'opacity-80'}`} 
                            style={{ height: `${heightPct}%` }}
                          />
                        </div>
                      )
                    })}
                  </div>
                  
                  <div className="flex gap-2 mt-2">
                    {stepMetrics.map((metric) => (
                      <div key={`lbl-${metric.stepIndex}`} className="flex-1 text-center text-[9px] font-mono text-chef-muted truncate mt-1">
                        {metric.stepIndex + 1}
                      </div>
                    ))}
                  </div>

                  {showLegend && (
                    <div className="mt-4 pt-3 border-t border-chef-border flex gap-4 text-[10px] text-chef-muted justify-center animate-slide-in">
                      <div className="flex items-center gap-1.5"><span className="w-3 h-3 rounded-sm bg-emerald-500 opacity-80" /> Success</div>
                      <div className="flex items-center gap-1.5"><span className="w-3 h-3 rounded-sm bg-rose-500 opacity-80" /> Failed</div>
                      <div className="flex items-center gap-1.5"><span className="w-3 h-3 rounded-sm bg-indigo-500" /> Selected</div>
                      <div className="flex items-center gap-1.5 ml-4 italic">Bar height indicates relative duration</div>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Step configuration */}
            <div className="px-6 pb-4">
              <div className="text-sm font-semibold text-chef-text mb-3">Step Configuration</div>
              <div className="space-y-2">
                {selected.steps.map((step, i) => {
                  const rs = runState.stepStates[i] ?? 'idle'
                  return (
                    <div key={step.id} className={`flex items-center gap-4 p-3.5 rounded-xl border transition-all ${
                      rs === 'running' ? 'border-indigo-500/50 bg-indigo-500/5' :
                      rs === 'done'    ? 'border-emerald-500/30 bg-emerald-500/5' :
                      rs === 'error'   ? 'border-rose-500/30 bg-rose-500/5' :
                      rs === 'skip'    ? 'border-chef-border bg-chef-bg opacity-50' :
                      step.status === 'error' ? 'border-rose-500/20 bg-rose-500/5' :
                      'border-chef-border bg-chef-card'
                    }`}>
                      <div className="w-7 h-7 rounded-lg bg-chef-bg border border-chef-border flex items-center justify-center text-[11px] font-mono text-chef-muted shrink-0">
                        {i + 1}
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="text-xs font-semibold text-chef-text">{step.label}</span>
                          <span className="text-[10px] text-chef-muted font-mono">{step.op}</span>
                          {rs === 'running' && <span className="text-[9px] text-indigo-400 font-mono animate-pulse">running…</span>}
                        </div>
                        <div className="text-[11px] text-chef-muted mt-0.5 truncate font-mono">{step.config}</div>
                        {stepMetrics[i] && (
                          <div className="mt-1.5 flex flex-wrap gap-3 text-[10px] font-mono text-chef-muted">
                            <span>{stepMetrics[i].rowsIn} in</span>
                            <span>{stepMetrics[i].rowsOut} out</span>
                            <span>{stepMetrics[i].removed} removed</span>
                            <span>{fmtMs(stepMetrics[i].durationMs)}</span>
                            <span>{stepMetrics[i].throughputPerSec}/s</span>
                          </div>
                        )}
                      </div>
                      {rs === 'running' && <Loader2    size={14} className="text-indigo-400 shrink-0 animate-spin" />}
                      {rs === 'done'    && <CheckCircle2  size={14} className="text-emerald-400 shrink-0" />}
                      {rs === 'error'   && <AlertTriangle size={14} className="text-rose-400 shrink-0" />}
                      {rs === 'skip'    && <Circle        size={14} className="text-chef-muted shrink-0" />}
                      {rs === 'idle' && step.status === 'error' && <AlertTriangle size={14} className="text-rose-400/60 shrink-0" />}
                      {rs === 'idle' && step.status === 'ok'    && <Circle        size={14} className="text-chef-border shrink-0" />}
                    </div>
                  )
                })}
              </div>
            </div>

            {/* Execution log */}
            {(showLog || runState.log.length > 0) && (
              <div className="px-6 pb-6">
                <button
                  onClick={() => setShowLog(v => !v)}
                  className="flex items-center gap-2 text-sm font-semibold text-chef-text mb-3 group"
                >
                  <Terminal size={14} className="text-chef-muted" />
                  Execution Log
                  {runState.log.length > 0 && (
                    <span className="text-[10px] text-chef-muted font-normal font-mono">({runState.log.length} lines)</span>
                  )}
                  <ChevronRight size={12} className={`text-chef-muted transition-transform ${showLog ? 'rotate-90' : ''}`} />
                </button>
                {showLog && (
                  <div className="bg-chef-bg border border-chef-border rounded-xl p-4 font-mono text-[11px] max-h-52 overflow-auto">
                    {runState.log.length === 0 ? (
                      <div className="text-chef-muted">Waiting for run…</div>
                    ) : (
                      runState.log.map((entry, i) => (
                        <div key={i} className={`flex items-start gap-2 mb-1 ${
                          entry.message.startsWith('✅') ? 'text-emerald-400' :
                          entry.message.startsWith('❌') ? 'text-rose-400' :
                          'text-chef-text-dim'
                        }`}>
                          {entry.stepIndex >= 0 && (
                            <span className="text-chef-border shrink-0 w-5 text-right">{entry.stepIndex + 1}</span>
                          )}
                          <span className={entry.stepIndex < 0 ? 'font-semibold' : ''}>{entry.message}</span>
                        </div>
                      ))
                    )}
                    <div ref={logEndRef} />
                  </div>
                )}
              </div>
            )}

            {latestResult && (
              <div className="px-6 pb-6">
                <div className="flex items-center justify-between mb-3">
                  <div className="text-sm font-semibold text-chef-text">Latest Result Preview</div>
                  <div className="text-[11px] text-chef-muted">
                    {latestResult.rowCount} rows {latestResult.error ? `· ${latestResult.error}` : '· stored in memory'}
                  </div>
                </div>
                <div className="rounded-xl border border-chef-border bg-chef-card overflow-auto max-h-[320px]">
                  {latestResult.columns.length === 0 ? (
                    <div className="p-4 text-[11px] text-chef-muted">No result rows captured for the latest run.</div>
                  ) : (
                    <table className="w-full">
                      <thead className="sticky top-0 bg-chef-card z-10">
                        <tr className="border-b border-chef-border">
                          {latestResult.columns.map(column => (
                            <th key={column} className="px-3 py-2 text-left text-[10px] font-semibold uppercase tracking-wider text-chef-muted font-mono whitespace-nowrap">
                              {column}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {latestResult.rows.map((row, rowIndex) => (
                          <tr key={rowIndex} className="border-b border-chef-border/50 last:border-b-0">
                            {row.map((cell, cellIndex) => (
                              <td key={cellIndex} className="px-3 py-2 text-[11px] font-mono text-chef-text whitespace-nowrap">
                                {cell}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              </div>
            )}

            {/* Post-run stats */}
            {(runState.status === 'succeeded' || runState.status === 'failed') && (
              <div className="mx-6 mb-6 p-4 rounded-xl border border-chef-border bg-chef-card">
                <div className="flex items-center gap-4 text-[11px]">
                  {runState.status === 'succeeded'
                    ? <span className="flex items-center gap-1.5 text-emerald-400 font-semibold"><CheckCircle2 size={13} /> Completed successfully</span>
                    : <span className="flex items-center gap-1.5 text-rose-400 font-semibold"><AlertTriangle size={13} /> Pipeline failed</span>}
                  <span className="flex items-center gap-1 text-chef-muted"><Zap size={10} /> {fmtMs(runState.durationMs)}</span>
                  <span className="text-chef-muted">
                    {runState.stepStates.filter(s => s === 'done').length} / {selected.steps.length} steps completed
                  </span>
                  <button onClick={runPipeline} className="ml-auto text-[11px] text-indigo-400 hover:text-indigo-300 transition-colors">
                    Run again →
                  </button>
                </div>
              </div>
            )}
            </div>

            {/* Right Panel */}
            {isHelpOpen && (
              <div className="w-80 shrink-0 border-l border-chef-border bg-chef-bg overflow-auto p-5 animate-slide-in">
                {selected.notes && (
                  <div className="mb-8">
                    <div className="text-sm font-semibold text-chef-text mb-2">Pipeline Notes</div>
                    <div className="text-[11px] text-chef-muted leading-relaxed whitespace-pre-wrap">
                      {selected.notes}
                    </div>
                    {selectedDagStep && <div className="mt-8 border-t border-chef-border" />}
                  </div>
                )}

                {selectedDagStep ? (
                  <>
                    <div className="mb-4">
                      <div className="flex items-center gap-2 mb-2">
                        <StepIcon op={selectedDagStep.icon} size={14} />
                        <div className="text-base font-semibold text-chef-text">{selectedDagStep.label}</div>
                      </div>
                      <div className="text-[10px] font-mono text-chef-muted uppercase tracking-widest">{selectedDagStep.op}</div>
                    </div>

                    <div className="text-[13px] text-chef-text leading-relaxed mb-6">
                      {stepPurpose(selectedDagStep.op)}
                    </div>

                    <div className="mb-6">
                      <div className="text-[10px] uppercase tracking-widest font-semibold text-chef-muted mb-2">Configuration</div>
                      <div className="rounded-lg border border-chef-border bg-chef-card px-3 py-2.5">
                        <div className="text-[11px] font-mono text-chef-text break-words whitespace-pre-wrap">{selectedDagStep.config || 'No runtime summary available'}</div>
                      </div>
                    </div>

                    <div className="text-[11px] text-chef-muted italic">
                      Tip: Use the graph to check performance and data volumes per step.
                    </div>
                  </>
                ) : (
                  <div className="text-[11px] text-chef-muted italic">
                    Select a node in the DAG to view its configuration and runtime details.
                  </div>
                )}
              </div>
            )}

          </div>
        </div>
      )}

      {/* Empty state */}
      {!selected && (
        <div className="flex-1 flex items-center justify-center text-center">
          <div>
            <GitBranch size={36} className="text-chef-muted mx-auto mb-3 opacity-40" />
            <div className="text-sm text-chef-text mb-1">Select a pipeline to view its DAG</div>
            <div className="text-[11px] text-chef-muted">
              Hit <span className="font-mono bg-chef-card px-1.5 py-0.5 rounded border border-chef-border">Run now</span> to execute live with SSE streaming
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
