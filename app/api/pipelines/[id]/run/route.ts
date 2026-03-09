/**
 * POST /api/pipelines/[id]/run
 *
 * Streams Server-Sent Events for each pipeline step.
 * Events:
 *   { type: 'start',      pipelineId, name, totalSteps }
 *   { type: 'step_start', stepIndex, label, rowsIn? }
 *   { type: 'step_done',  stepIndex, label, rowsIn, rowsOut, removed, durationMs, throughputPerSec }
 *   { type: 'step_error', stepIndex, label, message }
 *   { type: 'log',        stepIndex, message }
 *   { type: 'result',     columns, rows, rowCount, summary, stepMetrics }
 *   { type: 'done',       status: 'succeeded'|'failed', stepsCompleted, durationMs }
 */

import { NextRequest } from 'next/server'
import { getPipeline, workerStart, workerEnd, recordRun, setLatestRunResult, type StepRunMetric, type PipelineRunSummary } from '@/lib/pipelines'
import { executePipelineStep, loadPipelineSourceRows, previewCell } from '@/lib/pipeline-runtime'

function sse(data: object) {
  return new TextEncoder().encode(`data: ${JSON.stringify(data)}\n\n`)
}

export async function POST(
  _req: NextRequest,
  { params }: { params: { id: string } }
) {
  const pipeline = getPipeline(params.id)

  if (!pipeline) {
    return new Response(
      `data: ${JSON.stringify({ type: 'done', status: 'failed', error: `Unknown pipeline: ${params.id}` })}\n\n`,
      { status: 404, headers: { 'Content-Type': 'text/event-stream' } }
    )
  }

  workerStart()

  const stream = new ReadableStream({
    async start(controller) {
      const totalStart    = performance.now()
      const startedAt     = Date.now()
      let finalStatus: 'succeeded' | 'failed' = 'succeeded'
      let stepsCompleted  = 0
      let failed          = false
      let failedMessage: string | null = null
      const stepMetrics: StepRunMetric[] = []

      try {
        const runtimeSteps = pipeline.runtimeSteps
        controller.enqueue(sse({
          type: 'start',
          pipelineId: params.id,
          name:       pipeline.name,
          totalSteps: runtimeSteps?.length ?? pipeline.steps.length,
        }))

        if (runtimeSteps?.length) {
          let rows = await loadPipelineSourceRows(
            pipeline.sourceType ?? 'dataset',
            pipeline.sourceId ?? pipeline.dataset,
            pipeline.resource,
            200,
          )
          for (let i = 0; i < runtimeSteps.length && !failed; i++) {
            const step = runtimeSteps[i]
            const stepStart = performance.now()
            const rowsIn = rows.length

            controller.enqueue(sse({ type: 'step_start', stepIndex: i, label: step.label, rowsIn }))

            try {
              const result = await executePipelineStep(step, rows)
              for (const line of result.logs) {
                controller.enqueue(sse({ type: 'log', stepIndex: i, message: line }))
              }
              rows = result.rows
              const durationMs = Math.round(performance.now() - stepStart)
              const throughputPerSec = durationMs > 0 ? Math.round((rowsIn / durationMs) * 1000) : rowsIn
              const metric: StepRunMetric = {
                stepIndex: i,
                stepId: step.id,
                label: step.label,
                op: step.op,
                rowsIn,
                rowsOut: rows.length,
                removed: result.removed,
                failed: false,
                failedRows: 0,
                durationMs,
                throughputPerSec,
              }
              stepMetrics.push(metric)
              controller.enqueue(sse({
                type: 'step_done',
                stepIndex: i,
                stepId: step.id,
                label: step.label,
                op: step.op,
                rowsIn,
                rowsOut: rows.length,
                removed: result.removed,
                durationMs,
                throughputPerSec,
              }))
              stepsCompleted = i + 1
            } catch (e: unknown) {
              const durationMs = Math.round(performance.now() - stepStart)
              const message = e instanceof Error ? e.message : String(e)
              stepMetrics.push({
                stepIndex: i,
                stepId: step.id,
                label: step.label,
                op: step.op,
                rowsIn,
                rowsOut: 0,
                removed: 0,
                failed: true,
                failedRows: rowsIn,
                durationMs,
                throughputPerSec: 0,
              })
              controller.enqueue(sse({
                type: 'step_error',
                stepIndex: i,
                label: step.label,
                message,
              }))
              controller.enqueue(sse({
                type: 'done',
                status: 'failed',
                stepsCompleted: i,
                durationMs: Math.round(performance.now() - totalStart),
              }))
              finalStatus = 'failed'
              stepsCompleted = i
              failed = true
              failedMessage = message
            }
          }

          if (!failed) {
            const columns = rows[0] ? Object.keys(rows[0]) : []
            const previewRows = rows.slice(0, 50).map(row => columns.map(column => previewCell(row[column])))
            const durationMs = Math.round(performance.now() - totalStart)
            const totalRowsIn = stepMetrics[0]?.rowsIn ?? rows.length
            const totalRemoved = stepMetrics.reduce((sum, metric) => sum + metric.removed, 0)
            const failedRows = stepMetrics.reduce((sum, metric) => sum + metric.failedRows, 0)
            const failedSteps = stepMetrics.filter(metric => metric.failed).length
            const throughputPerSec = durationMs > 0 ? Math.round((rows.length / durationMs) * 1000) : rows.length
            const healthScore = Math.max(0, Math.round(
              100
              - (failedSteps * 25)
              - ((failedRows / Math.max(totalRowsIn, 1)) * 50)
              - ((totalRemoved / Math.max(totalRowsIn, 1)) * 10),
            ))
            const summary: PipelineRunSummary = {
              rowsIn: totalRowsIn,
              rowsOut: rows.length,
              removed: totalRemoved,
              failedRows,
              failedSteps,
              totalSteps: runtimeSteps.length,
              durationMs,
              throughputPerSec,
              errorRate: Number((failedRows / Math.max(totalRowsIn, 1)).toFixed(4)),
              healthScore,
            }
            const latestRun = {
              pipelineId: pipeline.id,
              status: 'succeeded' as const,
              startedAt,
              durationMs,
              columns,
              rows: previewRows,
              rowCount: rows.length,
              summary,
              stepMetrics,
              error: null,
            }
            setLatestRunResult(pipeline.id, latestRun)
            controller.enqueue(sse({
              type: 'result',
              columns,
              rows: previewRows,
              rowCount: rows.length,
              summary,
              stepMetrics,
            }))
          }
        } else {
          for (let i = 0; i < pipeline.steps.length && !failed; i++) {
            const step      = pipeline.steps[i]
            const stepStart = performance.now()

            controller.enqueue(sse({ type: 'step_start', stepIndex: i, label: step.label, rowsIn: step.rowsIn }))

            for (const line of step.logLines ?? []) {
              controller.enqueue(sse({ type: 'log', stepIndex: i, message: line }))
            }

            if (step.isError) {
              controller.enqueue(sse({
                type: 'step_error', stepIndex: i, label: step.label,
                message: step.errorMsg ?? 'Step failed',
              }))
              controller.enqueue(sse({
                type: 'done', status: 'failed', stepsCompleted: i,
                durationMs: Math.round(performance.now() - totalStart),
              }))
              finalStatus    = 'failed'
              stepsCompleted = i
              failed         = true
              failedMessage  = step.errorMsg ?? 'Step failed'
            } else {
              const rowsIn = step.rowsIn ?? step.rowsOut ?? 0
              const rowsOut = step.rowsOut ?? rowsIn
              const durationMs = Math.round(performance.now() - stepStart)
              stepMetrics.push({
                stepIndex: i,
                stepId: `legacy_${i}`,
                label: step.label,
                op: 'legacy',
                rowsIn,
                rowsOut,
                removed: Math.max(0, rowsIn - rowsOut),
                failed: false,
                failedRows: 0,
                durationMs,
                throughputPerSec: durationMs > 0 ? Math.round((rowsIn / durationMs) * 1000) : rowsIn,
              })
              controller.enqueue(sse({
                type: 'step_done', stepIndex: i, label: step.label,
                stepId: `legacy_${i}`,
                op: 'legacy',
                rowsIn,
                rowsOut,
                removed: Math.max(0, rowsIn - rowsOut),
                durationMs,
                throughputPerSec: durationMs > 0 ? Math.round((rowsIn / durationMs) * 1000) : rowsIn,
              }))
              stepsCompleted = i + 1
            }
          }
        }

        if (!failed) {
          const durationMs = Math.round(performance.now() - totalStart)
          controller.enqueue(sse({
            type: 'done', status: 'succeeded',
            stepsCompleted: runtimeSteps?.length ?? pipeline.steps.length, durationMs,
          }))
        }
      } finally {
        const durationMs = Math.round(performance.now() - totalStart)
        if (finalStatus === 'failed') {
          const totalRowsIn = stepMetrics[0]?.rowsIn ?? 0
          const totalRemoved = stepMetrics.reduce((sum, metric) => sum + metric.removed, 0)
          const failedRows = stepMetrics.reduce((sum, metric) => sum + metric.failedRows, 0)
          const failedSteps = stepMetrics.filter(metric => metric.failed).length
          const summary: PipelineRunSummary = {
            rowsIn: totalRowsIn,
            rowsOut: stepMetrics[stepMetrics.length - 1]?.rowsOut ?? 0,
            removed: totalRemoved,
            failedRows,
            failedSteps,
            totalSteps: pipeline.runtimeSteps?.length ?? pipeline.steps.length,
            durationMs,
            throughputPerSec: 0,
            errorRate: Number((failedRows / Math.max(totalRowsIn, 1)).toFixed(4)),
            healthScore: Math.max(0, 100 - failedSteps * 25 - Math.round((failedRows / Math.max(totalRowsIn, 1)) * 50)),
          }
          setLatestRunResult(pipeline.id, {
            pipelineId: pipeline.id,
            status: 'failed',
            startedAt,
            durationMs,
            columns: [],
            rows: [],
            rowCount: 0,
            summary,
            stepMetrics,
            error: failedMessage,
          })
        }
        recordRun(pipeline.id, finalStatus, durationMs, stepsCompleted)
        workerEnd()
        controller.close()
      }
    },
  })

  return new Response(stream, {
    headers: {
      'Content-Type':  'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection':    'keep-alive',
    },
  })
}
