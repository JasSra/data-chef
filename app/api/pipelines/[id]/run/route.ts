/**
 * POST /api/pipelines/[id]/run
 *
 * Streams Server-Sent Events for each pipeline step.
 * Events:
 *   { type: 'start',      pipelineId, name, totalSteps }
 *   { type: 'step_start', stepIndex, label, rowsIn? }
 *   { type: 'step_done',  stepIndex, label, rowsOut, durationMs }
 *   { type: 'step_error', stepIndex, label, message }
 *   { type: 'log',        stepIndex, message }
 *   { type: 'done',       status: 'succeeded'|'failed', stepsCompleted, durationMs }
 */

import { NextRequest } from 'next/server'
import { PIPELINE_MAP, workerStart, workerEnd, recordRun } from '@/lib/pipelines'
import { executePipelineStep, loadPipelineSourceRows } from '@/lib/pipeline-runtime'

function sse(data: object) {
  return new TextEncoder().encode(`data: ${JSON.stringify(data)}\n\n`)
}

export async function POST(
  _req: NextRequest,
  { params }: { params: { id: string } }
) {
  const pipeline = PIPELINE_MAP.get(params.id)

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
      let finalStatus: 'succeeded' | 'failed' = 'succeeded'
      let stepsCompleted  = 0
      let failed          = false

      try {
        const runtimeSteps = pipeline.runtimeSteps
        controller.enqueue(sse({
          type: 'start',
          pipelineId: params.id,
          name:       pipeline.name,
          totalSteps: runtimeSteps?.length ?? pipeline.steps.length,
        }))

        if (runtimeSteps?.length) {
          let rows = await loadPipelineSourceRows(pipeline.dataset, 200)
          for (let i = 0; i < runtimeSteps.length && !failed; i++) {
            const step = runtimeSteps[i]
            const stepStart = performance.now()

            controller.enqueue(sse({ type: 'step_start', stepIndex: i, label: step.label, rowsIn: rows.length }))

            try {
              const result = await executePipelineStep(step, rows)
              for (const line of result.logs) {
                controller.enqueue(sse({ type: 'log', stepIndex: i, message: line }))
              }
              rows = result.rows
              controller.enqueue(sse({
                type: 'step_done',
                stepIndex: i,
                label: step.label,
                rowsOut: rows.length,
                durationMs: Math.round(performance.now() - stepStart),
              }))
              stepsCompleted = i + 1
            } catch (e: unknown) {
              controller.enqueue(sse({
                type: 'step_error',
                stepIndex: i,
                label: step.label,
                message: e instanceof Error ? e.message : String(e),
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
            }
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
            } else {
              controller.enqueue(sse({
                type: 'step_done', stepIndex: i, label: step.label,
                rowsOut: step.rowsOut, durationMs: Math.round(performance.now() - stepStart),
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
