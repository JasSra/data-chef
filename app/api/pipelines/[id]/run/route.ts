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

function sse(data: object) {
  return new TextEncoder().encode(`data: ${JSON.stringify(data)}\n\n`)
}

function sleep(ms: number) {
  return new Promise<void>(r => setTimeout(r, ms))
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
        controller.enqueue(sse({
          type: 'start',
          pipelineId: params.id,
          name:       pipeline.name,
          totalSteps: pipeline.steps.length,
        }))

        for (let i = 0; i < pipeline.steps.length && !failed; i++) {
          const step      = pipeline.steps[i]
          const stepStart = performance.now()

          controller.enqueue(sse({ type: 'step_start', stepIndex: i, label: step.label, rowsIn: step.rowsIn }))

          // Stream log lines progressively during step execution
          const logLines = step.logLines ?? []
          const logDelay = step.durationMs / (logLines.length + 1)
          for (const line of logLines) {
            await sleep(logDelay)
            controller.enqueue(sse({ type: 'log', stepIndex: i, message: line }))
          }

          // Consume remaining step time
          const elapsed = performance.now() - stepStart
          if (elapsed < step.durationMs) await sleep(step.durationMs - elapsed)

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

        if (!failed) {
          const durationMs = Math.round(performance.now() - totalStart)
          controller.enqueue(sse({
            type: 'done', status: 'succeeded',
            stepsCompleted: pipeline.steps.length, durationMs,
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
