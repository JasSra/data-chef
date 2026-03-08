/**
 * POST /api/pipelines/preview
 *
 * Returns sample rows after executing pipeline steps against a real bounded
 * sample from the selected dataset.
 */

import { NextRequest, NextResponse } from 'next/server'
import { executePipelineStep, loadPipelineSourceRows, previewCell } from '@/lib/pipeline-runtime'

type Row = Record<string, unknown>

interface StepInput {
  id?: string
  op: string
  label?: string
  config: Record<string, unknown>
}

export async function POST(req: NextRequest) {
  try {
    const { dataset, stepIndex, steps, rowLimit = 50, cachedRows } = await req.json() as {
      dataset: string
      stepIndex: number
      steps: StepInput[]
      rowLimit?: number
      cachedRows?: Row[]
    }

    const cap = Math.min(Math.max(rowLimit, 10), 500)
    let rows: Row[]
    let sourceRows: Row[] | undefined

    if (cachedRows && Array.isArray(cachedRows) && cachedRows.length > 0) {
      rows = cachedRows.slice(0, cap)
    } else {
      rows = await loadPipelineSourceRows(dataset, cap)
      rows = rows.slice(0, cap)
      sourceRows = rows
    }

    let totalRemoved = 0
    for (let i = 0; i <= stepIndex && i < steps.length; i++) {
      const result = await executePipelineStep({
        id: steps[i].id ?? `step_${i}`,
        op: steps[i].op,
        label: steps[i].label ?? steps[i].op,
        config: steps[i].config,
      }, rows)
      rows = result.rows
      totalRemoved += result.removed
    }

    const columns = rows[0] ? Object.keys(rows[0]) : []
    const preview = rows.map(r => columns.map(col => previewCell(r[col])))
    return NextResponse.json({ columns, rows: preview, rowCount: rows.length, removed: totalRemoved, sourceRows })
  } catch (e: unknown) {
    return NextResponse.json({
      columns: [],
      rows: [],
      rowCount: 0,
      removed: 0,
      error: e instanceof Error ? e.message : String(e),
    }, { status: 400 })
  }
}
