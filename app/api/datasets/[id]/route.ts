import { NextRequest, NextResponse } from 'next/server'
import { deleteDataset, getDatasets } from '@/lib/datasets'
import { deletePipelinesReferencingSource, deletePipelinesWritingDataset } from '@/lib/pipelines'
import { updateConnectorDatasets } from '@/lib/connectors'

export const dynamic = 'force-dynamic'

export async function DELETE(
  _req: NextRequest,
  { params }: { params: { id: string } },
) {
  const id = String(params.id ?? '')
  if (!id) {
    return NextResponse.json({ error: 'Dataset id is required' }, { status: 400 })
  }

  const deleted = deleteDataset(id)
  if (!deleted) {
    return NextResponse.json({ error: 'Dataset not found' }, { status: 404 })
  }

  const deletedPipelines = (
    deletePipelinesReferencingSource('dataset', id) +
    deletePipelinesWritingDataset(id)
  )

  if (deleted.connectorId) {
    const linkedNames = getDatasets()
      .filter(dataset => dataset.connectorId === deleted.connectorId)
      .map(dataset => dataset.name)
    updateConnectorDatasets(deleted.connectorId, linkedNames)
  }

  return NextResponse.json({
    ok: true,
    deletedDatasetId: deleted.id,
    connectorId: deleted.connectorId ?? null,
    deletedPipelines,
  })
}
