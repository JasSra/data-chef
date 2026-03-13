import { NextRequest, NextResponse } from 'next/server'
import { deleteConnector, getConnectors } from '@/lib/connectors'
import { deleteDataset, getDatasets } from '@/lib/datasets'
import { clearDiscoveryCandidateConnector } from '@/lib/network-discovery'
import { deletePipelinesReferencingSource, deletePipelinesWritingDataset } from '@/lib/pipelines'
import { deleteSavedQueriesForConnector } from '@/lib/saved-queries'

export const dynamic = 'force-dynamic'

export async function DELETE(
  _req: NextRequest,
  { params }: { params: { id: string } },
) {
  const id = String(params.id ?? '')
  if (!id) {
    return NextResponse.json({ error: 'Connector id is required' }, { status: 400 })
  }

  const connector = getConnectors().find(item => item.id === id)
  if (!connector) {
    return NextResponse.json({ error: 'Connector not found' }, { status: 404 })
  }

  const relatedDatasets = getDatasets().filter(dataset => dataset.connectorId === id)
  let deletedPipelines = deletePipelinesReferencingSource('connector', id)
  let deletedDatasets = 0
  for (const dataset of relatedDatasets) {
    const deleted = deleteDataset(dataset.id)
    if (!deleted) continue
    deletedDatasets++
    deletedPipelines += deletePipelinesReferencingSource('dataset', dataset.id)
    deletedPipelines += deletePipelinesWritingDataset(dataset.id)
  }

  const deletedConnector = deleteConnector(id)
  if (!deletedConnector) {
    return NextResponse.json({ error: 'Connector not found' }, { status: 404 })
  }

  const deletedSavedQueries = deleteSavedQueriesForConnector(id)
  const resetDiscoveryCandidates = clearDiscoveryCandidateConnector(id)

  return NextResponse.json({
    ok: true,
    deletedConnectorId: id,
    deletedDatasets,
    deletedPipelines,
    deletedSavedQueries,
    resetDiscoveryCandidates,
  })
}
