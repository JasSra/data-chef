/**
 * GET  /api/connectors  — list all connectors with computed display fields
 * POST /api/connectors  — create a new connector
 */

import { NextRequest, NextResponse } from 'next/server'
import {
  getConnectors, addConnector, updateConnector, relativeTime, fmtRecords, getSparkValues,
  setAppInsightsCreds, setAzureB2CCreds, setAzureEntraIdCreds, setConnectorRuntimeConfig, setGitHubCreds, setObservabilityCreds, ConnectorRecord,
} from '@/lib/connectors'
import type { ConnectorId } from '@/components/ConnectorWizard'
import { seedDefaultQueries } from '@/lib/saved-queries'
import { getDatasets } from '@/lib/datasets'
import { ensureConnectorSchedulerStarted } from '@/lib/connector-sync'
import { ensureNetworkDiscoverySchedulerStarted, markDiscoveryCandidateAdded } from '@/lib/network-discovery'
import { consumeGitHubAuthTransaction } from '@/lib/github-auth'

export const dynamic = 'force-dynamic'

function toResponse(c: ConnectorRecord) {
  const linkedDatasets = getDatasets()
    .filter(dataset => dataset.connectorId === c.id)
    .map(dataset => dataset.name)

  const lastSync = c.lastSyncAt
    ? relativeTime(c.lastSyncAt)
    : c.status === 'connected' ? 'live' : 'never'
  const recordsSynced = c.recordsRaw > 0
    ? fmtRecords(c.recordsRaw) + ' total'
    : '—'
  return {
    id:           c.id,
    name:         c.name,
    type:         c.type,
    status:       c.status,
    authMethod:   c.authMethod,
    endpoint:     c.endpoint,
    description:  c.description,
    datasets:     linkedDatasets,
    syncInterval: c.syncInterval,
    latencyMs:    c.latencyMs,
    lastSync:     lastSync,
    recordsSynced: recordsSynced,
    sparkValues:  getSparkValues(c.syncHistory),
  }
}

export async function GET() {
  ensureConnectorSchedulerStarted()
  ensureNetworkDiscoverySchedulerStarted()
  return NextResponse.json(getConnectors().map(toResponse))
}

export async function POST(req: NextRequest) {
  ensureConnectorSchedulerStarted()
  ensureNetworkDiscoverySchedulerStarted()
  let body: Record<string, unknown>
  try { body = await req.json() } catch { return NextResponse.json({ error: 'Bad Request' }, { status: 400 }) }

  let githubCreds: Parameters<typeof setGitHubCreds>[1] | null = null
  if (body.type === 'github') {
    if (!(process.env.CONNECTOR_SECRET_KEY ?? '').trim()) {
      return NextResponse.json({ error: 'CONNECTOR_SECRET_KEY is required for GitHub connectors' }, { status: 400 })
    }
    if (body.githubAuthTransactionId) {
      const transaction = consumeGitHubAuthTransaction(String(body.githubAuthTransactionId))
      if (!transaction?.credentials) {
        return NextResponse.json({ error: 'GitHub authorization transaction not found or incomplete' }, { status: 400 })
      }
      githubCreds = transaction.credentials
    } else if (body.githubCredentials && typeof body.githubCredentials === 'object') {
      githubCreds = body.githubCredentials as Parameters<typeof setGitHubCreds>[1]
    } else {
      return NextResponse.json({ error: 'GitHub credentials or githubAuthTransactionId are required' }, { status: 400 })
    }
  }

  const rec = addConnector({
    name:         String(body.name         ?? 'New Connector'),
    type:         (body.type as ConnectorId) ?? 'http',
    status:       'connected',
    authMethod:   String(body.authMethod   ?? 'None'),
    endpoint:     String(body.endpoint     ?? ''),
    description:  String(body.description  ?? ''),
    datasets:     Array.isArray(body.datasets) ? body.datasets as string[] : [],
    syncInterval: String(body.syncInterval ?? 'on-demand'),
    latencyMs:    0,
    lastSyncAt:   Date.now(),
    recordsRaw:   0,
  })

  if (body.runtimeConfig && typeof body.runtimeConfig === 'object') {
    setConnectorRuntimeConfig(rec.id, body.runtimeConfig as Record<string, unknown>)
  }

  // Store App Insights credentials server-side (never returned to client)
  if (body.type === 'appinsights' && body.aiCredentials) {
    const c = body.aiCredentials as {
      authMode?: string
      appId?: string
      apiKey?: string
      connectionString?: string
      mode?: string
      workspaceId?: string
      tenantId?: string
      clientId?: string
      clientSecret?: string
    }
    setAppInsightsCreds(rec.id, {
      authMode:     c.authMode === 'api_key' ? 'api_key' : 'entra_client_secret',
      appId:        String(c.appId ?? ''),
      apiKey:       String(c.apiKey ?? ''),
      connectionString: String(c.connectionString ?? ''),
      mode:         (c.mode === 'workspace' ? 'workspace' : 'appinsights'),
      workspaceId:  String(c.workspaceId ?? ''),
      tenantId:     String(c.tenantId ?? ''),
      clientId:     String(c.clientId ?? ''),
      clientSecret: String(c.clientSecret ?? ''),
    })
    seedDefaultQueries(rec.id)
  }

  if (body.observabilityCredentials && typeof body.observabilityCredentials === 'object') {
    setObservabilityCreds(rec.id, body.observabilityCredentials as Parameters<typeof setObservabilityCreds>[1])
    const provider = body.type === 'azuremonitor'
      ? 'azuremonitor'
      : body.type === 'elasticsearch'
      ? 'elasticsearch'
      : body.type === 'datadog'
      ? 'datadog'
      : 'appinsights'
    seedDefaultQueries(rec.id, provider)
  }

  if (body.type === 'azureb2c' && body.azureB2cCredentials) {
    const creds = body.azureB2cCredentials as {
      tenantId?: string
      clientId?: string
      authMode?: string
      clientSecret?: string
      certificatePem?: string
      privateKeyPem?: string
      thumbprint?: string
      cloud?: string
    }
    setAzureB2CCreds(rec.id, {
      tenantId: String(creds.tenantId ?? ''),
      clientId: String(creds.clientId ?? ''),
      authMode: creds.authMode === 'client_certificate' ? 'client_certificate' : 'client_secret',
      clientSecret: String(creds.clientSecret ?? ''),
      certificatePem: String(creds.certificatePem ?? ''),
      privateKeyPem: String(creds.privateKeyPem ?? ''),
      thumbprint: String(creds.thumbprint ?? ''),
      cloud: 'global',
    })
  }

  if (body.type === 'azureentraid' && body.azureEntraIdCredentials) {
    const creds = body.azureEntraIdCredentials as {
      tenantId?: string
      clientId?: string
      authMode?: string
      clientSecret?: string
      certificatePem?: string
      privateKeyPem?: string
      thumbprint?: string
      cloud?: string
    }
    setAzureEntraIdCreds(rec.id, {
      tenantId: String(creds.tenantId ?? ''),
      clientId: String(creds.clientId ?? ''),
      authMode: creds.authMode === 'client_certificate' ? 'client_certificate' : 'client_secret',
      clientSecret: String(creds.clientSecret ?? ''),
      certificatePem: String(creds.certificatePem ?? ''),
      privateKeyPem: String(creds.privateKeyPem ?? ''),
      thumbprint: String(creds.thumbprint ?? ''),
      cloud: 'global',
    })
  }

  if (body.type === 'github') {
    setGitHubCreds(rec.id, githubCreds!)
  }

  const sourceDiscoveryId = String(body.sourceDiscoveryId ?? '')
  if (sourceDiscoveryId) {
    markDiscoveryCandidateAdded(sourceDiscoveryId, rec.id)
  }

  return NextResponse.json(toResponse(rec), { status: 201 })
}

export async function PATCH(req: NextRequest) {
  ensureConnectorSchedulerStarted()
  ensureNetworkDiscoverySchedulerStarted()
  let body: Record<string, unknown>
  try { body = await req.json() } catch { return NextResponse.json({ error: 'Bad Request' }, { status: 400 }) }

  const id = String(body.id ?? '')
  if (!id) return NextResponse.json({ error: 'id is required' }, { status: 400 })
  if (body.type === 'github' && !(process.env.CONNECTOR_SECRET_KEY ?? '').trim()) {
    return NextResponse.json({ error: 'CONNECTOR_SECRET_KEY is required for GitHub connectors' }, { status: 400 })
  }

  const updated = updateConnector(id, {
    name: String(body.name ?? 'Connector'),
    type: (body.type as ConnectorId) ?? 'http',
    authMethod: String(body.authMethod ?? 'None'),
    endpoint: String(body.endpoint ?? ''),
    description: String(body.description ?? ''),
    datasets: Array.isArray(body.datasets) ? body.datasets as string[] : undefined,
    syncInterval: String(body.syncInterval ?? 'on-demand'),
  })

  if (!updated) {
    return NextResponse.json({ error: 'Connector not found' }, { status: 404 })
  }

  if (body.runtimeConfig && typeof body.runtimeConfig === 'object') {
    setConnectorRuntimeConfig(id, body.runtimeConfig as Record<string, unknown>)
  }

  if (body.type === 'appinsights' && body.aiCredentials) {
    const c = body.aiCredentials as {
      authMode?: string
      appId?: string
      apiKey?: string
      connectionString?: string
      mode?: string
      workspaceId?: string
      tenantId?: string
      clientId?: string
      clientSecret?: string
    }
    setAppInsightsCreds(id, {
      authMode: c.authMode === 'api_key' ? 'api_key' : 'entra_client_secret',
      appId: String(c.appId ?? ''),
      apiKey: String(c.apiKey ?? ''),
      connectionString: String(c.connectionString ?? ''),
      mode: c.mode === 'workspace' ? 'workspace' : 'appinsights',
      workspaceId: String(c.workspaceId ?? ''),
      tenantId: String(c.tenantId ?? ''),
      clientId: String(c.clientId ?? ''),
      clientSecret: String(c.clientSecret ?? ''),
    })
  }

  if (body.observabilityCredentials && typeof body.observabilityCredentials === 'object') {
    setObservabilityCreds(id, body.observabilityCredentials as Parameters<typeof setObservabilityCreds>[1])
  }

  if (body.type === 'azureb2c' && body.azureB2cCredentials) {
    const creds = body.azureB2cCredentials as {
      tenantId?: string
      clientId?: string
      authMode?: string
      clientSecret?: string
      certificatePem?: string
      privateKeyPem?: string
      thumbprint?: string
    }
    setAzureB2CCreds(id, {
      tenantId: String(creds.tenantId ?? ''),
      clientId: String(creds.clientId ?? ''),
      authMode: creds.authMode === 'client_certificate' ? 'client_certificate' : 'client_secret',
      clientSecret: String(creds.clientSecret ?? ''),
      certificatePem: String(creds.certificatePem ?? ''),
      privateKeyPem: String(creds.privateKeyPem ?? ''),
      thumbprint: String(creds.thumbprint ?? ''),
      cloud: 'global',
    })
  }

  if (body.type === 'azureentraid' && body.azureEntraIdCredentials) {
    const creds = body.azureEntraIdCredentials as {
      tenantId?: string
      clientId?: string
      authMode?: string
      clientSecret?: string
      certificatePem?: string
      privateKeyPem?: string
      thumbprint?: string
    }
    setAzureEntraIdCreds(id, {
      tenantId: String(creds.tenantId ?? ''),
      clientId: String(creds.clientId ?? ''),
      authMode: creds.authMode === 'client_certificate' ? 'client_certificate' : 'client_secret',
      clientSecret: String(creds.clientSecret ?? ''),
      certificatePem: String(creds.certificatePem ?? ''),
      privateKeyPem: String(creds.privateKeyPem ?? ''),
      thumbprint: String(creds.thumbprint ?? ''),
      cloud: 'global',
    })
  }

  if (body.type === 'github') {
    if (body.githubAuthTransactionId) {
      const transaction = consumeGitHubAuthTransaction(String(body.githubAuthTransactionId))
      if (!transaction?.credentials) {
        return NextResponse.json({ error: 'GitHub authorization transaction not found or incomplete' }, { status: 400 })
      }
      setGitHubCreds(id, transaction.credentials)
    } else if (body.githubCredentials && typeof body.githubCredentials === 'object') {
      setGitHubCreds(id, body.githubCredentials as Parameters<typeof setGitHubCreds>[1])
    }
  }

  return NextResponse.json(toResponse(updated))
}
