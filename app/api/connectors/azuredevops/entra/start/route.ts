import { NextRequest, NextResponse } from 'next/server'

import { createAzureDevOpsAuthTransaction } from '@/lib/azure-devops-auth'

export const dynamic = 'force-dynamic'

const AZDO_SCOPE = '499b84ac-1321-427f-aa17-267ca6975798/user_impersonation offline_access openid profile'

export async function POST(req: NextRequest) {
  let body: {
    connectorName?: string
    connectorDescription?: string
    tenantId?: string
    clientId?: string
    clientSecret?: string
    organization?: string
  }
  try {
    body = await req.json()
  } catch {
    return NextResponse.json({ error: 'Bad Request' }, { status: 400 })
  }

  const tenantId = String(body.tenantId ?? '')
  const clientId = String(body.clientId ?? '')
  const clientSecret = String(body.clientSecret ?? '')
  const organization = String(body.organization ?? '')
  if (!tenantId || !clientId || !clientSecret || !organization) {
    return NextResponse.json({ error: 'tenantId, clientId, clientSecret, and organization are required' }, { status: 400 })
  }
  if (!(process.env.CONNECTOR_SECRET_KEY ?? '').trim()) {
    return NextResponse.json({ error: 'CONNECTOR_SECRET_KEY is required for delegated Azure DevOps auth' }, { status: 400 })
  }

  const transaction = createAzureDevOpsAuthTransaction({
    connectorName: String(body.connectorName ?? 'Azure DevOps'),
    connectorDescription: String(body.connectorDescription ?? ''),
    organization,
    entraConfig: { tenantId, clientId, clientSecret, organization },
  })
  const redirectUri = new URL('/api/connectors/azuredevops/entra/callback', req.nextUrl.origin).toString()
  const authorizeUrl = new URL(`https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/authorize`)
  authorizeUrl.searchParams.set('client_id', clientId)
  authorizeUrl.searchParams.set('response_type', 'code')
  authorizeUrl.searchParams.set('redirect_uri', redirectUri)
  authorizeUrl.searchParams.set('response_mode', 'query')
  authorizeUrl.searchParams.set('scope', AZDO_SCOPE)
  authorizeUrl.searchParams.set('state', transaction.state)

  return NextResponse.json({ authorizeUrl: authorizeUrl.toString(), transactionId: transaction.id })
}
