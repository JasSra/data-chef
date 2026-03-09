import { NextRequest, NextResponse } from 'next/server'

import { createGitHubAuthTransaction } from '@/lib/github-auth'

export const dynamic = 'force-dynamic'

export async function POST(req: NextRequest) {
  let body: {
    connectorName?: string
    connectorDescription?: string
    clientId?: string
    clientSecret?: string
  }
  try {
    body = await req.json()
  } catch {
    return NextResponse.json({ error: 'Bad Request' }, { status: 400 })
  }

  const clientId = String(body.clientId ?? '')
  const clientSecret = String(body.clientSecret ?? '')
  if (!clientId) {
    return NextResponse.json({ error: 'GitHub OAuth client ID is required' }, { status: 400 })
  }
  if (!clientSecret) {
    return NextResponse.json({ error: 'GitHub OAuth client secret is required' }, { status: 400 })
  }
  if (!(process.env.CONNECTOR_SECRET_KEY ?? '').trim()) {
    return NextResponse.json({ error: 'CONNECTOR_SECRET_KEY is required for delegated GitHub auth' }, { status: 400 })
  }

  const connectorName = String(body.connectorName ?? 'GitHub')
  const connectorDescription = String(body.connectorDescription ?? '')
  let transaction
  try {
    transaction = createGitHubAuthTransaction({
      provider: 'oauth',
      connectorName,
      connectorDescription,
      oauthConfig: { clientId, clientSecret },
    })
  } catch (error) {
    return NextResponse.json({
      error: error instanceof Error ? error.message : String(error),
    }, { status: 500 })
  }
  const redirectUri = new URL('/api/connectors/github/oauth/callback', req.nextUrl.origin).toString()
  const authorizeUrl = new URL('https://github.com/login/oauth/authorize')
  authorizeUrl.searchParams.set('client_id', clientId)
  authorizeUrl.searchParams.set('redirect_uri', redirectUri)
  authorizeUrl.searchParams.set('state', transaction.state)
  authorizeUrl.searchParams.set('scope', 'repo read:user read:org')

  return NextResponse.json({ authorizeUrl: authorizeUrl.toString(), transactionId: transaction.id })
}
