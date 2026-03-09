import { NextRequest, NextResponse } from 'next/server'

import { createGitHubAuthTransaction } from '@/lib/github-auth'

export const dynamic = 'force-dynamic'

export async function POST(req: NextRequest) {
  let body: {
    connectorName?: string
    connectorDescription?: string
    appSlug?: string
    appId?: string
    clientId?: string
    clientSecret?: string
    privateKey?: string
  }
  try {
    body = await req.json()
  } catch {
    return NextResponse.json({ error: 'Bad Request' }, { status: 400 })
  }

  const appSlug = String(body.appSlug ?? '')
  if (!appSlug) {
    return NextResponse.json({ error: 'GitHub App slug is required' }, { status: 400 })
  }
  if (!(process.env.CONNECTOR_SECRET_KEY ?? '').trim()) {
    return NextResponse.json({ error: 'CONNECTOR_SECRET_KEY is required for delegated GitHub auth' }, { status: 400 })
  }

  const connectorName = String(body.connectorName ?? 'GitHub')
  const connectorDescription = String(body.connectorDescription ?? '')
  let transaction
  try {
    transaction = createGitHubAuthTransaction({
      provider: 'app',
      connectorName,
      connectorDescription,
      appConfig: {
        appSlug,
        appId: String(body.appId ?? ''),
        clientId: String(body.clientId ?? ''),
        clientSecret: String(body.clientSecret ?? ''),
        privateKey: String(body.privateKey ?? ''),
      },
    })
  } catch (error) {
    return NextResponse.json({
      error: error instanceof Error ? error.message : String(error),
    }, { status: 500 })
  }

  const installUrl = new URL(`https://github.com/apps/${appSlug}/installations/new`)
  installUrl.searchParams.set('state', transaction.state)
  return NextResponse.json({ installUrl: installUrl.toString(), transactionId: transaction.id })
}
