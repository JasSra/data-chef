import { NextRequest, NextResponse } from 'next/server'

import { getGitHubAuthTransaction } from '@/lib/github-auth'
import { getGitHubCreds } from '@/lib/connectors'
import { listAccessibleGitHubRepos } from '@/lib/github'

export const dynamic = 'force-dynamic'

export async function GET(req: NextRequest) {
  const connectorId = req.nextUrl.searchParams.get('connectorId') ?? ''
  const transactionId = req.nextUrl.searchParams.get('transactionId') ?? ''
  const search = (req.nextUrl.searchParams.get('search') ?? '').trim().toLowerCase()
  const page = Math.max(1, Number(req.nextUrl.searchParams.get('page') ?? 1))
  const pageSize = Math.min(100, Math.max(1, Number(req.nextUrl.searchParams.get('pageSize') ?? 50)))

  const patHeader = req.headers.get('x-datachef-github-pat') ?? ''
  let credentials = null
  try {
    credentials = connectorId
      ? getGitHubCreds(connectorId)
      : transactionId
      ? getGitHubAuthTransaction(transactionId)?.credentials ?? null
      : patHeader
      ? { mode: 'pat' as const, token: patHeader }
      : null
  } catch (error) {
    return NextResponse.json({
      error: error instanceof Error ? error.message : String(error),
    }, { status: 500 })
  }

  if (!credentials) {
    return NextResponse.json({ error: 'connectorId or completed transactionId is required' }, { status: 400 })
  }

  try {
    const repos = await listAccessibleGitHubRepos(credentials, connectorId ? { connectorId } : {})
    const filtered = search
      ? repos.filter(repo => repo.fullName.toLowerCase().includes(search) || repo.owner.toLowerCase().includes(search))
      : repos
    const start = (page - 1) * pageSize
    return NextResponse.json({
      repos: filtered.slice(start, start + pageSize),
      total: filtered.length,
      page,
      pageSize,
      hasMore: start + pageSize < filtered.length,
    })
  } catch (error) {
    return NextResponse.json({
      error: error instanceof Error ? error.message : String(error),
    }, { status: 502 })
  }
}
