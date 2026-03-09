import 'server-only'

import { createSign } from 'node:crypto'

import {
  getConnectorRuntimeConfig,
  getGitHubCreds,
  setGitHubCreds,
  type ConnectorRuntimeConfig,
  type GitHubCredentials,
  type GitHubRepoSelection,
  type GitHubResource,
} from '@/lib/connectors'

export interface GitHubRepoRecord extends GitHubRepoSelection {
  id: number
  defaultBranch: string
  visibility: string
  archived: boolean
  url: string
}

interface GitHubAccessContext {
  authHeader: string
  credentials: GitHubCredentials
}

interface GitHubRestOptions {
  method?: string
  headers?: Record<string, string>
  body?: string
}

interface GitHubPagedResponse<T> {
  items: T[]
  nextUrl: string | null
}

const GITHUB_API = 'https://api.github.com'

function base64UrlEncode(input: string | Buffer): string {
  return Buffer.from(input)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '')
}

function parseNextLink(linkHeader: string | null): string | null {
  if (!linkHeader) return null
  const parts = linkHeader.split(',').map(part => part.trim())
  for (const part of parts) {
    const match = part.match(/<([^>]+)>;\s*rel="next"/)
    if (match) return match[1]
  }
  return null
}

function normalizeRepo(repo: Record<string, unknown>): GitHubRepoRecord {
  const owner = (repo.owner as Record<string, unknown> | undefined) ?? {}
  return {
    id: Number(repo.id ?? 0),
    owner: String(owner.login ?? ''),
    repo: String(repo.name ?? ''),
    fullName: String(repo.full_name ?? ''),
    private: Boolean(repo.private),
    ownerType: owner.type === 'Organization' ? 'Organization' : 'User',
    defaultBranch: String(repo.default_branch ?? ''),
    visibility: String(repo.visibility ?? (repo.private ? 'private' : 'public')),
    archived: Boolean(repo.archived),
    url: String(repo.html_url ?? ''),
  }
}

function parseSelectedRepos(config: ConnectorRuntimeConfig | null): GitHubRepoSelection[] {
  const selected = config?.selectedRepos
  if (!Array.isArray(selected)) return []
  return selected
    .filter(item => item && typeof item === 'object')
    .map(item => {
      const repo = item as Record<string, unknown>
      return {
        owner: String(repo.owner ?? ''),
        repo: String(repo.repo ?? ''),
        fullName: String(repo.fullName ?? `${String(repo.owner ?? '')}/${String(repo.repo ?? '')}`),
        private: Boolean(repo.private),
        ownerType: (repo.ownerType === 'Organization' ? 'Organization' : 'User') as 'User' | 'Organization',
      }
    })
    .filter(item => item.owner && item.repo)
}

function resolveRequestedResource(resource: string | undefined, config: ConnectorRuntimeConfig | null): {
  resource: GitHubResource
  state: 'open' | 'closed' | 'all'
} {
  const trimmed = (resource ?? '').trim()
  if (!trimmed) {
    const fallback = String(config?.defaultResource ?? 'repos')
    return {
      resource: fallback === 'pullRequests' || fallback === 'issues' ? fallback : 'repos',
      state: 'open',
    }
  }
  const url = new URL(trimmed.includes('?') ? `https://local/${trimmed}` : `https://local/${trimmed}?`)
  const pathname = url.pathname.replace(/^\//, '')
  const resolved: GitHubResource = pathname === 'pullRequests' || pathname === 'issues' ? pathname : 'repos'
  const state = url.searchParams.get('state')
  return {
    resource: resolved,
    state: state === 'closed' || state === 'all' ? state : 'open',
  }
}

async function exchangeRefreshToken(credentials: Extract<GitHubCredentials, { mode: 'oauth' }>): Promise<Extract<GitHubCredentials, { mode: 'oauth' }>> {
  if (!credentials.refreshToken) return credentials
  const clientId = credentials.clientId
  const clientSecret = credentials.clientSecret
  if (!clientId || !clientSecret) return credentials

  const response = await fetch('https://github.com/login/oauth/access_token', {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      client_id: clientId,
      client_secret: clientSecret,
      grant_type: 'refresh_token',
      refresh_token: credentials.refreshToken,
    }),
    signal: AbortSignal.timeout(15_000),
  })

  if (!response.ok) return credentials
  const body = await response.json() as {
    access_token?: string
    refresh_token?: string
    expires_in?: number
    refresh_token_expires_in?: number
    scope?: string
    token_type?: string
  }
  if (!body.access_token) return credentials
  return {
    ...credentials,
    accessToken: body.access_token,
    refreshToken: body.refresh_token ?? credentials.refreshToken,
    expiresAt: Date.now() + Number(body.expires_in ?? 28_800) * 1000,
    scope: body.scope ?? credentials.scope,
    tokenType: body.token_type ?? credentials.tokenType,
  }
}

function buildGitHubAppJwt(credentials: Extract<GitHubCredentials, { mode: 'app' }>): string {
  const now = Math.floor(Date.now() / 1000)
  const header = base64UrlEncode(JSON.stringify({ alg: 'RS256', typ: 'JWT' }))
  const payload = base64UrlEncode(JSON.stringify({
    iat: now - 60,
    exp: now + 540,
    iss: credentials.appId,
  }))
  const signer = createSign('RSA-SHA256')
  signer.update(`${header}.${payload}`)
  signer.end()
  const signature = signer.sign(credentials.privateKey)
  return `${header}.${payload}.${base64UrlEncode(signature)}`
}

async function fetchInstallationAccessToken(credentials: Extract<GitHubCredentials, { mode: 'app' }>): Promise<string> {
  const jwt = buildGitHubAppJwt(credentials)
  const response = await fetch(`${GITHUB_API}/app/installations/${credentials.installationId}/access_tokens`, {
    method: 'POST',
    headers: {
      Accept: 'application/vnd.github+json',
      Authorization: `Bearer ${jwt}`,
      'X-GitHub-Api-Version': '2022-11-28',
    },
    signal: AbortSignal.timeout(15_000),
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(`GitHub App token error ${response.status}: ${text.slice(0, 300)}`)
  }
  const body = await response.json() as { token: string }
  return body.token
}

async function resolveAccessContext(credentials: GitHubCredentials, options: { connectorId?: string } = {}): Promise<GitHubAccessContext> {
  if (credentials.mode === 'pat') {
    return {
      authHeader: `Bearer ${credentials.token}`,
      credentials,
    }
  }

  if (credentials.mode === 'oauth') {
    let nextCreds = credentials
    if (credentials.expiresAt && credentials.expiresAt - Date.now() < 120_000) {
      nextCreds = await exchangeRefreshToken(credentials)
      if (options.connectorId && nextCreds.accessToken !== credentials.accessToken) {
        setGitHubCreds(options.connectorId, nextCreds)
      }
    }
    return {
      authHeader: `Bearer ${nextCreds.accessToken}`,
      credentials: nextCreds,
    }
  }

  const installationToken = await fetchInstallationAccessToken(credentials)
  return {
    authHeader: `Bearer ${installationToken}`,
    credentials,
  }
}

async function githubRequest<T>(url: string, context: GitHubAccessContext, options: GitHubRestOptions = {}): Promise<{ body: T; response: Response }> {
  let lastError: Error | null = null
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const response = await fetch(url, {
        method: options.method ?? 'GET',
        headers: {
          Accept: 'application/vnd.github+json',
          Authorization: context.authHeader,
          'X-GitHub-Api-Version': '2022-11-28',
          ...(options.headers ?? {}),
        },
        body: options.body,
        signal: AbortSignal.timeout(20_000),
      })

      if ((response.status >= 500 || response.status === 403) && attempt < 2) {
        await new Promise(resolve => setTimeout(resolve, 500 * (attempt + 1)))
        continue
      }

      if (!response.ok) {
        const text = await response.text()
        throw new Error(`GitHub API ${response.status}: ${text.slice(0, 300)}`)
      }

      const body = await response.json() as T
      return { body, response }
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error))
    }
  }
  throw lastError ?? new Error('GitHub request failed')
}

async function githubPagedRequest<T>(url: string, context: GitHubAccessContext): Promise<GitHubPagedResponse<T>> {
  const { body, response } = await githubRequest<T[]>(url, context)
  return {
    items: Array.isArray(body) ? body : [],
    nextUrl: parseNextLink(response.headers.get('link')),
  }
}

async function fetchViewer(context: GitHubAccessContext): Promise<{ login: string; type: 'User' | 'Organization' }> {
  const { body } = await githubRequest<Record<string, unknown>>(`${GITHUB_API}/user`, context)
  return {
    login: String(body.login ?? ''),
    type: body.type === 'Organization' ? 'Organization' : 'User',
  }
}

export async function listAccessibleGitHubRepos(
  credentials: GitHubCredentials,
  options: { connectorId?: string } = {},
): Promise<GitHubRepoRecord[]> {
  const context = await resolveAccessContext(credentials, options)
  const repos: GitHubRepoRecord[] = []

  if (credentials.mode === 'app') {
    let nextUrl: string | null = `${GITHUB_API}/installation/repositories?per_page=100`
    while (nextUrl) {
      const page = await githubRequest<{ repositories?: Record<string, unknown>[] }>(nextUrl, context)
      for (const repo of page.body.repositories ?? []) repos.push(normalizeRepo(repo))
      nextUrl = parseNextLink(page.response.headers.get('link'))
    }
    return repos
  }

  let nextUrl: string | null = `${GITHUB_API}/user/repos?per_page=100&sort=updated&affiliation=owner,collaborator,organization_member`
  while (nextUrl) {
    const page: GitHubPagedResponse<Record<string, unknown>> = await githubPagedRequest<Record<string, unknown>>(nextUrl, context)
    for (const repo of page.items) repos.push(normalizeRepo(repo))
    nextUrl = page.nextUrl
  }
  return repos
}

export async function getGitHubViewerForCredentials(
  credentials: GitHubCredentials,
  options: { connectorId?: string } = {},
): Promise<{ login: string; type: 'User' | 'Organization' }> {
  const context = await resolveAccessContext(credentials, options)
  if (credentials.mode === 'app' && credentials.accountLogin) {
    return {
      login: credentials.accountLogin,
      type: 'Organization',
    }
  }
  return fetchViewer(context)
}

export async function validateGitHubCredentials(
  credentials: GitHubCredentials,
  options: { connectorId?: string } = {},
): Promise<{ viewer: { login: string; type: 'User' | 'Organization' }; repoCount: number }> {
  const viewer = await getGitHubViewerForCredentials(credentials, options)
  const repos = await listAccessibleGitHubRepos(credentials, options)
  return { viewer, repoCount: repos.length }
}

function normalizeIssueRow(repo: GitHubRepoSelection, issue: Record<string, unknown>): Record<string, unknown> | null {
  if (issue.pull_request) return null
  return {
    repoOwner: repo.owner,
    repoName: repo.repo,
    repoFullName: repo.fullName,
    ...issue,
  }
}

function normalizePullRequestRow(repo: GitHubRepoSelection, pr: Record<string, unknown>): Record<string, unknown> {
  return {
    repoOwner: repo.owner,
    repoName: repo.repo,
    repoFullName: repo.fullName,
    ...pr,
  }
}

export async function fetchGitHubRows(
  connectorId: string,
  resourceInput: string | undefined,
  options: { rowLimit?: number } = {},
): Promise<Record<string, unknown>[]> {
  const credentials = getGitHubCreds(connectorId)
  if (!credentials) throw new Error('GitHub credentials not found')

  const config = getConnectorRuntimeConfig(connectorId)
  const selectedRepos = parseSelectedRepos(config)
  if (selectedRepos.length === 0) throw new Error('No GitHub repositories selected for this connector')

  const { resource, state } = resolveRequestedResource(resourceInput, config)
  const rowLimit = Math.max(1, options.rowLimit ?? 500)

  if (resource === 'repos') {
    const allRepos = await listAccessibleGitHubRepos(credentials, { connectorId })
    const allowed = new Set(selectedRepos.map(repo => repo.fullName.toLowerCase()))
    return allRepos
      .filter(repo => allowed.has(repo.fullName.toLowerCase()))
      .slice(0, rowLimit)
      .map(repo => ({ ...repo }))
  }

  const context = await resolveAccessContext(credentials, { connectorId })
  const rows: Record<string, unknown>[] = []
  for (const repo of selectedRepos) {
    if (rows.length >= rowLimit) break
    const endpoint = resource === 'pullRequests'
      ? `${GITHUB_API}/repos/${repo.owner}/${repo.repo}/pulls?state=${state}&per_page=100`
      : `${GITHUB_API}/repos/${repo.owner}/${repo.repo}/issues?state=${state}&per_page=100`
    let nextUrl: string | null = endpoint
    while (nextUrl && rows.length < rowLimit) {
      const page: GitHubPagedResponse<Record<string, unknown>> = await githubPagedRequest<Record<string, unknown>>(nextUrl, context)
      for (const item of page.items) {
        if (rows.length >= rowLimit) break
        if (resource === 'pullRequests') {
          rows.push(normalizePullRequestRow(repo, item))
          continue
        }
        const issue = normalizeIssueRow(repo, item)
        if (issue) rows.push(issue)
      }
      nextUrl = page.nextUrl
    }
  }

  return rows
}
