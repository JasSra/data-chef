import 'server-only'

import {
  getAzureDevOpsCreds,
  setAzureDevOpsCreds,
  getConnectorRuntimeConfig,
  type AzureDevOpsCredentials,
  type AzureDevOpsProjectSelection,
  type AzureDevOpsRepoSelection,
  type AzureDevOpsResource,
  type ConnectorRuntimeConfig,
} from '@/lib/connectors'

const AZDO_SCOPE = '499b84ac-1321-427f-aa17-267ca6975798/user_impersonation offline_access openid profile'
const AZDO_APP_ID = '499b84ac-1321-427f-aa17-267ca6975798'
const AZDO_API_VERSION = '7.1-preview.1'
const AZDO_ACCOUNTS_URL = 'https://app.vssps.visualstudio.com'

interface AzureDevOpsAccessContext {
  authHeader: string
  credentials: AzureDevOpsCredentials
}

export interface AzureDevOpsOrganizationRecord {
  accountId: string
  accountName: string
  accountUri: string
}

export interface AzureDevOpsProjectRecord {
  id: string
  name: string
  description?: string
  visibility?: string
}

export interface AzureDevOpsRepositoryRecord {
  projectId: string
  projectName: string
  repositoryId: string
  repositoryName: string
  fullName: string
  defaultBranch?: string
  size?: number
  remoteUrl?: string
}

function basicPatHeader(pat: string): string {
  return `Basic ${Buffer.from(`:${pat}`).toString('base64')}`
}

function normalizeOrganizationName(input: string): string {
  return input
    .replace(/^https?:\/\/dev\.azure\.com\//i, '')
    .replace(/^https?:\/\/[^/]+\.visualstudio\.com\/?/i, '')
    .replace(/^\/+|\/+$/g, '')
    .split('/')[0] ?? ''
}

function parseSelectedProjects(config: ConnectorRuntimeConfig | null): AzureDevOpsProjectSelection[] {
  const selected = config?.selectedProjects
  if (!Array.isArray(selected)) return []
  return selected
    .filter(item => item && typeof item === 'object')
    .map(item => {
      const project = item as Record<string, unknown>
      return {
        id: String(project.id ?? ''),
        name: String(project.name ?? ''),
        description: project.description ? String(project.description) : undefined,
        visibility: project.visibility ? String(project.visibility) : undefined,
      }
    })
    .filter(project => project.id && project.name)
}

function parseSelectedRepos(config: ConnectorRuntimeConfig | null): AzureDevOpsRepoSelection[] {
  const selected = config?.selectedRepos
  if (!Array.isArray(selected)) return []
  return selected
    .filter(item => item && typeof item === 'object')
    .map(item => {
      const repo = item as Record<string, unknown>
      return {
        projectId: String(repo.projectId ?? ''),
        projectName: String(repo.projectName ?? ''),
        repositoryId: String(repo.repositoryId ?? ''),
        repositoryName: String(repo.repositoryName ?? ''),
        fullName: String(repo.fullName ?? ''),
        defaultBranch: repo.defaultBranch ? String(repo.defaultBranch) : undefined,
      }
    })
    .filter(repo => repo.projectId && repo.repositoryId)
}

function resolveResource(resource: string | undefined, config: ConnectorRuntimeConfig | null): {
  resource: AzureDevOpsResource
  state?: string
  days?: number
  branch?: string
} {
  const trimmed = (resource ?? '').trim()
  const fallback = String(config?.defaultResource ?? 'repositories')
  if (!trimmed) {
    return {
      resource: isAzureDevOpsResource(fallback) ? fallback : 'repositories',
      state: String(config?.pullRequestState ?? 'active'),
      days: Number(config?.daysLookback ?? 30),
      branch: String(config?.branchFilter ?? ''),
    }
  }
  const url = new URL(trimmed.includes('?') ? `https://local/${trimmed}` : `https://local/${trimmed}?`)
  const pathname = url.pathname.replace(/^\//, '')
  return {
    resource: isAzureDevOpsResource(pathname) ? pathname : 'repositories',
    state: url.searchParams.get('state') ?? undefined,
    days: url.searchParams.get('days') ? Number(url.searchParams.get('days')) : undefined,
    branch: url.searchParams.get('branch') ?? undefined,
  }
}

function isAzureDevOpsResource(value: string): value is AzureDevOpsResource {
  return ['repositories', 'commits', 'pullRequests', 'branches', 'workItems', 'pipelines', 'pipelineRuns'].includes(value)
}

async function refreshEntraToken(credentials: Extract<AzureDevOpsCredentials, { mode: 'entra' }>): Promise<Extract<AzureDevOpsCredentials, { mode: 'entra' }>> {
  if (!credentials.refreshToken) return credentials
  const params = new URLSearchParams({
    client_id: credentials.clientId,
    client_secret: credentials.clientSecret,
    grant_type: 'refresh_token',
    refresh_token: credentials.refreshToken,
    scope: AZDO_SCOPE,
  })
  const response = await fetch(`https://login.microsoftonline.com/${credentials.tenantId}/oauth2/v2.0/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: params.toString(),
    signal: AbortSignal.timeout(15_000),
  })
  if (!response.ok) return credentials
  const body = await response.json() as {
    access_token?: string
    refresh_token?: string
    expires_in?: number
    scope?: string
    token_type?: string
  }
  if (!body.access_token) return credentials
  return {
    ...credentials,
    accessToken: body.access_token,
    refreshToken: body.refresh_token ?? credentials.refreshToken,
    expiresAt: Date.now() + Number(body.expires_in ?? 3600) * 1000,
    scope: body.scope ?? credentials.scope,
    tokenType: body.token_type ?? credentials.tokenType,
  }
}

async function resolveAccessContext(credentials: AzureDevOpsCredentials, options: { connectorId?: string } = {}): Promise<AzureDevOpsAccessContext> {
  if (credentials.mode === 'pat') {
    return {
      authHeader: basicPatHeader(credentials.pat),
      credentials,
    }
  }

  let nextCreds = credentials
  if (credentials.expiresAt && credentials.expiresAt - Date.now() < 120_000) {
    nextCreds = await refreshEntraToken(credentials)
    if (options.connectorId && nextCreds.accessToken !== credentials.accessToken) {
      setAzureDevOpsCreds(options.connectorId, nextCreds)
    }
  }
  return {
    authHeader: `Bearer ${nextCreds.accessToken}`,
    credentials: nextCreds,
  }
}

async function adoRequest<T>(url: string, context: AzureDevOpsAccessContext, options: { method?: string; body?: string; headers?: Record<string, string> } = {}): Promise<T> {
  let lastError: Error | null = null
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const response = await fetch(url, {
        method: options.method ?? 'GET',
        headers: {
          Accept: 'application/json',
          Authorization: context.authHeader,
          ...(options.body ? { 'Content-Type': 'application/json' } : {}),
          ...(options.headers ?? {}),
        },
        body: options.body,
        signal: AbortSignal.timeout(20_000),
      })
      if ((response.status >= 500 || response.status === 429) && attempt < 2) {
        await new Promise(resolve => setTimeout(resolve, 500 * (attempt + 1)))
        continue
      }
      if (!response.ok) {
        const text = await response.text()
        throw new Error(`Azure DevOps API ${response.status}: ${text.slice(0, 400)}`)
      }
      return await response.json() as T
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error))
    }
  }
  throw lastError ?? new Error('Azure DevOps request failed')
}

function orgBase(organization: string): string {
  return `https://dev.azure.com/${normalizeOrganizationName(organization)}`
}

export async function listAzureDevOpsOrganizations(credentials: AzureDevOpsCredentials): Promise<AzureDevOpsOrganizationRecord[]> {
  const context = await resolveAccessContext(credentials)
  const profile = await adoRequest<{ publicAlias?: string; emailAddress?: string; id?: string }>(
    `${AZDO_ACCOUNTS_URL}/_apis/profile/profiles/me?api-version=7.1-preview.3`,
    context,
  )
  const memberId = String(profile.id ?? '')
  if (!memberId) return []
  const accounts = await adoRequest<{ value?: Array<Record<string, unknown>> }>(
    `${AZDO_ACCOUNTS_URL}/_apis/accounts?memberId=${encodeURIComponent(memberId)}&api-version=7.1-preview.1`,
    context,
  )
  return (accounts.value ?? []).map(account => ({
    accountId: String(account.accountId ?? ''),
    accountName: String(account.accountName ?? ''),
    accountUri: String(account.accountUri ?? ''),
  })).filter(account => account.accountName)
}

export async function listAzureDevOpsProjects(credentials: AzureDevOpsCredentials, organization: string): Promise<AzureDevOpsProjectRecord[]> {
  const context = await resolveAccessContext(credentials)
  const response = await adoRequest<{ value?: Array<Record<string, unknown>> }>(
    `${orgBase(organization)}/_apis/projects?api-version=7.1-preview.4`,
    context,
  )
  return (response.value ?? []).map(project => ({
    id: String(project.id ?? ''),
    name: String(project.name ?? ''),
    description: project.description ? String(project.description) : undefined,
    visibility: project.visibility ? String(project.visibility) : undefined,
  })).filter(project => project.id && project.name)
}

export async function listAzureDevOpsRepositories(
  credentials: AzureDevOpsCredentials,
  organization: string,
  projects: AzureDevOpsProjectSelection[],
): Promise<AzureDevOpsRepositoryRecord[]> {
  const context = await resolveAccessContext(credentials)
  const repos: AzureDevOpsRepositoryRecord[] = []
  for (const project of projects) {
    const response = await adoRequest<{ value?: Array<Record<string, unknown>> }>(
      `${orgBase(organization)}/${encodeURIComponent(project.name)}/_apis/git/repositories?api-version=7.1`,
      context,
    )
    for (const repo of response.value ?? []) {
      repos.push({
        projectId: project.id,
        projectName: project.name,
        repositoryId: String(repo.id ?? ''),
        repositoryName: String(repo.name ?? ''),
        fullName: `${project.name}/${String(repo.name ?? '')}`,
        defaultBranch: repo.defaultBranch ? String(repo.defaultBranch) : undefined,
        size: typeof repo.size === 'number' ? repo.size : undefined,
        remoteUrl: repo.remoteUrl ? String(repo.remoteUrl) : undefined,
      })
    }
  }
  return repos
}

export async function validateAzureDevOpsCredentials(
  credentials: AzureDevOpsCredentials,
): Promise<{ organizations: AzureDevOpsOrganizationRecord[]; accountName: string }> {
  const organizations = await listAzureDevOpsOrganizations(credentials)
  const accountName = credentials.mode === 'pat'
    ? credentials.username ?? ''
    : credentials.accountName ?? ''
  return { organizations, accountName }
}

export async function fetchAzureDevOpsRows(
  connectorId: string,
  resourceInput: string | undefined,
  options: { rowLimit?: number } = {},
): Promise<Record<string, unknown>[]> {
  const credentials = getAzureDevOpsCreds(connectorId)
  if (!credentials) throw new Error('Azure DevOps credentials not found')

  const config = getConnectorRuntimeConfig(connectorId)
  const organization = normalizeOrganizationName(String(config?.organization ?? credentials.organization ?? ''))
  if (!organization) throw new Error('Azure DevOps organization is required')
  const projects = parseSelectedProjects(config)
  const repos = parseSelectedRepos(config)
  const { resource, state, days, branch } = resolveResource(resourceInput, config)
  const rowLimit = Math.max(1, options.rowLimit ?? 500)
  const context = await resolveAccessContext(credentials, { connectorId })

  if (resource === 'repositories') {
    const selectedRepos = await listAzureDevOpsRepositories(credentials, organization, projects)
    const allowed = new Set(repos.map(repo => repo.repositoryId))
    return selectedRepos
      .filter(repo => allowed.size === 0 || allowed.has(repo.repositoryId))
      .slice(0, rowLimit)
      .map(repo => ({
        organization,
        projectId: repo.projectId,
        projectName: repo.projectName,
        repositoryId: repo.repositoryId,
        repositoryName: repo.repositoryName,
        fullName: repo.fullName,
        defaultBranch: repo.defaultBranch ?? null,
        size: repo.size ?? null,
        remoteUrl: repo.remoteUrl ?? null,
      }))
  }

  if (resource === 'pipelines' || resource === 'pipelineRuns') {
    const rows: Record<string, unknown>[] = []
    for (const project of projects) {
      if (rows.length >= rowLimit) break
      const pipelines = await adoRequest<{ value?: Array<Record<string, unknown>> }>(
        `${orgBase(organization)}/${encodeURIComponent(project.name)}/_apis/pipelines?api-version=7.1`,
        context,
      )
      if (resource === 'pipelines') {
        for (const pipeline of pipelines.value ?? []) {
          if (rows.length >= rowLimit) break
          rows.push({
            organization,
            projectId: project.id,
            projectName: project.name,
            pipelineId: Number(pipeline.id ?? 0),
            pipelineName: String(pipeline.name ?? ''),
            folder: (pipeline.folder as string | undefined) ?? null,
            revision: pipeline.revision ?? null,
            url: pipeline.url ?? null,
          })
        }
        continue
      }

      const daysBack = Math.max(1, days ?? Number(config?.pipelineRunDays ?? 14) ?? 14)
      for (const pipeline of pipelines.value ?? []) {
        if (rows.length >= rowLimit) break
        const pipelineId = Number(pipeline.id ?? 0)
        const runs = await adoRequest<{ value?: Array<Record<string, unknown>> }>(
          `${orgBase(organization)}/${encodeURIComponent(project.name)}/_apis/pipelines/${pipelineId}/runs?api-version=7.1`,
          context,
        )
        const cutoff = Date.now() - daysBack * 24 * 60 * 60 * 1000
        for (const run of runs.value ?? []) {
          const createdDate = Date.parse(String(run.createdDate ?? run.createdOn ?? ''))
          if (Number.isFinite(createdDate) && createdDate < cutoff) continue
          if (rows.length >= rowLimit) break
          rows.push({
            organization,
            projectId: project.id,
            projectName: project.name,
            pipelineId,
            pipelineName: String(pipeline.name ?? ''),
            runId: Number(run.id ?? 0),
            name: String(run.name ?? ''),
            state: String(run.state ?? ''),
            result: String(run.result ?? ''),
            createdDate: run.createdDate ?? run.createdOn ?? null,
            finishedDate: run.finishedDate ?? null,
          })
        }
      }
    }
    return rows
  }

  if (resource === 'workItems') {
    const rows: Record<string, unknown>[] = []
    const stateFilter = state ?? String(config?.workItemState ?? 'Active')
    const daysBack = Math.max(1, days ?? Number(config?.workItemDays ?? 30) ?? 30)
    const changedAfter = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000).toISOString()
    for (const project of projects) {
      if (rows.length >= rowLimit) break
      const wiql = {
        query: `Select [System.Id] From WorkItems Where [System.TeamProject] = '${project.name.replace(/'/g, "''")}' And [System.State] = '${stateFilter.replace(/'/g, "''")}' And [System.ChangedDate] >= '${changedAfter}' Order By [System.ChangedDate] Desc`,
      }
      const ids = await adoRequest<{ workItems?: Array<{ id: number }> }>(
        `${orgBase(organization)}/${encodeURIComponent(project.name)}/_apis/wit/wiql?api-version=7.1`,
        context,
        { method: 'POST', body: JSON.stringify(wiql) },
      )
      const workItemIds = (ids.workItems ?? []).map(item => item.id).slice(0, rowLimit - rows.length)
      if (workItemIds.length === 0) continue
      const batch = await adoRequest<{ value?: Array<Record<string, unknown>> }>(
        `${orgBase(organization)}/${encodeURIComponent(project.name)}/_apis/wit/workitemsbatch?api-version=7.1`,
        context,
        {
          method: 'POST',
          body: JSON.stringify({
            ids: workItemIds,
            fields: [
              'System.Id',
              'System.Title',
              'System.State',
              'System.WorkItemType',
              'System.AssignedTo',
              'System.AreaPath',
              'System.IterationPath',
              'System.CreatedDate',
              'System.ChangedDate',
            ],
          }),
        },
      )
      for (const item of batch.value ?? []) {
        if (rows.length >= rowLimit) break
        const fields = (item.fields as Record<string, unknown> | undefined) ?? {}
        rows.push({
          organization,
          projectId: project.id,
          projectName: project.name,
          workItemId: Number(item.id ?? 0),
          title: String(fields['System.Title'] ?? ''),
          state: String(fields['System.State'] ?? ''),
          workItemType: String(fields['System.WorkItemType'] ?? ''),
          assignedTo: typeof fields['System.AssignedTo'] === 'object'
            ? String((fields['System.AssignedTo'] as Record<string, unknown>).displayName ?? '')
            : String(fields['System.AssignedTo'] ?? ''),
          areaPath: String(fields['System.AreaPath'] ?? ''),
          iterationPath: String(fields['System.IterationPath'] ?? ''),
          createdDate: fields['System.CreatedDate'] ?? null,
          changedDate: fields['System.ChangedDate'] ?? null,
        })
      }
    }
    return rows
  }

  const rows: Record<string, unknown>[] = []
  const recentDays = Math.max(1, days ?? Number(config?.commitDays ?? 30) ?? 30)
  const cutoffIso = new Date(Date.now() - recentDays * 24 * 60 * 60 * 1000).toISOString()
  const status = state ?? String(config?.pullRequestState ?? 'active')
  const branchFilter = branch ?? String(config?.branchFilter ?? '')
  for (const repo of repos) {
    if (rows.length >= rowLimit) break
    if (resource === 'branches') {
      const refs = await adoRequest<{ value?: Array<Record<string, unknown>> }>(
        `${orgBase(organization)}/${encodeURIComponent(repo.projectName)}/_apis/git/repositories/${repo.repositoryId}/refs?filter=heads/&api-version=7.1`,
        context,
      )
      for (const ref of refs.value ?? []) {
        if (rows.length >= rowLimit) break
        rows.push({
          organization,
          projectId: repo.projectId,
          projectName: repo.projectName,
          repositoryId: repo.repositoryId,
          repositoryName: repo.repositoryName,
          name: String(ref.name ?? ''),
          objectId: String(ref.objectId ?? ''),
          creator: typeof ref.creator === 'object' ? String((ref.creator as Record<string, unknown>).displayName ?? '') : null,
        })
      }
      continue
    }

    if (resource === 'pullRequests') {
      const prs = await adoRequest<{ value?: Array<Record<string, unknown>> }>(
        `${orgBase(organization)}/${encodeURIComponent(repo.projectName)}/_apis/git/repositories/${repo.repositoryId}/pullrequests?searchCriteria.status=${encodeURIComponent(status)}&api-version=7.1`,
        context,
      )
      for (const pr of prs.value ?? []) {
        if (rows.length >= rowLimit) break
        rows.push({
          organization,
          projectId: repo.projectId,
          projectName: repo.projectName,
          repositoryId: repo.repositoryId,
          repositoryName: repo.repositoryName,
          pullRequestId: Number(pr.pullRequestId ?? 0),
          title: String(pr.title ?? ''),
          status: String(pr.status ?? ''),
          createdBy: typeof pr.createdBy === 'object' ? String((pr.createdBy as Record<string, unknown>).displayName ?? '') : null,
          creationDate: pr.creationDate ?? null,
          sourceRefName: pr.sourceRefName ?? null,
          targetRefName: pr.targetRefName ?? null,
          mergeStatus: pr.mergeStatus ?? null,
        })
      }
      continue
    }

    if (resource === 'commits') {
      const qs = new URLSearchParams({
        'searchCriteria.fromDate': cutoffIso,
        '$top': String(Math.min(100, rowLimit)),
      })
      if (branchFilter) qs.set('searchCriteria.itemVersion.version', branchFilter)
      const commits = await adoRequest<{ value?: Array<Record<string, unknown>> }>(
        `${orgBase(organization)}/${encodeURIComponent(repo.projectName)}/_apis/git/repositories/${repo.repositoryId}/commits?${qs.toString()}&api-version=7.1`,
        context,
      )
      for (const commit of commits.value ?? []) {
        if (rows.length >= rowLimit) break
        rows.push({
          organization,
          projectId: repo.projectId,
          projectName: repo.projectName,
          repositoryId: repo.repositoryId,
          repositoryName: repo.repositoryName,
          commitId: String(commit.commitId ?? ''),
          comment: String(commit.comment ?? ''),
          authorName: typeof commit.author === 'object' ? String((commit.author as Record<string, unknown>).name ?? '') : null,
          authorEmail: typeof commit.author === 'object' ? String((commit.author as Record<string, unknown>).email ?? '') : null,
          authorDate: typeof commit.author === 'object' ? (commit.author as Record<string, unknown>).date ?? null : null,
        })
      }
    }
  }
  return rows
}
