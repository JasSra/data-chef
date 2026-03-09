import 'server-only'

import { getDatasets } from '@/lib/datasets'
import { getConnectors } from '@/lib/connectors'
import { getPipelines } from '@/lib/pipelines'
import { listRecipes } from '@/lib/query-recipes'
import { listAllSavedQueries } from '@/lib/saved-queries'
import { getAppSettings } from '@/lib/app-settings'
import { getSearchVersion } from '@/lib/search-cache'
import { getCurrentTenantContext } from '@/lib/tenant'

export type SearchEntityKind = 'page' | 'dataset' | 'connector' | 'pipeline' | 'recipe' | 'saved_query'

export interface SearchDocument {
  id: string
  tenantId: string
  kind: SearchEntityKind
  title: string
  subtitle: string
  keywords: string[]
  href: string
  status?: string
  icon?: string
  updatedAt?: number
  scoreHints?: string[]
}

export interface SearchResult {
  document: SearchDocument
  score: number
  matchedFields: string[]
}

export interface SearchResultGroup {
  kind: SearchEntityKind
  label: string
  results: SearchResult[]
}

export interface SearchIndexProvider {
  getDocuments(): SearchDocument[]
}

const pageDocs = [
  { id: 'page_datasets', kind: 'page', title: 'Datasets', subtitle: 'Browse datasets', href: '/datasets', icon: 'database', keywords: ['data', 'schema', 'ingest'] },
  { id: 'page_query', kind: 'page', title: 'Query', subtitle: 'Run SQL, JSONPath, JMESPath, KQL, and Redis queries', href: '/query', icon: 'code', keywords: ['editor', 'sql', 'kql', 'jsonpath'] },
  { id: 'page_pipelines', kind: 'page', title: 'Pipelines', subtitle: 'Build and run pipelines', href: '/pipelines', icon: 'git-branch', keywords: ['etl', 'workflow', 'builder'] },
  { id: 'page_connections', kind: 'page', title: 'Connections', subtitle: 'Manage connectors', href: '/connections', icon: 'plug', keywords: ['connectors', 'sources', 'integrations'] },
  { id: 'page_settings', kind: 'page', title: 'Settings', subtitle: 'Configure tenant defaults, branding, and runtime settings', href: '/settings', icon: 'settings', keywords: ['workspace', 'tenant', 'branding', 'region'] },
  { id: 'page_about', kind: 'page', title: 'About', subtitle: 'Product, build, and support information', href: '/about', icon: 'info', keywords: ['version', 'branding', 'support'] },
] as const

const kindLabels: Record<SearchEntityKind, string> = {
  page: 'Pages',
  dataset: 'Datasets',
  connector: 'Connectors',
  pipeline: 'Pipelines',
  recipe: 'Recipes',
  saved_query: 'Saved Queries',
}

function tokenize(query: string): string[] {
  return query
    .toLowerCase()
    .split(/[^a-z0-9]+/i)
    .map(token => token.trim())
    .filter(Boolean)
}

function extractHost(endpoint: string): string {
  try {
    return new URL(endpoint).host
  } catch {
    return endpoint.replace(/^https?:\/\//, '').split('/')[0] ?? endpoint
  }
}

function buildDocuments(): SearchDocument[] {
  const tenant = getCurrentTenantContext()
  const settings = getAppSettings()
  const connectors = getConnectors()
  const datasets = getDatasets()
  const pipelines = getPipelines()
  const recipes = listRecipes()
  const savedQueries = listAllSavedQueries()

  const pages: SearchDocument[] = pageDocs.map(doc => ({
    ...doc,
    keywords: [...doc.keywords],
    tenantId: tenant.tenantId,
    scoreHints: [settings.branding.productName, settings.workspace.workspaceName],
  }))

  const datasetDocs: SearchDocument[] = datasets.map(dataset => ({
    id: dataset.id,
    tenantId: tenant.tenantId,
    kind: 'dataset',
    title: dataset.name,
    subtitle: dataset.description,
    keywords: [
      dataset.source,
      dataset.connection,
      dataset.format,
      dataset.status,
      ...(dataset.schema?.map(field => field.field) ?? []),
    ].filter(Boolean),
    href: '/datasets',
    status: dataset.status,
    icon: 'database',
    updatedAt: dataset.createdAt,
    scoreHints: [dataset.id, dataset.connectorId ?? '', dataset.resource ?? ''],
  }))

  const connectorDocs: SearchDocument[] = connectors.map(connector => ({
    id: connector.id,
    tenantId: tenant.tenantId,
    kind: 'connector',
    title: connector.name,
    subtitle: connector.description || connector.endpoint,
    keywords: [
      connector.type,
      connector.status,
      extractHost(connector.endpoint),
      ...connector.datasets,
    ].filter(Boolean),
    href: '/connections',
    status: connector.status,
    icon: 'plug',
    updatedAt: connector.lastSyncAt ?? connector.createdAt,
    scoreHints: [connector.id, connector.endpoint],
  }))

  const pipelineDocs: SearchDocument[] = pipelines.map(pipeline => ({
    id: pipeline.id,
    tenantId: tenant.tenantId,
    kind: 'pipeline',
    title: pipeline.name,
    subtitle: pipeline.description,
    keywords: [
      pipeline.notes ?? '',
      pipeline.status,
      pipeline.sourceId ?? pipeline.dataset,
      pipeline.outputTarget?.datasetName ?? '',
      pipeline.resource ?? '',
    ].filter(Boolean),
    href: '/pipelines',
    status: pipeline.status,
    icon: 'git-branch',
    updatedAt: pipeline.runtimeSteps?.length ? Date.now() : undefined,
    scoreHints: [pipeline.id, pipeline.dataset],
  }))

  const recipeDocs: SearchDocument[] = recipes.map(recipe => ({
    id: recipe.id,
    tenantId: tenant.tenantId,
    kind: 'recipe',
    title: recipe.name,
    subtitle: recipe.description,
    keywords: [
      recipe.lang,
      ...recipe.sources.map(source => source.alias),
      ...recipe.sources.map(source => source.sourceId),
    ].filter(Boolean),
    href: '/query',
    icon: 'book-open',
    updatedAt: recipe.updatedAt,
    scoreHints: [recipe.id],
  }))

  const savedQueryDocs: SearchDocument[] = savedQueries.map(query => {
    const connector = connectors.find(item => item.id === query.connectorId)
    return {
      id: query.id,
      tenantId: tenant.tenantId,
      kind: 'saved_query' as const,
      title: query.name,
      subtitle: connector ? `${connector.name} saved query` : 'Saved query',
      keywords: [
        connector?.name ?? '',
        connector?.type ?? '',
        query.kql.split(/\s+/).slice(0, 12).join(' '),
      ].filter(Boolean),
      href: '/query',
      status: connector?.status,
      icon: 'bookmark',
      updatedAt: query.createdAt,
      scoreHints: [query.id, query.connectorId],
    }
  })

  return [...pages, ...datasetDocs, ...connectorDocs, ...pipelineDocs, ...recipeDocs, ...savedQueryDocs]
}

class InMemorySearchIndexProvider implements SearchIndexProvider {
  private cache = new Map<string, { version: number; documents: SearchDocument[] }>()

  getDocuments(): SearchDocument[] {
    const tenant = getCurrentTenantContext()
    const version = getSearchVersion(tenant.tenantId)
    const cached = this.cache.get(tenant.tenantId)
    if (cached && cached.version === version) return cached.documents

    const documents = buildDocuments()
    this.cache.set(tenant.tenantId, { version, documents })
    return documents
  }
}

const indexProvider: SearchIndexProvider = new InMemorySearchIndexProvider()

export function searchDocuments(query: string, limit = 20): SearchResultGroup[] {
  const normalizedQuery = query.trim()
  if (!normalizedQuery) return []

  const tokens = tokenize(normalizedQuery)
  if (tokens.length === 0) return []

  const results = indexProvider.getDocuments()
    .map(document => scoreDocument(document, tokens))
    .filter((result): result is SearchResult => result !== null)
    .sort((a, b) => b.score - a.score || (b.document.updatedAt ?? 0) - (a.document.updatedAt ?? 0))
    .slice(0, limit)

  const grouped = new Map<SearchEntityKind, SearchResult[]>()
  for (const result of results) {
    const list = grouped.get(result.document.kind) ?? []
    list.push(result)
    grouped.set(result.document.kind, list)
  }

  return Array.from(grouped.entries()).map(([kind, groupResults]) => ({
    kind,
    label: kindLabels[kind],
    results: groupResults,
  }))
}

function scoreDocument(document: SearchDocument, tokens: string[]): SearchResult | null {
  let score = 0
  const matchedFields = new Set<string>()
  const title = document.title.toLowerCase()
  const subtitle = document.subtitle.toLowerCase()
  const keywords = document.keywords.join(' ').toLowerCase()
  const exacts = [document.id, ...(document.scoreHints ?? [])].map(value => value.toLowerCase())

  for (const token of tokens) {
    let matched = false
    if (title === token || exacts.includes(token)) {
      score += 120
      matchedFields.add('exact')
      matched = true
    }
    if (title.includes(token)) {
      score += 60
      matchedFields.add('title')
      matched = true
    }
    if (subtitle.includes(token)) {
      score += 24
      matchedFields.add('subtitle')
      matched = true
    }
    if (keywords.includes(token)) {
      score += 12
      matchedFields.add('keywords')
      matched = true
    }
    if (!matched) return null
  }

  return {
    document,
    score,
    matchedFields: Array.from(matchedFields),
  }
}
