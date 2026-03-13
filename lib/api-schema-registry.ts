/**
 * API Schema Registry — extracts autocomplete-friendly structures from OpenAPI specs.
 * Parses endpoints, parameters, schemas, and enums for the query editor.
 */

import type { OpenApiSpec, OperationObject, ParameterObject, SchemaObject, PathItem } from '@/lib/openapi-normalizer'

/* ── Extracted types ───────────────────────────────────────────────── */

export interface EndpointInfo {
  path: string
  method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH'
  operationId?: string
  summary?: string
  description?: string
  tags: string[]
  parameters: ParamInfo[]
  requestBody?: RequestBodyInfo
  responseSchema?: ResolvedSchema
  deprecated: boolean
}

export interface ParamInfo {
  name: string
  in: 'query' | 'path' | 'header' | 'cookie'
  description?: string
  required: boolean
  type: string
  format?: string
  enum?: unknown[]
  default?: unknown
}

export interface RequestBodyInfo {
  contentType: string
  required: boolean
  schema?: ResolvedSchema
}

export interface ResolvedSchema {
  type: string
  properties?: Record<string, FieldInfo>
  items?: ResolvedSchema
  enum?: unknown[]
}

export interface FieldInfo {
  name: string
  type: string
  format?: string
  description?: string
  required: boolean
  enum?: unknown[]
  nested?: Record<string, FieldInfo>
}

export interface SchemaRegistry {
  serviceId: string
  version: string
  title: string
  baseUrl: string
  endpoints: EndpointInfo[]
  tags: string[]
  schemas: Record<string, ResolvedSchema>
  /** Flat list of all parameter names across all endpoints for autocomplete */
  allParamNames: string[]
  /** Flat list of all enum values grouped by parameter name */
  enumsByParam: Record<string, unknown[]>
}

/* ── Extract ───────────────────────────────────────────────────────── */

export function extractSchemaRegistry(
  spec: OpenApiSpec,
  serviceId: string,
  version: string,
  excludedEndpoints?: string[],
): SchemaRegistry {
  const exclusionSet = new Set(excludedEndpoints ?? [])
  const endpoints: EndpointInfo[] = []
  const allParamSet = new Set<string>()
  const enumsByParam: Record<string, unknown[]> = {}

  const schemas: Record<string, ResolvedSchema> = {}
  if (spec.components?.schemas) {
    for (const [name, schema] of Object.entries(spec.components.schemas)) {
      schemas[name] = resolveSchema(schema, spec)
    }
  }

  const methods = ['get', 'post', 'put', 'delete', 'patch'] as const

  for (const [pathStr, pathItem] of Object.entries(spec.paths)) {
    const sharedParams = (pathItem as PathItem).parameters ?? []

    for (const method of methods) {
      const op = (pathItem as Record<string, unknown>)[method] as OperationObject | undefined
      if (!op) continue

      const allParams = [...sharedParams, ...(op.parameters ?? [])]
      const params = allParams.map(p => extractParam(p, spec))

      for (const p of params) {
        allParamSet.add(p.name)
        if (p.enum && p.enum.length > 0) {
          enumsByParam[p.name] = p.enum
        }
      }

      let requestBody: RequestBodyInfo | undefined
      if (op.requestBody?.content) {
        const [contentType, mediaType] = Object.entries(op.requestBody.content)[0] ?? []
        if (contentType && mediaType?.schema) {
          requestBody = {
            contentType,
            required: op.requestBody.required ?? false,
            schema: resolveSchema(mediaType.schema, spec),
          }
        }
      }

      let responseSchema: ResolvedSchema | undefined
      const successResponse = op.responses?.['200'] ?? op.responses?.['201'] ?? op.responses?.['default']
      if (successResponse?.content) {
        const [, mediaType] = Object.entries(successResponse.content)[0] ?? []
        if (mediaType?.schema) {
          responseSchema = resolveSchema(mediaType.schema, spec)
        }
      }

      // Skip excluded endpoints (key format: "GET /path")
      const endpointKey = `${method.toUpperCase()} ${pathStr}`
      if (exclusionSet.has(endpointKey)) continue

      endpoints.push({
        path: pathStr,
        method: method.toUpperCase() as EndpointInfo['method'],
        operationId: op.operationId,
        summary: op.summary,
        description: op.description,
        tags: op.tags ?? [],
        parameters: params,
        requestBody,
        responseSchema,
        deprecated: op.deprecated ?? false,
      })
    }
  }

  const tags = [...new Set(endpoints.flatMap(e => e.tags))].sort()

  return {
    serviceId,
    version,
    title: spec.info.title,
    baseUrl: spec.servers?.[0]?.url ?? '',
    endpoints,
    tags,
    schemas,
    allParamNames: [...allParamSet].sort(),
    enumsByParam,
  }
}

/* ── Helpers ───────────────────────────────────────────────────────── */

function extractParam(p: ParameterObject, spec: OpenApiSpec): ParamInfo {
  const schema = p.schema ?? {}
  const resolved = resolveSchemaRef(schema, spec)
  return {
    name: p.name,
    in: p.in,
    description: p.description ?? resolved.description,
    required: p.required ?? false,
    type: resolved.type ?? p.type ?? 'string',
    format: resolved.format,
    enum: resolved.enum ?? p.enum,
    default: resolved.default ?? p.default,
  }
}

function resolveSchemaRef(schema: SchemaObject, spec: OpenApiSpec): SchemaObject {
  if (!schema) return {}
  if (schema.$ref) {
    const refPath = schema.$ref.replace('#/components/schemas/', '')
    const resolved = spec.components?.schemas?.[refPath]
    return resolved ? resolveSchemaRef(resolved, spec) : {}
  }
  return schema
}

function resolveSchema(schema: SchemaObject, spec: OpenApiSpec, depth = 0): ResolvedSchema {
  if (depth > 10) return { type: 'object' }

  const resolved = resolveSchemaRef(schema, spec)
  const result: ResolvedSchema = {
    type: resolved.type ?? 'object',
    enum: resolved.enum,
  }

  if (resolved.properties) {
    result.properties = {}
    const requiredSet = new Set(resolved.required ?? [])
    for (const [name, propSchema] of Object.entries(resolved.properties)) {
      const prop = resolveSchemaRef(propSchema, spec)
      const field: FieldInfo = {
        name,
        type: prop.type ?? 'string',
        format: prop.format,
        description: prop.description,
        required: requiredSet.has(name),
        enum: prop.enum,
      }
      if (prop.properties) {
        const nested = resolveSchema(prop, spec, depth + 1)
        field.nested = nested.properties
      }
      result.properties[name] = field
    }
  }

  if (resolved.items) {
    result.items = resolveSchema(resolved.items, spec, depth + 1)
  }

  // Handle allOf by merging
  if (resolved.allOf) {
    const merged: ResolvedSchema = { type: 'object', properties: {} }
    for (const sub of resolved.allOf) {
      const subResolved = resolveSchema(sub, spec, depth + 1)
      if (subResolved.properties) {
        merged.properties = { ...merged.properties, ...subResolved.properties }
      }
    }
    return merged
  }

  return result
}

/* ── Autocomplete helpers ──────────────────────────────────────────── */

export interface AutocompleteSuggestion {
  label: string
  type: 'keyword' | 'endpoint' | 'param' | 'value' | 'method' | 'operator'
  detail?: string
  insertText: string
}

const KEYWORDS: AutocompleteSuggestion[] = [
  { label: 'service', type: 'keyword', detail: 'Select API service', insertText: 'service("' },
  { label: 'endpoint', type: 'keyword', detail: 'Select endpoint', insertText: 'endpoint("' },
  { label: 'where', type: 'keyword', detail: 'Filter results', insertText: 'where(' },
  { label: 'select', type: 'keyword', detail: 'Choose fields', insertText: 'select(' },
  { label: 'order_by', type: 'keyword', detail: 'Sort results', insertText: 'order_by(' },
  { label: 'limit', type: 'keyword', detail: 'Limit results', insertText: 'limit(' },
  { label: 'offset', type: 'keyword', detail: 'Skip results', insertText: 'offset(' },
  { label: 'body', type: 'keyword', detail: 'Set request body', insertText: 'body(' },
  { label: 'header', type: 'keyword', detail: 'Add custom header', insertText: 'header("' },
  { label: 'chain', type: 'keyword', detail: 'Chain to next endpoint', insertText: 'chain("' },
  { label: 'group_by', type: 'keyword', detail: 'Group results', insertText: 'group_by(' },
  { label: 'aggregate', type: 'keyword', detail: 'Aggregate values', insertText: 'aggregate(' },
  { label: 'no_cache', type: 'keyword', detail: 'Bypass cache', insertText: 'no_cache' },
  { label: 'benchmark', type: 'keyword', detail: 'Run throughput test', insertText: 'benchmark(runs: ' },
]

const OPERATORS: AutocompleteSuggestion[] = [
  { label: '=', type: 'operator', insertText: '= ' },
  { label: '!=', type: 'operator', insertText: '!= ' },
  { label: '>', type: 'operator', insertText: '> ' },
  { label: '<', type: 'operator', insertText: '< ' },
  { label: '>=', type: 'operator', insertText: '>= ' },
  { label: '<=', type: 'operator', insertText: '<= ' },
  { label: 'contains', type: 'operator', insertText: 'contains ' },
  { label: 'startswith', type: 'operator', insertText: 'startswith ' },
]

const AGG_FUNCTIONS: AutocompleteSuggestion[] = [
  { label: 'count()', type: 'keyword', detail: 'Count items', insertText: 'count()' },
  { label: 'sum()', type: 'keyword', detail: 'Sum values', insertText: 'sum(' },
  { label: 'avg()', type: 'keyword', detail: 'Average values', insertText: 'avg(' },
  { label: 'min()', type: 'keyword', detail: 'Minimum value', insertText: 'min(' },
  { label: 'max()', type: 'keyword', detail: 'Maximum value', insertText: 'max(' },
]

export function getSuggestions(
  context: 'stage' | 'endpoint' | 'param' | 'value' | 'aggregate',
  registry?: SchemaRegistry,
  currentEndpoint?: string,
): AutocompleteSuggestion[] {
  switch (context) {
    case 'stage':
      return KEYWORDS

    case 'endpoint':
      if (!registry) return []
      return registry.endpoints.map(e => ({
        label: `${e.method} ${e.path}`,
        type: 'endpoint' as const,
        detail: e.summary ?? e.operationId,
        insertText: `"${e.path}"${e.method !== 'GET' ? `, ${e.method}` : ''}`,
      }))

    case 'param': {
      if (!registry) return []
      if (currentEndpoint) {
        const ep = registry.endpoints.find(e => e.path === currentEndpoint)
        if (ep) {
          return ep.parameters.map(p => ({
            label: p.name,
            type: 'param' as const,
            detail: `${p.type}${p.required ? ' (required)' : ''}`,
            insertText: p.name,
          }))
        }
      }
      return registry.allParamNames.map(name => ({
        label: name,
        type: 'param' as const,
        insertText: name,
      }))
    }

    case 'value': {
      if (!registry) return OPERATORS
      return OPERATORS
    }

    case 'aggregate':
      return AGG_FUNCTIONS

    default:
      return []
  }
}
