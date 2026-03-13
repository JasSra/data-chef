/**
 * OpenAPI Normalizer — converts Swagger v2 specs to OpenAPI v3 format.
 * All downstream code works with v3 only.
 */

/* ── Types ─────────────────────────────────────────────────────────── */

export interface OpenApiSpec {
  openapi: string
  info: { title: string; version: string; description?: string }
  servers: { url: string; description?: string }[]
  paths: Record<string, PathItem>
  components?: { schemas?: Record<string, SchemaObject>; securitySchemes?: Record<string, SecurityScheme> }
  tags?: { name: string; description?: string }[]
  security?: Record<string, string[]>[]
}

export interface PathItem {
  get?: OperationObject
  post?: OperationObject
  put?: OperationObject
  delete?: OperationObject
  patch?: OperationObject
  parameters?: ParameterObject[]
}

export interface OperationObject {
  operationId?: string
  summary?: string
  description?: string
  tags?: string[]
  parameters?: ParameterObject[]
  requestBody?: RequestBodyObject
  responses?: Record<string, ResponseObject>
  security?: Record<string, string[]>[]
  deprecated?: boolean
}

export interface ParameterObject {
  name: string
  in: 'query' | 'path' | 'header' | 'cookie'
  description?: string
  required?: boolean
  schema?: SchemaObject
  type?: string        // v2 compat
  enum?: unknown[]     // v2 compat
  default?: unknown
}

export interface RequestBodyObject {
  description?: string
  required?: boolean
  content?: Record<string, { schema?: SchemaObject }>
}

export interface ResponseObject {
  description?: string
  content?: Record<string, { schema?: SchemaObject }>
}

export interface SchemaObject {
  type?: string
  format?: string
  description?: string
  properties?: Record<string, SchemaObject>
  items?: SchemaObject
  required?: string[]
  enum?: unknown[]
  default?: unknown
  $ref?: string
  allOf?: SchemaObject[]
  oneOf?: SchemaObject[]
  anyOf?: SchemaObject[]
  additionalProperties?: boolean | SchemaObject
  nullable?: boolean
}

export interface SecurityScheme {
  type: string
  name?: string
  in?: string
  scheme?: string
  bearerFormat?: string
  flows?: Record<string, unknown>
  description?: string
}

/* ── Swagger v2 types (partial) ────────────────────────────────────── */

interface SwaggerV2Spec {
  swagger: string
  info: { title: string; version: string; description?: string }
  host?: string
  basePath?: string
  schemes?: string[]
  paths: Record<string, Record<string, SwaggerV2Operation>>
  definitions?: Record<string, SchemaObject>
  securityDefinitions?: Record<string, SecurityScheme>
  tags?: { name: string; description?: string }[]
  security?: Record<string, string[]>[]
  produces?: string[]
  consumes?: string[]
}

interface SwaggerV2Operation {
  operationId?: string
  summary?: string
  description?: string
  tags?: string[]
  parameters?: SwaggerV2Parameter[]
  responses?: Record<string, SwaggerV2Response>
  security?: Record<string, string[]>[]
  deprecated?: boolean
  produces?: string[]
  consumes?: string[]
}

interface SwaggerV2Parameter {
  name: string
  in: 'query' | 'path' | 'header' | 'body' | 'formData'
  description?: string
  required?: boolean
  type?: string
  format?: string
  enum?: unknown[]
  default?: unknown
  schema?: SchemaObject
  items?: SchemaObject
}

interface SwaggerV2Response {
  description?: string
  schema?: SchemaObject
}

/* ── Detection ─────────────────────────────────────────────────────── */

export type SpecVersion = 'swagger-2.0' | 'openapi-3.x' | 'unknown'

export function detectSpecVersion(spec: Record<string, unknown>): SpecVersion {
  if (typeof spec.swagger === 'string' && spec.swagger.startsWith('2.')) return 'swagger-2.0'
  if (typeof spec.openapi === 'string' && spec.openapi.startsWith('3.')) return 'openapi-3.x'
  return 'unknown'
}

/* ── Normalize ─────────────────────────────────────────────────────── */

export function normalizeToOpenApiV3(raw: Record<string, unknown>): OpenApiSpec {
  const version = detectSpecVersion(raw)
  if (version === 'openapi-3.x') return raw as unknown as OpenApiSpec
  if (version === 'swagger-2.0') return convertSwaggerV2ToV3(raw as unknown as SwaggerV2Spec)
  throw new Error('Unsupported spec format: expected Swagger 2.0 or OpenAPI 3.x')
}

function convertSwaggerV2ToV3(v2: SwaggerV2Spec): OpenApiSpec {
  const scheme = v2.schemes?.[0] ?? 'https'
  const host = v2.host ?? 'localhost'
  const basePath = v2.basePath ?? '/'
  const serverUrl = `${scheme}://${host}${basePath === '/' ? '' : basePath}`

  const spec: OpenApiSpec = {
    openapi: '3.0.3',
    info: { ...v2.info },
    servers: [{ url: serverUrl }],
    paths: {},
    components: {
      schemas: convertRefs(v2.definitions ?? {}),
      securitySchemes: convertSecurityDefs(v2.securityDefinitions ?? {}),
    },
    tags: v2.tags,
    security: v2.security,
  }

  for (const [path, methods] of Object.entries(v2.paths)) {
    const pathItem: PathItem = {}
    const sharedParams: ParameterObject[] = []

    for (const [method, operation] of Object.entries(methods)) {
      if (method === 'parameters') {
        // Shared path-level parameters
        sharedParams.push(...(operation as unknown as SwaggerV2Parameter[]).map(convertParameter).filter(isNonBody))
        pathItem.parameters = sharedParams
        continue
      }

      const httpMethod = method.toLowerCase() as keyof PathItem
      if (!['get', 'post', 'put', 'delete', 'patch'].includes(httpMethod)) continue

      const op = operation as SwaggerV2Operation
      const v3Op: OperationObject = {
        operationId: op.operationId,
        summary: op.summary,
        description: op.description,
        tags: op.tags,
        deprecated: op.deprecated,
        security: op.security,
      }

      // Split parameters: body -> requestBody, others -> parameters
      const params = op.parameters ?? []
      const nonBodyParams = params.filter(p => p.in !== 'body').map(convertParameter)
      const bodyParam = params.find(p => p.in === 'body')

      if (nonBodyParams.length > 0) v3Op.parameters = nonBodyParams
      if (bodyParam) {
        const contentType = op.consumes?.[0] ?? v2.consumes?.[0] ?? 'application/json'
        v3Op.requestBody = {
          description: bodyParam.description,
          required: bodyParam.required,
          content: {
            [contentType]: { schema: rewriteRef(bodyParam.schema ?? {}) },
          },
        }
      }

      // Convert responses
      if (op.responses) {
        const produces = op.produces?.[0] ?? v2.produces?.[0] ?? 'application/json'
        v3Op.responses = {}
        for (const [code, resp] of Object.entries(op.responses)) {
          const v3Resp: ResponseObject = { description: resp.description ?? '' }
          if (resp.schema) {
            v3Resp.content = { [produces]: { schema: rewriteRef(resp.schema) } }
          }
          v3Op.responses[code] = v3Resp
        }
      }

      ;(pathItem as Record<string, unknown>)[httpMethod] = v3Op
    }

    spec.paths[path] = pathItem
  }

  return spec
}

function convertParameter(p: SwaggerV2Parameter): ParameterObject {
  const param: ParameterObject = {
    name: p.name,
    in: p.in === 'body' ? 'query' : p.in === 'formData' ? 'query' : p.in,
    description: p.description,
    required: p.required,
  }

  if (p.schema) {
    param.schema = rewriteRef(p.schema)
  } else {
    param.schema = {
      type: p.type,
      format: p.format,
      enum: p.enum,
      default: p.default,
      items: p.items ? rewriteRef(p.items) : undefined,
    }
  }

  return param
}

function isNonBody(p: ParameterObject): boolean {
  return (p.in as string) !== 'body'
}

function convertSecurityDefs(defs: Record<string, SecurityScheme>): Record<string, SecurityScheme> {
  const result: Record<string, SecurityScheme> = {}
  for (const [name, def] of Object.entries(defs)) {
    if (def.type === 'apiKey') {
      result[name] = { ...def }
    } else if (def.type === 'basic') {
      result[name] = { type: 'http', scheme: 'basic', description: def.description }
    } else if (def.type === 'oauth2') {
      result[name] = { ...def }
    } else {
      result[name] = { ...def }
    }
  }
  return result
}

/** Rewrite `#/definitions/Foo` to `#/components/schemas/Foo` */
function rewriteRef(schema: SchemaObject): SchemaObject {
  if (!schema) return schema
  const result = { ...schema }
  if (result.$ref) {
    result.$ref = result.$ref.replace('#/definitions/', '#/components/schemas/')
  }
  if (result.items) result.items = rewriteRef(result.items)
  if (result.properties) {
    result.properties = Object.fromEntries(
      Object.entries(result.properties).map(([k, v]) => [k, rewriteRef(v)]),
    )
  }
  if (result.allOf) result.allOf = result.allOf.map(rewriteRef)
  if (result.oneOf) result.oneOf = result.oneOf.map(rewriteRef)
  if (result.anyOf) result.anyOf = result.anyOf.map(rewriteRef)
  if (typeof result.additionalProperties === 'object' && result.additionalProperties) {
    result.additionalProperties = rewriteRef(result.additionalProperties as SchemaObject)
  }
  return result
}

function convertRefs(definitions: Record<string, SchemaObject>): Record<string, SchemaObject> {
  return Object.fromEntries(
    Object.entries(definitions).map(([k, v]) => [k, rewriteRef(v)]),
  )
}
