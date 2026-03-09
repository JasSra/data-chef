import 'server-only'

import { createHash, createSign, randomUUID } from 'node:crypto'

import type { AzureB2CCredentials, AzureEntraIdCredentials } from '@/lib/connectors'

interface TokenEntry {
  token: string
  expiresAt: number
}

export interface AzureGraphResourceSpec {
  family: 'users' | 'userFlows' | 'customPolicies' | 'groups' | 'applications'
  label: 'users' | 'userFlows' | 'customPolicies' | 'groups' | 'applications'
  path: string
  isBeta: boolean
}

export interface AzureGraphRowsResult {
  rows: Record<string, unknown>[]
  spec: AzureGraphResourceSpec
}

type GraphCredentials = AzureB2CCredentials | AzureEntraIdCredentials

const GRAPH_SCOPE = 'https://graph.microsoft.com/.default'
const GRAPH_BASE_URL = 'https://graph.microsoft.com'
const USER_SELECT = 'id,displayName,givenName,surname,mail,userPrincipalName,accountEnabled,createdDateTime,identities'
const tokenCache = new Map<string, TokenEntry>()

function base64UrlEncode(input: string | Buffer): string {
  return Buffer.from(input)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '')
}

function normalizeHexThumbprint(thumbprint: string): string {
  const compact = thumbprint.replace(/[^a-fA-F0-9]/g, '')
  if (compact.length < 40 || compact.length % 2 !== 0) return thumbprint.trim()
  return base64UrlEncode(Buffer.from(compact, 'hex'))
}

function deriveThumbprintFromCertificate(certificatePem: string): string {
  const base64 = certificatePem
    .replace(/-----BEGIN CERTIFICATE-----/g, '')
    .replace(/-----END CERTIFICATE-----/g, '')
    .replace(/\s+/g, '')
  if (!base64) return ''
  const digest = createHash('sha1').update(Buffer.from(base64, 'base64')).digest()
  return base64UrlEncode(digest)
}

function buildClientAssertion(creds: GraphCredentials): string {
  if (!creds.privateKeyPem.trim()) throw new Error('Client certificate private key is required')
  if (!creds.certificatePem.trim()) throw new Error('Client certificate PEM is required')

  const tokenUrl = `https://login.microsoftonline.com/${creds.tenantId}/oauth2/v2.0/token`
  const thumbprint = creds.thumbprint.trim()
    ? normalizeHexThumbprint(creds.thumbprint)
    : deriveThumbprintFromCertificate(creds.certificatePem)

  const header: Record<string, string> = {
    alg: 'RS256',
    typ: 'JWT',
  }
  if (thumbprint) header.x5t = thumbprint

  const now = Math.floor(Date.now() / 1000)
  const payload = {
    aud: tokenUrl,
    iss: creds.clientId,
    sub: creds.clientId,
    jti: randomUUID(),
    nbf: now - 30,
    iat: now,
    exp: now + 600,
  }

  const encodedHeader = base64UrlEncode(JSON.stringify(header))
  const encodedPayload = base64UrlEncode(JSON.stringify(payload))
  const signingInput = `${encodedHeader}.${encodedPayload}`
  const signer = createSign('RSA-SHA256')
  signer.update(signingInput)
  signer.end()
  const signature = signer.sign(creds.privateKeyPem)
  return `${signingInput}.${base64UrlEncode(signature)}`
}

export async function getAzureGraphToken(creds: GraphCredentials): Promise<string> {
  const cacheKey = [
    creds.tenantId,
    creds.clientId,
    creds.authMode,
    creds.thumbprint,
    GRAPH_SCOPE,
  ].join(':')
  const cached = tokenCache.get(cacheKey)
  if (cached && cached.expiresAt - Date.now() > 120_000) return cached.token

  const params = new URLSearchParams({
    grant_type: 'client_credentials',
    client_id: creds.clientId,
    scope: GRAPH_SCOPE,
  })

  if (creds.authMode === 'client_certificate') {
    params.set('client_assertion_type', 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer')
    params.set('client_assertion', buildClientAssertion(creds))
  } else {
    if (!creds.clientSecret.trim()) throw new Error('Client secret is required')
    params.set('client_secret', creds.clientSecret)
  }

  const response = await fetch(`https://login.microsoftonline.com/${creds.tenantId}/oauth2/v2.0/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: params.toString(),
    signal: AbortSignal.timeout(10_000),
  })

  if (!response.ok) {
    const text = await response.text()
    throw new Error(`Microsoft identity token error ${response.status}: ${text.slice(0, 300)}`)
  }

  const body = await response.json() as { access_token: string; expires_in: number }
  tokenCache.set(cacheKey, {
    token: body.access_token,
    expiresAt: Date.now() + body.expires_in * 1000,
  })
  return body.access_token
}

function ensureQueryDefaults(path: string, family: AzureGraphResourceSpec['family'], rowLimit: number): string {
  const url = new URL(path, GRAPH_BASE_URL)
  if (family === 'users' && !url.searchParams.has('$select')) {
    url.searchParams.set('$select', USER_SELECT)
  }
  if (family === 'groups' && !url.searchParams.has('$select')) {
    url.searchParams.set('$select', 'id,displayName,description,mail,mailEnabled,securityEnabled,visibility,createdDateTime')
  }
  if (family === 'applications' && !url.searchParams.has('$select')) {
    url.searchParams.set('$select', 'id,appId,displayName,createdDateTime,signInAudience,publisherDomain')
  }
  if (!url.searchParams.has('$top')) {
    url.searchParams.set('$top', String(Math.max(1, Math.min(rowLimit, 999))))
  }
  return `${url.pathname}${url.search}`
}

export function resolveAzureB2CResource(resource: string | undefined, rowLimit = 500): AzureGraphResourceSpec {
  const trimmed = (resource ?? 'users').trim()
  const alias = trimmed.toLowerCase()

  if (!trimmed || alias === 'users') {
    return {
      family: 'users',
      label: 'users',
      path: ensureQueryDefaults('/v1.0/users', 'users', rowLimit),
      isBeta: false,
    }
  }
  if (alias === 'userflows') {
    return {
      family: 'userFlows',
      label: 'userFlows',
      path: ensureQueryDefaults('/beta/identity/b2cUserFlows', 'userFlows', rowLimit),
      isBeta: true,
    }
  }
  if (alias === 'custompolicies') {
    return {
      family: 'customPolicies',
      label: 'customPolicies',
      path: ensureQueryDefaults('/beta/trustFramework/policies', 'customPolicies', rowLimit),
      isBeta: true,
    }
  }

  const rawPath = trimmed.startsWith('/') ? trimmed : `/${trimmed}`
  if (/^\/(?:v1\.0\/)?users\b/i.test(rawPath)) {
    const normalized = rawPath.startsWith('/v1.0/') ? rawPath : rawPath.replace(/^\/users\b/i, '/v1.0/users')
    return {
      family: 'users',
      label: 'users',
      path: ensureQueryDefaults(normalized, 'users', rowLimit),
      isBeta: false,
    }
  }
  if (/^\/(?:beta\/)?identity\/b2cUserFlows\b/i.test(rawPath)) {
    const normalized = rawPath.startsWith('/beta/') ? rawPath : rawPath.replace(/^\/identity\/b2cUserFlows\b/i, '/beta/identity/b2cUserFlows')
    return {
      family: 'userFlows',
      label: 'userFlows',
      path: ensureQueryDefaults(normalized, 'userFlows', rowLimit),
      isBeta: true,
    }
  }
  if (/^\/(?:beta\/)?trustFramework\/policies\b/i.test(rawPath)) {
    const normalized = rawPath.startsWith('/beta/') ? rawPath : rawPath.replace(/^\/trustFramework\/policies\b/i, '/beta/trustFramework/policies')
    return {
      family: 'customPolicies',
      label: 'customPolicies',
      path: ensureQueryDefaults(normalized, 'customPolicies', rowLimit),
      isBeta: true,
    }
  }

  throw new Error('Unsupported Azure AD B2C resource. Use users, userFlows, customPolicies, or a matching Microsoft Graph path.')
}

export function resolveAzureEntraIdResource(resource: string | undefined, rowLimit = 500): AzureGraphResourceSpec {
  const trimmed = (resource ?? 'users').trim()
  const alias = trimmed.toLowerCase()

  if (!trimmed || alias === 'users') {
    return {
      family: 'users',
      label: 'users',
      path: ensureQueryDefaults('/v1.0/users', 'users', rowLimit),
      isBeta: false,
    }
  }
  if (alias === 'groups') {
    return {
      family: 'groups',
      label: 'groups',
      path: ensureQueryDefaults('/v1.0/groups', 'groups', rowLimit),
      isBeta: false,
    }
  }
  if (alias === 'applications') {
    return {
      family: 'applications',
      label: 'applications',
      path: ensureQueryDefaults('/v1.0/applications', 'applications', rowLimit),
      isBeta: false,
    }
  }

  const rawPath = trimmed.startsWith('/') ? trimmed : `/${trimmed}`
  if (/^\/(?:v1\.0\/)?users\b/i.test(rawPath)) {
    const normalized = rawPath.startsWith('/v1.0/') ? rawPath : rawPath.replace(/^\/users\b/i, '/v1.0/users')
    return {
      family: 'users',
      label: 'users',
      path: ensureQueryDefaults(normalized, 'users', rowLimit),
      isBeta: false,
    }
  }
  if (/^\/(?:v1\.0\/)?groups\b/i.test(rawPath)) {
    const normalized = rawPath.startsWith('/v1.0/') ? rawPath : rawPath.replace(/^\/groups\b/i, '/v1.0/groups')
    return {
      family: 'groups',
      label: 'groups',
      path: ensureQueryDefaults(normalized, 'groups', rowLimit),
      isBeta: false,
    }
  }
  if (/^\/(?:v1\.0\/)?applications\b/i.test(rawPath)) {
    const normalized = rawPath.startsWith('/v1.0/') ? rawPath : rawPath.replace(/^\/applications\b/i, '/v1.0/applications')
    return {
      family: 'applications',
      label: 'applications',
      path: ensureQueryDefaults(normalized, 'applications', rowLimit),
      isBeta: false,
    }
  }

  throw new Error('Unsupported Azure Entra ID resource. Use users, groups, applications, or a matching Microsoft Graph path.')
}

function normalizeGraphValue(value: unknown): unknown {
  if (value === null || value === undefined) return null
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return value
  if (Array.isArray(value)) return value.map(item => normalizeGraphValue(item))
  if (typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, entry]) => [key, normalizeGraphValue(entry)]),
    )
  }
  return String(value)
}

export async function fetchAzureB2CRows(
  creds: GraphCredentials,
  resource: string | undefined,
  options: { rowLimit?: number } = {},
): Promise<AzureGraphRowsResult> {
  const rowLimit = Math.max(1, options.rowLimit ?? 500)
  const spec = resolveAzureB2CResource(resource, rowLimit)
  const token = await getAzureGraphToken(creds)
  const rows: Record<string, unknown>[] = []
  let nextUrl: string | null = `${GRAPH_BASE_URL}${spec.path}`

  while (nextUrl && rows.length < rowLimit) {
    const response = await fetch(nextUrl, {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/json',
      },
      signal: AbortSignal.timeout(20_000),
    })

    if (!response.ok) {
      const text = await response.text()
      throw new Error(`Microsoft Graph ${response.status}: ${text.slice(0, 400)}`)
    }

    const body = await response.json() as {
      value?: unknown[]
      '@odata.nextLink'?: string
    }

    const page = Array.isArray(body.value) ? body.value : []
    for (const item of page) {
      if (rows.length >= rowLimit) break
      rows.push(normalizeGraphValue(item) as Record<string, unknown>)
    }
    nextUrl = typeof body['@odata.nextLink'] === 'string' ? body['@odata.nextLink'] : null
  }

  return { rows, spec }
}

export async function fetchAzureEntraIdRows(
  creds: GraphCredentials,
  resource: string | undefined,
  options: { rowLimit?: number } = {},
): Promise<AzureGraphRowsResult> {
  const rowLimit = Math.max(1, options.rowLimit ?? 500)
  const spec = resolveAzureEntraIdResource(resource, rowLimit)
  const token = await getAzureGraphToken(creds)
  const rows: Record<string, unknown>[] = []
  let nextUrl: string | null = `${GRAPH_BASE_URL}${spec.path}`

  while (nextUrl && rows.length < rowLimit) {
    const response = await fetch(nextUrl, {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/json',
      },
      signal: AbortSignal.timeout(20_000),
    })

    if (!response.ok) {
      const text = await response.text()
      throw new Error(`Microsoft Graph ${response.status}: ${text.slice(0, 400)}`)
    }

    const body = await response.json() as {
      value?: unknown[]
      '@odata.nextLink'?: string
    }

    const page = Array.isArray(body.value) ? body.value : []
    for (const item of page) {
      if (rows.length >= rowLimit) break
      rows.push(normalizeGraphValue(item) as Record<string, unknown>)
    }
    nextUrl = typeof body['@odata.nextLink'] === 'string' ? body['@odata.nextLink'] : null
  }

  return { rows, spec }
}
