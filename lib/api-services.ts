/**
 * Server-side API service registry.
 * Manages Swagger/OpenAPI service definitions with encrypted credentials.
 * Follows the same readState/writeState pattern as connectors.ts.
 */

import 'server-only'

import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import path from 'node:path'
import { readJsonFile, writeJsonFile } from '@/lib/json-store'
import { decryptSecret, encryptSecret, isEncryptedValue } from '@/lib/secret-crypto'
import { getTenantStoreDir } from '@/lib/tenant'

/* ── Types ─────────────────────────────────────────────────────────── */

export type AuthScheme = 'none' | 'api_key' | 'bearer' | 'basic' | 'oauth2'
export type ApiKeyLocation = 'query' | 'header' | 'cookie'

export interface ApiServiceAuth {
  scheme: AuthScheme
  apiKeyName?: string
  apiKeyLocation?: ApiKeyLocation
  apiKeyValue?: string
  bearerToken?: string
  basicUsername?: string
  basicPassword?: string
  oauth2TokenUrl?: string
  oauth2ClientId?: string
  oauth2ClientSecret?: string
  oauth2Scopes?: string[]
}

export interface ApiServiceVersion {
  version: string
  specFileName: string
  fetchedAt: number
  endpointCount: number
  openApiVersion: string
}

export interface ApiServiceRecord {
  id: string
  name: string
  description: string
  baseUrl: string
  swaggerUrl: string
  status: 'active' | 'error' | 'pending'
  auth: ApiServiceAuth
  versions: ApiServiceVersion[]
  activeVersion: string
  tags: string[]
  /** Endpoints to hide from the schema registry, e.g. ["GET /catalog/exports"] */
  excludedEndpoints?: string[]
  /** Allow requests to private/intranet IP ranges (e.g. 10.x, 192.168.x) */
  allowPrivate?: boolean
  /** Custom headers injected into every request for this service */
  customHeaders?: Record<string, string>
  createdAt: number
  updatedAt: number
  lastError?: string
}

interface ApiServiceStateFile {
  services: ApiServiceRecord[]
  authCredsById: Record<string, ApiServiceAuth>
}

/* ── Constants ─────────────────────────────────────────────────────── */

const STATE_FILE = 'api-services.json'

/* ── State management ──────────────────────────────────────────────── */

function seedState(): ApiServiceStateFile {
  return { services: [], authCredsById: {} }
}

function readState(): ApiServiceStateFile {
  const state = readJsonFile<ApiServiceStateFile>(STATE_FILE, seedState())
  return {
    services: (state.services ?? []).map(s => ({
      ...s,
      versions: [...(s.versions ?? [])],
      tags: [...(s.tags ?? [])],
    })),
    authCredsById: { ...(state.authCredsById ?? {}) },
  }
}

function writeState(state: ApiServiceStateFile): void {
  writeJsonFile(STATE_FILE, state)
}

/* ── Credential encryption ─────────────────────────────────────────── */

const SECRET_FIELDS: (keyof ApiServiceAuth)[] = [
  'apiKeyValue', 'bearerToken', 'basicPassword', 'oauth2ClientSecret',
]

function encodeAuth(auth: ApiServiceAuth): ApiServiceAuth {
  const encoded = { ...auth }
  for (const field of SECRET_FIELDS) {
    const val = encoded[field]
    if (typeof val === 'string' && val.trim()) {
      ;(encoded as Record<string, unknown>)[field] = encryptSecret(val)
    }
  }
  return encoded
}

function decodeAuth(auth: ApiServiceAuth): ApiServiceAuth {
  const decoded = { ...auth }
  for (const field of SECRET_FIELDS) {
    const val = decoded[field]
    if (isEncryptedValue(val)) {
      ;(decoded as Record<string, unknown>)[field] = decryptSecret(val)
    } else if (typeof val === 'string') {
      ;(decoded as Record<string, unknown>)[field] = val
    }
  }
  return decoded
}

/* ── Spec file storage ─────────────────────────────────────────────── */

function getSpecDir(serviceId: string): string {
  const dir = path.join(getTenantStoreDir(), 'api-specs', serviceId)
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true })
  return dir
}

export function saveSpec(serviceId: string, version: string, spec: unknown): string {
  const dir = getSpecDir(serviceId)
  const fileName = `${version.replace(/[^a-zA-Z0-9._-]/g, '_')}.json`
  const filePath = path.join(dir, fileName)
  writeFileSync(filePath, JSON.stringify(spec, null, 2), 'utf8')
  return fileName
}

export function loadSpec(serviceId: string, fileName: string): Record<string, unknown> | null {
  const dir = getSpecDir(serviceId)
  const filePath = path.join(dir, fileName)
  if (!existsSync(filePath)) return null
  try {
    return JSON.parse(readFileSync(filePath, 'utf8'))
  } catch {
    return null
  }
}

/* ── CRUD ──────────────────────────────────────────────────────────── */

export function getApiServices(): ApiServiceRecord[] {
  return readState().services
}

export function getApiService(id: string): ApiServiceRecord | null {
  return readState().services.find(s => s.id === id) ?? null
}

export function addApiService(data: {
  name: string
  description?: string
  baseUrl: string
  swaggerUrl: string
  auth?: ApiServiceAuth
  tags?: string[]
  allowPrivate?: boolean
}): ApiServiceRecord {
  const state = readState()
  const id = `api_${Date.now().toString(36)}`
  const auth = data.auth ?? { scheme: 'none' as const }

  const rec: ApiServiceRecord = {
    id,
    name: data.name,
    description: data.description ?? '',
    baseUrl: data.baseUrl.replace(/\/+$/, ''),
    swaggerUrl: data.swaggerUrl,
    status: 'pending',
    auth: { scheme: auth.scheme },
    versions: [],
    activeVersion: '',
    tags: data.tags ?? [],
    allowPrivate: data.allowPrivate ?? false,
    createdAt: Date.now(),
    updatedAt: Date.now(),
  }

  state.services.unshift(rec)

  // Store encrypted credentials separately
  if (auth.scheme !== 'none') {
    state.authCredsById[id] = encodeAuth(auth)
  }

  writeState(state)
  return rec
}

export function updateApiService(
  id: string,
  changes: Partial<Pick<ApiServiceRecord, 'name' | 'description' | 'baseUrl' | 'swaggerUrl' | 'status' | 'tags' | 'activeVersion' | 'lastError' | 'excludedEndpoints' | 'allowPrivate' | 'customHeaders'>>,
): ApiServiceRecord | null {
  const state = readState()
  const index = state.services.findIndex(s => s.id === id)
  if (index === -1) return null

  const current = state.services[index]
  state.services[index] = {
    ...current,
    ...changes,
    id: current.id,
    createdAt: current.createdAt,
    updatedAt: Date.now(),
    versions: current.versions,
    auth: current.auth,
  }

  writeState(state)
  return state.services[index]
}

export function updateApiServiceAuth(id: string, auth: ApiServiceAuth): void {
  const state = readState()
  const index = state.services.findIndex(s => s.id === id)
  if (index === -1) return

  state.services[index].auth = { scheme: auth.scheme }
  state.services[index].updatedAt = Date.now()
  state.authCredsById[id] = encodeAuth(auth)
  writeState(state)
}

/**
 * Combined update — applies metadata changes AND auth credentials in a single
 * readState/writeState cycle, avoiding the two-write race condition on Windows.
 */
export function updateApiServiceFull(
  id: string,
  changes: Partial<Pick<ApiServiceRecord, 'name' | 'description' | 'baseUrl' | 'swaggerUrl' | 'status' | 'tags' | 'activeVersion' | 'lastError' | 'excludedEndpoints' | 'allowPrivate' | 'customHeaders'>>,
  auth?: ApiServiceAuth,
): ApiServiceRecord | null {
  const state = readState()
  const index = state.services.findIndex(s => s.id === id)
  if (index === -1) return null

  const current = state.services[index]
  state.services[index] = {
    ...current,
    ...changes,
    id: current.id,
    createdAt: current.createdAt,
    updatedAt: Date.now(),
    versions: current.versions,
    auth: auth ? { scheme: auth.scheme } : current.auth,
  }

  if (auth) {
    state.authCredsById[id] = encodeAuth(auth)
  }

  writeState(state)  // Single write — no race condition
  return state.services[index]
}

export function addApiServiceVersion(id: string, version: ApiServiceVersion): void {
  const state = readState()
  const index = state.services.findIndex(s => s.id === id)
  if (index === -1) return

  const service = state.services[index]
  const existingIdx = service.versions.findIndex(v => v.version === version.version)
  if (existingIdx >= 0) {
    service.versions[existingIdx] = version
  } else {
    service.versions.push(version)
  }

  if (!service.activeVersion) {
    service.activeVersion = version.version
  }
  service.status = 'active'
  service.updatedAt = Date.now()

  writeState(state)
}

export function deleteApiService(id: string): boolean {
  const state = readState()
  const before = state.services.length
  state.services = state.services.filter(s => s.id !== id)
  delete state.authCredsById[id]
  if (state.services.length === before) return false
  writeState(state)
  return true
}

export function getApiServiceAuth(id: string): ApiServiceAuth | null {
  const state = readState()
  const creds = state.authCredsById[id]
  return creds ? decodeAuth(creds) : null
}

/* ── Helpers ───────────────────────────────────────────────────────── */

export function countEndpoints(paths: Record<string, Record<string, unknown>>): number {
  const methods = ['get', 'post', 'put', 'delete', 'patch']
  let count = 0
  for (const pathItem of Object.values(paths)) {
    for (const method of methods) {
      if (pathItem[method]) count++
    }
  }
  return count
}
