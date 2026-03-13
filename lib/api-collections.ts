import { readJsonFile, writeJsonFile } from '@/lib/json-store'

/* ── Types ─────────────────────────────────────────────────────────── */

export interface ApiCollection {
  id: string
  name: string
  description: string
  serviceId: string
  queries: ApiSavedQuery[]
  createdAt: number
  updatedAt: number
}

export interface ApiSavedQuery {
  id: string
  name: string
  query: string
  createdAt: number
}

interface CollectionsState {
  collections: ApiCollection[]
}

/* ── Store ─────────────────────────────────────────────────────────── */

const STORE_FILE = 'api-collections.json'

function readState(): CollectionsState {
  return readJsonFile<CollectionsState>(STORE_FILE, { collections: [] })
}

function writeState(state: CollectionsState): void {
  writeJsonFile(STORE_FILE, state)
}

/* ── CRUD ──────────────────────────────────────────────────────────── */

export function getApiCollections(serviceId?: string): ApiCollection[] {
  const state = readState()
  if (serviceId) return state.collections.filter(c => c.serviceId === serviceId)
  return state.collections
}

export function getApiCollection(id: string): ApiCollection | undefined {
  return readState().collections.find(c => c.id === id)
}

export function createApiCollection(
  name: string,
  serviceId: string,
  description = '',
): ApiCollection {
  const state = readState()
  const collection: ApiCollection = {
    id: `col_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`,
    name,
    description,
    serviceId,
    queries: [],
    createdAt: Date.now(),
    updatedAt: Date.now(),
  }
  state.collections.push(collection)
  writeState(state)
  return collection
}

export function deleteApiCollection(id: string): boolean {
  const state = readState()
  const before = state.collections.length
  state.collections = state.collections.filter(c => c.id !== id)
  if (state.collections.length === before) return false
  writeState(state)
  return true
}

export function addQueryToCollection(
  collectionId: string,
  name: string,
  query: string,
): ApiSavedQuery | null {
  const state = readState()
  const col = state.collections.find(c => c.id === collectionId)
  if (!col) return null
  const saved: ApiSavedQuery = {
    id: `sq_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`,
    name,
    query,
    createdAt: Date.now(),
  }
  col.queries.push(saved)
  col.updatedAt = Date.now()
  writeState(state)
  return saved
}

export function removeQueryFromCollection(
  collectionId: string,
  queryId: string,
): boolean {
  const state = readState()
  const col = state.collections.find(c => c.id === collectionId)
  if (!col) return false
  const before = col.queries.length
  col.queries = col.queries.filter(q => q.id !== queryId)
  if (col.queries.length === before) return false
  col.updatedAt = Date.now()
  writeState(state)
  return true
}

export function renameApiCollection(id: string, name: string): boolean {
  const state = readState()
  const col = state.collections.find(c => c.id === id)
  if (!col) return false
  col.name = name
  col.updatedAt = Date.now()
  writeState(state)
  return true
}
