import 'server-only'

import { randomUUID } from 'node:crypto'

import { readJsonFile, writeJsonFile, removeJsonFile } from '@/lib/json-store'
import type { AzureDevOpsCredentials } from '@/lib/connectors'
import { decryptSecret, encryptSecret, isEncryptedValue } from '@/lib/secret-crypto'

interface AzureDevOpsEntraConfig {
  tenantId: string
  clientId: string
  clientSecret: string
  organization: string
}

export interface AzureDevOpsPendingAuthRecord {
  id: string
  state: string
  createdAt: number
  completedAt: number | null
  connectorName: string
  connectorDescription: string
  organization: string
  credentials: AzureDevOpsCredentials | null
  accountName: string | null
  error: string | null
  entraConfig?: AzureDevOpsEntraConfig | null
}

interface AzureDevOpsAuthStateFile {
  transactions: AzureDevOpsPendingAuthRecord[]
}

const STORE_FILE = 'azure-devops-auth.json'
const MAX_AGE_MS = 30 * 60 * 1000

function seedState(): AzureDevOpsAuthStateFile {
  return { transactions: [] }
}

function readState(): AzureDevOpsAuthStateFile {
  const state = readJsonFile<AzureDevOpsAuthStateFile>(STORE_FILE, seedState())
  const now = Date.now()
  return {
    transactions: (state.transactions ?? [])
      .filter(record => now - record.createdAt <= MAX_AGE_MS)
      .map(record => ({ ...record })),
  }
}

function writeState(state: AzureDevOpsAuthStateFile): void {
  if (state.transactions.length === 0) {
    removeJsonFile(STORE_FILE)
    return
  }
  writeJsonFile(STORE_FILE, state)
}

function maybeDecrypt(value: unknown): string {
  if (typeof value === 'string') return value
  if (isEncryptedValue(value)) return decryptSecret(value)
  return ''
}

function hydrate(record: AzureDevOpsPendingAuthRecord): AzureDevOpsPendingAuthRecord {
  return {
    ...record,
    entraConfig: record.entraConfig
      ? {
          tenantId: record.entraConfig.tenantId,
          clientId: record.entraConfig.clientId,
          clientSecret: maybeDecrypt(record.entraConfig.clientSecret),
          organization: record.entraConfig.organization,
        }
      : null,
  }
}

export function createAzureDevOpsAuthTransaction(input: {
  connectorName: string
  connectorDescription?: string
  organization: string
  entraConfig: AzureDevOpsEntraConfig
}): AzureDevOpsPendingAuthRecord {
  const state = readState()
  const record: AzureDevOpsPendingAuthRecord = {
    id: randomUUID(),
    state: randomUUID(),
    createdAt: Date.now(),
    completedAt: null,
    connectorName: input.connectorName,
    connectorDescription: input.connectorDescription ?? '',
    organization: input.organization,
    credentials: null,
    accountName: null,
    error: null,
    entraConfig: {
      tenantId: input.entraConfig.tenantId,
      clientId: input.entraConfig.clientId,
      clientSecret: encryptSecret(input.entraConfig.clientSecret) as unknown as string,
      organization: input.entraConfig.organization,
    },
  }
  state.transactions.unshift(record)
  writeState(state)
  return hydrate(record)
}

export function getAzureDevOpsAuthTransaction(id: string): AzureDevOpsPendingAuthRecord | null {
  const record = readState().transactions.find(item => item.id === id) ?? null
  return record ? hydrate(record) : null
}

export function getAzureDevOpsAuthTransactionByState(stateValue: string): AzureDevOpsPendingAuthRecord | null {
  const record = readState().transactions.find(item => item.state === stateValue) ?? null
  return record ? hydrate(record) : null
}

export function completeAzureDevOpsAuthTransaction(
  id: string,
  updates: {
    credentials: AzureDevOpsCredentials
    accountName?: string | null
  },
): AzureDevOpsPendingAuthRecord | null {
  const state = readState()
  const record = state.transactions.find(item => item.id === id)
  if (!record) return null
  record.completedAt = Date.now()
  record.credentials = updates.credentials
  record.accountName = updates.accountName ?? null
  record.error = null
  writeState(state)
  return hydrate(record)
}

export function failAzureDevOpsAuthTransaction(id: string, error: string): AzureDevOpsPendingAuthRecord | null {
  const state = readState()
  const record = state.transactions.find(item => item.id === id)
  if (!record) return null
  record.error = error
  writeState(state)
  return hydrate(record)
}

export function consumeAzureDevOpsAuthTransaction(id: string): AzureDevOpsPendingAuthRecord | null {
  const state = readState()
  const index = state.transactions.findIndex(item => item.id === id)
  if (index === -1) return null
  const [record] = state.transactions.splice(index, 1)
  writeState(state)
  return hydrate(record)
}
