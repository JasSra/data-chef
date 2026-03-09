import 'server-only'

import { randomUUID } from 'node:crypto'

import { readJsonFile, writeJsonFile, removeJsonFile } from '@/lib/json-store'
import type { GitHubCredentials } from '@/lib/connectors'
import { decryptSecret, encryptSecret, isEncryptedValue } from '@/lib/secret-crypto'

interface GitHubOAuthAppConfig {
  clientId: string
  clientSecret: string
}

interface GitHubAppConfig {
  appSlug: string
  appId: string
  clientId: string
  clientSecret: string
  privateKey: string
}

export interface GitHubPendingAuthRecord {
  id: string
  provider: 'oauth' | 'app'
  state: string
  createdAt: number
  completedAt: number | null
  connectorName: string
  connectorDescription: string
  credentials: GitHubCredentials | null
  accountLogin: string | null
  accountType: 'User' | 'Organization' | null
  installationId: number | null
  error: string | null
  oauthConfig?: GitHubOAuthAppConfig | null
  appConfig?: GitHubAppConfig | null
}

interface GitHubAuthStateFile {
  transactions: GitHubPendingAuthRecord[]
}

const STORE_FILE = 'github-auth.json'
const MAX_AGE_MS = 30 * 60 * 1000

function seedState(): GitHubAuthStateFile {
  return { transactions: [] }
}

function readState(): GitHubAuthStateFile {
  const state = readJsonFile<GitHubAuthStateFile>(STORE_FILE, seedState())
  const now = Date.now()
  return {
    transactions: (state.transactions ?? [])
      .filter(record => now - record.createdAt <= MAX_AGE_MS)
      .map(record => ({ ...record })),
  }
}

function writeState(state: GitHubAuthStateFile): void {
  if (state.transactions.length === 0) {
    removeJsonFile(STORE_FILE)
    return
  }
  writeJsonFile(STORE_FILE, state)
}

export function createGitHubAuthTransaction(input: {
  provider: 'oauth' | 'app'
  connectorName: string
  connectorDescription?: string
  oauthConfig?: GitHubOAuthAppConfig
  appConfig?: GitHubAppConfig
}): GitHubPendingAuthRecord {
  const state = readState()
  const record: GitHubPendingAuthRecord = {
    id: randomUUID(),
    provider: input.provider,
    state: randomUUID(),
    createdAt: Date.now(),
    completedAt: null,
    connectorName: input.connectorName,
    connectorDescription: input.connectorDescription ?? '',
    credentials: null,
    accountLogin: null,
    accountType: null,
    installationId: null,
    error: null,
    oauthConfig: input.oauthConfig
      ? {
          clientId: input.oauthConfig.clientId,
          clientSecret: encryptSecret(input.oauthConfig.clientSecret) as unknown as string,
        }
      : null,
    appConfig: input.appConfig
      ? {
          appSlug: input.appConfig.appSlug,
          appId: input.appConfig.appId,
          clientId: input.appConfig.clientId,
          clientSecret: encryptSecret(input.appConfig.clientSecret) as unknown as string,
          privateKey: encryptSecret(input.appConfig.privateKey) as unknown as string,
        }
      : null,
  }
  state.transactions.unshift(record)
  writeState(state)
  return record
}

export function getGitHubAuthTransaction(id: string): GitHubPendingAuthRecord | null {
  const record = readState().transactions.find(item => item.id === id) ?? null
  if (!record) return null
  return {
    ...record,
    oauthConfig: record.oauthConfig
      ? {
          clientId: record.oauthConfig.clientId,
          clientSecret: maybeDecrypt(record.oauthConfig.clientSecret),
        }
      : null,
    appConfig: record.appConfig
      ? {
          appSlug: record.appConfig.appSlug,
          appId: record.appConfig.appId,
          clientId: record.appConfig.clientId,
          clientSecret: maybeDecrypt(record.appConfig.clientSecret),
          privateKey: maybeDecrypt(record.appConfig.privateKey),
        }
      : null,
  }
}

export function getGitHubAuthTransactionByState(stateValue: string): GitHubPendingAuthRecord | null {
  const record = readState().transactions.find(item => item.state === stateValue) ?? null
  if (!record) return null
  return getGitHubAuthTransaction(record.id)
}

export function completeGitHubAuthTransaction(
  id: string,
  updates: {
    credentials: GitHubCredentials
    accountLogin?: string | null
    accountType?: 'User' | 'Organization' | null
    installationId?: number | null
  },
): GitHubPendingAuthRecord | null {
  const state = readState()
  const record = state.transactions.find(entry => entry.id === id)
  if (!record) return null
  record.completedAt = Date.now()
  record.credentials = updates.credentials
  record.accountLogin = updates.accountLogin ?? null
  record.accountType = updates.accountType ?? null
  record.installationId = updates.installationId ?? null
  record.error = null
  writeState(state)
  return { ...record }
}

export function failGitHubAuthTransaction(id: string, error: string): GitHubPendingAuthRecord | null {
  const state = readState()
  const record = state.transactions.find(entry => entry.id === id)
  if (!record) return null
  record.error = error
  writeState(state)
  return { ...record }
}

export function consumeGitHubAuthTransaction(id: string): GitHubPendingAuthRecord | null {
  const state = readState()
  const index = state.transactions.findIndex(record => record.id === id)
  if (index === -1) return null
  const [record] = state.transactions.splice(index, 1)
  writeState(state)
  return {
    ...record,
    oauthConfig: record.oauthConfig
      ? {
          clientId: record.oauthConfig.clientId,
          clientSecret: maybeDecrypt(record.oauthConfig.clientSecret),
        }
      : null,
    appConfig: record.appConfig
      ? {
          appSlug: record.appConfig.appSlug,
          appId: record.appConfig.appId,
          clientId: record.appConfig.clientId,
          clientSecret: maybeDecrypt(record.appConfig.clientSecret),
          privateKey: maybeDecrypt(record.appConfig.privateKey),
        }
      : null,
  }
}

function maybeDecrypt(value: unknown): string {
  if (typeof value === 'string') return value
  if (isEncryptedValue(value)) return decryptSecret(value)
  return ''
}
