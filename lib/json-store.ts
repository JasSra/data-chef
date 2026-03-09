import { existsSync, mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from 'node:fs'
import path from 'node:path'
import { getTenantStoreDir } from '@/lib/tenant'

const STORE_DIR = path.join(process.cwd(), '.datachef')

function ensureStoreDir(dir: string) {
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true })
}

function getTenantFilePath(fileName: string): string {
  const tenantDir = getTenantStoreDir()
  ensureStoreDir(tenantDir)
  return path.join(tenantDir, fileName)
}

function getLegacyFilePath(fileName: string): string {
  return path.join(STORE_DIR, fileName)
}

function migrateLegacyFileIfNeeded(fileName: string, filePath: string): void {
  const legacyPath = getLegacyFilePath(fileName)
  if (existsSync(filePath) || !existsSync(legacyPath)) return
  ensureStoreDir(path.dirname(filePath))
  const raw = readFileSync(legacyPath, 'utf8')
  writeFileSync(filePath, raw, 'utf8')
}

export function readJsonFile<T>(fileName: string, fallback: T): T {
  const filePath = getTenantFilePath(fileName)
  migrateLegacyFileIfNeeded(fileName, filePath)
  if (!existsSync(filePath)) return fallback

  try {
    const raw = readFileSync(filePath, 'utf8')
    return JSON.parse(raw) as T
  } catch {
    return fallback
  }
}

export function writeJsonFile<T>(fileName: string, value: T): void {
  const filePath = getTenantFilePath(fileName)
  const tmpPath = `${filePath}.${process.pid}.${Math.random().toString(36).slice(2, 8)}.tmp`
  writeFileSync(tmpPath, JSON.stringify(value, null, 2), 'utf8')
  renameSync(tmpPath, filePath)
}

export function removeJsonFile(fileName: string): void {
  const filePath = getTenantFilePath(fileName)
  if (existsSync(filePath)) unlinkSync(filePath)
}
