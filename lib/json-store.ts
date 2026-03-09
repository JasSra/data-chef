import { existsSync, mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from 'node:fs'
import path from 'node:path'

const STORE_DIR = path.join(process.cwd(), '.datachef')

function ensureStoreDir() {
  if (!existsSync(STORE_DIR)) mkdirSync(STORE_DIR, { recursive: true })
}

export function readJsonFile<T>(fileName: string, fallback: T): T {
  ensureStoreDir()
  const filePath = path.join(STORE_DIR, fileName)
  if (!existsSync(filePath)) return fallback

  try {
    const raw = readFileSync(filePath, 'utf8')
    return JSON.parse(raw) as T
  } catch {
    return fallback
  }
}

export function writeJsonFile<T>(fileName: string, value: T): void {
  ensureStoreDir()
  const filePath = path.join(STORE_DIR, fileName)
  const tmpPath = `${filePath}.tmp`
  writeFileSync(tmpPath, JSON.stringify(value, null, 2), 'utf8')
  renameSync(tmpPath, filePath)
}

export function removeJsonFile(fileName: string): void {
  ensureStoreDir()
  const filePath = path.join(STORE_DIR, fileName)
  if (existsSync(filePath)) unlinkSync(filePath)
}
