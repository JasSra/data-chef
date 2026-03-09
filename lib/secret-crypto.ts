import 'server-only'

import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto'

interface EncryptedValue {
  __enc_v1: string
}

function getSecretKeyMaterial(): Buffer {
  const raw = process.env.CONNECTOR_SECRET_KEY ?? ''
  if (!raw.trim()) {
    throw new Error('CONNECTOR_SECRET_KEY is required for encrypted connector credentials')
  }
  return createHash('sha256').update(raw).digest()
}

export function encryptSecret(value: string): EncryptedValue {
  const iv = randomBytes(12)
  const key = getSecretKeyMaterial()
  const cipher = createCipheriv('aes-256-gcm', key, iv)
  const encrypted = Buffer.concat([cipher.update(value, 'utf8'), cipher.final()])
  const tag = cipher.getAuthTag()
  return {
    __enc_v1: Buffer.concat([iv, tag, encrypted]).toString('base64'),
  }
}

export function decryptSecret(value: string | EncryptedValue): string {
  if (typeof value === 'string') return value
  const payload = Buffer.from(value.__enc_v1, 'base64')
  const iv = payload.subarray(0, 12)
  const tag = payload.subarray(12, 28)
  const encrypted = payload.subarray(28)
  const key = getSecretKeyMaterial()
  const decipher = createDecipheriv('aes-256-gcm', key, iv)
  decipher.setAuthTag(tag)
  return Buffer.concat([decipher.update(encrypted), decipher.final()]).toString('utf8')
}

export function isEncryptedValue(value: unknown): value is EncryptedValue {
  return Boolean(value && typeof value === 'object' && '__enc_v1' in value)
}
