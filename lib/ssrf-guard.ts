/**
 * SSRF protection — validate URLs before proxying requests.
 * Rejects requests to private/internal IP ranges.
 */

import 'server-only'

const BLOCKED_HOSTS = [
  'localhost',
  '127.0.0.1',
  '0.0.0.0',
  '[::1]',
  '169.254.169.254',   // AWS metadata
  'metadata.google.internal',
]

const PRIVATE_RANGES = [
  { prefix: '10.', bits: 8 },
  { prefix: '172.', minSecond: 16, maxSecond: 31 },
  { prefix: '192.168.', bits: 16 },
  { prefix: '169.254.', bits: 16 },
  { prefix: '127.', bits: 8 },
  { prefix: '0.', bits: 8 },
]

function isPrivateIP(host: string): boolean {
  for (const range of PRIVATE_RANGES) {
    if (!host.startsWith(range.prefix)) continue
    if ('minSecond' in range) {
      const second = parseInt(host.split('.')[1], 10)
      if (second >= range.minSecond! && second <= range.maxSecond!) return true
    } else {
      return true
    }
  }
  return false
}

export function validateProxyUrl(url: string, opts?: { allowPrivate?: boolean }): { valid: boolean; error?: string } {
  let parsed: URL
  try {
    parsed = new URL(url)
  } catch {
    return { valid: false, error: 'Invalid URL' }
  }

  if (!['http:', 'https:'].includes(parsed.protocol)) {
    return { valid: false, error: `Unsupported protocol: ${parsed.protocol}` }
  }

  const hostname = parsed.hostname.toLowerCase()

  if (!opts?.allowPrivate) {
    if (BLOCKED_HOSTS.includes(hostname)) {
      return { valid: false, error: `Blocked host: ${hostname}` }
    }

    if (isPrivateIP(hostname)) {
      return { valid: false, error: `Private IP range not allowed: ${hostname}. Enable "Allow private/intranet URLs" when adding the service.` }
    }

    // Block IPv6 link-local
    if (hostname.startsWith('[fe80:') || hostname.startsWith('fe80:')) {
      return { valid: false, error: 'Link-local IPv6 not allowed' }
    }
  }

  return { valid: true }
}
