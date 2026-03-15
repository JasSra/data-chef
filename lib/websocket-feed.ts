import 'server-only'

/**
 * WebSocket live-feed client for dataChef connectors.
 *
 * - Connects to a WS/WSS endpoint, collects messages for a time window
 * - Supports custom headers and auth (bearer, API key, basic via URL params or
 *   protocol-level headers where the WS library allows)
 * - Returns collected messages as flat rows for dataset materialization
 * - Delta appending: caller tracks lastSeenId, only new messages returned
 */

export interface WsFeedMessage {
  id: string
  ts: string
  type: string        // 'text' | 'json' | 'binary'
  payload: string
  size: number
}

export interface WsFeedResult {
  messages: WsFeedMessage[]
  newMessages: WsFeedMessage[]   // delta
  connected: boolean
  error?: string
  durationMs: number
}

export interface WsFeedConfig {
  url: string
  auth?: 'none' | 'bearer' | 'apikey' | 'basic'
  bearerToken?: string
  apiKeyHeader?: string
  apiKeyValue?: string
  basicUser?: string
  basicPass?: string
  customHeaders?: Record<string, string>
  subscribeMessage?: string      // JSON message to send on connect (e.g. subscribe to a channel)
  windowMs?: number              // how long to collect messages (default 5s, max 30s)
}

function buildWsHeaders(config: WsFeedConfig): Record<string, string> {
  const headers: Record<string, string> = {}

  if (config.customHeaders) {
    for (const [k, v] of Object.entries(config.customHeaders)) {
      if (k && v) headers[k] = v
    }
  }

  if (config.auth === 'bearer' && config.bearerToken) {
    headers['Authorization'] = `Bearer ${config.bearerToken}`
  }
  if (config.auth === 'apikey' && config.apiKeyHeader && config.apiKeyValue) {
    headers[config.apiKeyHeader] = config.apiKeyValue
  }
  if (config.auth === 'basic' && config.basicUser) {
    headers['Authorization'] = `Basic ${Buffer.from(`${config.basicUser}:${config.basicPass ?? ''}`).toString('base64')}`
  }

  return headers
}

/** Connect to a WebSocket, collect messages for `windowMs`, return results */
export async function collectWsFeed(
  config: WsFeedConfig,
  options: {
    lastSeenIds?: Set<string>
    limit?: number
  } = {},
): Promise<WsFeedResult> {
  const t0 = performance.now()
  const windowMs = Math.min(Math.max(config.windowMs ?? 5_000, 1_000), 30_000)
  const limit = options.limit ?? 500
  const lastSeenIds = options.lastSeenIds ?? new Set<string>()

  const { default: WebSocket } = await import('ws')

  const headers = buildWsHeaders(config)
  const messages: WsFeedMessage[] = []
  let msgCounter = 0

  return new Promise<WsFeedResult>(resolve => {
    let resolved = false
    const done = (error?: string, connected = true) => {
      if (resolved) return
      resolved = true
      try { ws.close() } catch {}
      const newMessages = lastSeenIds.size > 0
        ? messages.filter(m => !lastSeenIds.has(m.id))
        : messages
      resolve({
        messages,
        newMessages,
        connected,
        error,
        durationMs: Math.round(performance.now() - t0),
      })
    }

    const deadline = setTimeout(() => done(), windowMs)

    let ws: InstanceType<typeof WebSocket>
    try {
      ws = new WebSocket(config.url, {
        headers,
        handshakeTimeout: 10_000,
      })
    } catch (e) {
      clearTimeout(deadline)
      resolve({
        messages: [],
        newMessages: [],
        connected: false,
        error: e instanceof Error ? e.message : String(e),
        durationMs: Math.round(performance.now() - t0),
      })
      return
    }

    ws.on('open', () => {
      // Send subscribe message if configured
      if (config.subscribeMessage?.trim()) {
        try {
          ws.send(config.subscribeMessage)
        } catch {
          // Non-fatal: continue collecting
        }
      }
    })

    ws.on('message', (data, isBinary) => {
      msgCounter++
      const payload = isBinary
        ? `[binary ${(data as Buffer).length}B]`
        : data.toString()

      let msgType: 'json' | 'text' | 'binary' = isBinary ? 'binary' : 'text'
      if (!isBinary) {
        try { JSON.parse(payload); msgType = 'json' } catch {}
      }

      const id = `ws-${Date.now()}-${msgCounter}`
      messages.push({
        id,
        ts: new Date().toISOString(),
        type: msgType,
        payload: payload.slice(0, 4096),
        size: isBinary ? (data as Buffer).length : Buffer.byteLength(payload),
      })

      if (messages.length >= limit) {
        clearTimeout(deadline)
        done()
      }
    })

    ws.on('error', (err) => {
      clearTimeout(deadline)
      done(err.message, false)
    })

    ws.on('close', () => {
      clearTimeout(deadline)
      done(messages.length === 0 ? 'Connection closed before receiving messages' : undefined)
    })
  })
}

/** Quick probe — connect, optionally send subscribe, wait for first message or timeout */
export async function probeWsFeed(
  config: WsFeedConfig,
): Promise<{ ok: boolean; latencyMs?: number; firstMessageType?: string; error?: string }> {
  const result = await collectWsFeed(
    { ...config, windowMs: Math.min(config.windowMs ?? 8_000, 10_000) },
    { limit: 1 },
  )
  if (result.error && !result.connected) {
    return { ok: false, error: result.error }
  }
  if (result.messages.length > 0) {
    return {
      ok: true,
      latencyMs: result.durationMs,
      firstMessageType: result.messages[0].type,
    }
  }
  return {
    ok: result.connected,
    latencyMs: result.durationMs,
    error: result.connected ? 'Connected but no messages received in window' : result.error,
  }
}

/** Convert WS messages to flat row records for dataset materialization.
 *  For JSON messages, attempts to flatten top-level keys into columns. */
export function wsMessagesToRows(messages: WsFeedMessage[]): Record<string, unknown>[] {
  return messages.map(msg => {
    const base: Record<string, unknown> = {
      _ws_id: msg.id,
      _ws_ts: msg.ts,
      _ws_type: msg.type,
      _ws_size: msg.size,
    }

    if (msg.type === 'json') {
      try {
        const parsed = JSON.parse(msg.payload)
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          for (const [k, v] of Object.entries(parsed)) {
            base[k] = typeof v === 'object' ? JSON.stringify(v) : v
          }
          return base
        }
      } catch {
        // Fall through to raw payload
      }
    }

    base.payload = msg.payload
    return base
  })
}
