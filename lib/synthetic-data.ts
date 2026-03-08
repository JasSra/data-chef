/**
 * Deterministic 500K synthetic billing-event rows.
 * Generated once and cached in server module memory — no disk, no network.
 * Uses Knuth multiplicative hashing for fast, reproducible pseudorandomness.
 */

export interface EventRow {
  id:         number
  user_id:    number
  event_type: string
  amount:     number
  country:    string
  device:     string
  ts:         string
}

const EVENT_TYPES = ['purchase', 'refund', 'signup', 'click', 'view', 'error']
const COUNTRIES   = ['US', 'GB', 'DE', 'FR', 'JP', 'AU', 'CA', 'BR', 'IN', 'MX']
const DEVICES     = ['mobile', 'desktop', 'tablet']

/* Knuth multiplicative hashing — fast, deterministic, good distribution */
function h(n: number): number { return ((n >>> 0) * 2654435761) >>> 0 }

let _events: EventRow[] | null = null

export function getSyntheticEvents(): EventRow[] {
  if (_events) return _events

  console.log('[dataChef] Generating 500K synthetic events…')
  const N    = 500_000
  const BASE = 1_700_000_000_000  // 2023-11-14 epoch baseline

  _events = Array.from({ length: N }, (_, i) => {
    const a = h(i + 1), b = h(a), c = h(b), d = h(c)
    return {
      id:         i + 1,
      user_id:    (a % 10_000) + 1,
      event_type: EVENT_TYPES[b % EVENT_TYPES.length],
      amount:     Math.round((c % 10_000) / 100 * 100) / 100,  // 0.00–99.99
      country:    COUNTRIES[d % COUNTRIES.length],
      device:     DEVICES[h(d) % DEVICES.length],
      ts:         new Date(BASE + (a % 31_536_000) * 1000).toISOString().split('T')[0],
    }
  })

  console.log('[dataChef] 500K events cached in memory')
  return _events
}

/* Rough byte size for scan-cost display */
export const EVENTS_BYTES = 500_000 * 60  // ~30 MB estimate
