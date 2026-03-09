export type TimeWindowPreset =
  | 'last_1h'
  | 'last_6h'
  | 'last_24h'
  | 'last_7d'
  | 'last_30d'
  | 'today'
  | 'yesterday'
  | 'month_to_date'

export interface ResolvedTimeWindow {
  preset: TimeWindowPreset
  label: string
  startTime: string
  endTime: string
  timespanIso: string
  bucketHint: string
}

function startOfDay(date: Date): Date {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 0, 0, 0, 0))
}

function startOfMonth(date: Date): Date {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1, 0, 0, 0, 0))
}

function isoDurationFromMs(ms: number): string {
  const hours = Math.max(1, Math.round(ms / 3_600_000))
  if (hours % 24 === 0) return `P${Math.round(hours / 24)}D`
  return `PT${hours}H`
}

export function resolveTimeWindow(preset: TimeWindowPreset, now = new Date()): ResolvedTimeWindow {
  const end = new Date(now)
  let start = new Date(now)
  let label = ''
  let bucketHint = '1h'

  switch (preset) {
    case 'last_1h':
      start = new Date(end.getTime() - 3_600_000)
      label = 'Last 1h'
      bucketHint = '5m'
      break
    case 'last_6h':
      start = new Date(end.getTime() - 6 * 3_600_000)
      label = 'Last 6h'
      bucketHint = '15m'
      break
    case 'last_24h':
      start = new Date(end.getTime() - 24 * 3_600_000)
      label = 'Last 24h'
      bucketHint = '1h'
      break
    case 'last_7d':
      start = new Date(end.getTime() - 7 * 24 * 3_600_000)
      label = 'Last 7d'
      bucketHint = '6h'
      break
    case 'last_30d':
      start = new Date(end.getTime() - 30 * 24 * 3_600_000)
      label = 'Last 30d'
      bucketHint = '1d'
      break
    case 'today':
      start = startOfDay(end)
      label = 'Today'
      bucketHint = '1h'
      break
    case 'yesterday': {
      const today = startOfDay(end)
      start = new Date(today.getTime() - 24 * 3_600_000)
      end.setTime(today.getTime())
      label = 'Yesterday'
      bucketHint = '1h'
      break
    }
    case 'month_to_date':
      start = startOfMonth(end)
      label = 'Month to date'
      bucketHint = '1d'
      break
  }

  return {
    preset,
    label,
    startTime: start.toISOString(),
    endTime: end.toISOString(),
    timespanIso: isoDurationFromMs(end.getTime() - start.getTime()),
    bucketHint,
  }
}
