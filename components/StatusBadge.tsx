type Status = 'active' | 'draft' | 'failed' | 'running' | 'connected' | 'disconnected' | 'succeeded' | 'quarantined'

const config: Record<Status, { bg: string; text: string; dot: string; label: string }> = {
  active:        { bg: 'bg-emerald-500/10', text: 'text-emerald-400', dot: 'bg-emerald-400',              label: 'Active' },
  running:       { bg: 'bg-indigo-500/10',  text: 'text-indigo-400',  dot: 'bg-indigo-400 animate-pulse', label: 'Running' },
  draft:         { bg: 'bg-slate-500/10',   text: 'text-slate-400',   dot: 'bg-slate-500',               label: 'Draft' },
  failed:        { bg: 'bg-rose-500/10',    text: 'text-rose-400',    dot: 'bg-rose-400',                label: 'Failed' },
  succeeded:     { bg: 'bg-emerald-500/10', text: 'text-emerald-400', dot: 'bg-emerald-400',             label: 'Succeeded' },
  connected:     { bg: 'bg-emerald-500/10', text: 'text-emerald-400', dot: 'bg-emerald-400 animate-pulse', label: 'Connected' },
  disconnected:  { bg: 'bg-slate-500/10',   text: 'text-slate-400',   dot: 'bg-slate-500',              label: 'Disconnected' },
  quarantined:   { bg: 'bg-amber-500/10',   text: 'text-amber-400',   dot: 'bg-amber-400',              label: 'Quarantined' },
}

export default function StatusBadge({ status }: { status: Status }) {
  const c = config[status]
  return (
    <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium ${c.bg} ${c.text}`}>
      <span className={`w-1.5 h-1.5 rounded-full shrink-0 ${c.dot}`} />
      {c.label}
    </span>
  )
}
