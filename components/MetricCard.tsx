import { LucideIcon } from 'lucide-react'

interface MetricCardProps {
  title: string
  value: string
  subtitle?: string
  icon: LucideIcon
  trend?: { value: string; positive: boolean }
  accent?: string
}

export default function MetricCard({
  title,
  value,
  subtitle,
  icon: Icon,
  trend,
  accent = 'text-indigo-400',
}: MetricCardProps) {
  return (
    <div className="bg-chef-card border border-chef-border rounded-xl p-5 flex flex-col gap-3 hover:border-indigo-500/30 transition-colors">
      <div className="flex items-center justify-between">
        <span className="text-[10px] font-semibold uppercase tracking-widest text-chef-muted">
          {title}
        </span>
        <div className={`${accent} opacity-60`}>
          <Icon size={14} />
        </div>
      </div>
      <div>
        <div className="text-2xl font-bold text-chef-text tabular-nums font-mono">
          {value}
        </div>
        {subtitle && (
          <div className="text-[11px] text-chef-muted mt-0.5">{subtitle}</div>
        )}
      </div>
      {trend && (
        <div className={`text-[11px] font-medium ${trend.positive ? 'text-emerald-400' : 'text-rose-400'}`}>
          {trend.positive ? '↑' : '↓'} {trend.value} vs yesterday
        </div>
      )}
    </div>
  )
}
