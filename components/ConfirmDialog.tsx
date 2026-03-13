'use client'

import { Loader2, X } from 'lucide-react'

interface ConfirmDialogProps {
  open: boolean
  title: string
  description: string
  confirmLabel: string
  cancelLabel?: string
  tone?: 'danger' | 'default'
  busy?: boolean
  details?: string[]
  onConfirm: () => void
  onCancel: () => void
}

export default function ConfirmDialog({
  open,
  title,
  description,
  confirmLabel,
  cancelLabel = 'Cancel',
  tone = 'default',
  busy = false,
  details = [],
  onConfirm,
  onCancel,
}: ConfirmDialogProps) {
  if (!open) return null

  const confirmClass = tone === 'danger'
    ? 'border-rose-500/30 bg-rose-500/10 text-rose-300 hover:bg-rose-500/15'
    : 'border-indigo-500/30 bg-indigo-500/10 text-indigo-300 hover:bg-indigo-500/15'

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-[#02050a]/75 px-4 backdrop-blur-sm">
      <div className="w-full max-w-md rounded-2xl border border-chef-border bg-chef-surface shadow-2xl shadow-black/40">
        <div className="flex items-start justify-between gap-4 border-b border-chef-border px-5 py-4">
          <div className="min-w-0">
            <h2 className="text-sm font-semibold text-chef-text">{title}</h2>
            <p className="mt-1 text-xs leading-relaxed text-chef-muted">{description}</p>
          </div>
          <button
            onClick={onCancel}
            disabled={busy}
            className="rounded-lg p-1.5 text-chef-muted transition-colors hover:bg-chef-card hover:text-chef-text disabled:cursor-not-allowed disabled:opacity-50"
          >
            <X size={14} />
          </button>
        </div>
        {details.length > 0 && (
          <div className="border-b border-chef-border px-5 py-4">
            <div className="space-y-2 text-[11px] text-chef-muted">
              {details.map(detail => (
                <div key={detail}>{detail}</div>
              ))}
            </div>
          </div>
        )}
        <div className="flex items-center justify-end gap-2 px-5 py-4">
          <button
            onClick={onCancel}
            disabled={busy}
            className="rounded-lg border border-chef-border px-3 py-1.5 text-xs text-chef-muted transition-colors hover:border-indigo-500/20 hover:text-chef-text disabled:cursor-not-allowed disabled:opacity-50"
          >
            {cancelLabel}
          </button>
          <button
            onClick={onConfirm}
            disabled={busy}
            className={`inline-flex items-center gap-1.5 rounded-lg border px-3 py-1.5 text-xs transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${confirmClass}`}
          >
            {busy ? <Loader2 size={12} className="animate-spin" /> : null}
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  )
}
