'use client'

import { useEffect, useMemo, useState } from 'react'
import {
  Settings, Shield, Bell, Cpu, ChevronRight, CheckCircle2, Copy, Eye, EyeOff,
  Globe, AlertTriangle, Zap, Loader2, RefreshCw, Radar, Palette,
} from 'lucide-react'
import { useAppSettings } from '@/components/SettingsProvider'
import { REGION_OPTIONS, ROLE_OPTIONS, TIMEZONE_OPTIONS, type AppSettings } from '@/lib/app-settings-schema'

type Section = 'workspace' | 'branding' | 'query-engine' | 'api-keys' | 'notifications' | 'network-discovery' | 'danger'

function Toggle({ value, onChange }: { value: boolean; onChange: (v: boolean) => void }) {
  return (
    <button
      onClick={() => onChange(!value)}
      className={`relative w-9 h-5 rounded-full transition-colors shrink-0 ${value ? 'bg-indigo-500' : 'bg-chef-border'}`}
    >
      <span
        className="absolute top-0.5 w-4 h-4 rounded-full bg-white shadow-sm transition-all"
        style={{ left: value ? 18 : 2 }}
      />
    </button>
  )
}

function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <div className="flex items-start gap-4 py-4 border-b border-chef-border last:border-b-0">
      <div className="w-44 shrink-0 pt-0.5">
        <div className="text-xs font-medium text-chef-text">{label}</div>
        {hint && <div className="text-[10px] text-chef-muted mt-0.5 leading-snug">{hint}</div>}
      </div>
      <div className="flex-1 min-w-0">{children}</div>
    </div>
  )
}

function TextInput({
  value,
  onChange,
  mono = false,
  placeholder,
  type = 'text',
}: {
  value: string
  onChange: (v: string) => void
  mono?: boolean
  placeholder?: string
  type?: string
}) {
  return (
    <input
      type={type}
      value={value}
      onChange={e => onChange(e.target.value)}
      placeholder={placeholder}
      className={`w-full bg-chef-bg border border-chef-border rounded-lg px-3 py-1.5 text-xs text-chef-text placeholder:text-chef-muted outline-none focus:border-indigo-500/50 transition-colors ${mono ? 'font-mono' : ''}`}
    />
  )
}

function SelectInput({ value, onChange, options }: {
  value: string
  onChange: (v: string) => void
  options: { value: string; label: string }[]
}) {
  return (
    <select
      value={value}
      onChange={e => onChange(e.target.value)}
      className="w-full bg-chef-bg border border-chef-border rounded-lg px-3 py-1.5 text-xs text-chef-text outline-none focus:border-indigo-500/50 transition-colors appearance-none cursor-pointer"
    >
      {options.map(o => (
        <option key={o.value} value={o.value}>{o.label}</option>
      ))}
    </select>
  )
}

function NavItem({ icon: Icon, label, active, onClick }: {
  icon: React.ElementType
  label: string
  active: boolean
  onClick: () => void
}) {
  return (
    <button
      onClick={onClick}
      className={`w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-left text-xs font-medium transition-colors ${
        active ? 'bg-indigo-500/10 text-indigo-400' : 'text-chef-muted hover:text-chef-text hover:bg-chef-card'
      }`}
    >
      <Icon size={14} className={active ? 'text-indigo-400' : 'text-chef-muted'} />
      <span className="flex-1">{label}</span>
      <ChevronRight size={12} className="text-chef-muted" />
    </button>
  )
}

function ApiKeyRow({
  label,
  value,
  onRotate,
}: {
  label: string
  value: string
  onRotate: () => Promise<unknown>
}) {
  const [show, setShow] = useState(false)
  const [copied, setCopied] = useState(false)
  const [rotating, setRotating] = useState(false)

  const masked = value.length > 12
    ? value.slice(0, 8) + '•'.repeat(18) + value.slice(-4)
    : value

  async function handleRotate() {
    try {
      setRotating(true)
      await onRotate()
    } finally {
      setRotating(false)
    }
  }

  function handleCopy() {
    navigator.clipboard.writeText(value).catch(() => {})
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }

  return (
    <div className="flex items-center gap-3 py-3 border-b border-chef-border last:border-b-0">
      <div className="flex-1 min-w-0">
        <div className="text-xs font-medium text-chef-text">{label}</div>
        <div className="font-mono text-[11px] text-chef-muted mt-0.5 truncate">
          {show ? value : masked}
        </div>
      </div>
      <div className="flex items-center gap-1.5 shrink-0">
        <button onClick={() => setShow(!show)} className="p-1.5 text-chef-muted hover:text-chef-text hover:bg-chef-card rounded transition-colors" title={show ? 'Hide' : 'Reveal'}>
          {show ? <EyeOff size={13} /> : <Eye size={13} />}
        </button>
        <button onClick={handleCopy} className="p-1.5 text-chef-muted hover:text-chef-text hover:bg-chef-card rounded transition-colors" title="Copy to clipboard">
          {copied ? <CheckCircle2 size={13} className="text-emerald-400" /> : <Copy size={13} />}
        </button>
        <button
          onClick={handleRotate}
          className="inline-flex items-center gap-1 text-[11px] text-rose-400 border border-rose-500/20 hover:bg-rose-500/10 rounded px-2 py-1 transition-colors"
        >
          {rotating ? <Loader2 size={11} className="animate-spin" /> : <RefreshCw size={11} />}
          Rotate
        </button>
      </div>
    </div>
  )
}

function cloneSettings(settings: AppSettings): AppSettings {
  return JSON.parse(JSON.stringify(settings)) as AppSettings
}

export default function SettingsPage() {
  const { settings, loading, saveSettings, rotateKey, purgeData, deleteWorkspace } = useAppSettings()
  const productName = settings?.branding.productName ?? 'dataChef'
  const [section, setSection] = useState<Section>('workspace')
  const [draft, setDraft] = useState<AppSettings | null>(null)
  const [datasets, setDatasets] = useState<Array<{ id: string; name: string; queryDataset: string | null }>>([])
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)
  const [busyAction, setBusyAction] = useState<string | null>(null)

  useEffect(() => {
    if (settings) setDraft(cloneSettings(settings))
  }, [settings])

  useEffect(() => {
    fetch('/api/datasets')
      .then(res => res.json())
      .then(data => setDatasets(Array.isArray(data) ? data : []))
      .catch(() => setDatasets([]))
  }, [])

  const dirty = useMemo(() => {
    if (!settings || !draft) return false
    return JSON.stringify(settings) !== JSON.stringify(draft)
  }, [draft, settings])

  if (loading || !draft || !settings) {
    return <div className="h-full flex items-center justify-center text-sm text-chef-muted">Loading settings…</div>
  }
  const activeDraft = draft

  async function handleSave() {
    try {
      setSaving(true)
      await saveSettings(activeDraft)
      setSaved(true)
      setTimeout(() => setSaved(false), 2000)
    } finally {
      setSaving(false)
    }
  }

  function resetDraft() {
    if (!settings) return
    setDraft(cloneSettings(settings))
  }

  function setField<K extends keyof AppSettings>(key: K, value: AppSettings[K]) {
    setDraft(current => current ? { ...current, [key]: value } : current)
  }

  const datasetOptions = [
    { value: '', label: 'No built-in default dataset' },
    ...datasets.map(ds => ({
      value: ds.queryDataset ?? ds.name,
      label: ds.name,
    })),
  ]

  const navItems: { id: Section; icon: React.ElementType; label: string }[] = [
    { id: 'workspace', icon: Globe, label: 'Workspace' },
    { id: 'branding', icon: Palette, label: 'Branding' },
    { id: 'query-engine', icon: Cpu, label: 'Query Engine' },
    { id: 'api-keys', icon: Shield, label: 'API Keys' },
    { id: 'notifications', icon: Bell, label: 'Notifications' },
    { id: 'network-discovery', icon: Radar, label: 'Network Discovery' },
    { id: 'danger', icon: AlertTriangle, label: 'Danger Zone' },
  ]

  async function handleDangerAction(action: 'purge-data' | 'delete-workspace') {
    const confirmed = window.confirm(
      action === 'purge-data'
        ? 'Purge all datasets from this workspace?'
        : 'Delete the workspace and reset setup?'
    )
    if (!confirmed) return

    try {
      setBusyAction(action)
      if (action === 'purge-data') {
        await purgeData()
      } else {
        await deleteWorkspace()
      }
    } finally {
      setBusyAction(null)
    }
  }

  return (
    <div className="flex h-full">
      <div className="w-52 shrink-0 border-r border-chef-border flex flex-col">
        <div className="px-4 py-4 border-b border-chef-border flex items-center gap-2">
          <Settings size={15} className="text-indigo-400" />
          <span className="text-sm font-semibold text-chef-text">Settings</span>
        </div>
        <nav className="flex-1 p-2 space-y-0.5">
          {navItems.map(item => (
            <NavItem key={item.id} icon={item.icon} label={item.label} active={section === item.id} onClick={() => setSection(item.id)} />
          ))}
        </nav>
        <div className="px-3 py-3 border-t border-chef-border text-[10px] font-mono text-chef-muted space-y-0.5">
          <div>workspace: {draft.workspace.workspaceName}</div>
          <div>region: {draft.tenant.region}</div>
          <div className="text-indigo-400">owner: {draft.owner.role}</div>
        </div>
      </div>

      <div className="flex-1 overflow-auto">
        <div className="max-w-2xl mx-auto px-8 py-6">
          {section === 'workspace' && (
            <div>
              <div className="mb-6">
                <h2 className="text-base font-bold text-chef-text">Workspace</h2>
                <p className="text-xs text-chef-muted mt-1">Configure your tenant identity, data residency region, and operating timezone.</p>
              </div>
              <div className="bg-chef-card border border-chef-border rounded-xl divide-y divide-chef-border px-5">
                <Field label="Company name" hint="Shown during setup and workspace administration">
                  <TextInput value={draft.workspace.companyName} onChange={value => setField('workspace', { ...draft.workspace, companyName: value })} />
                </Field>
                <Field label="Workspace name" hint="Shown in the sidebar and top bar">
                  <TextInput value={draft.workspace.workspaceName} onChange={value => setField('workspace', { ...draft.workspace, workspaceName: value })} placeholder="my-workspace" />
                </Field>
                <Field label="Tenant slug" hint="Stable internal identifier for this tenant">
                  <TextInput value={draft.tenant.slug} onChange={value => setField('tenant', { ...draft.tenant, slug: value })} placeholder="customer-a" />
                </Field>
                <Field label="Hostnames" hint="Comma-separated hostnames that resolve to this tenant">
                  <TextInput
                    value={draft.tenant.hostnames.join(', ')}
                    onChange={value => setField('tenant', {
                      ...draft.tenant,
                      hostnames: value.split(',').map(item => item.trim()).filter(Boolean),
                    })}
                    placeholder="localhost, tenant.example.com"
                  />
                </Field>
                <Field label="Region" hint="Controls where this tenant's data is stored and processed">
                  <SelectInput value={draft.tenant.region} onChange={value => {
                    setField('tenant', { ...draft.tenant, region: value })
                    setField('workspace', { ...draft.workspace, region: value })
                  }} options={[...REGION_OPTIONS]} />
                </Field>
                <Field label="Timezone" hint="Used for timestamps, schedules, and reporting labels">
                  <SelectInput value={draft.tenant.timezone} onChange={value => {
                    setField('tenant', { ...draft.tenant, timezone: value })
                    setField('workspace', { ...draft.workspace, timezone: value })
                  }} options={[...TIMEZONE_OPTIONS]} />
                </Field>
                <Field label="Owner name" hint="Primary workspace operator">
                  <TextInput value={draft.owner.name} onChange={value => setField('owner', { ...draft.owner, name: value })} />
                </Field>
                <Field label="Owner email" hint="Used for notifications and workspace contact">
                  <TextInput value={draft.owner.email} onChange={value => {
                    setField('owner', { ...draft.owner, email: value })
                    setField('notifications', { ...draft.notifications, emailAddress: value })
                  }} type="email" />
                </Field>
                <Field label="Owner role" hint="Displayed in the workspace footer">
                  <SelectInput value={draft.owner.role} onChange={value => setField('owner', { ...draft.owner, role: value })} options={[...ROLE_OPTIONS]} />
                </Field>
              </div>
            </div>
          )}

          {section === 'branding' && (
            <div>
              <div className="mb-6">
                <h2 className="text-base font-bold text-chef-text">Branding</h2>
                <p className="text-xs text-chef-muted mt-1">White-label the shell, about page, support links, and default product identity for this tenant.</p>
              </div>
              <div className="bg-chef-card border border-chef-border rounded-xl divide-y divide-chef-border px-5">
                <Field label="Product name" hint="Shown in the shell and About page">
                  <TextInput value={draft.branding.productName} onChange={value => setField('branding', { ...draft.branding, productName: value })} />
                </Field>
                <Field label="Logo mode" hint="Controls whether the shell shows the icon, wordmark, or both">
                  <SelectInput
                    value={draft.branding.logoMode}
                    onChange={value => setField('branding', { ...draft.branding, logoMode: value as 'icon' | 'wordmark' | 'both' })}
                    options={[
                      { value: 'both', label: 'Icon + wordmark' },
                      { value: 'icon', label: 'Icon only' },
                      { value: 'wordmark', label: 'Wordmark only' },
                    ]}
                  />
                </Field>
                <Field label="Logo URL" hint="Optional custom logo or icon used in the shell">
                  <TextInput value={draft.branding.logoUrl ?? ''} onChange={value => setField('branding', { ...draft.branding, logoUrl: value })} />
                </Field>
                <Field label="Favicon URL" hint="Optional browser/app icon for this tenant">
                  <TextInput value={draft.branding.faviconUrl ?? ''} onChange={value => setField('branding', { ...draft.branding, faviconUrl: value })} />
                </Field>
                <Field label="Website URL" hint="Primary external destination for this tenant">
                  <TextInput value={draft.branding.websiteUrl ?? ''} onChange={value => setField('branding', { ...draft.branding, websiteUrl: value })} />
                </Field>
                <Field label="Support URL" hint="Used for operator-facing support links">
                  <TextInput value={draft.branding.supportUrl ?? ''} onChange={value => setField('branding', { ...draft.branding, supportUrl: value })} />
                </Field>
                <Field label="Parent company" hint="Shown in the footer and About page">
                  <TextInput value={draft.branding.parentCompanyLabel ?? ''} onChange={value => setField('branding', { ...draft.branding, parentCompanyLabel: value })} />
                </Field>
                <Field label="Primary color" hint="Reserved for brand theming tokens">
                  <TextInput value={draft.branding.primaryColor} onChange={value => setField('branding', { ...draft.branding, primaryColor: value })} mono />
                </Field>
                <Field label="Accent color" hint="Reserved for secondary brand theming tokens">
                  <TextInput value={draft.branding.accentColor} onChange={value => setField('branding', { ...draft.branding, accentColor: value })} mono />
                </Field>
                <Field label="About headline" hint="Hero line on the About page">
                  <TextInput value={draft.branding.aboutHeadline ?? ''} onChange={value => setField('branding', { ...draft.branding, aboutHeadline: value })} />
                </Field>
                <Field label="About body" hint="Summary copy used on the About page">
                  <TextInput value={draft.branding.aboutBody ?? ''} onChange={value => setField('branding', { ...draft.branding, aboutBody: value })} />
                </Field>
              </div>
            </div>
          )}

          {section === 'query-engine' && (
            <div>
              <div className="mb-6">
                <h2 className="text-base font-bold text-chef-text">Query Engine</h2>
                <p className="text-xs text-chef-muted mt-1">Control the defaults used when opening the query editor.</p>
              </div>
              <div className="bg-chef-card border border-chef-border rounded-xl divide-y divide-chef-border px-5">
                <Field label="Max rows" hint="Default row cap used by the editor">
                  <TextInput value={String(draft.queryEngine.maxRows)} onChange={value => setField('queryEngine', { ...draft.queryEngine, maxRows: Number(value) || 0 })} mono placeholder="5000" />
                </Field>
                <Field label="Execution timeout" hint="Default timeout budget in seconds">
                  <TextInput value={String(draft.queryEngine.timeoutSeconds)} onChange={value => setField('queryEngine', { ...draft.queryEngine, timeoutSeconds: Number(value) || 0 })} mono placeholder="30" />
                </Field>
                <Field label="Default dataset" hint="Pre-selected when opening the Query Editor">
                  <SelectInput value={draft.queryEngine.defaultDataset} onChange={value => setField('queryEngine', { ...draft.queryEngine, defaultDataset: value })} options={datasetOptions} />
                </Field>
                <Field label="Auto-execute on open" hint="Run the starter query when the editor loads">
                  <Toggle value={draft.queryEngine.autoExecuteOnOpen} onChange={value => setField('queryEngine', { ...draft.queryEngine, autoExecuteOnOpen: value })} />
                </Field>
              </div>

              <div className="mt-4 p-3 bg-indigo-500/5 border border-indigo-500/15 rounded-xl flex items-start gap-2.5 text-[11px]">
                <Zap size={13} className="text-indigo-400 shrink-0 mt-0.5" />
                <div className="text-chef-muted">
                  These defaults are live. The query editor now reads them for dataset selection, auto-run behavior, and row limits.
                </div>
              </div>
            </div>
          )}

          {section === 'api-keys' && (
            <div>
              <div className="mb-6">
                <h2 className="text-base font-bold text-chef-text">API Keys</h2>
                <p className="text-xs text-chef-muted mt-1">Manage the active credentials used for ingestion, query access, and webhooks.</p>
              </div>
              <div className="bg-chef-card border border-chef-border rounded-xl px-5 divide-y divide-chef-border">
                <ApiKeyRow label="Ingest API key" value={settings.apiKeys.ingestKey} onRotate={() => rotateKey('ingestKey')} />
                <ApiKeyRow label="Query API key" value={settings.apiKeys.queryKey} onRotate={() => rotateKey('queryKey')} />
                <ApiKeyRow label="Webhook secret" value={settings.apiKeys.webhookSecret} onRotate={() => rotateKey('webhookSecret')} />
              </div>
              <div className="mt-4 p-3 bg-amber-500/5 border border-amber-500/15 rounded-xl flex items-start gap-2.5 text-[11px]">
                <AlertTriangle size={13} className="text-amber-400 shrink-0 mt-0.5" />
                <div className="text-chef-muted">
                  Rotating a key writes the new secret immediately and invalidates the previous one.
                </div>
              </div>
            </div>
          )}

          {section === 'notifications' && (
            <div>
              <div className="mb-6">
                <h2 className="text-base font-bold text-chef-text">Notifications</h2>
                <p className="text-xs text-chef-muted mt-1">Choose when and where {productName} alerts the workspace owner.</p>
              </div>

              <div className="space-y-4">
                <div className="bg-chef-card border border-chef-border rounded-xl px-5 divide-y divide-chef-border">
                  <div className="py-3 flex items-center justify-between">
                    <div className="text-xs font-semibold text-chef-text">Pipeline alerts</div>
                  </div>
                  {[
                    { label: 'Pipeline failure', hint: 'Notify when a pipeline step fails', value: draft.notifications.pipelineFailure, set: (value: boolean) => setField('notifications', { ...draft.notifications, pipelineFailure: value }) },
                    { label: 'Pipeline success', hint: 'Notify on every successful run', value: draft.notifications.pipelineSuccess, set: (value: boolean) => setField('notifications', { ...draft.notifications, pipelineSuccess: value }) },
                  ].map(({ label, hint, value, set }) => (
                    <div key={label} className="py-3.5 flex items-center justify-between">
                      <div>
                        <div className="text-xs font-medium text-chef-text">{label}</div>
                        <div className="text-[10px] text-chef-muted mt-0.5">{hint}</div>
                      </div>
                      <Toggle value={value} onChange={set} />
                    </div>
                  ))}
                </div>

                <div className="bg-chef-card border border-chef-border rounded-xl px-5 divide-y divide-chef-border">
                  <div className="py-3">
                    <div className="text-xs font-semibold text-chef-text">Delivery channels</div>
                  </div>
                  <Field label="Email address" hint="Primary address for workspace alerts">
                    <TextInput value={draft.notifications.emailAddress} onChange={value => setField('notifications', { ...draft.notifications, emailAddress: value })} type="email" />
                  </Field>
                  <Field label="Slack channel" hint="Reference channel or webhook target">
                    <TextInput value={draft.notifications.slackChannel} onChange={value => setField('notifications', { ...draft.notifications, slackChannel: value })} mono placeholder="#data-alerts" />
                  </Field>
                  <div className="py-3.5 flex items-center justify-between">
                    <div>
                      <div className="text-xs font-medium text-chef-text">Email</div>
                      <div className="text-[10px] text-chef-muted font-mono mt-0.5">{draft.notifications.emailAddress}</div>
                    </div>
                    <Toggle value={draft.notifications.emailEnabled} onChange={value => setField('notifications', { ...draft.notifications, emailEnabled: value })} />
                  </div>
                  <div className="py-3.5 flex items-center justify-between">
                    <div>
                      <div className="text-xs font-medium text-chef-text">Slack</div>
                      <div className="text-[10px] text-chef-muted font-mono mt-0.5">{draft.notifications.slackChannel}</div>
                    </div>
                    <Toggle value={draft.notifications.slackEnabled} onChange={value => setField('notifications', { ...draft.notifications, slackEnabled: value })} />
                  </div>
                </div>
              </div>
            </div>
          )}

          {section === 'network-discovery' && (
            <div>
              <div className="mb-6">
                <h2 className="text-base font-bold text-chef-text">Network Discovery</h2>
                <p className="text-xs text-chef-muted mt-1">Manage local-network scanning for addable connector candidates.</p>
              </div>

              <div className="space-y-4">
                <div className="bg-chef-card border border-chef-border rounded-xl px-5 divide-y divide-chef-border">
                  <Field label="Enable discovery" hint={`Allows ${productName} to scan private-network hosts for supported connector types.`}>
                    <Toggle value={draft.networkDiscovery.enabled} onChange={value => setField('networkDiscovery', { ...draft.networkDiscovery, enabled: value })} />
                  </Field>
                  <Field label="Scan during setup" hint="Run an initial discovery scan when a new workspace completes setup.">
                    <Toggle value={draft.networkDiscovery.scanOnSetup} onChange={value => setField('networkDiscovery', { ...draft.networkDiscovery, scanOnSetup: value })} />
                  </Field>
                  <Field label="Background refresh" hint="Keep suggestions up to date in the background once discovery is enabled.">
                    <Toggle value={draft.networkDiscovery.backgroundRefreshEnabled} onChange={value => setField('networkDiscovery', { ...draft.networkDiscovery, backgroundRefreshEnabled: value })} />
                  </Field>
                  <Field label="Refresh interval" hint="Minutes between automatic discovery scans.">
                    <TextInput
                      value={String(draft.networkDiscovery.refreshIntervalMinutes)}
                      onChange={value => setField('networkDiscovery', {
                        ...draft.networkDiscovery,
                        refreshIntervalMinutes: Math.max(15, Number(value) || 15),
                      })}
                      mono
                      placeholder="60"
                    />
                  </Field>
                  <Field label="Subnet scope" hint="V1 is limited to private/local subnet scanning only.">
                    <SelectInput
                      value={draft.networkDiscovery.subnetMode}
                      onChange={value => setField('networkDiscovery', { ...draft.networkDiscovery, subnetMode: value as typeof draft.networkDiscovery.subnetMode })}
                      options={[{ value: 'local-subnet', label: 'Local subnet only' }]}
                    />
                  </Field>
                </div>

                <div className="rounded-xl border border-indigo-500/20 bg-indigo-500/5 p-4 text-[11px] text-chef-muted space-y-2">
                  <div className="flex items-center gap-2 text-chef-text">
                    <Radar size={12} className="text-indigo-300" />
                    <span className="font-semibold">Current state</span>
                  </div>
                  <div>Enabled: <span className="font-mono text-chef-text">{draft.networkDiscovery.enabled ? 'true' : 'false'}</span></div>
                  <div>Last scan: <span className="font-mono text-chef-text">{draft.networkDiscovery.lastScanAt ? new Date(draft.networkDiscovery.lastScanAt).toLocaleString() : 'never'}</span></div>
                </div>

                <div className="rounded-xl border border-amber-500/20 bg-amber-500/5 p-4 text-[11px] text-amber-200">
                  Discovery records only service metadata such as type, host, port, and match confidence. Credentials are never collected during scans.
                </div>
              </div>
            </div>
          )}

          {section === 'danger' && (
            <div>
              <div className="mb-6">
                <h2 className="text-base font-bold text-chef-text">Danger Zone</h2>
                <p className="text-xs text-chef-muted mt-1">These actions change live workspace state.</p>
              </div>
              <div className="space-y-3">
                <div className="bg-chef-card border border-chef-border rounded-xl p-4 flex items-start justify-between gap-4">
                  <div className="min-w-0">
                    <div className="text-sm font-semibold text-chef-text">Clear query history</div>
                    <div className="text-xs text-chef-muted mt-0.5">Deletes locally stored query history in this browser.</div>
                  </div>
                  <button
                    onClick={() => {
                      localStorage.removeItem('datachef:queryHistory')
                      window.dispatchEvent(new Event('storage'))
                    }}
                    className="shrink-0 text-xs border rounded-lg px-3 py-1.5 transition-colors border-amber-500/20 text-amber-400 hover:bg-amber-500/10"
                  >
                    Clear history
                  </button>
                </div>
                <div className="bg-chef-card border border-chef-border rounded-xl p-4 flex items-start justify-between gap-4">
                  <div className="min-w-0">
                    <div className="text-sm font-semibold text-chef-text">Purge all datasets</div>
                    <div className="text-xs text-chef-muted mt-0.5">Removes all ingested datasets while leaving workspace settings intact.</div>
                  </div>
                  <button
                    onClick={() => handleDangerAction('purge-data')}
                    className="shrink-0 inline-flex items-center gap-1 text-xs border rounded-lg px-3 py-1.5 transition-colors border-rose-500/20 text-rose-400 hover:bg-rose-500/10"
                  >
                    {busyAction === 'purge-data' ? <Loader2 size={12} className="animate-spin" /> : null}
                    Purge data
                  </button>
                </div>
                <div className="bg-chef-card border border-chef-border rounded-xl p-4 flex items-start justify-between gap-4">
                  <div className="min-w-0">
                    <div className="text-sm font-semibold text-chef-text">Delete workspace</div>
                    <div className="text-xs text-chef-muted mt-0.5">Resets setup, clears datasets, connectors, pipelines, and generated saved queries.</div>
                  </div>
                  <button
                    onClick={() => handleDangerAction('delete-workspace')}
                    className="shrink-0 inline-flex items-center gap-1 text-xs border rounded-lg px-3 py-1.5 transition-colors border-rose-500/20 text-rose-400 hover:bg-rose-500/10"
                  >
                    {busyAction === 'delete-workspace' ? <Loader2 size={12} className="animate-spin" /> : null}
                    Delete workspace
                  </button>
                </div>
              </div>
            </div>
          )}

          {section !== 'api-keys' && section !== 'danger' && (
            <div className="mt-6 flex items-center justify-end gap-3">
              <button onClick={resetDraft} className="text-xs text-chef-muted hover:text-chef-text transition-colors px-4 py-2">
                Reset
              </button>
              <button
                onClick={handleSave}
                disabled={!dirty || saving}
                className="flex items-center gap-1.5 text-xs font-medium bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-2 rounded-lg transition-colors disabled:opacity-50"
              >
                {saving ? <Loader2 size={13} className="animate-spin" /> : saved ? <CheckCircle2 size={13} /> : null}
                {saved ? 'Saved' : 'Save changes'}
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
