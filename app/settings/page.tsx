'use client'

import { useState } from 'react'
import {
  Settings, User, Shield, Bell, Database, Cpu,
  ChevronRight, CheckCircle2, Copy, Eye, EyeOff,
  Globe, Clock, AlertTriangle, Zap,
} from 'lucide-react'

/* ── Types ── */
type Section = 'workspace' | 'query-engine' | 'api-keys' | 'notifications' | 'danger'

/* ── Toggle component ── */
function Toggle({ value, onChange }: { value: boolean; onChange: (v: boolean) => void }) {
  return (
    <button
      onClick={() => onChange(!value)}
      className={`relative w-9 h-5 rounded-full transition-colors shrink-0 ${value ? 'bg-indigo-500' : 'bg-chef-border'}`}
    >
      <span className={`absolute top-0.5 w-4 h-4 rounded-full bg-white shadow-sm transition-all ${value ? 'left-4.5' : 'left-0.5'}`}
        style={{ left: value ? 18 : 2 }}
      />
    </button>
  )
}

/* ── Masked API key ── */
function ApiKeyRow({ label, value }: { label: string; value: string }) {
  const [show, setShow] = useState(false)
  const [copied, setCopied] = useState(false)

  function handleCopy() {
    navigator.clipboard.writeText(value).catch(() => {})
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }

  const masked = value.slice(0, 8) + '•'.repeat(24) + value.slice(-4)

  return (
    <div className="flex items-center gap-3 py-3 border-b border-chef-border last:border-b-0">
      <div className="flex-1 min-w-0">
        <div className="text-xs font-medium text-chef-text">{label}</div>
        <div className="font-mono text-[11px] text-chef-muted mt-0.5 truncate">
          {show ? value : masked}
        </div>
      </div>
      <div className="flex items-center gap-1.5 shrink-0">
        <button
          onClick={() => setShow(!show)}
          className="p-1.5 text-chef-muted hover:text-chef-text hover:bg-chef-card rounded transition-colors"
          title={show ? 'Hide' : 'Reveal'}
        >
          {show ? <EyeOff size={13} /> : <Eye size={13} />}
        </button>
        <button
          onClick={handleCopy}
          className="p-1.5 text-chef-muted hover:text-chef-text hover:bg-chef-card rounded transition-colors"
          title="Copy to clipboard"
        >
          {copied ? <CheckCircle2 size={13} className="text-emerald-400" /> : <Copy size={13} />}
        </button>
        <button className="text-[11px] text-rose-400 border border-rose-500/20 hover:bg-rose-500/10 rounded px-2 py-1 transition-colors">
          Rotate
        </button>
      </div>
    </div>
  )
}

/* ── Field wrapper ── */
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

/* ── Text input ── */
function TextInput({ value, onChange, mono = false, placeholder }: {
  value: string; onChange: (v: string) => void; mono?: boolean; placeholder?: string
}) {
  return (
    <input
      value={value}
      onChange={e => onChange(e.target.value)}
      placeholder={placeholder}
      className={`w-full bg-chef-bg border border-chef-border rounded-lg px-3 py-1.5 text-xs text-chef-text placeholder:text-chef-muted outline-none focus:border-indigo-500/50 transition-colors ${mono ? 'font-mono' : ''}`}
    />
  )
}

/* ── Select ── */
function SelectInput({ value, onChange, options }: {
  value: string; onChange: (v: string) => void; options: { value: string; label: string }[]
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

/* ── Section nav item ── */
function NavItem({ id, icon: Icon, label, active, onClick }: {
  id: Section; icon: React.ElementType; label: string; active: boolean; onClick: () => void
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

/* ── Main page ── */
export default function SettingsPage() {
  const [section, setSection] = useState<Section>('workspace')

  /* Workspace state */
  const [wsName,   setWsName]   = useState('acme-labs')
  const [wsRegion, setWsRegion] = useState('ap-southeast-2')
  const [wsTz,     setWsTz]     = useState('Australia/Sydney')

  /* Query engine state */
  const [maxRows,   setMaxRows]   = useState('5000')
  const [timeout,   setTimeout_]  = useState('30')
  const [defDs,     setDefDs]     = useState('')
  const [autoExec,  setAutoExec]  = useState(false)

  /* Notification state */
  const [notifyFail,    setNotifyFail]    = useState(true)
  const [notifySuccess, setNotifySuccess] = useState(false)
  const [notifySlack,   setNotifySlack]   = useState(false)
  const [notifyEmail,   setNotifyEmail]   = useState(true)

  const [saved, setSaved] = useState(false)
  function handleSave() {
    setSaved(true)
    setTimeout(() => setSaved(false), 2000)
  }

  const navItems: { id: Section; icon: React.ElementType; label: string }[] = [
    { id: 'workspace',    icon: Globe,     label: 'Workspace' },
    { id: 'query-engine', icon: Cpu,       label: 'Query Engine' },
    { id: 'api-keys',     icon: Shield,    label: 'API Keys' },
    { id: 'notifications',icon: Bell,      label: 'Notifications' },
    { id: 'danger',       icon: AlertTriangle, label: 'Danger Zone' },
  ]

  return (
    <div className="flex h-full">
      {/* ── Left nav ── */}
      <div className="w-52 shrink-0 border-r border-chef-border flex flex-col">
        <div className="px-4 py-4 border-b border-chef-border flex items-center gap-2">
          <Settings size={15} className="text-indigo-400" />
          <span className="text-sm font-semibold text-chef-text">Settings</span>
        </div>
        <nav className="flex-1 p-2 space-y-0.5">
          {navItems.map(item => (
            <NavItem
              key={item.id}
              {...item}
              active={section === item.id}
              onClick={() => setSection(item.id)}
            />
          ))}
        </nav>
        <div className="px-3 py-3 border-t border-chef-border text-[10px] font-mono text-chef-muted space-y-0.5">
          <div>workspace: acme-labs</div>
          <div>region: ap-southeast-2</div>
          <div className="text-indigo-400">plan: Pro</div>
        </div>
      </div>

      {/* ── Right content ── */}
      <div className="flex-1 overflow-auto">
        <div className="max-w-2xl mx-auto px-8 py-6">

          {/* ── WORKSPACE ── */}
          {section === 'workspace' && (
            <div>
              <div className="mb-6">
                <h2 className="text-base font-bold text-chef-text">Workspace</h2>
                <p className="text-xs text-chef-muted mt-1">Configure your workspace identity and regional settings.</p>
              </div>
              <div className="bg-chef-card border border-chef-border rounded-xl divide-y divide-chef-border px-5">
                <Field label="Workspace name" hint="Shown in the sidebar and URLs">
                  <TextInput value={wsName} onChange={setWsName} placeholder="my-workspace" />
                </Field>
                <Field label="Region" hint="Data residency — cannot be changed after creation">
                  <SelectInput
                    value={wsRegion}
                    onChange={setWsRegion}
                    options={[
                      { value: 'ap-southeast-2', label: 'Asia Pacific — Sydney (ap-southeast-2)' },
                      { value: 'us-east-1',       label: 'US East — N. Virginia (us-east-1)' },
                      { value: 'eu-west-1',        label: 'Europe — Ireland (eu-west-1)' },
                    ]}
                  />
                </Field>
                <Field label="Timezone" hint="Used for scheduled runs and timestamps">
                  <SelectInput
                    value={wsTz}
                    onChange={setWsTz}
                    options={[
                      { value: 'Australia/Sydney',  label: 'Australia/Sydney (AEDT +11)' },
                      { value: 'America/New_York',  label: 'America/New_York (EST -5)' },
                      { value: 'Europe/London',     label: 'Europe/London (GMT +0)' },
                      { value: 'Asia/Tokyo',        label: 'Asia/Tokyo (JST +9)' },
                    ]}
                  />
                </Field>
                <Field label="Owner" hint="Workspace owner account">
                  <div className="flex items-center gap-2">
                    <div className="w-6 h-6 rounded-full bg-gradient-to-br from-violet-500 to-indigo-600 flex items-center justify-center text-white text-[9px] font-bold shrink-0">JD</div>
                    <span className="text-xs text-chef-text">Jane Doe</span>
                    <span className="text-[10px] text-chef-muted font-mono">jane@acme-labs.io</span>
                  </div>
                </Field>
              </div>
            </div>
          )}

          {/* ── QUERY ENGINE ── */}
          {section === 'query-engine' && (
            <div>
              <div className="mb-6">
                <h2 className="text-base font-bold text-chef-text">Query Engine</h2>
                <p className="text-xs text-chef-muted mt-1">Control execution limits, defaults, and behaviour of the server-side query runner.</p>
              </div>
              <div className="bg-chef-card border border-chef-border rounded-xl divide-y divide-chef-border px-5">
                <Field label="Max rows returned" hint="Results are paginated above this limit">
                  <div className="flex items-center gap-2">
                    <TextInput value={maxRows} onChange={setMaxRows} mono placeholder="5000" />
                    <span className="text-[11px] text-chef-muted shrink-0">rows</span>
                  </div>
                </Field>
                <Field label="Execution timeout" hint="Query cancelled if it exceeds this duration">
                  <div className="flex items-center gap-2">
                    <TextInput value={timeout} onChange={setTimeout_} mono placeholder="30" />
                    <span className="text-[11px] text-chef-muted shrink-0">seconds</span>
                  </div>
                </Field>
                <Field label="Default dataset" hint="Pre-selected when opening the Query Editor">
                  <SelectInput
                    value={defDs}
                    onChange={setDefDs}
                    options={[
                      { value: '', label: 'No built-in default dataset' },
                    ]}
                  />
                </Field>
                <Field label="Auto-execute on open" hint="Run the default query when the editor loads">
                  <Toggle value={autoExec} onChange={setAutoExec} />
                </Field>
              </div>

              <div className="mt-4 p-3 bg-indigo-500/5 border border-indigo-500/15 rounded-xl flex items-start gap-2.5 text-[11px]">
                <Zap size={13} className="text-indigo-400 shrink-0 mt-0.5" />
                <div className="text-chef-muted">
                  Server-side execution is enabled. Query performance depends on the live dataset or connector you select; there are no built-in sample datasets preloaded anymore.
                </div>
              </div>
            </div>
          )}

          {/* ── API KEYS ── */}
          {section === 'api-keys' && (
            <div>
              <div className="mb-6">
                <h2 className="text-base font-bold text-chef-text">API Keys</h2>
                <p className="text-xs text-chef-muted mt-1">Manage programmatic access credentials. Rotate keys regularly.</p>
              </div>
              <div className="bg-chef-card border border-chef-border rounded-xl px-5 divide-y divide-chef-border">
                <ApiKeyRow label="Ingest API key"  value="dc_live_ingest_aX9kZq2mPv7nWrTsL4eY8cFbJdHuNgKo" />
                <ApiKeyRow label="Query API key"   value="dc_live_query_fM5pRxBwQzEi3hVcDs6tAkUjNyOlCgXn" />
                <ApiKeyRow label="Webhook secret"  value="whsec_7T4vLm9XuPqNrHzJaEcKsYiBdFgWoCtSe" />
              </div>
              <div className="mt-4 p-3 bg-amber-500/5 border border-amber-500/15 rounded-xl flex items-start gap-2.5 text-[11px]">
                <AlertTriangle size={13} className="text-amber-400 shrink-0 mt-0.5" />
                <div className="text-chef-muted">
                  Keys are shown masked. Click the eye icon to reveal. Never share keys in version control or public channels. Rotating a key immediately invalidates the previous one.
                </div>
              </div>
              <div className="mt-4">
                <button className="flex items-center gap-1.5 text-xs text-indigo-400 border border-indigo-500/30 rounded-lg px-3 py-2 hover:bg-indigo-500/10 transition-colors">
                  + Generate new key
                </button>
              </div>
            </div>
          )}

          {/* ── NOTIFICATIONS ── */}
          {section === 'notifications' && (
            <div>
              <div className="mb-6">
                <h2 className="text-base font-bold text-chef-text">Notifications</h2>
                <p className="text-xs text-chef-muted mt-1">Choose when and how dataChef alerts you about pipeline and query events.</p>
              </div>

              <div className="space-y-4">
                {/* Pipeline alerts */}
                <div className="bg-chef-card border border-chef-border rounded-xl px-5 divide-y divide-chef-border">
                  <div className="py-3 flex items-center justify-between">
                    <div>
                      <div className="text-xs font-semibold text-chef-text">Pipeline alerts</div>
                    </div>
                  </div>
                  {[
                    { label: 'Pipeline failure', hint: 'Notify when a pipeline step fails', value: notifyFail, set: setNotifyFail },
                    { label: 'Pipeline success', hint: 'Notify on every successful run', value: notifySuccess, set: setNotifySuccess },
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

                {/* Delivery channels */}
                <div className="bg-chef-card border border-chef-border rounded-xl px-5 divide-y divide-chef-border">
                  <div className="py-3">
                    <div className="text-xs font-semibold text-chef-text">Delivery channels</div>
                  </div>
                  {[
                    { label: 'Email',       hint: 'jane@acme-labs.io',                   value: notifyEmail, set: setNotifyEmail },
                    { label: 'Slack',       hint: '#data-alerts · webhook not configured', value: notifySlack, set: setNotifySlack },
                  ].map(({ label, hint, value, set }) => (
                    <div key={label} className="py-3.5 flex items-center justify-between">
                      <div>
                        <div className="text-xs font-medium text-chef-text">{label}</div>
                        <div className="text-[10px] text-chef-muted font-mono mt-0.5">{hint}</div>
                      </div>
                      <Toggle value={value} onChange={set} />
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* ── DANGER ZONE ── */}
          {section === 'danger' && (
            <div>
              <div className="mb-6">
                <h2 className="text-base font-bold text-chef-text">Danger Zone</h2>
                <p className="text-xs text-chef-muted mt-1">Irreversible workspace operations. Proceed with caution.</p>
              </div>
              <div className="space-y-3">
                {[
                  {
                    title: 'Clear query history',
                    desc: 'Permanently deletes all stored query history for this workspace.',
                    label: 'Clear history',
                    color: 'border-amber-500/20 text-amber-400 hover:bg-amber-500/10',
                  },
                  {
                    title: 'Purge all datasets',
                    desc: 'Removes all ingested data. Connections and pipelines are kept.',
                    label: 'Purge data',
                    color: 'border-rose-500/20 text-rose-400 hover:bg-rose-500/10',
                  },
                  {
                    title: 'Delete workspace',
                    desc: 'Permanently deletes acme-labs, all data, pipelines, and connections.',
                    label: 'Delete workspace',
                    color: 'border-rose-500/20 text-rose-400 hover:bg-rose-500/10',
                  },
                ].map(({ title, desc, label, color }) => (
                  <div key={title} className="bg-chef-card border border-chef-border rounded-xl p-4 flex items-start justify-between gap-4">
                    <div className="min-w-0">
                      <div className="text-sm font-semibold text-chef-text">{title}</div>
                      <div className="text-xs text-chef-muted mt-0.5">{desc}</div>
                    </div>
                    <button className={`shrink-0 text-xs border rounded-lg px-3 py-1.5 transition-colors ${color}`}>
                      {label}
                    </button>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* ── Save bar ── */}
          {section !== 'api-keys' && section !== 'danger' && (
            <div className="mt-6 flex items-center justify-end gap-3">
              <button className="text-xs text-chef-muted hover:text-chef-text transition-colors px-4 py-2">
                Reset
              </button>
              <button
                onClick={handleSave}
                className="flex items-center gap-1.5 text-xs font-medium bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-2 rounded-lg transition-colors"
              >
                {saved ? <><CheckCircle2 size={13} /> Saved</> : 'Save changes'}
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
