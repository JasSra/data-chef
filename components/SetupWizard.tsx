'use client'

import { useMemo, useState } from 'react'
import { ArrowRight, CheckCircle2, ChefHat, Loader2, Radar, Sparkles } from 'lucide-react'
import { ROLE_OPTIONS, REGION_OPTIONS, TIMEZONE_OPTIONS } from '@/lib/app-settings-schema'
import { useAppSettings } from '@/components/SettingsProvider'

function slugify(value: string): string {
  return value
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 40) || 'my-workspace'
}

export default function SetupWizard() {
  const { settings, loading, saveSettings } = useAppSettings()
  const [step, setStep] = useState(0)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [companyName, setCompanyName] = useState(settings?.workspace.companyName ?? 'Acme Labs')
  const [workspaceName, setWorkspaceName] = useState(settings?.workspace.workspaceName ?? 'acme-labs')
  const [region, setRegion] = useState(settings?.workspace.region ?? REGION_OPTIONS[0].value)
  const [timezone, setTimezone] = useState(settings?.workspace.timezone ?? TIMEZONE_OPTIONS[0].value)
  const [ownerName, setOwnerName] = useState(settings?.owner.name ?? 'Jane Doe')
  const [ownerEmail, setOwnerEmail] = useState(settings?.owner.email ?? 'owner@acme-labs.io')
  const [ownerRole, setOwnerRole] = useState(settings?.owner.role ?? ROLE_OPTIONS[0].value)
  const [networkDiscoveryEnabled, setNetworkDiscoveryEnabled] = useState(settings?.networkDiscovery.enabled ?? false)

  const steps = useMemo(() => [
    { title: 'Workspace identity', body: 'Set the company and workspace names that appear across the app.' },
    { title: 'Owner profile', body: 'Capture the primary operator for notifications, approvals, and workspace ownership.' },
    { title: 'Defaults', body: 'We will preload safe query and notification defaults, and generate live API keys.' },
    { title: 'Network discovery', body: 'Optionally scan the local subnet for addable connector candidates and keep suggestions fresh in the background.' },
  ], [])

  if (loading || !settings || settings.setupCompleted) return null
  const activeSettings = settings

  async function handleFinish() {
    if (!companyName.trim() || !workspaceName.trim() || !ownerName.trim() || !ownerEmail.trim()) {
      setError('Complete all setup fields before continuing.')
      return
    }

    try {
      setSaving(true)
      setError(null)
      await saveSettings({
        setupCompleted: true,
        workspace: {
          companyName: companyName.trim(),
          workspaceName: workspaceName.trim(),
          region,
          timezone,
        },
        owner: {
          name: ownerName.trim(),
          email: ownerEmail.trim(),
          role: ownerRole,
        },
        notifications: {
          ...activeSettings.notifications,
          emailAddress: ownerEmail.trim(),
        },
        networkDiscovery: {
          ...activeSettings.networkDiscovery,
          enabled: networkDiscoveryEnabled,
          scanOnSetup: true,
          backgroundRefreshEnabled: true,
          refreshIntervalMinutes: 60,
          subnetMode: 'local-subnet',
        },
      })

      if (networkDiscoveryEnabled) {
        void fetch('/api/discovery', { method: 'POST' }).catch(() => {})
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to save setup')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="fixed inset-0 z-[100] bg-[#05070b]/80 backdrop-blur-sm flex items-center justify-center p-6">
      <div className="w-full max-w-4xl rounded-[28px] border border-white/10 bg-[linear-gradient(180deg,rgba(17,24,39,0.96),rgba(10,14,20,0.98))] shadow-2xl shadow-black/60 overflow-hidden">
        <div className="grid md:grid-cols-[280px_1fr]">
          <div className="border-b md:border-b-0 md:border-r border-white/10 p-8 bg-[radial-gradient(circle_at_top,rgba(99,102,241,0.22),transparent_60%)]">
            <div className="w-12 h-12 rounded-2xl bg-gradient-to-br from-indigo-500 to-violet-600 flex items-center justify-center shadow-lg shadow-indigo-900/50">
              <ChefHat size={24} className="text-white" />
            </div>
            <h2 className="mt-5 text-2xl font-semibold text-white">Set up dataChef</h2>
            <p className="mt-2 text-sm text-slate-300 leading-relaxed">
              This runs once, writes to real app settings, and preloads sane defaults so the workspace is usable immediately.
            </p>
            <div className="mt-8 space-y-3">
              {steps.map((item, index) => (
                <div key={item.title} className={`rounded-2xl border px-4 py-3 transition-colors ${index === step ? 'border-indigo-400/40 bg-indigo-500/10' : 'border-white/10 bg-white/5'}`}>
                  <div className="flex items-center gap-2">
                    <div className={`w-6 h-6 rounded-full text-xs font-semibold flex items-center justify-center ${index < step ? 'bg-emerald-500 text-white' : index === step ? 'bg-indigo-500 text-white' : 'bg-white/10 text-slate-300'}`}>
                      {index < step ? <CheckCircle2 size={14} /> : index + 1}
                    </div>
                    <span className="text-sm font-medium text-white">{item.title}</span>
                  </div>
                  <p className="mt-2 text-xs text-slate-400 leading-relaxed">{item.body}</p>
                </div>
              ))}
            </div>
          </div>

          <div className="p-8 md:p-10">
            {step === 0 && (
              <div>
                <div className="text-sm font-medium text-indigo-300">Step 1</div>
                <h3 className="mt-2 text-2xl font-semibold text-white">Name the workspace</h3>
                <p className="mt-2 text-sm text-slate-400">These values drive the shell labels, workspace settings, and ownership defaults.</p>
                <div className="mt-8 grid gap-5">
                  <label className="block">
                    <div className="text-xs font-medium text-slate-300 mb-2">Company name</div>
                    <input
                      value={companyName}
                      onChange={(e) => {
                        const value = e.target.value
                        setCompanyName(value)
                        setWorkspaceName(slugify(value))
                      }}
                      className="w-full rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none focus:border-indigo-400/60"
                    />
                  </label>
                  <label className="block">
                    <div className="text-xs font-medium text-slate-300 mb-2">Workspace name</div>
                    <input
                      value={workspaceName}
                      onChange={(e) => setWorkspaceName(slugify(e.target.value))}
                      className="w-full rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none focus:border-indigo-400/60"
                    />
                  </label>
                  <div className="grid md:grid-cols-2 gap-5">
                    <label className="block">
                      <div className="text-xs font-medium text-slate-300 mb-2">Region</div>
                      <select value={region} onChange={(e) => setRegion(e.target.value)} className="w-full rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none focus:border-indigo-400/60">
                        {REGION_OPTIONS.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
                      </select>
                    </label>
                    <label className="block">
                      <div className="text-xs font-medium text-slate-300 mb-2">Timezone</div>
                      <select value={timezone} onChange={(e) => setTimezone(e.target.value)} className="w-full rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none focus:border-indigo-400/60">
                        {TIMEZONE_OPTIONS.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
                      </select>
                    </label>
                  </div>
                </div>
              </div>
            )}

            {step === 1 && (
              <div>
                <div className="text-sm font-medium text-indigo-300">Step 2</div>
                <h3 className="mt-2 text-2xl font-semibold text-white">Add the owner</h3>
                <p className="mt-2 text-sm text-slate-400">The owner details feed the profile chip, notification defaults, and workspace settings page.</p>
                <div className="mt-8 grid gap-5">
                  <label className="block">
                    <div className="text-xs font-medium text-slate-300 mb-2">User name</div>
                    <input value={ownerName} onChange={(e) => setOwnerName(e.target.value)} className="w-full rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none focus:border-indigo-400/60" />
                  </label>
                  <label className="block">
                    <div className="text-xs font-medium text-slate-300 mb-2">Email</div>
                    <input value={ownerEmail} onChange={(e) => setOwnerEmail(e.target.value)} className="w-full rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none focus:border-indigo-400/60" />
                  </label>
                  <label className="block">
                    <div className="text-xs font-medium text-slate-300 mb-2">Role</div>
                    <select value={ownerRole} onChange={(e) => setOwnerRole(e.target.value)} className="w-full rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-white outline-none focus:border-indigo-400/60">
                      {ROLE_OPTIONS.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
                    </select>
                  </label>
                </div>
              </div>
            )}

            {step === 2 && (
              <div>
                <div className="text-sm font-medium text-indigo-300">Step 3</div>
                <h3 className="mt-2 text-2xl font-semibold text-white">Apply operating defaults</h3>
                <p className="mt-2 text-sm text-slate-400">These are live defaults, not placeholders. You can change them later in Settings.</p>
                <div className="mt-8 grid gap-4">
                  {[
                    'Query results capped at 5,000 rows by default',
                    '30 second query timeout',
                    'Pipeline failure email notifications enabled',
                    'Fresh ingest, query, and webhook secrets already generated',
                  ].map(item => (
                    <div key={item} className="flex items-center gap-3 rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
                      <Sparkles size={14} className="text-indigo-300 shrink-0" />
                      <span className="text-sm text-slate-200">{item}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {step === 3 && (
              <div>
                <div className="text-sm font-medium text-indigo-300">Step 4</div>
                <h3 className="mt-2 text-2xl font-semibold text-white">Network discovery</h3>
                <p className="mt-2 text-sm text-slate-400">Opt in if you want dataChef to scan your local/private network for connector candidates like PostgreSQL, MySQL, MongoDB, SFTP, and S3-compatible storage.</p>
                <div className="mt-8 space-y-4">
                  <button
                    type="button"
                    onClick={() => setNetworkDiscoveryEnabled(true)}
                    className={`w-full rounded-2xl border p-5 text-left transition-colors ${networkDiscoveryEnabled ? 'border-indigo-400/50 bg-indigo-500/10' : 'border-white/10 bg-white/5 hover:border-indigo-400/30'}`}
                  >
                    <div className="flex items-start gap-3">
                      <div className="mt-0.5 w-10 h-10 rounded-2xl bg-indigo-500/15 flex items-center justify-center">
                        <Radar size={18} className="text-indigo-300" />
                      </div>
                      <div>
                        <div className="text-sm font-semibold text-white">Enable local network discovery</div>
                        <div className="mt-1 text-xs leading-relaxed text-slate-300">Runs an initial scan after setup, limits probing to private/local addresses, and keeps suggestions refreshed hourly in the background.</div>
                      </div>
                    </div>
                  </button>

                  <button
                    type="button"
                    onClick={() => setNetworkDiscoveryEnabled(false)}
                    className={`w-full rounded-2xl border p-5 text-left transition-colors ${!networkDiscoveryEnabled ? 'border-white/20 bg-white/10' : 'border-white/10 bg-white/5 hover:border-white/20'}`}
                  >
                    <div className="text-sm font-semibold text-white">Skip discovery for now</div>
                    <div className="mt-1 text-xs leading-relaxed text-slate-400">No network scans will run until the feature is enabled later.</div>
                  </button>

                  <div className="rounded-2xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-xs leading-relaxed text-amber-200">
                    Discovery only inspects private-network hosts and stores service metadata such as host, port, and match confidence. It never stores credentials.
                  </div>
                </div>
              </div>
            )}

            {error && <div className="mt-6 rounded-xl border border-rose-500/30 bg-rose-500/10 px-4 py-3 text-sm text-rose-300">{error}</div>}

            <div className="mt-8 flex items-center justify-between">
              <button
                onClick={() => setStep((current) => Math.max(0, current - 1))}
                className="text-sm text-slate-400 hover:text-white transition-colors disabled:opacity-40"
                disabled={step === 0 || saving}
              >
                Back
              </button>
              {step < steps.length - 1 ? (
                <button
                  onClick={() => setStep((current) => Math.min(steps.length - 1, current + 1))}
                  className="inline-flex items-center gap-2 rounded-xl bg-indigo-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-indigo-500 transition-colors"
                >
                  Continue <ArrowRight size={16} />
                </button>
              ) : (
                <button
                  onClick={handleFinish}
                  disabled={saving}
                  className="inline-flex items-center gap-2 rounded-xl bg-indigo-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-indigo-500 transition-colors disabled:opacity-60"
                >
                  {saving ? <Loader2 size={16} className="animate-spin" /> : <CheckCircle2 size={16} />}
                  Finish setup
                </button>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
