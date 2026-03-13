'use client'

import { useState } from 'react'
import {
  X, Loader2, Check, AlertCircle, Globe, Key, Shield, ChevronRight,
} from 'lucide-react'

interface ApiServiceWizardProps {
  open: boolean
  onClose: () => void
  onCreated: () => void
}

interface SpecPreview {
  valid: boolean
  title: string
  description: string
  apiVersion: string
  baseUrl: string
  endpointCount: number
  tags: string[]
  detectedAuth: { type: string; name?: string; in?: string }[]
}

type AuthScheme = 'none' | 'api_key' | 'bearer' | 'basic'
type Step = 'url' | 'auth' | 'confirm'

export default function ApiServiceWizard({ open, onClose, onCreated }: ApiServiceWizardProps) {
  const [step, setStep] = useState<Step>('url')
  const [name, setName] = useState('')
  const [swaggerUrl, setSwaggerUrl] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [preview, setPreview] = useState<SpecPreview | null>(null)

  // Auth state
  const [authScheme, setAuthScheme] = useState<AuthScheme>('none')
  const [apiKeyName, setApiKeyName] = useState('apikey')
  const [apiKeyLocation, setApiKeyLocation] = useState<'query' | 'header'>('query')
  const [apiKeyValue, setApiKeyValue] = useState('')
  const [bearerToken, setBearerToken] = useState('')
  const [basicUsername, setBasicUsername] = useState('')
  const [basicPassword, setBasicPassword] = useState('')

  const [allowPrivate, setAllowPrivate] = useState(false)
  const [creating, setCreating] = useState(false)

  if (!open) return null

  async function fetchSpec() {
    setLoading(true)
    setError('')
    try {
      const res = await fetch('/api/api-proxy/fetch-spec', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: swaggerUrl, allowPrivate }),
      })
      const data = await res.json()
      if (!res.ok) { setError(data.error ?? 'Failed to fetch spec'); return }
      setPreview(data)
      if (!name) setName(data.title ?? '')

      // Auto-detect auth from spec
      if (data.detectedAuth?.length > 0) {
        const first = data.detectedAuth[0]
        if (first.type === 'apiKey') {
          setAuthScheme('api_key')
          setApiKeyName(first.name ?? 'apikey')
          setApiKeyLocation(first.in === 'header' ? 'header' : 'query')
        } else if (first.type === 'http' && first.scheme === 'bearer') {
          setAuthScheme('bearer')
        } else if (first.type === 'http' && first.scheme === 'basic') {
          setAuthScheme('basic')
        }
      }
      setStep('auth')
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Network error')
    } finally {
      setLoading(false)
    }
  }

  async function createService() {
    setCreating(true)
    setError('')
    try {
      const auth: Record<string, unknown> = { scheme: authScheme }
      if (authScheme === 'api_key') {
        auth.apiKeyName = apiKeyName
        auth.apiKeyLocation = apiKeyLocation
        auth.apiKeyValue = apiKeyValue
      } else if (authScheme === 'bearer') {
        auth.bearerToken = bearerToken
      } else if (authScheme === 'basic') {
        auth.basicUsername = basicUsername
        auth.basicPassword = basicPassword
      }

      const res = await fetch('/api/api-services', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name,
          swaggerUrl,
          baseUrl: preview?.baseUrl,
          description: preview?.description,
          auth,
          allowPrivate,
        }),
      })
      const data = await res.json()
      if (!res.ok) { setError(data.error ?? 'Failed to create service'); return }
      onCreated()
      resetAndClose()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Network error')
    } finally {
      setCreating(false)
    }
  }

  function resetAndClose() {
    setStep('url')
    setName('')
    setSwaggerUrl('')
    setPreview(null)
    setError('')
    setAuthScheme('none')
    setApiKeyValue('')
    setBearerToken('')
    setBasicUsername('')
    setBasicPassword('')
    setAllowPrivate(false)
    onClose()
  }

  return (
    <div className="fixed inset-0 z-50 bg-black/60 backdrop-blur-sm flex items-center justify-center p-4">
      <div className="bg-chef-surface border border-chef-border rounded-2xl shadow-2xl w-full max-w-[560px] overflow-hidden">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-chef-border">
          <div>
            <h2 className="text-base font-semibold text-chef-text">Add API Service</h2>
            <p className="text-xs text-chef-muted mt-0.5">Import a Swagger/OpenAPI spec</p>
          </div>
          <button onClick={resetAndClose} className="p-1.5 rounded-lg hover:bg-white/[0.06] text-chef-muted hover:text-chef-text transition-colors">
            <X size={16} />
          </button>
        </div>

        {/* Steps indicator */}
        <div className="px-5 py-3 flex items-center gap-2 text-xs border-b border-chef-border/50">
          {(['url', 'auth', 'confirm'] as Step[]).map((s, i) => (
            <div key={s} className="flex items-center gap-2">
              {i > 0 && <ChevronRight size={12} className="text-chef-muted" />}
              <span className={`px-2 py-0.5 rounded-md ${step === s ? 'bg-indigo-500/20 text-indigo-400 font-medium' : 'text-chef-muted'}`}>
                {s === 'url' ? 'Swagger URL' : s === 'auth' ? 'Authentication' : 'Confirm'}
              </span>
            </div>
          ))}
        </div>

        {/* Body */}
        <div className="px-5 py-4 space-y-4 max-h-[60vh] overflow-y-auto">
          {error && (
            <div className="flex items-start gap-2 px-3 py-2.5 rounded-lg bg-rose-500/10 border border-rose-500/20 text-rose-400 text-xs">
              <AlertCircle size={14} className="shrink-0 mt-0.5" />
              <span>{error}</span>
            </div>
          )}

          {/* Step 1: URL */}
          {step === 'url' && (
            <>
              <div>
                <label className="block text-xs font-medium text-chef-muted mb-1.5">Swagger/OpenAPI URL</label>
                <div className="flex gap-2">
                  <div className="flex-1 relative">
                    <Globe size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-chef-muted" />
                    <input
                      type="url"
                      value={swaggerUrl}
                      onChange={e => setSwaggerUrl(e.target.value)}
                      placeholder="https://api.example.com/swagger.json"
                      className="w-full pl-9 pr-3 py-2 bg-chef-card border border-chef-border rounded-lg text-sm text-chef-text placeholder:text-chef-muted/50 focus:outline-none focus:border-indigo-500/50 focus:ring-1 focus:ring-indigo-500/20"
                      onKeyDown={e => { if (e.key === 'Enter' && swaggerUrl.trim()) fetchSpec() }}
                    />
                  </div>
                </div>
              </div>
              <div>
                <label className="block text-xs font-medium text-chef-muted mb-1.5">Service Name</label>
                <input
                  type="text"
                  value={name}
                  onChange={e => setName(e.target.value)}
                  placeholder="Auto-detected from spec"
                  className="w-full px-3 py-2 bg-chef-card border border-chef-border rounded-lg text-sm text-chef-text placeholder:text-chef-muted/50 focus:outline-none focus:border-indigo-500/50"
                />
              </div>
              <label className="flex items-start gap-2.5 cursor-pointer">
                <input
                  type="checkbox"
                  checked={allowPrivate}
                  onChange={e => setAllowPrivate(e.target.checked)}
                  className="mt-0.5 accent-indigo-500 w-3.5 h-3.5 shrink-0"
                />
                <div>
                  <span className="text-xs text-chef-text">Allow private / intranet URLs</span>
                  <p className="text-[10px] text-chef-muted mt-0.5">
                    Enable for internal Swagger specs hosted on 10.x, 192.168.x, or other private networks.
                  </p>
                </div>
              </label>
            </>
          )}

          {/* Step 2: Auth */}
          {step === 'auth' && preview && (
            <>
              {/* Spec preview */}
              <div className="rounded-xl bg-chef-card border border-chef-border p-3 space-y-2">
                <div className="flex items-center gap-2">
                  <Check size={14} className="text-emerald-400" />
                  <span className="text-sm font-medium text-chef-text">{preview.title}</span>
                </div>
                <div className="flex gap-3 text-xs text-chef-muted">
                  <span>v{preview.apiVersion}</span>
                  <span>{preview.endpointCount} endpoints</span>
                  <span>{preview.tags.length} tags</span>
                </div>
                {preview.description && (
                  <p className="text-xs text-chef-muted line-clamp-2">{preview.description}</p>
                )}
              </div>

              {/* Auth config */}
              <div>
                <label className="block text-xs font-medium text-chef-muted mb-1.5">Authentication</label>
                <div className="grid grid-cols-4 gap-1.5">
                  {(['none', 'api_key', 'bearer', 'basic'] as AuthScheme[]).map(s => (
                    <button
                      key={s}
                      onClick={() => setAuthScheme(s)}
                      className={`px-2 py-1.5 rounded-lg text-xs font-medium transition-colors ${
                        authScheme === s
                          ? 'bg-indigo-500/20 text-indigo-400 border border-indigo-500/30'
                          : 'bg-chef-card border border-chef-border text-chef-muted hover:text-chef-text'
                      }`}
                    >
                      {s === 'none' ? 'None' : s === 'api_key' ? 'API Key' : s === 'bearer' ? 'Bearer' : 'Basic'}
                    </button>
                  ))}
                </div>
              </div>

              {authScheme === 'api_key' && (
                <div className="space-y-3">
                  <div className="grid grid-cols-2 gap-3">
                    <div>
                      <label className="block text-xs text-chef-muted mb-1">Key Name</label>
                      <input value={apiKeyName} onChange={e => setApiKeyName(e.target.value)} className="w-full px-3 py-2 bg-chef-card border border-chef-border rounded-lg text-sm text-chef-text focus:outline-none focus:border-indigo-500/50" />
                    </div>
                    <div>
                      <label className="block text-xs text-chef-muted mb-1">Location</label>
                      <select value={apiKeyLocation} onChange={e => setApiKeyLocation(e.target.value as 'query' | 'header')} className="w-full px-3 py-2 bg-chef-card border border-chef-border rounded-lg text-sm text-chef-text focus:outline-none">
                        <option value="query">Query Parameter</option>
                        <option value="header">Header</option>
                      </select>
                    </div>
                  </div>
                  <div>
                    <label className="block text-xs text-chef-muted mb-1">API Key Value</label>
                    <div className="relative">
                      <Key size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-chef-muted" />
                      <input type="password" value={apiKeyValue} onChange={e => setApiKeyValue(e.target.value)} placeholder="Enter API key" className="w-full pl-9 pr-3 py-2 bg-chef-card border border-chef-border rounded-lg text-sm text-chef-text focus:outline-none focus:border-indigo-500/50" />
                    </div>
                  </div>
                </div>
              )}

              {authScheme === 'bearer' && (
                <div>
                  <label className="block text-xs text-chef-muted mb-1">Bearer Token</label>
                  <div className="relative">
                    <Shield size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-chef-muted" />
                    <input type="password" value={bearerToken} onChange={e => setBearerToken(e.target.value)} placeholder="Enter bearer token" className="w-full pl-9 pr-3 py-2 bg-chef-card border border-chef-border rounded-lg text-sm text-chef-text focus:outline-none focus:border-indigo-500/50" />
                  </div>
                </div>
              )}

              {authScheme === 'basic' && (
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="block text-xs text-chef-muted mb-1">Username</label>
                    <input value={basicUsername} onChange={e => setBasicUsername(e.target.value)} className="w-full px-3 py-2 bg-chef-card border border-chef-border rounded-lg text-sm text-chef-text focus:outline-none focus:border-indigo-500/50" />
                  </div>
                  <div>
                    <label className="block text-xs text-chef-muted mb-1">Password</label>
                    <input type="password" value={basicPassword} onChange={e => setBasicPassword(e.target.value)} className="w-full px-3 py-2 bg-chef-card border border-chef-border rounded-lg text-sm text-chef-text focus:outline-none focus:border-indigo-500/50" />
                  </div>
                </div>
              )}
            </>
          )}

          {/* Step 3: Confirm */}
          {step === 'confirm' && preview && (
            <div className="space-y-3">
              <div className="rounded-xl bg-chef-card border border-chef-border p-4 space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium text-chef-text">{name || preview.title}</span>
                  <span className="text-xs text-emerald-400 bg-emerald-500/10 px-2 py-0.5 rounded-md">Ready</span>
                </div>
                <div className="space-y-1.5 text-xs text-chef-muted">
                  <div className="flex justify-between"><span>Base URL</span><span className="text-chef-text font-mono">{preview.baseUrl}</span></div>
                  <div className="flex justify-between"><span>API Version</span><span className="text-chef-text">{preview.apiVersion}</span></div>
                  <div className="flex justify-between"><span>Endpoints</span><span className="text-chef-text">{preview.endpointCount}</span></div>
                  <div className="flex justify-between"><span>Authentication</span><span className="text-chef-text">{authScheme === 'none' ? 'None' : authScheme.replace('_', ' ')}</span></div>
                </div>
                {preview.tags.length > 0 && (
                  <div className="flex flex-wrap gap-1 mt-2">
                    {preview.tags.slice(0, 10).map(tag => (
                      <span key={tag} className="px-1.5 py-0.5 rounded text-[10px] bg-indigo-500/10 text-indigo-400">{tag}</span>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-5 py-3 border-t border-chef-border flex items-center justify-between">
          <button
            onClick={() => {
              if (step === 'auth') setStep('url')
              else if (step === 'confirm') setStep('auth')
              else resetAndClose()
            }}
            className="px-3 py-1.5 text-sm text-chef-muted hover:text-chef-text transition-colors"
          >
            {step === 'url' ? 'Cancel' : 'Back'}
          </button>

          {step === 'url' && (
            <button
              onClick={fetchSpec}
              disabled={!swaggerUrl.trim() || loading}
              className="flex items-center gap-2 px-4 py-1.5 rounded-lg bg-indigo-500 hover:bg-indigo-600 text-white text-sm font-medium disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {loading ? <Loader2 size={14} className="animate-spin" /> : null}
              Fetch & Preview
            </button>
          )}

          {step === 'auth' && (
            <button
              onClick={() => setStep('confirm')}
              className="px-4 py-1.5 rounded-lg bg-indigo-500 hover:bg-indigo-600 text-white text-sm font-medium transition-colors"
            >
              Next
            </button>
          )}

          {step === 'confirm' && (
            <button
              onClick={createService}
              disabled={creating}
              className="flex items-center gap-2 px-4 py-1.5 rounded-lg bg-emerald-500 hover:bg-emerald-600 text-white text-sm font-medium disabled:opacity-50 transition-colors"
            >
              {creating ? <Loader2 size={14} className="animate-spin" /> : <Check size={14} />}
              Add Service
            </button>
          )}
        </div>
      </div>
    </div>
  )
}
