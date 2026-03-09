'use client'

import { useState, useEffect } from 'react'
import {
  X, Globe, Database, Cloud, Upload, Plug2, ChevronRight,
  CheckCircle2, Loader2, ArrowLeft, AlertCircle, FileJson, Table,
} from 'lucide-react'

/* ── Types ───────────────────────────────────────────────────────────────────── */
type SourceId = 'http' | 'pg' | 'mysql' | 's3' | 'file' | 'conn'
type AuthType  = 'none' | 'apikey' | 'bearer' | 'basic'

interface WizardProps {
  onClose:    () => void
  onCreated?: (ds: { id: string; name: string }) => void
}

interface SavedConnectorOption {
  id: string
  name: string
  type: string
}

interface SchemaField { field: string; type: string; nullable: boolean; example: string }
interface PreviewResult {
  schema:        SchemaField[]
  sampleRows:    Record<string, unknown>[]
  totalRows:     number
  cannotConnect?: boolean
  message?:      string
  error?:        string
}

/* ── Source type definitions ─────────────────────────────────────────────────── */
const SOURCES = [
  { id: 'http'  as SourceId, Icon: Globe,    label: 'HTTP API',           desc: 'REST, GraphQL, webhooks',   color: 'text-sky-400',     bg: 'bg-sky-500/10',     border: 'border-sky-500/30' },
  { id: 'pg'    as SourceId, Icon: Database, label: 'PostgreSQL',         desc: 'Direct table or SQL query', color: 'text-blue-400',    bg: 'bg-blue-500/10',    border: 'border-blue-500/30' },
  { id: 'mysql' as SourceId, Icon: Database, label: 'MySQL / MariaDB',    desc: 'Table or custom query',     color: 'text-violet-400',  bg: 'bg-violet-500/10',  border: 'border-violet-500/30' },
  { id: 's3'    as SourceId, Icon: Cloud,    label: 'S3 / R2 / GCS',      desc: 'Object storage buckets',    color: 'text-violet-400',  bg: 'bg-violet-500/10',  border: 'border-violet-500/30' },
  { id: 'file'  as SourceId, Icon: Upload,   label: 'File Upload',        desc: 'JSON, JSONL, CSV, Parquet', color: 'text-emerald-400', bg: 'bg-emerald-500/10', border: 'border-emerald-500/30' },
  { id: 'conn'  as SourceId, Icon: Plug2,    label: 'Existing Connector', desc: 'Reuse a saved connection',  color: 'text-indigo-400',  bg: 'bg-indigo-500/10',  border: 'border-indigo-500/30' },
]

/* ── Stepper ─────────────────────────────────────────────────────────────────── */
const STEPS = ['Source', 'Configure', 'Preview', 'Done']

function Stepper({ current }: { current: number }) {
  return (
    <div className="flex items-center gap-0 px-6 py-4 border-b border-chef-border">
      {STEPS.map((label, i) => (
        <div key={i} className="flex items-center flex-1 last:flex-none">
          <div className="flex flex-col items-center gap-1">
            <div className={`w-6 h-6 rounded-full flex items-center justify-center text-[11px] font-bold border-2 transition-all ${
              i < current   ? 'bg-emerald-500 border-emerald-500 text-white' :
              i === current ? 'bg-indigo-500 border-indigo-500 text-white' :
              'border-chef-border bg-transparent text-chef-muted'
            }`}>
              {i < current ? <CheckCircle2 size={12} /> : i + 1}
            </div>
            <span className={`text-[10px] font-medium whitespace-nowrap ${
              i === current ? 'text-indigo-400' : i < current ? 'text-emerald-400' : 'text-chef-muted'
            }`}>{label}</span>
          </div>
          {i < STEPS.length - 1 && (
            <div className={`flex-1 h-px mx-2 mb-4 transition-colors ${i < current ? 'bg-emerald-500/40' : 'bg-chef-border'}`} />
          )}
        </div>
      ))}
    </div>
  )
}

/* ── Form helpers ────────────────────────────────────────────────────────────── */
function Label({ children }: { children: React.ReactNode }) {
  return <label className="block text-xs font-semibold text-chef-text mb-1.5">{children}</label>
}
function Input({ placeholder, type = 'text', value, onChange, className = '' }: {
  placeholder?: string; type?: string; value: string; onChange: (v: string) => void; className?: string
}) {
  return (
    <input
      type={type}
      value={value}
      onChange={e => onChange(e.target.value)}
      placeholder={placeholder}
      className={`w-full bg-chef-bg border border-chef-border text-chef-text text-sm rounded-lg px-3 py-2 placeholder-chef-muted focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors ${className}`}
    />
  )
}
function Select({ value, onChange, children }: { value: string; onChange: (v: string) => void; children: React.ReactNode }) {
  return (
    <select
      value={value}
      onChange={e => onChange(e.target.value)}
      className="w-full bg-chef-bg border border-chef-border text-chef-text text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 transition-colors"
    >
      {children}
    </select>
  )
}
function FieldRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <Label>{label}</Label>
      {children}
    </div>
  )
}

/* ── Step 1 — Source selection ───────────────────────────────────────────────── */
function StepSource({ selected, onSelect }: { selected: SourceId | null; onSelect: (id: SourceId) => void }) {
  return (
    <div className="animate-fade-in">
      <p className="text-sm text-chef-muted mb-5">Choose where your data lives. You can configure multiple sources later.</p>
      <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
        {SOURCES.map(({ id, Icon, label, desc, color, bg, border }) => (
          <button
            key={id}
            onClick={() => onSelect(id)}
            className={`group flex flex-col gap-3 p-4 rounded-xl border text-left transition-all ${
              selected === id
                ? `${border} ${bg} ring-1 ring-indigo-500/30`
                : 'border-chef-border bg-chef-card hover:border-indigo-500/30 hover:bg-chef-card-hover'
            }`}
          >
            <div className={`w-9 h-9 rounded-lg flex items-center justify-center ${bg} ${border} border`}>
              <Icon size={17} className={color} />
            </div>
            <div>
              <div className="text-sm font-semibold text-chef-text leading-tight">{label}</div>
              <div className="text-[11px] text-chef-muted mt-0.5 leading-tight">{desc}</div>
            </div>
            {selected === id && (
              <CheckCircle2 size={14} className="text-indigo-400 absolute top-3 right-3" />
            )}
          </button>
        ))}
      </div>
    </div>
  )
}

/* ── Step 2 — Configure ──────────────────────────────────────────────────────── */
interface FormState {
  name: string
  // HTTP
  url: string; auth: AuthType; apiKeyHeader: string; apiKeyValue: string; bearerToken: string; basicUser: string; basicPass: string; format: string; refresh: string
  // DB
  host: string; port: string; database: string; dbUser: string; dbPass: string; ssl: boolean; tableOrQuery: string
  // S3
  bucket: string; region: string; accessKey: string; secretKey: string; prefix: string
  // File
  fileName: string
  // Conn
  connection: string; resource: string
}

const INIT_FORM: FormState = {
  name: '', url: '', auth: 'none', apiKeyHeader: 'X-API-Key', apiKeyValue: '', bearerToken: '', basicUser: '', basicPass: '', format: 'json', refresh: 'manual',
  host: '', port: '5432', database: '', dbUser: '', dbPass: '', ssl: true, tableOrQuery: '',
  bucket: '', region: 'us-east-1', accessKey: '', secretKey: '', prefix: '',
  fileName: '', connection: '', resource: '',
}

function StepConfigure({
  source, form, setForm, connectors,
}: {
  source: SourceId
  form: FormState
  setForm: (f: FormState) => void
  connectors: SavedConnectorOption[]
}) {
  const f = (field: keyof FormState) => ({
    value: form[field] as string,
    onChange: (v: string) => setForm({ ...form, [field]: v }),
  })

  const AuthTabs = () => (
    <div className="flex gap-1 p-1 bg-chef-bg rounded-lg border border-chef-border w-fit">
      {(['none','apikey','bearer','basic'] as AuthType[]).map(a => (
        <button key={a} onClick={() => setForm({ ...form, auth: a })}
          className={`px-2.5 py-1 rounded-md text-[11px] font-medium transition-colors ${form.auth === a ? 'bg-indigo-500 text-white' : 'text-chef-muted hover:text-chef-text'}`}>
          {a === 'none' ? 'None' : a === 'apikey' ? 'API Key' : a === 'bearer' ? 'Bearer' : 'Basic'}
        </button>
      ))}
    </div>
  )

  return (
    <div className="animate-fade-in space-y-4">
      {source === 'conn' && form.connection && connectors.find(c => c.id === form.connection)?.type === 'azureb2c' && (
        <div className="p-3 rounded-xl border border-sky-500/20 bg-sky-500/5 text-[11px] text-sky-200">
          Azure AD B2C resources: <span className="font-mono">users</span>, <span className="font-mono">userFlows</span>, <span className="font-mono">customPolicies</span>, or a matching Microsoft Graph path under those families.
        </div>
      )}
      {source === 'conn' && form.connection && connectors.find(c => c.id === form.connection)?.type === 'azureentraid' && (
        <div className="p-3 rounded-xl border border-sky-500/20 bg-sky-500/5 text-[11px] text-sky-200">
          Azure Entra ID resources: <span className="font-mono">users</span>, <span className="font-mono">groups</span>, <span className="font-mono">applications</span>, or a matching Microsoft Graph path under those families.
        </div>
      )}
      {source === 'conn' && form.connection && connectors.find(c => c.id === form.connection)?.type === 'github' && (
        <div className="p-3 rounded-xl border border-sky-500/20 bg-sky-500/5 text-[11px] text-sky-200">
          GitHub resources: <span className="font-mono">repos</span>, <span className="font-mono">pullRequests?state=open</span>, or <span className="font-mono">issues?state=open</span>. Repository allowlists come from the connector, not the dataset resource field.
        </div>
      )}
      {source === 'conn' && form.connection && connectors.find(c => c.id === form.connection)?.type === 'azuredevops' && (
        <div className="p-3 rounded-xl border border-sky-500/20 bg-sky-500/5 text-[11px] text-sky-200">
          Azure DevOps resources: <span className="font-mono">repositories</span>, <span className="font-mono">branches</span>, <span className="font-mono">commits?days=30</span>, <span className="font-mono">pullRequests?state=active</span>, <span className="font-mono">workItems?state=Active</span>, <span className="font-mono">pipelines</span>, or <span className="font-mono">pipelineRuns?days=14</span>. Organization, project, and repository allowlists come from the connector.
        </div>
      )}
      <FieldRow label="Dataset Name">
        <Input placeholder="e.g. my-api-data" {...f('name')} />
      </FieldRow>

      {source === 'http' && (
        <>
          <FieldRow label="Endpoint URL">
            <Input placeholder="https://api.example.com/data" {...f('url')} />
          </FieldRow>
          <div>
            <Label>Authentication</Label>
            <AuthTabs />
            {form.auth === 'apikey' && (
              <div className="grid grid-cols-2 gap-3 mt-3">
                <FieldRow label="Header Name"><Input placeholder="X-API-Key" {...f('apiKeyHeader')} /></FieldRow>
                <FieldRow label="Key Value"><Input type="password" placeholder="sk-••••••••" {...f('apiKeyValue')} /></FieldRow>
              </div>
            )}
            {form.auth === 'bearer' && (
              <div className="mt-3">
                <FieldRow label="Bearer Token"><Input type="password" placeholder="eyJ..." {...f('bearerToken')} /></FieldRow>
              </div>
            )}
            {form.auth === 'basic' && (
              <div className="grid grid-cols-2 gap-3 mt-3">
                <FieldRow label="Username"><Input placeholder="user" {...f('basicUser')} /></FieldRow>
                <FieldRow label="Password"><Input type="password" placeholder="••••••" {...f('basicPass')} /></FieldRow>
              </div>
            )}
          </div>
          <div className="grid grid-cols-2 gap-4">
            <FieldRow label="Response Format">
              <Select {...f('format')}>
                <option value="json">JSON (auto-detect)</option>
                <option value="jsonl">JSONL (newline-delimited)</option>
                <option value="csv">CSV</option>
              </Select>
            </FieldRow>
            <FieldRow label="Refresh Interval">
              <Select {...f('refresh')}>
                <option value="manual">Manual only</option>
                <option value="1h">Every hour</option>
                <option value="6h">Every 6 hours</option>
                <option value="24h">Daily</option>
              </Select>
            </FieldRow>
          </div>
        </>
      )}

      {(source === 'pg' || source === 'mysql') && (
        <>
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2">
              <FieldRow label="Host"><Input placeholder={source === 'pg' ? 'localhost' : 'db.example.com'} {...f('host')} /></FieldRow>
            </div>
            <FieldRow label="Port"><Input placeholder={source === 'pg' ? '5432' : '3306'} {...f('port')} /></FieldRow>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <FieldRow label="Database"><Input placeholder="my_database" {...f('database')} /></FieldRow>
            <FieldRow label="Username"><Input placeholder="postgres" {...f('dbUser')} /></FieldRow>
          </div>
          <FieldRow label="Password"><Input type="password" placeholder="••••••••" {...f('dbPass')} /></FieldRow>
          {source === 'pg' && (
            <div className="flex items-center gap-3">
              <button
                onClick={() => setForm({ ...form, ssl: !form.ssl })}
                className={`relative w-9 h-5 rounded-full transition-colors ${form.ssl ? 'bg-indigo-500' : 'bg-chef-border'}`}
              >
                <span className={`absolute top-0.5 left-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform ${form.ssl ? 'translate-x-4' : ''}`} />
              </button>
              <span className="text-sm text-chef-text">SSL / TLS</span>
            </div>
          )}
          <FieldRow label="Table or SQL Query">
            <Input placeholder="SELECT * FROM events LIMIT 10000" {...f('tableOrQuery')} />
          </FieldRow>
        </>
      )}

      {source === 's3' && (
        <>
          <div className="grid grid-cols-2 gap-3">
            <FieldRow label="Bucket"><Input placeholder="my-data-bucket" {...f('bucket')} /></FieldRow>
            <FieldRow label="Region">
              <Select {...f('region')}>
                {['us-east-1','us-west-2','eu-west-1','eu-central-1','ap-southeast-1','ap-southeast-2'].map(r => (
                  <option key={r} value={r}>{r}</option>
                ))}
              </Select>
            </FieldRow>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <FieldRow label="Access Key ID"><Input placeholder="AKIA..." {...f('accessKey')} /></FieldRow>
            <FieldRow label="Secret Access Key"><Input type="password" placeholder="••••••••" {...f('secretKey')} /></FieldRow>
          </div>
          <FieldRow label="Path Prefix (optional)"><Input placeholder="data/events/" {...f('prefix')} /></FieldRow>
          <FieldRow label="File Format">
            <Select {...f('format')}>
              <option value="json">JSON</option>
              <option value="jsonl">JSONL</option>
              <option value="parquet">Parquet</option>
              <option value="csv">CSV</option>
            </Select>
          </FieldRow>
        </>
      )}

      {source === 'file' && (
        <>
          <div className="border-2 border-dashed border-chef-border rounded-xl p-8 text-center hover:border-indigo-500/50 transition-colors cursor-pointer group">
            <Upload size={28} className="text-chef-muted mx-auto mb-3 group-hover:text-indigo-400 transition-colors" />
            <div className="text-sm font-medium text-chef-text mb-1">Drop a file here or click to browse</div>
            <div className="text-[11px] text-chef-muted">JSON · JSONL · CSV · Parquet up to 500 MB</div>
            <div className="mt-4 px-3 py-1.5 bg-chef-card border border-chef-border rounded-lg text-xs text-chef-muted inline-block">
              Browse Files
            </div>
          </div>
          <FieldRow label="Format Override">
            <Select {...f('format')}>
              <option value="auto">Auto-detect</option>
              <option value="json">JSON</option>
              <option value="jsonl">JSONL</option>
              <option value="csv">CSV</option>
              <option value="parquet">Parquet</option>
            </Select>
          </FieldRow>
        </>
      )}

      {source === 'conn' && (
        <>
          <FieldRow label="Connection">
            <Select {...f('connection')}>
              {connectors.length === 0
                ? <option value="">No saved connectors available</option>
                : connectors.map(c => <option key={c.id} value={c.id}>{c.name} ({c.type})</option>)}
            </Select>
          </FieldRow>
          <FieldRow label="Resource / Path">
            <Input
              placeholder={form.connection && connectors.find(c => c.id === form.connection)?.type === 'azureb2c'
                ? 'e.g. users, userFlows, customPolicies, or /users?$filter=...'
                : form.connection && connectors.find(c => c.id === form.connection)?.type === 'azureentraid'
                ? 'e.g. users, groups, applications, or /groups?$filter=...'
                : 'e.g. /v1/charges or SELECT * FROM users'}
              {...f('resource')}
            />
          </FieldRow>
          <FieldRow label="Format">
            <Select {...f('format')}>
              <option value="json">JSON (auto-detect)</option>
              <option value="jsonl">JSONL</option>
              <option value="csv">CSV</option>
            </Select>
          </FieldRow>
        </>
      )}
    </div>
  )
}

/* ── Step 3 — Preview ────────────────────────────────────────────────────────── */
const TYPE_COLORS: Record<string, string> = {
  integer:   'text-orange-400',
  float:     'text-amber-400',
  string:    'text-emerald-400',
  timestamp: 'text-sky-400',
  date:      'text-sky-300',
  boolean:   'text-rose-400',
  object:    'text-violet-400',
  array:     'text-violet-300',
  null:      'text-chef-muted',
}

function StepPreview({
  source, form, onResult,
}: {
  source: SourceId
  form: FormState
  onResult: (r: PreviewResult) => void
}) {
  const [loading,  setLoading]  = useState(true)
  const [result,   setResult]   = useState<PreviewResult | null>(null)
  const [retryKey, setRetryKey] = useState(0)
  const [tab,      setTab]      = useState<'schema' | 'data'>('schema')
  const datasetName = form.name || 'new-dataset'

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setResult(null)

    fetch('/api/wizard/preview', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        source,
        connectorId:  source === 'conn' ? form.connection : undefined,
        resource:     source === 'conn' ? form.resource : undefined,
        url:          form.url,
        auth:         form.auth,
        apiKeyHeader: form.apiKeyHeader,
        apiKeyValue:  form.apiKeyValue,
        bearerToken:  form.bearerToken,
        basicUser:    form.basicUser,
        basicPass:    form.basicPass,
        format:       form.format,
      }),
    })
      .then(r => r.json())
      .then((data: PreviewResult) => {
        if (cancelled) return
        setResult(data)
        setLoading(false)
        onResult(data)
      })
      .catch((e: Error) => {
        if (cancelled) return
        const r: PreviewResult = { schema: [], sampleRows: [], totalRows: 0, error: `Network error: ${e.message}` }
        setResult(r)
        setLoading(false)
        onResult(r)
      })

    return () => { cancelled = true }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [retryKey])

  /* ── Loading ─── */
  if (loading) {
    return (
      <div className="animate-fade-in flex flex-col items-center justify-center py-16 gap-4">
        <Loader2 size={32} className="text-indigo-400 animate-spin" />
        <div className="text-sm text-chef-text font-medium">Connecting and detecting schema…</div>
        <div className="flex flex-col gap-1 text-[11px] text-chef-muted text-center">
          <span>Fetching data from {form.url || source}</span>
          <span>Inferring field types from real records</span>
        </div>
      </div>
    )
  }

  if (!result) return null

  /* ── Cannot connect (non-HTTP sources) ─── */
  if (result.cannotConnect) {
    return (
      <div className="animate-fade-in space-y-4">
        <div className="flex gap-3 p-4 bg-amber-500/5 border border-amber-500/20 rounded-xl">
          <AlertCircle size={16} className="text-amber-400 shrink-0 mt-0.5" />
          <div>
            <div className="text-sm font-semibold text-chef-text mb-1">Live preview not available</div>
            <div className="text-[12px] text-chef-muted leading-relaxed">{result.message}</div>
          </div>
        </div>
        <p className="text-[12px] text-chef-muted">
          You can still create this dataset — credentials will be validated when the dataset is saved and the first sync runs.
        </p>
      </div>
    )
  }

  /* ── Error ─── */
  if (result.error) {
    return (
      <div className="animate-fade-in space-y-4">
        <div className="flex gap-3 p-4 bg-rose-500/5 border border-rose-500/20 rounded-xl">
          <AlertCircle size={16} className="text-rose-400 shrink-0 mt-0.5" />
          <div>
            <div className="text-sm font-semibold text-chef-text mb-1">Preview failed</div>
            <div className="text-[12px] text-chef-muted font-mono break-all">{result.error}</div>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={() => setRetryKey(k => k + 1)}
            className="text-xs px-3 py-1.5 bg-chef-card border border-chef-border rounded-lg text-chef-text hover:border-indigo-500/40 transition-colors"
          >
            Retry
          </button>
          <span className="text-[11px] text-chef-muted">Or go back and check your URL / credentials.</span>
        </div>
      </div>
    )
  }

  /* ── Success ─── */
  const { schema, sampleRows, totalRows } = result
  const colKeys = schema.slice(0, 6).map(f => f.field)

  function cellStr(v: unknown): string {
    if (v === null || v === undefined) return 'null'
    if (typeof v === 'object') return JSON.stringify(v).slice(0, 40)
    return String(v).slice(0, 50)
  }

  return (
    <div className="animate-fade-in space-y-4">
      {/* Stats bar */}
      <div className="flex items-center gap-3 p-3.5 bg-emerald-500/5 border border-emerald-500/20 rounded-xl">
        <CheckCircle2 size={16} className="text-emerald-400 shrink-0" />
        <div className="text-sm text-chef-text">
          Schema detected for <span className="font-mono font-semibold text-emerald-300">{datasetName}</span>
        </div>
        <div className="ml-auto flex items-center gap-3 text-[11px] text-chef-muted font-mono">
          <span className="flex items-center gap-1"><FileJson size={11} /> {schema.length} fields</span>
          <span className="flex items-center gap-1"><Table size={11} /> {totalRows.toLocaleString()} rows</span>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex items-center gap-1 p-1 bg-chef-bg rounded-lg border border-chef-border w-fit">
        {(['schema', 'data'] as const).map(t => (
          <button key={t} onClick={() => setTab(t)}
            className={`px-3 py-1 rounded-md text-[11px] font-medium transition-colors ${tab === t ? 'bg-indigo-500 text-white' : 'text-chef-muted hover:text-chef-text'}`}>
            {t === 'schema' ? 'Schema' : 'Sample Data'}
          </button>
        ))}
      </div>

      {/* Schema table */}
      {tab === 'schema' && (
        <div className="rounded-xl border border-chef-border overflow-hidden">
          <table className="w-full text-[12px]">
            <thead>
              <tr className="border-b border-chef-border bg-chef-card">
                <th className="px-4 py-2.5 text-left text-[10px] font-semibold text-chef-muted uppercase tracking-wider">Field</th>
                <th className="px-4 py-2.5 text-left text-[10px] font-semibold text-chef-muted uppercase tracking-wider">Type</th>
                <th className="px-4 py-2.5 text-left text-[10px] font-semibold text-chef-muted uppercase tracking-wider">Nullable</th>
                <th className="px-4 py-2.5 text-left text-[10px] font-semibold text-chef-muted uppercase tracking-wider">Example</th>
              </tr>
            </thead>
            <tbody>
              {schema.map((row, i) => (
                <tr key={i} className="border-b last:border-0 border-chef-border hover:bg-chef-card/40 transition-colors">
                  <td className="px-4 py-2.5 font-mono font-semibold text-chef-text">{row.field}</td>
                  <td className="px-4 py-2.5 font-mono">
                    <span className={TYPE_COLORS[row.type] ?? 'text-chef-muted'}>{row.type}</span>
                  </td>
                  <td className="px-4 py-2.5 text-chef-muted">{row.nullable ? 'yes' : 'no'}</td>
                  <td className="px-4 py-2.5 font-mono text-chef-muted truncate max-w-[140px]">{row.example}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Sample data table */}
      {tab === 'data' && (
        <div className="rounded-xl border border-chef-border overflow-hidden overflow-x-auto">
          {sampleRows.length === 0 ? (
            <div className="px-4 py-8 text-center text-[12px] text-chef-muted">No sample rows available.</div>
          ) : (
            <table className="w-full text-[11px]">
              <thead>
                <tr className="border-b border-chef-border bg-chef-card">
                  {colKeys.map(k => (
                    <th key={k} className="px-3 py-2.5 text-left font-mono text-[10px] text-chef-muted whitespace-nowrap">{k}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sampleRows.map((row, i) => (
                  <tr key={i} className="border-b last:border-0 border-chef-border hover:bg-chef-card/40 transition-colors">
                    {colKeys.map(k => (
                      <td key={k} className="px-3 py-2 font-mono text-chef-text-dim max-w-[120px] truncate">
                        {cellStr(row[k])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  )
}

/* ── Step 4 — Done ───────────────────────────────────────────────────────────── */
function StepDone({ form, source, previewResult, onClose, onAddAnother }: {
  form: FormState
  source: SourceId
  previewResult: PreviewResult | null
  onClose: () => void
  onAddAnother: () => void
}) {
  const name       = form.name || 'new-dataset'
  const src        = SOURCES.find(s => s.id === source)!
  const fieldCount = previewResult?.schema.length
  const rowCount   = previewResult?.totalRows

  return (
    <div className="animate-fade-in flex flex-col items-center text-center py-8 gap-6">
      <div className="w-16 h-16 rounded-full bg-emerald-500/10 border border-emerald-500/30 flex items-center justify-center">
        <CheckCircle2 size={32} className="text-emerald-400" />
      </div>

      <div>
        <h3 className="text-lg font-bold text-chef-text mb-1">Dataset Created</h3>
        <p className="text-sm text-chef-muted">
          <span className="font-mono text-indigo-400 font-semibold">{name}</span> has been added to your workspace.
        </p>
      </div>

      <div className="w-full max-w-sm p-4 rounded-xl border border-chef-border bg-chef-card text-left space-y-2">
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-chef-muted">Source</span>
          <span className="flex items-center gap-1.5 text-chef-text font-medium">
            <src.Icon size={12} className={src.color} /> {src.label}
          </span>
        </div>
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-chef-muted">Fields detected</span>
          <span className="text-chef-text font-mono">
            {fieldCount != null ? fieldCount : '–'}
          </span>
        </div>
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-chef-muted">Sample rows</span>
          <span className="text-chef-text font-mono">
            {rowCount != null ? rowCount.toLocaleString() : '–'}
          </span>
        </div>
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-chef-muted">Schema version</span>
          <span className="text-chef-text font-mono">v1 (auto)</span>
        </div>
      </div>

      <div className="flex items-center gap-3">
        <button
          onClick={onClose}
          className="flex items-center gap-1.5 bg-indigo-600 hover:bg-indigo-500 text-white text-sm font-semibold px-4 py-2 rounded-lg transition-colors"
        >
          View Dataset <ChevronRight size={14} />
        </button>
        <button
          onClick={onAddAnother}
          className="text-sm text-chef-muted hover:text-chef-text px-4 py-2 rounded-lg border border-chef-border hover:border-indigo-500/40 transition-colors"
        >
          Add Another
        </button>
      </div>
    </div>
  )
}

/* ── Main wizard ─────────────────────────────────────────────────────────────── */
export default function NewDatasetWizard({ onClose, onCreated }: WizardProps) {
  const [step,          setStep]          = useState(0)
  const [source,        setSource]        = useState<SourceId | null>(null)
  const [form,          setForm]          = useState<FormState>(INIT_FORM)
  const [previewResult, setPreviewResult] = useState<PreviewResult | null>(null)
  const [saving,        setSaving]        = useState(false)
  const [connectors,    setConnectors]    = useState<SavedConnectorOption[]>([])

  useEffect(() => {
    fetch('/api/connectors')
      .then(r => r.json())
      .then((list: Array<{ id: string; name: string; type: string }>) => {
        setConnectors(list)
        if (!form.connection && list[0]) {
          setForm(prev => ({ ...prev, connection: list[0].id }))
        }
      })
      .catch(() => {})
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  function reset() {
    setStep(0); setSource(null); setForm(INIT_FORM); setPreviewResult(null); setSaving(false)
  }

  async function handleCreate() {
    if (saving) return
    setSaving(true)
    try {
      const res = await fetch('/api/datasets', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name:       form.name,
          source:     source,
          url:        form.url,
          auth:       form.auth,
          connectorId: source === 'conn' ? form.connection : undefined,
          connection: source === 'conn'
            ? (connectors.find(c => c.id === form.connection)?.name ?? form.connection)
            : undefined,
          resource:   source === 'conn' ? form.resource : undefined,
          format:     form.format,
          schema:     previewResult?.schema     ?? null,
          sampleRows: previewResult?.sampleRows ?? null,
          totalRows:  previewResult?.totalRows  ?? null,
        }),
      })
      const ds = await res.json()
      setStep(3)
      onCreated?.(ds)
    } catch {
      // still advance — creation is best-effort in this demo
      setStep(3)
    } finally {
      setSaving(false)
    }
  }

  const canAdvance = step === 0 ? source !== null : step === 1 ? form.name.trim().length > 0 : true

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />

      {/* Card */}
      <div className="relative w-full max-w-[660px] bg-chef-surface rounded-2xl border border-chef-border shadow-2xl flex flex-col max-h-[90vh] animate-fade-in">

        {/* Header */}
        <div className="flex items-center justify-between px-6 pt-5 pb-0 shrink-0">
          <div>
            <h2 className="text-base font-bold text-chef-text">Add New Dataset</h2>
            <p className="text-[11px] text-chef-muted mt-0.5">Step {step + 1} of {STEPS.length}</p>
          </div>
          <button onClick={onClose} className="p-1.5 text-chef-muted hover:text-chef-text hover:bg-chef-card rounded-lg transition-colors">
            <X size={16} />
          </button>
        </div>

        {/* Stepper */}
        <Stepper current={step} />

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-6 py-5">
          {step === 0 && (
            <StepSource selected={source} onSelect={id => setSource(id)} />
          )}
          {step === 1 && source && (
            <StepConfigure source={source} form={form} setForm={setForm} connectors={connectors} />
          )}
          {step === 2 && source && (
            <StepPreview source={source} form={form} onResult={setPreviewResult} />
          )}
          {step === 3 && source && (
            <StepDone
              form={form}
              source={source}
              previewResult={previewResult}
              onClose={onClose}
              onAddAnother={reset}
            />
          )}
        </div>

        {/* Footer (hidden on Done step) */}
        {step < 3 && (
          <div className="px-6 py-4 border-t border-chef-border flex items-center justify-between shrink-0">
            <button
              onClick={() => step > 0 ? setStep(s => s - 1) : onClose()}
              className="flex items-center gap-1.5 text-sm text-chef-muted hover:text-chef-text transition-colors px-3 py-1.5 rounded-lg hover:bg-chef-card border border-transparent hover:border-chef-border"
            >
              <ArrowLeft size={13} /> {step === 0 ? 'Cancel' : 'Back'}
            </button>

            <button
              onClick={step === 2 ? handleCreate : () => setStep(s => s + 1)}
              disabled={!canAdvance || saving}
              className={`flex items-center gap-1.5 text-sm font-semibold px-4 py-2 rounded-lg transition-colors ${
                canAdvance && !saving
                  ? 'bg-indigo-600 hover:bg-indigo-500 text-white'
                  : 'bg-chef-card text-chef-muted cursor-not-allowed border border-chef-border'
              }`}
            >
              {saving
                ? <><Loader2 size={14} className="animate-spin" /> Saving…</>
                : <>{step === 1 ? 'Preview' : step === 2 ? 'Create Dataset' : 'Continue'}<ChevronRight size={14} /></>
              }
            </button>
          </div>
        )}
      </div>
    </div>
  )
}
