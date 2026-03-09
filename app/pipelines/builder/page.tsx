'use client'

import { useState, useEffect, useRef, useCallback, Suspense } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import {
  ArrowLeft, Plus, Package, Filter, Layers, RefreshCw, Database, Sparkles,
  ChevronLeft, ChevronRight, X, Save, Eye, EyeOff, Loader2, AlertTriangle,
  CheckCircle2, Zap, GripVertical, Circle, GitBranch, Search, Tag, Undo2, Redo2,
} from 'lucide-react'

/* ── Types ─────────────────────────────────────────────────────────────────── */
type OpType = 'extract' | 'validate' | 'query' | 'map' | 'coerce' | 'flatten' | 'enrich' | 'dedupe' | 'condition' | 'write'

interface Mapping { from: string; to: string; transform: string }

interface BuilderStepConfig {
  // extract
  sourceType?: string; url?: string; format?: string; authType?: string; authValue?: string
  // validate
  schemaText?: string; validateMode?: string; quarantine?: boolean
  // query
  queryDataset?: string; queryType?: string; queryText?: string
  // map
  mappings?: Mapping[]
  // coerce
  coerceField?: string; coerceType?: string
  // flatten
  flattenField?: string; flattenMode?: string
  // enrich
  lookupUrl?: string; joinKey?: string; enrichFields?: string
  // dedupe
  dedupeKey?: string; dedupeWindow?: string
  // condition
  conditionField?: string; conditionOp?: string; conditionValue?: string
  trueBranch?: string; falseBranch?: string
  // write
  destType?: string; destFormat?: string; destPath?: string
  createDataset?: boolean; newDatasetName?: string; targetDatasetId?: string
}

interface BuilderStep {
  id: string
  op: OpType
  label: string
  config: BuilderStepConfig
  invalidated?: boolean
  invalidationReason?: string
}

interface SchemaField {
  field: string; type: string; example?: string
}

interface DatasetMeta {
  id: string; name: string; schema?: SchemaField[]
}

interface ConnectorMeta {
  id: string
  name: string
  type: string
}

interface RuntimeStepRecord {
  id: string
  op: string
  label: string
  config: BuilderStepConfig
}

interface PipelineTemplate {
  version: 1
  name: string
  description: string
  notes: string
  status: 'active' | 'draft'
  source: {
    sourceType: 'dataset' | 'connector'
    sourceName: string
    resource?: string
  }
  outputTarget: {
    mode: 'none' | 'dataset'
    datasetId?: string
    datasetName?: string
    refreshMode?: 'manual' | 'scheduled'
    refreshIntervalMinutes?: number | null
  } | null
  steps: RuntimeStepRecord[]
}

interface BuilderState {
  pipelineId:     string | null
  name:           string
  description:    string
  notes:          string
  sourceType:     'dataset' | 'connector'
  dataset:        string
  resource?:      string
  status:         'active' | 'draft'
  steps:          BuilderStep[]
  selectedStepId: string | null
  previewOpen:    boolean
  saving:         boolean
  dirty:          boolean
}

interface PreviewState {
  columns: string[]; rows: string[][]; rowCount: number; removed: number
  loading: boolean; error: string | null
}

/* ── Helpers ───────────────────────────────────────────────────────────────── */
const uid = () => Math.random().toString(36).slice(2, 10)

function defaultLabel(op: OpType): string {
  const m: Record<OpType, string> = {
    extract: 'HTTP Fetch', validate: 'Schema Validate', query: 'Run Query',
    map: 'Transform / Map', coerce: 'Coerce Types', flatten: 'Flatten Fields', enrich: 'Enrich Lookup', dedupe: 'Deduplicate',
    condition: 'Branch Condition', write: 'Write / Project',
  }
  return m[op]
}

function defaultConfig(op: OpType): BuilderStepConfig {
  switch (op) {
    case 'extract':   return { sourceType: 'http', format: 'json', authType: 'none' }
    case 'validate':  return { validateMode: 'strict', quarantine: true }
    case 'query':     return { queryDataset: '', queryType: 'sql', queryText: '' }
    case 'map':       return { mappings: [{ from: '', to: '', transform: '' }] }
    case 'coerce':    return { coerceField: '', coerceType: 'string' }
    case 'flatten':   return { flattenField: '', flattenMode: 'object' }
    case 'enrich':    return { lookupUrl: '', joinKey: '', enrichFields: '' }
    case 'dedupe':    return { dedupeKey: '', dedupeWindow: '7d' }
    case 'condition': return { conditionField: '', conditionOp: '==', conditionValue: '', trueBranch: 'Pass', falseBranch: 'Drop' }
    case 'write':     return { destType: 'S3', destFormat: 'parquet', destPath: '', createDataset: false, newDatasetName: '', targetDatasetId: '' }
  }
}

function configSummary(op: OpType, c: BuilderStepConfig): string {
  const requirement = stepRequirementMessage(op, c)
  if (requirement) return requirement
  switch (op) {
    case 'extract':   return `${c.sourceType ?? 'http'} · ${(c.format ?? 'json').toUpperCase()}`
    case 'validate':  return `${c.validateMode ?? 'strict'}${c.quarantine ? ' · quarantine' : ''}`
    case 'query':     return `${c.queryType ?? 'sql'}${c.queryDataset ? ` on ${c.queryDataset}` : ''}`
    case 'map':       return `${c.mappings?.filter(m => m.from).length ?? 0} field mappings`
    case 'coerce':    return `${c.coerceField || '$.field'} → ${c.coerceType ?? 'string'}`
    case 'flatten':   return `${c.flattenField || '$.field'} · ${c.flattenMode ?? 'object'}`
    case 'enrich':    return c.lookupUrl ? `lookup: ${c.lookupUrl.slice(0, 22)}…` : 'configure lookup'
    case 'dedupe':    return `key: ${c.dedupeKey || '—'} · ${c.dedupeWindow ?? '7d'}`
    case 'condition': return `${c.conditionField || '$.field'} ${c.conditionOp ?? '=='} ${c.conditionValue || '…'}`
    case 'write':
      if (c.createDataset) return `→ dataset: ${c.newDatasetName || 'new'}`
      if (c.targetDatasetId) return '→ refresh dataset'
      return `${c.destType ?? 'S3'} · ${c.destFormat ?? 'parquet'}`
  }
}

function stepRequirementMessage(op: OpType, config: BuilderStepConfig): string | null {
  switch (op) {
    case 'extract':
      return !String(config.url ?? '').trim() ? 'Add source endpoint or connection string' : null
    case 'validate':
      return !String(config.schemaText ?? '').trim() ? 'Import or define schema checks' : null
    case 'query':
      return !String(config.queryText ?? '').trim() ? 'Add query or filter expression' : null
    case 'map':
      return (config.mappings ?? []).some(mapping => mapping.from && mapping.to) ? null : 'Add at least one field mapping'
    case 'coerce':
      return !String(config.coerceField ?? '').trim() ? 'Pick a field to cast' : null
    case 'flatten':
      return !String(config.flattenField ?? '').trim() ? 'Pick a field to flatten' : null
    case 'enrich':
      return !String(config.lookupUrl ?? '').trim() || !String(config.joinKey ?? '').trim()
        ? 'Set lookup URL and join key'
        : null
    case 'dedupe':
      return !String(config.dedupeKey ?? '').trim() ? 'Choose a dedupe key' : null
    case 'condition':
      return !String(config.conditionField ?? '').trim() ? 'Choose a field for branching' : null
    case 'write':
      if (config.createDataset) return null
      if (config.targetDatasetId) return null
      return !String(config.destPath ?? '').trim() ? 'Choose output target or dataset' : null
    default:
      return null
  }
}

const NODE_GUIDES: Record<OpType, { purpose: string; example: string; tips: string[] }> = {
  extract: {
    purpose: 'Pull rows directly from an endpoint or source connection as part of the pipeline.',
    example: 'Use this only when the source selector is not enough and the pipeline itself must fetch from HTTP, S3, or a database.',
    tips: ['Most pipelines should start from the top-level source node.', 'This node is best for special fetch workflows, not ordinary dataset selection.'],
  },
  validate: {
    purpose: 'Check that required fields and types exist before downstream transforms assume they are valid.',
    example: 'Schema example: `id: string`, `createdDateTime: timestamp`, `identities: array`.',
    tips: ['Use strict mode for curated outputs.', 'Use quarantine when you want bad rows separated instead of aborting the flow.'],
  },
  query: {
    purpose: 'Filter, project, aggregate, or reshape rows using SQL, JSONPath, JMESPath, or KQL.',
    example: "SQL example: `SELECT id, userPrincipalName FROM upstream WHERE accountEnabled = true ORDER BY createdDateTime DESC`",
    tips: ['Use SQL or KQL for tabular filtering and sorting.', 'Use JSONPath or JMESPath when you need to extract nested JSON fragments directly.'],
  },
  map: {
    purpose: 'Rename and curate fields into business-facing output columns.',
    example: 'Map `$.identities_issuerAssignedId` to `identityKey` and `$.enrich_domain` to `identityDomain`.',
    tips: ['Use this after flatten or enrich.', 'A final map step makes the dataset much easier to explain.'],
  },
  coerce: {
    purpose: 'Normalize one field into a known type before comparison or writing.',
    example: 'Convert `$.createdDateTime` to `timestamp` before sorting or comparing dates.',
    tips: ['Coerce before filters that depend on numeric or date comparisons.', 'Failed casts become null.'],
  },
  flatten: {
    purpose: 'Expand nested JSON arrays or objects into a shape later nodes can treat like ordinary columns and rows.',
    example: 'Array flatten on `$.identities` creates one row per identity; object flatten then exposes `identities_signInType`, `identities_issuerAssignedId`, etc.',
    tips: ['Use array mode to explode lists.', 'Use object mode to expose nested keys as columns.'],
  },
  enrich: {
    purpose: 'Call an HTTP endpoint to derive extra metadata for each row.',
    example: 'Enrich a sign-in ID to derive domain, tenant, or synthetic/test-user flags.',
    tips: ['Use explicit enrich fields to keep output understandable.', 'The URL can accept the row value through `?value=` or `{{value}}`.'],
  },
  dedupe: {
    purpose: 'Collapse repeated events or identities down to one row per logical key.',
    example: 'Dedupe on `$.signInId` for identity pipelines or `$.orderId` for log/event pipelines.',
    tips: ['Use after flatten/query when the row grain is already correct.', 'Pick a key that really represents one business entity.'],
  },
  condition: {
    purpose: 'Apply a readable if/else gate to keep only rows matching a rule.',
    example: 'Keep rows where `$.accountEnabled == true` or where `$.status >= 500`.',
    tips: ['Think of this as a branch/filter node.', 'Use true/false branch labels to document intent even when the runtime currently filters rows.'],
  },
  write: {
    purpose: 'Mark the final shaped output of the pipeline and prepare it for preview or an optional sink.',
    example: 'Use this as the last semantic step before the output node so the run page shows the finished dataset shape.',
    tips: ['Place this last.', 'Use the output node plus preview to validate final shape before running.'],
  },
}

function dependsOnSourceDataset(op: OpType): boolean {
  return op === 'validate' || op === 'query' || op === 'map' || op === 'coerce' || op === 'flatten' || op === 'enrich' || op === 'dedupe' || op === 'condition'
}

function invalidateStepsForDatasetChange(steps: BuilderStep[], previousDataset: string, nextDataset: string): BuilderStep[] {
  return steps.map(step => {
    if (!dependsOnSourceDataset(step.op)) return step
    return {
      ...step,
      config: defaultConfig(step.op),
      invalidated: true,
      invalidationReason: `Source changed from ${previousDataset} to ${nextDataset}`,
    }
  })
}

/* ── Op metadata ───────────────────────────────────────────────────────────── */
const OP_META = [
  { op: 'extract'   as OpType, label: 'Extract / Fetch',    desc: 'HTTP, S3, DB, file',       color: 'border-sky-500/40',     bg: 'bg-sky-500/5',     iconColor: 'text-sky-400' },
  { op: 'validate'  as OpType, label: 'Validate',           desc: 'Schema check, coerce types', color: 'border-amber-500/40',   bg: 'bg-amber-500/5',   iconColor: 'text-amber-400' },
  { op: 'query'     as OpType, label: 'Query / Filter',     desc: 'SQL, JSONPath, JMESPath, KQL',color: 'border-cyan-500/40',   bg: 'bg-cyan-500/5',    iconColor: 'text-cyan-400' },
  { op: 'map'       as OpType, label: 'Transform / Map',    desc: 'Rename, remap, field expressions', color: 'border-violet-500/40',  bg: 'bg-violet-500/5',  iconColor: 'text-violet-400' },
  { op: 'coerce'    as OpType, label: 'Coerce Types',       desc: 'Cast a field to a target type', color: 'border-fuchsia-500/40', bg: 'bg-fuchsia-500/5', iconColor: 'text-fuchsia-400' },
  { op: 'flatten'   as OpType, label: 'Flatten',            desc: 'Expand object or array fields', color: 'border-teal-500/40',    bg: 'bg-teal-500/5',    iconColor: 'text-teal-400' },
  { op: 'enrich'    as OpType, label: 'Enrich',             desc: 'HTTP lookup, join field',    color: 'border-emerald-500/40', bg: 'bg-emerald-500/5', iconColor: 'text-emerald-400' },
  { op: 'dedupe'    as OpType, label: 'Deduplicate',        desc: 'Remove duplicate rows',      color: 'border-orange-500/40',  bg: 'bg-orange-500/5',  iconColor: 'text-orange-400' },
  { op: 'condition' as OpType, label: 'Branch / Condition', desc: 'If/else row routing',        color: 'border-rose-500/40',    bg: 'bg-rose-500/5',    iconColor: 'text-rose-400' },
  { op: 'write'     as OpType, label: 'Write / Project',    desc: 'S3, PG, new dataset',        color: 'border-indigo-500/40',  bg: 'bg-indigo-500/5',  iconColor: 'text-indigo-400' },
]

function OpIcon({ op, className = 'w-4 h-4' }: { op: OpType; className?: string }) {
  if (op === 'extract')   return <Package    className={className} />
  if (op === 'validate')  return <Filter     className={className} />
  if (op === 'query')     return <Search     className={className} />
  if (op === 'map')       return <Layers     className={className} />
  if (op === 'coerce')    return <Layers     className={className} />
  if (op === 'flatten')   return <Layers     className={className} />
  if (op === 'enrich')    return <Sparkles   className={className} />
  if (op === 'dedupe')    return <RefreshCw  className={className} />
  if (op === 'condition') return <GitBranch  className={className} />
  if (op === 'write')     return <Database   className={className} />
  return <Circle className={className} />
}

function opAccent(op: OpType): string {
  if (op === 'extract')   return 'text-sky-400 border-sky-500/50 bg-sky-500/10'
  if (op === 'validate')  return 'text-amber-400 border-amber-500/50 bg-amber-500/10'
  if (op === 'query')     return 'text-cyan-400 border-cyan-500/50 bg-cyan-500/10'
  if (op === 'map')       return 'text-violet-400 border-violet-500/50 bg-violet-500/10'
  if (op === 'coerce')    return 'text-fuchsia-400 border-fuchsia-500/50 bg-fuchsia-500/10'
  if (op === 'flatten')   return 'text-teal-400 border-teal-500/50 bg-teal-500/10'
  if (op === 'enrich')    return 'text-emerald-400 border-emerald-500/50 bg-emerald-500/10'
  if (op === 'dedupe')    return 'text-orange-400 border-orange-500/50 bg-orange-500/10'
  if (op === 'condition') return 'text-rose-400 border-rose-500/50 bg-rose-500/10'
  return 'text-indigo-400 border-indigo-500/50 bg-indigo-500/10'
}

/* ── Form helpers ──────────────────────────────────────────────────────────── */
function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="mb-3">
      <label className="block text-[10px] font-semibold text-chef-muted uppercase tracking-widest mb-1.5">{label}</label>
      {children}
    </div>
  )
}

const inputCls    = 'w-full bg-chef-bg border border-chef-border rounded-lg px-3 py-2 text-xs text-chef-text placeholder:text-chef-muted/50 focus:outline-none focus:border-indigo-500/60 focus:ring-1 focus:ring-indigo-500/30 transition-colors'
const selectCls   = inputCls
const textareaCls = `${inputCls} resize-none font-mono leading-relaxed`

function ToggleGroup({ options, value, onChange }: { options: string[]; value: string; onChange: (v: string) => void }) {
  return (
    <div className="flex flex-wrap gap-1">
      {options.map(o => (
        <button key={o} onClick={() => onChange(o)}
          className={`px-2.5 py-1.5 rounded-lg text-xs font-medium border transition-colors ${
            value === o
              ? 'border-indigo-500/60 bg-indigo-500/15 text-indigo-300'
              : 'border-chef-border bg-chef-bg text-chef-muted hover:border-chef-border-dim hover:text-chef-text'
          }`}>
          {o}
        </button>
      ))}
    </div>
  )
}

/* ── Schema field chips ────────────────────────────────────────────────────── */
function FieldChips({ fields, onInsert, label = 'Dataset fields' }: {
  fields: SchemaField[]; onInsert: (path: string) => void; label?: string
}) {
  if (!fields.length) return null
  return (
    <div className="mb-2.5">
      <div className="text-[9px] font-semibold text-chef-muted uppercase tracking-widest mb-1.5">{label}</div>
      <div className="flex flex-wrap gap-1 max-h-24 overflow-y-auto">
        {fields.map(f => (
          <button key={f.field} onClick={() => onInsert(`$.${f.field}`)}
            title={`${f.type}${f.example ? ` · e.g. ${f.example}` : ''}`}
            className="flex items-center gap-1 px-1.5 py-0.5 rounded border border-chef-border bg-chef-bg hover:border-indigo-500/40 hover:bg-indigo-500/5 transition-colors group">
            <Tag size={7} className="text-chef-muted group-hover:text-indigo-400 shrink-0" />
            <span className="text-[9px] font-mono text-chef-text-dim group-hover:text-indigo-300">$.{f.field}</span>
            <span className="text-[8px] text-chef-muted/60">{f.type.slice(0, 3)}</span>
          </button>
        ))}
      </div>
    </div>
  )
}

/* ── Config sub-forms ──────────────────────────────────────────────────────── */
function ExtractConfig({ config, onChange }: { config: BuilderStepConfig; onChange: (p: Partial<BuilderStepConfig>) => void }) {
  return (
    <>
      <Field label="Source Type">
        <select className={selectCls} value={config.sourceType ?? 'http'} onChange={e => onChange({ sourceType: e.target.value })}>
          <option value="http">HTTP / REST API</option>
          <option value="s3">Amazon S3</option>
          <option value="pg">PostgreSQL</option>
          <option value="mysql">MySQL</option>
          <option value="file">Local File</option>
        </select>
      </Field>
      <Field label={config.sourceType === 'pg' || config.sourceType === 'mysql' ? 'Connection String' : 'URL'}>
        <input className={inputCls} value={config.url ?? ''} onChange={e => onChange({ url: e.target.value })}
          placeholder={config.sourceType === 'pg' ? 'postgresql://user:pass@host/db' : config.sourceType === 's3' ? 's3://bucket/prefix/' : 'https://api.example.com/data'} />
      </Field>
      <Field label="Format">
        <ToggleGroup options={['json', 'jsonl', 'csv', 'parquet']} value={config.format ?? 'json'} onChange={v => onChange({ format: v })} />
      </Field>
      <Field label="Authentication">
        <select className={selectCls} value={config.authType ?? 'none'} onChange={e => onChange({ authType: e.target.value })}>
          <option value="none">None</option>
          <option value="apikey">API Key</option>
          <option value="bearer">Bearer Token</option>
          <option value="basic">Basic Auth</option>
          <option value="oauth2">OAuth 2.0</option>
        </select>
      </Field>
      {config.authType && config.authType !== 'none' && (
        <Field label={config.authType === 'apikey' ? 'API Key' : config.authType === 'basic' ? 'user:password' : 'Token'}>
          <input className={inputCls} type="password" value={config.authValue ?? ''} onChange={e => onChange({ authValue: e.target.value })}
            placeholder={config.authType === 'basic' ? 'user:pass' : 'ey…'} />
        </Field>
      )}
    </>
  )
}

function ValidateConfig({ config, onChange, schemaFields }: {
  config: BuilderStepConfig; onChange: (p: Partial<BuilderStepConfig>) => void; schemaFields: SchemaField[]
}) {
  return (
    <>
      <Field label="Schema Definition (field: type)">
        {schemaFields.length > 0 && (
          <button onClick={() => onChange({ schemaText: schemaFields.map(f => `${f.field}: ${f.type}`).join('\n') })}
            className="mb-1.5 text-[10px] text-indigo-400 hover:text-indigo-300 border border-indigo-500/30 rounded px-2 py-0.5 hover:bg-indigo-500/10 transition-colors block">
            ↑ Import from dataset schema
          </button>
        )}
        <textarea className={textareaCls} rows={6} value={config.schemaText ?? ''}
          onChange={e => onChange({ schemaText: e.target.value })}
          placeholder={'event_id: string\namount: integer\ntimestamp: timestamp\ncustomer_id: string'} />
      </Field>
      <Field label="Validation Mode">
        <ToggleGroup options={['strict', 'lenient']} value={config.validateMode ?? 'strict'} onChange={v => onChange({ validateMode: v })} />
      </Field>
      <Field label="On invalid rows">
        <div className="flex items-center gap-2.5 cursor-pointer" onClick={() => onChange({ quarantine: !config.quarantine })}>
          <div className={`w-9 h-5 rounded-full relative transition-colors ${config.quarantine ? 'bg-indigo-500' : 'bg-chef-border'}`}>
            <div className={`absolute top-0.5 w-4 h-4 rounded-full bg-white transition-transform ${config.quarantine ? 'translate-x-4' : 'translate-x-0.5'}`} />
          </div>
          <span className="text-xs text-chef-text">{config.quarantine ? 'Quarantine to /quarantine/' : 'Halt on failure'}</span>
        </div>
      </Field>
    </>
  )
}

const QUERY_PLACEHOLDERS: Record<string, string> = {
  sql:      'SELECT $.customer_id, SUM($.amount) AS total\nFROM upstream\nWHERE $.status = \'active\'\nGROUP BY $.customer_id\nORDER BY total DESC\nLIMIT 100',
  jsonpath: '$.events[?(@.amount > 100 && @.status == "active")]',
  jmespath: 'events[?amount > `100`] | [*].{id: customer_id, total: amount}',
  kql:      'upstream\n| where $.amount > 100 and $.status == "active"\n| summarize total=sum($.amount) by $.customer_id\n| order by total desc\n| take 100',
}

function QueryConfig({ config, onChange, datasets, schemaFields }: {
  config: BuilderStepConfig; onChange: (p: Partial<BuilderStepConfig>) => void
  datasets: DatasetMeta[]; schemaFields: SchemaField[]
}) {
  return (
    <>
      <Field label="Source Dataset">
        <select className={selectCls} value={config.queryDataset ?? ''} onChange={e => onChange({ queryDataset: e.target.value })}>
          <option value="">— use upstream step output —</option>
          {datasets.map(d => <option key={d.id} value={d.name}>{d.name}</option>)}
        </select>
      </Field>
      <Field label="Query Language">
        <ToggleGroup options={['sql', 'jsonpath', 'jmespath', 'kql']} value={config.queryType ?? 'sql'} onChange={v => onChange({ queryType: v })} />
      </Field>
      <FieldChips fields={schemaFields} onInsert={path => onChange({ queryText: (config.queryText ?? '') + path })} label="Insert field path" />
      <Field label="Query">
        <textarea className={`${textareaCls} text-[10px]`} rows={8}
          value={config.queryText ?? ''}
          onChange={e => onChange({ queryText: e.target.value })}
          placeholder={QUERY_PLACEHOLDERS[config.queryType ?? 'sql']}
          spellCheck={false} />
      </Field>
      <div className="text-[10px] text-chef-muted bg-chef-bg border border-chef-border rounded-lg p-2.5 leading-relaxed">
        This node is your filter/reshape step. Use it to reduce rows, project columns, aggregate, or branch the pipeline before later nodes.
        <br />
        Use <span className="text-indigo-300 font-mono">$.</span> to reference fields:
        {' '}<span className="text-cyan-300 font-mono">$.amount</span>,
        {' '}<span className="text-cyan-300 font-mono">$.customer.name</span>,
        {' '}<span className="text-cyan-300 font-mono">$.items[0].price</span>
      </div>
    </>
  )
}

/* ── Transform operator catalogue ─────────────────────────────────────────── */
const TRANSFORM_CATS = ['Math', 'String', 'Type', 'Date', 'Null', 'Array'] as const
type TransformCat = typeof TRANSFORM_CATS[number]

const TRANSFORM_OPS: Record<TransformCat, { label: string; tmpl: string; hint: string }[]> = {
  Math: [
    { label: '÷ 100',       tmpl: ' / 100',                             hint: 'Divide by 100' },
    { label: '× n',         tmpl: ' * 1',                               hint: 'Multiply by factor' },
    { label: '+ n',         tmpl: ' + 0',                               hint: 'Add constant' },
    { label: '% n',         tmpl: ' % 2',                               hint: 'Modulo' },
    { label: 'round()',     tmpl: 'Math.round($)',                       hint: 'Round to nearest integer' },
    { label: 'floor()',     tmpl: 'Math.floor($)',                       hint: 'Round down' },
    { label: 'ceil()',      tmpl: 'Math.ceil($)',                        hint: 'Round up' },
    { label: 'abs()',       tmpl: 'Math.abs($)',                         hint: 'Absolute value' },
    { label: 'toFixed(2)',  tmpl: 'Number($).toFixed(2)',                hint: 'N decimal places' },
    { label: 'min(a,b)',    tmpl: 'Math.min($, 0)',                      hint: 'Minimum of two' },
    { label: 'max(a,b)',    tmpl: 'Math.max($, 0)',                      hint: 'Maximum of two' },
    { label: 'pow(n)',      tmpl: 'Math.pow($, 2)',                      hint: 'Raise to power' },
    { label: 'sqrt()',      tmpl: 'Math.sqrt($)',                        hint: 'Square root' },
  ],
  String: [
    { label: 'upper()',     tmpl: '$.toUpperCase()',                     hint: 'Uppercase' },
    { label: 'lower()',     tmpl: '$.toLowerCase()',                     hint: 'Lowercase' },
    { label: 'trim()',      tmpl: '$.trim()',                            hint: 'Trim whitespace' },
    { label: 'trimStart()', tmpl: '$.trimStart()',                       hint: 'Trim leading space' },
    { label: 'trimEnd()',   tmpl: '$.trimEnd()',                         hint: 'Trim trailing space' },
    { label: 'slice()',     tmpl: '$.slice(0, 10)',                      hint: 'Substring (start, end)' },
    { label: 'replace()',   tmpl: "$.replace('old', 'new')",            hint: 'Replace first match' },
    { label: 'replaceAll()',tmpl: "$.replaceAll('old', 'new')",         hint: 'Replace all matches' },
    { label: 'split()',     tmpl: "$.split(',')",                        hint: 'Split into array' },
    { label: 'concat()',    tmpl: "$.concat(' ', $.other)",             hint: 'Concatenate strings' },
    { label: 'padStart()',  tmpl: "$.padStart(5, '0')",                 hint: 'Pad start' },
    { label: 'padEnd()',    tmpl: "$.padEnd(5, ' ')",                   hint: 'Pad end' },
    { label: 'repeat(n)',   tmpl: '$.repeat(2)',                         hint: 'Repeat string N times' },
    { label: 'includes()',  tmpl: "$.includes('text')",                  hint: 'Contains substring?' },
    { label: 'startsWith()',tmpl: "$.startsWith('prefix')",             hint: 'Starts with?' },
    { label: 'endsWith()',  tmpl: "$.endsWith('suffix')",               hint: 'Ends with?' },
    { label: 'match()',     tmpl: "$.match(/pattern/)?.[0] ?? ''",      hint: 'Regex first match' },
    { label: 'matchAll()',  tmpl: '[...$.matchAll(/pattern/g)]',         hint: 'All regex matches' },
    { label: 'indexOf()',   tmpl: "$.indexOf('text')",                   hint: 'Index of substring' },
    { label: 'charAt()',    tmpl: '$.charAt(0)',                         hint: 'Character at index' },
  ],
  Type: [
    { label: 'Number()',    tmpl: 'Number($)',                           hint: 'Cast to number' },
    { label: 'String()',    tmpl: 'String($)',                           hint: 'Cast to string' },
    { label: 'Boolean()',   tmpl: 'Boolean($)',                          hint: 'Cast to boolean' },
    { label: 'parseInt()',  tmpl: 'parseInt($, 10)',                     hint: 'Parse as base-10 int' },
    { label: 'parseFloat()',tmpl: 'parseFloat($)',                       hint: 'Parse as float' },
    { label: 'BigInt()',    tmpl: 'BigInt($)',                           hint: 'Cast to bigint' },
    { label: 'JSON.parse()',tmpl: 'JSON.parse($)',                       hint: 'Parse JSON string' },
    { label: 'JSON.stringify()',tmpl: 'JSON.stringify($)',               hint: 'Stringify to JSON' },
    { label: 'Array.from()',tmpl: 'Array.from($)',                       hint: 'Convert to array' },
    { label: 'Object.keys()',tmpl: 'Object.keys($)',                     hint: 'Object keys as array' },
    { label: 'Object.values()',tmpl: 'Object.values($)',                 hint: 'Object values as array' },
    { label: 'typeof',      tmpl: 'typeof $',                            hint: 'Get type name' },
  ],
  Date: [
    { label: 'toISO()',     tmpl: 'new Date($).toISOString()',           hint: 'ISO 8601 full timestamp' },
    { label: 'toDate()',    tmpl: 'new Date($).toLocaleDateString()',    hint: 'Locale date string' },
    { label: 'toTime()',    tmpl: 'new Date($).toLocaleTimeString()',    hint: 'Locale time string' },
    { label: 'dateOnly()',  tmpl: 'new Date($).toISOString().slice(0, 10)', hint: 'YYYY-MM-DD only' },
    { label: 'toUnix()',    tmpl: 'new Date($).getTime()',               hint: 'Unix epoch ms' },
    { label: 'toUnixSec()', tmpl: 'Math.floor(new Date($).getTime() / 1000)', hint: 'Unix epoch seconds' },
    { label: 'getYear()',   tmpl: 'new Date($).getFullYear()',           hint: 'Extract year' },
    { label: 'getMonth()',  tmpl: 'new Date($).getMonth() + 1',         hint: 'Extract month (1–12)' },
    { label: 'getDay()',    tmpl: 'new Date($).getDate()',               hint: 'Extract day of month' },
    { label: 'getHour()',   tmpl: 'new Date($).getHours()',             hint: 'Extract hour (0–23)' },
    { label: 'addDays()',   tmpl: 'new Date(new Date($).getTime() + 86400000 * 1).toISOString()', hint: 'Add N days' },
    { label: 'addHours()',  tmpl: 'new Date(new Date($).getTime() + 3600000 * 1).toISOString()', hint: 'Add N hours' },
    { label: 'now()',       tmpl: 'new Date().toISOString()',            hint: 'Current UTC timestamp' },
    { label: 'diffDays()',  tmpl: 'Math.floor((Date.now() - new Date($).getTime()) / 86400000)', hint: 'Days since date' },
  ],
  Null: [
    { label: '?? default',  tmpl: " ?? 'default'",                      hint: 'Nullish coalesce fallback' },
    { label: '|| fallback', tmpl: " || 'fallback'",                     hint: 'Falsy fallback' },
    { label: 'ifNull()',    tmpl: "$ !== null ? $ : 'N/A'",             hint: 'Replace null with value' },
    { label: 'ifEmpty()',   tmpl: "$ || ''",                            hint: 'Replace empty with value' },
    { label: 'ifUndef()',   tmpl: "$ !== undefined ? $ : 'default'",    hint: 'Replace undefined' },
    { label: 'exists?',     tmpl: "$ !== undefined && $ !== null",       hint: 'Is field defined and non-null?' },
    { label: 'toEmpty()',   tmpl: "$ ?? ''",                            hint: 'Null → empty string' },
    { label: 'toZero()',    tmpl: "$ ?? 0",                             hint: 'Null → zero' },
    { label: 'toBool()',    tmpl: "Boolean($ ?? false)",                 hint: 'Null-safe boolean' },
    { label: 'try()',       tmpl: "(() => { try { return $; } catch { return null } })()", hint: 'Wrap in try/catch' },
  ],
  Array: [
    { label: '[0]',         tmpl: '$[0]',                               hint: 'First element' },
    { label: '[-1]',        tmpl: '$[$?.length - 1]',                   hint: 'Last element' },
    { label: '.length',     tmpl: '$.length',                           hint: 'Count elements' },
    { label: 'join()',      tmpl: "$.join(', ')",                        hint: 'Join to string' },
    { label: 'map()',       tmpl: '$.map(x => x)',                       hint: 'Transform each item' },
    { label: 'filter()',    tmpl: '$.filter(x => Boolean(x))',           hint: 'Filter truthy items' },
    { label: 'find()',      tmpl: "$.find(x => x.id === 'value')",      hint: 'Find first match' },
    { label: 'findIndex()', tmpl: "$.findIndex(x => x === 'value')",    hint: 'Index of first match' },
    { label: 'includes()',  tmpl: "$.includes('value')",                 hint: 'Array contains value?' },
    { label: 'flat()',      tmpl: '$.flat()',                            hint: 'Flatten one level' },
    { label: 'flatMap()',   tmpl: '$.flatMap(x => x)',                   hint: 'Map then flatten' },
    { label: 'reduce()',    tmpl: '$.reduce((a, x) => a + x, 0)',        hint: 'Accumulate to value' },
    { label: 'sort()',      tmpl: '[...$.sort()]',                        hint: 'Sort ascending (copy)' },
    { label: 'reverse()',   tmpl: '[...$.reverse()]',                    hint: 'Reverse (copy)' },
    { label: 'slice()',     tmpl: '$.slice(0, 5)',                        hint: 'Take first N items' },
    { label: 'uniq()',      tmpl: '[...new Set($)]',                      hint: 'Deduplicate' },
    { label: 'concat(b)',   tmpl: '$.concat($.other)',                   hint: 'Concatenate two arrays' },
    { label: 'every()',     tmpl: '$.every(x => Boolean(x))',            hint: 'All items match?' },
    { label: 'some()',      tmpl: '$.some(x => Boolean(x))',             hint: 'Any item matches?' },
    { label: 'count()',     tmpl: '$.filter(x => Boolean(x)).length',    hint: 'Count truthy items' },
  ],
}

function MapConfig({ config, onChange, schemaFields }: {
  config: BuilderStepConfig; onChange: (p: Partial<BuilderStepConfig>) => void; schemaFields: SchemaField[]
}) {
  const mappings: Mapping[] = config.mappings ?? []
  const [activeRow,   setActiveRow]   = useState<number | null>(null)
  const [activeTxRow, setActiveTxRow] = useState<number | null>(null)
  const [txCat,       setTxCat]       = useState<TransformCat>('Math')
  const txInputRefs = useRef<Map<number, HTMLInputElement>>(new Map())

  function updateMapping(i: number, key: keyof Mapping, val: string) {
    onChange({ mappings: mappings.map((m, j) => j === i ? { ...m, [key]: val } : m) })
  }

  function insertField(path: string) {
    if (activeRow !== null) {
      updateMapping(activeRow, 'from', path)
    } else {
      const bare = path.replace(/^\$\./, '')
      onChange({ mappings: [...mappings, { from: path, to: bare, transform: '' }] })
    }
  }

  function insertOp(row: number, tmpl: string) {
    const input   = txInputRefs.current.get(row)
    const current = mappings[row]?.transform ?? ''
    const from    = mappings[row]?.from ?? '$'
    const resolved = tmpl.replace(/\$/g, from || '$')

    if (input) {
      const start  = input.selectionStart ?? current.length
      const end    = input.selectionEnd   ?? current.length
      const newVal = current.slice(0, start) + resolved + current.slice(end)
      updateMapping(row, 'transform', newVal)
      setTimeout(() => {
        input.focus()
        input.setSelectionRange(start + resolved.length, start + resolved.length)
      }, 0)
    } else {
      updateMapping(row, 'transform', current + resolved)
    }
  }

  const hasAnyMapping = mappings.some(m => m.from)

  return (
    <>
      <FieldChips
        fields={schemaFields}
        onInsert={insertField}
        label={activeRow !== null ? `Click to fill row ${activeRow + 1} "from" field` : 'Click field to add mapping'}
      />
      <div className="mb-3">
        {/* ── From / To columns ── */}
        <div className="grid grid-cols-[1fr_1fr_auto] gap-1 mb-1">
          <div className="text-[9px] text-chef-muted px-1">From ($.path)</div>
          <div className="text-[9px] text-chef-muted px-1">To field</div>
          <div className="w-7" />
        </div>
        {mappings.map((m, i) => (
          <div key={i} className={`grid grid-cols-[1fr_1fr_auto] gap-1 mb-1 rounded transition-colors ${activeRow === i ? 'ring-1 ring-indigo-500/30' : ''}`}>
            <input className={inputCls} value={m.from} placeholder="$.source_field"
              onChange={e => updateMapping(i, 'from', e.target.value)}
              onFocus={() => setActiveRow(i)} onBlur={() => setActiveRow(null)} />
            <input className={inputCls} value={m.to} placeholder="dest_field"
              onChange={e => updateMapping(i, 'to', e.target.value)} />
            <button onClick={() => onChange({ mappings: mappings.filter((_, j) => j !== i) })}
              className="w-7 h-7 flex items-center justify-center text-chef-muted hover:text-rose-400 hover:bg-rose-500/10 rounded-lg transition-colors mt-0.5">
              <X size={11} />
            </button>
          </div>
        ))}

        {/* ── Transform column + operator panel ── */}
        {hasAnyMapping && (
          <div className="mt-3">
            <div className="flex items-center justify-between mb-1.5">
              <div className="text-[9px] font-semibold text-chef-muted uppercase tracking-widest">Transform expression</div>
              {activeTxRow !== null && (
                <span className="text-[9px] text-indigo-400 font-mono">row {activeTxRow + 1} active</span>
              )}
            </div>

            {mappings.map((m, i) => !m.from ? null : (
              <div key={`t${i}`} className={`flex items-center gap-1.5 mb-1 rounded transition-all ${activeTxRow === i ? 'outline outline-1 outline-indigo-500/40' : ''}`}>
                <span className="text-[9px] text-chef-muted font-mono w-4 text-right shrink-0 select-none">{i + 1}</span>
                <input
                  ref={el => { if (el) txInputRefs.current.set(i, el); else txInputRefs.current.delete(i) }}
                  className={`${inputCls} font-mono text-[10px]`}
                  value={m.transform}
                  placeholder={`${m.from || '$'} → click operators ↓`}
                  onChange={e => updateMapping(i, 'transform', e.target.value)}
                  onFocus={() => setActiveTxRow(i)}
                  onBlur={() => setActiveTxRow(null)}
                />
              </div>
            ))}

            {/* ── Operator panel ── */}
            <div className="mt-1.5 border border-chef-border rounded-xl bg-chef-bg overflow-hidden">
              {/* category tabs */}
              <div className="flex border-b border-chef-border">
                {TRANSFORM_CATS.map(cat => (
                  <button key={cat}
                    onMouseDown={e => { e.preventDefault(); setTxCat(cat) }}
                    className={`flex-1 py-1.5 text-[9px] font-semibold transition-colors border-r border-chef-border last:border-r-0 ${
                      txCat === cat
                        ? 'bg-indigo-500/15 text-indigo-300'
                        : 'text-chef-muted hover:text-chef-text hover:bg-chef-card'
                    }`}
                  >
                    {cat}
                  </button>
                ))}
              </div>
              {/* op chips */}
              <div className="p-2 flex flex-wrap gap-1 max-h-28 overflow-y-auto">
                {TRANSFORM_OPS[txCat].map(op => (
                  <button key={op.label}
                    onMouseDown={e => {
                      e.preventDefault()
                      if (activeTxRow !== null) insertOp(activeTxRow, op.tmpl)
                    }}
                    title={op.hint}
                    className={`px-2 py-0.5 rounded border text-[9px] font-mono transition-colors ${
                      activeTxRow !== null
                        ? 'border-indigo-500/30 bg-indigo-500/5 text-indigo-200 hover:bg-indigo-500/20 hover:border-indigo-500/60 cursor-pointer'
                        : 'border-chef-border/50 text-chef-muted/40 cursor-default'
                    }`}
                  >
                    {op.label}
                  </button>
                ))}
              </div>
              <div className={`px-2.5 py-1.5 text-[9px] border-t border-chef-border transition-colors ${
                activeTxRow !== null ? 'text-indigo-400/70' : 'text-chef-muted/50'
              }`}>
                {activeTxRow !== null
                  ? `Inserting into row ${activeTxRow + 1} · hover a chip to see hint · $→field path`
                  : 'Focus a transform input above to activate operators'}
              </div>
            </div>
          </div>
        )}

        <button onClick={() => onChange({ mappings: [...mappings, { from: '', to: '', transform: '' }] })}
          className="flex items-center gap-1.5 text-xs text-indigo-400 hover:text-indigo-300 transition-colors mt-2">
          <Plus size={12} /> Add mapping
        </button>
      </div>
      <div className="text-[10px] text-chef-muted bg-chef-bg border border-chef-border rounded-lg p-2.5 leading-relaxed">
        Nested paths: <span className="text-cyan-300 font-mono">$.address.city</span>
        {' · '}<span className="text-cyan-300 font-mono">$.items[0].sku</span>
        {' · '}<span className="text-cyan-300 font-mono">$.tags[*]</span>
      </div>
    </>
  )
}

function EnrichConfig({ config, onChange, schemaFields }: {
  config: BuilderStepConfig; onChange: (p: Partial<BuilderStepConfig>) => void; schemaFields: SchemaField[]
}) {
  return (
    <>
      <Field label="Lookup URL">
        <input className={inputCls} value={config.lookupUrl ?? ''} onChange={e => onChange({ lookupUrl: e.target.value })}
          placeholder="https://geo.acme.io/v1/lookup" />
      </Field>
      <Field label="Join Key ($.field to look up)">
        <FieldChips fields={schemaFields} onInsert={v => onChange({ joinKey: v })} />
        <input className={inputCls} value={config.joinKey ?? ''} onChange={e => onChange({ joinKey: e.target.value })}
          placeholder="$.ip_address" />
      </Field>
      <Field label="Fields to Add (comma-separated)">
        <input className={inputCls} value={config.enrichFields ?? ''} onChange={e => onChange({ enrichFields: e.target.value })}
          placeholder="country, region, city, org" />
      </Field>
    </>
  )
}

function CoerceConfig({ config, onChange, schemaFields }: {
  config: BuilderStepConfig; onChange: (p: Partial<BuilderStepConfig>) => void; schemaFields: SchemaField[]
}) {
  return (
    <>
      <Field label="Field to cast">
        <FieldChips fields={schemaFields} onInsert={v => onChange({ coerceField: v })} />
        <input className={inputCls} value={config.coerceField ?? ''} onChange={e => onChange({ coerceField: e.target.value })}
          placeholder="$.amount" />
      </Field>
      <Field label="Target type">
        <ToggleGroup options={['string', 'integer', 'float', 'boolean', 'date', 'timestamp', 'json']} value={config.coerceType ?? 'string'} onChange={v => onChange({ coerceType: v })} />
      </Field>
      <div className="text-[10px] text-chef-muted bg-chef-bg border border-chef-border rounded-lg p-2.5 leading-relaxed">
        Casts the selected field in-place during preview and runtime. Invalid casts are set to null.
      </div>
    </>
  )
}

function FlattenConfig({ config, onChange, schemaFields }: {
  config: BuilderStepConfig; onChange: (p: Partial<BuilderStepConfig>) => void; schemaFields: SchemaField[]
}) {
  return (
    <>
      <Field label="Field to flatten">
        <FieldChips fields={schemaFields} onInsert={v => onChange({ flattenField: v })} />
        <input className={inputCls} value={config.flattenField ?? ''} onChange={e => onChange({ flattenField: e.target.value })}
          placeholder="$.customer" />
      </Field>
      <Field label="Flatten mode">
        <ToggleGroup options={['object', 'array']} value={config.flattenMode ?? 'object'} onChange={v => onChange({ flattenMode: v })} />
      </Field>
      <div className="text-[10px] text-chef-muted bg-chef-bg border border-chef-border rounded-lg p-2.5 leading-relaxed">
        Object mode expands nested keys into top-level columns. Array mode unwinds one array item into one output row.
      </div>
    </>
  )
}

function DedupeConfig({ config, onChange, schemaFields }: {
  config: BuilderStepConfig; onChange: (p: Partial<BuilderStepConfig>) => void; schemaFields: SchemaField[]
}) {
  return (
    <>
      <Field label="Deduplication Key ($.field)">
        <FieldChips fields={schemaFields} onInsert={v => onChange({ dedupeKey: v })} />
        <input className={inputCls} value={config.dedupeKey ?? ''} onChange={e => onChange({ dedupeKey: e.target.value })}
          placeholder="$.event_id" />
      </Field>
      <Field label="Time Window">
        <ToggleGroup options={['1d', '7d', '30d', 'all']} value={config.dedupeWindow ?? '7d'} onChange={v => onChange({ dedupeWindow: v })} />
      </Field>
      <div className="text-[10px] text-chef-muted bg-chef-bg border border-chef-border rounded-lg p-2.5 leading-relaxed">
        Rows with the same key within the window are collapsed to one, keeping the most recent.
      </div>
    </>
  )
}

const CONDITION_OPS = ['==', '!=', '>', '>=', '<', '<=', 'contains', 'startsWith', 'exists', 'isNull']

function ConditionConfig({ config, onChange, schemaFields }: {
  config: BuilderStepConfig; onChange: (p: Partial<BuilderStepConfig>) => void; schemaFields: SchemaField[]
}) {
  return (
    <>
      <div className="mb-3 p-3 bg-rose-500/5 border border-rose-500/20 rounded-xl">
        <div className="text-[10px] font-semibold text-rose-300 mb-2.5 flex items-center gap-1.5">
          <GitBranch size={10} /> Condition
        </div>
        <FieldChips fields={schemaFields} onInsert={v => onChange({ conditionField: v })} label="Pick field" />
        <div className="grid grid-cols-[1fr_auto_1fr] gap-1.5 items-end">
          <Field label="Field">
            <input className={inputCls} value={config.conditionField ?? ''} onChange={e => onChange({ conditionField: e.target.value })}
              placeholder="$.status" />
          </Field>
          <Field label="Op">
            <select className={`${selectCls} !w-auto`} value={config.conditionOp ?? '=='} onChange={e => onChange({ conditionOp: e.target.value })}>
              {CONDITION_OPS.map(o => <option key={o} value={o}>{o}</option>)}
            </select>
          </Field>
          <Field label="Value">
            <input className={inputCls} value={config.conditionValue ?? ''} onChange={e => onChange({ conditionValue: e.target.value })}
              placeholder="active" />
          </Field>
        </div>
      </div>
      <div className="grid grid-cols-2 gap-2 mb-3">
        <Field label="True branch">
          <input className={`${inputCls} !border-emerald-500/30 focus:!border-emerald-500/60`}
            value={config.trueBranch ?? 'Pass'} onChange={e => onChange({ trueBranch: e.target.value })} placeholder="Pass" />
        </Field>
        <Field label="False branch">
          <input className={`${inputCls} !border-rose-500/30 focus:!border-rose-500/60`}
            value={config.falseBranch ?? 'Drop'} onChange={e => onChange({ falseBranch: e.target.value })} placeholder="Drop" />
        </Field>
      </div>
      <div className="text-[10px] text-chef-muted bg-chef-bg border border-chef-border rounded-lg p-2.5 leading-relaxed">
        Rows matching the condition follow the <span className="text-emerald-400">true</span> branch. Others follow the <span className="text-rose-400">false</span> branch or are dropped.
        Use this as a row-level filter gate: for example, <span className="text-cyan-300 font-mono">$.status == active</span> or <span className="text-cyan-300 font-mono">$.amount &gt; 100</span>.
      </div>
    </>
  )
}

function WriteConfig({ config, onChange, datasets }: {
  config: BuilderStepConfig
  onChange: (p: Partial<BuilderStepConfig>) => void
  datasets: DatasetMeta[]
}) {
  const mode = config.createDataset ? 'new-dataset' : config.targetDatasetId ? 'existing-dataset' : 'sink'
  return (
    <>
      <Field label="Output mode">
        <div className="grid grid-cols-3 gap-1.5">
          <button onClick={() => onChange({ createDataset: false, targetDatasetId: '' })}
            className={`px-3 py-2 rounded-lg text-xs font-medium border transition-colors ${
              mode === 'sink' ? 'border-indigo-500/60 bg-indigo-500/15 text-indigo-300' : 'border-chef-border bg-chef-bg text-chef-muted hover:text-chef-text'
            }`}>
            Export to sink
          </button>
          <button onClick={() => onChange({ createDataset: true, targetDatasetId: '' })}
            className={`px-3 py-2 rounded-lg text-xs font-medium border transition-colors ${
              mode === 'new-dataset' ? 'border-violet-500/60 bg-violet-500/15 text-violet-300' : 'border-chef-border bg-chef-bg text-chef-muted hover:text-chef-text'
            }`}>
            New dataset
          </button>
          <button onClick={() => onChange({ createDataset: false, targetDatasetId: datasets[0]?.id ?? '' })}
            className={`px-3 py-2 rounded-lg text-xs font-medium border transition-colors ${
              mode === 'existing-dataset' ? 'border-cyan-500/60 bg-cyan-500/15 text-cyan-300' : 'border-chef-border bg-chef-bg text-chef-muted hover:text-chef-text'
            }`}>
            Refresh dataset
          </button>
        </div>
      </Field>

      {mode === 'new-dataset' ? (
        <>
          <Field label="New Dataset Name">
            <input className={inputCls} value={config.newDatasetName ?? ''} onChange={e => onChange({ newDatasetName: e.target.value })}
              placeholder="my-processed-events" />
          </Field>
          <Field label="Format">
            <ToggleGroup options={['jsonl', 'parquet', 'csv']} value={config.destFormat ?? 'jsonl'} onChange={v => onChange({ destFormat: v })} />
          </Field>
          <div className="text-[10px] text-chef-muted bg-violet-500/5 border border-violet-500/20 rounded-lg p-2.5 leading-relaxed">
            Output is registered as a new live dataset — queryable in Query, usable in other pipelines, visible on the Datasets page.
          </div>
        </>
      ) : mode === 'existing-dataset' ? (
        <>
          <Field label="Dataset to Refresh">
            <select className={selectCls} value={config.targetDatasetId ?? ''} onChange={e => onChange({ targetDatasetId: e.target.value })}>
              {datasets.map(dataset => (
                <option key={dataset.id} value={dataset.id}>{dataset.name}</option>
              ))}
            </select>
          </Field>
          <div className="text-[10px] text-chef-muted bg-cyan-500/5 border border-cyan-500/20 rounded-lg p-2.5 leading-relaxed">
            Each successful run replaces the selected dataset sample/schema snapshot and preserves its identity for downstream queries and pipelines.
          </div>
        </>
      ) : (
        <>
          <Field label="Destination">
            <select className={selectCls} value={config.destType ?? 'S3'} onChange={e => onChange({ destType: e.target.value })}>
              <option value="S3">Amazon S3</option>
              <option value="PG">PostgreSQL</option>
              <option value="HTTP">HTTP API / Webhook</option>
              <option value="local">Local File</option>
            </select>
          </Field>
          <Field label="Output Format">
            <ToggleGroup options={['parquet', 'jsonl', 'csv', 'json']} value={config.destFormat ?? 'parquet'} onChange={v => onChange({ destFormat: v })} />
          </Field>
          <Field label={config.destType === 'PG' ? 'Table' : config.destType === 'HTTP' ? 'Endpoint URL' : 'Path'}>
            <input className={inputCls} value={config.destPath ?? ''} onChange={e => onChange({ destPath: e.target.value })}
              placeholder={
                config.destType === 'PG'    ? 'public.billing_clean' :
                config.destType === 'HTTP'  ? 'https://api.acme.io/ingest' :
                config.destType === 'local' ? '/tmp/output.parquet' : 's3://acme-data/clean/'
              } />
          </Field>
        </>
      )}
    </>
  )
}

/* ── Zigzag canvas ─────────────────────────────────────────────────────────── */
const COLS   = 4
const NODE_W = 164
const NODE_H = 82
const H_GAP  = 52
const V_GAP  = 68
const PAD_T  = 32   // room above nodes for hover controls
const PAD_B  = 24   // room below for condition branch labels

function nodePos(index: number) {
  const row = Math.floor(index / COLS)
  const col = index % COLS
  const displayCol = row % 2 === 0 ? col : COLS - 1 - col
  return { x: displayCol * (NODE_W + H_GAP), y: row * (NODE_H + V_GAP) }
}

interface Arrow { x1: number; y1: number; x2: number; y2: number }

function buildArrows(n: number): Arrow[] {
  const arrows: Arrow[] = []
  for (let i = 0; i < n - 1; i++) {
    const a = nodePos(i), b = nodePos(i + 1)
    const rowA = Math.floor(i / COLS), rowB = Math.floor((i + 1) / COLS)
    if (rowA === rowB) {
      const isEven = rowA % 2 === 0
      if (isEven) arrows.push({ x1: a.x + NODE_W, y1: a.y + NODE_H / 2, x2: b.x,         y2: b.y + NODE_H / 2 })
      else        arrows.push({ x1: a.x,           y1: a.y + NODE_H / 2, x2: b.x + NODE_W, y2: b.y + NODE_H / 2 })
    } else {
      // vertical drop — same x column
      arrows.push({ x1: a.x + NODE_W / 2, y1: a.y + NODE_H, x2: b.x + NODE_W / 2, y2: b.y })
    }
  }
  return arrows
}

type CanvasItem =
  | { id: '__source__'; kind: 'source'; label: string; summary: string }
  | { id: '__output__'; kind: 'output'; label: string; summary: string }
  | { id: string; kind: 'step'; step: BuilderStep }

function CanvasNode({ item, index, total, selected, posX, posY, onSelect, onMoveLeft, onMoveRight, onDelete }: {
  item: CanvasItem; index: number; total: number; selected: boolean
  posX: number; posY: number
  onSelect: () => void; onMoveLeft: () => void; onMoveRight: () => void; onDelete: () => void
}) {
  const step = item.kind === 'step' ? item.step : null
  const canMoveLeft = item.kind === 'step' && index > 1
  const canMoveRight = item.kind === 'step' && index < total - 2
  const accent = item.kind === 'source'
    ? 'text-sky-400 border-sky-500/50 bg-sky-500/10'
    : item.kind === 'output'
    ? 'text-emerald-400 border-emerald-500/50 bg-emerald-500/10'
    : opAccent(step!.op)
  const isCondition = step?.op === 'condition'

  return (
    <div
      className="absolute group"
      style={{ left: posX, top: posY - 28, width: NODE_W, height: NODE_H + 28 }}
    >
      {/* hover controls */}
      <div className="absolute top-0 left-0 right-0 opacity-0 group-hover:opacity-100 group-focus-within:opacity-100 flex items-center justify-end gap-1 px-1 transition-opacity">
        <button onClick={e => { e.stopPropagation(); onMoveLeft() }} disabled={!canMoveLeft}
          className="w-5 h-5 flex items-center justify-center rounded text-chef-muted hover:text-chef-text hover:bg-chef-card border border-chef-border disabled:opacity-30 disabled:cursor-not-allowed transition-colors">
          <ChevronLeft size={10} />
        </button>
        <button onClick={e => { e.stopPropagation(); onMoveRight() }} disabled={!canMoveRight}
          className="w-5 h-5 flex items-center justify-center rounded text-chef-muted hover:text-chef-text hover:bg-chef-card border border-chef-border disabled:opacity-30 disabled:cursor-not-allowed transition-colors">
          <ChevronRight size={10} />
        </button>
        <button onClick={e => { e.stopPropagation(); onDelete() }} disabled={item.kind !== 'step'}
          className="w-5 h-5 flex items-center justify-center rounded text-chef-muted hover:text-rose-400 hover:bg-rose-500/10 border border-chef-border transition-colors">
          <X size={10} />
        </button>
      </div>

      <div
        className={`absolute inset-x-0 top-7 cursor-pointer transition-all duration-150 rounded-xl border ${
          selected
            ? 'ring-2 ring-indigo-500 ring-offset-1 ring-offset-chef-surface border-indigo-500/60 bg-indigo-500/10 shadow-[0_0_16px_rgba(99,102,241,0.25)]'
            : isCondition
            ? 'border-rose-500/30 bg-rose-500/5 hover:border-rose-500/50 hover:bg-rose-500/8'
            : 'border-chef-border bg-chef-card hover:border-chef-border-dim hover:bg-chef-card-hover'
        }`}
        style={{ height: NODE_H }}
        onClick={onSelect}
      >
        {/* step number badge */}
        <div className="absolute -top-2 -left-2 w-5 h-5 rounded-full bg-chef-surface border border-chef-border text-[9px] font-mono text-chef-muted flex items-center justify-center z-10">
          {index + 1}
        </div>

        {/* condition branch labels below node */}
        {isCondition && step && (
          <div className="absolute -bottom-5 left-0 right-0 flex justify-between px-2">
            <span className="text-[9px] text-emerald-400 font-mono">T: {step.config.trueBranch || 'Pass'}</span>
            <span className="text-[9px] text-rose-400 font-mono">F: {step.config.falseBranch || 'Drop'}</span>
          </div>
        )}

        {/* content */}
        <div className="px-3 py-2.5 h-full flex flex-col justify-between">
          <div className="flex items-center gap-2">
            <span className={`shrink-0 ${accent.split(' ')[0]}`}>
              {item.kind === 'step'
                ? <OpIcon op={step!.op} className="w-3.5 h-3.5" />
                : item.kind === 'source'
                ? <Database className="w-3.5 h-3.5" />
                : <Eye className="w-3.5 h-3.5" />}
            </span>
            <span className="text-[11px] font-semibold text-chef-text leading-tight truncate">{item.kind === 'step' ? step!.label : item.label}</span>
          </div>
          <div className={`self-start text-[9px] font-mono px-1.5 py-0.5 rounded border ${accent}`}>
            {item.kind === 'step' ? step!.op : item.kind}
          </div>
          <div className={`text-[9px] leading-tight truncate font-mono ${step?.invalidated ? 'text-amber-300' : 'text-chef-muted'}`}>
            {item.kind === 'step'
              ? (step!.invalidated ? 'Needs review after source change' : configSummary(step!.op, step!.config))
              : item.summary}
          </div>
        </div>
      </div>
    </div>
  )
}

function BuilderCanvas({ sourceType, sourceLabel, steps, selectedId, onSelect, onReorder, onDelete }: {
  sourceType: 'dataset' | 'connector'
  sourceLabel: string
  steps: BuilderStep[]; selectedId: string | null
  onSelect: (id: string) => void; onReorder: (from: number, dir: 'left' | 'right') => void; onDelete: (id: string) => void
}) {
  const items: CanvasItem[] = [
    { id: '__source__', kind: 'source', label: sourceType === 'connector' ? 'Connector Source' : 'Dataset Source', summary: sourceLabel || 'Select a source above' },
    ...steps.map(step => ({ id: step.id, kind: 'step', step }) as CanvasItem),
    { id: '__output__', kind: 'output', label: 'Preview Output', summary: steps.length ? 'Select to inspect full pipeline output' : 'Add steps to shape the final output' },
  ]

  const numRows = Math.ceil(items.length / COLS)
  const canvasW = COLS * NODE_W + (COLS - 1) * H_GAP
  const quarantineIndex = steps.findIndex(step => step.op === 'validate' && step.config.quarantine)
  const quarantineStep = quarantineIndex >= 0 ? steps[quarantineIndex] : null
  const quarantinePos = quarantineStep ? nodePos(quarantineIndex + 1) : null
  const canvasH = numRows * NODE_H + (numRows - 1) * V_GAP + (quarantineStep ? NODE_H + 56 : 0)
  const arrows  = buildArrows(items.length)

  return (
    <div className="overflow-auto pb-6">
      <div className="relative" style={{ width: canvasW + 24, height: canvasH + PAD_T + PAD_B }}>
        <svg className="absolute inset-0 pointer-events-none overflow-visible" width={canvasW + 24} height={canvasH + PAD_T + PAD_B}>
          <defs>
            <marker id="arr-b" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto">
              <polygon points="0 0, 8 3, 0 6" fill="#6366f1" fillOpacity="0.75" />
            </marker>
          </defs>
          {arrows.map((a, i) => (
            <line key={i}
              x1={a.x1} y1={a.y1 + PAD_T}
              x2={a.x2} y2={a.y2 + PAD_T}
              stroke="#6366f1" strokeWidth="1.5" strokeOpacity="0.5"
              markerEnd="url(#arr-b)" />
          ))}
          {quarantinePos && (
            <line
              x1={quarantinePos.x + NODE_W / 2}
              y1={quarantinePos.y + NODE_H + PAD_T}
              x2={quarantinePos.x + NODE_W / 2}
              y2={quarantinePos.y + NODE_H + 44 + PAD_T}
              stroke="#f59e0b"
              strokeWidth="1.5"
              strokeOpacity="0.8"
              strokeDasharray="5 3"
              markerEnd="url(#arr-b)"
            />
          )}
        </svg>

        {items.map((item, i) => {
          const pos = nodePos(i)
          return (
            <CanvasNode key={item.id}
              item={item} index={i} total={items.length} selected={item.id === selectedId}
              posX={pos.x} posY={pos.y + PAD_T}
              onSelect={() => onSelect(item.id)}
              onMoveLeft={() => item.kind === 'step' && onReorder(i - 1, 'left')}
              onMoveRight={() => item.kind === 'step' && onReorder(i - 1, 'right')}
              onDelete={() => item.kind === 'step' && onDelete(item.id)} />
          )
        })}
        {quarantinePos && (
          <div
            className="absolute border rounded-xl px-3 py-2.5 border-amber-500/40 bg-amber-500/5 text-amber-300"
            style={{ left: quarantinePos.x, top: quarantinePos.y + NODE_H + 44 + PAD_T, width: NODE_W, height: NODE_H }}
          >
            <div className="flex items-center gap-2 mb-1.5">
              <AlertTriangle size={14} className="shrink-0" />
              <span className="text-[11px] font-semibold truncate">Quarantine</span>
            </div>
            <div className="text-[9px] text-chef-muted leading-tight truncate">
              invalid rows → /quarantine/
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

/* ── Config panel ──────────────────────────────────────────────────────────── */
function ConfigPanel({ step, schemaFields, datasets, onLabelChange, onConfigChange }: {
  step: BuilderStep; schemaFields: SchemaField[]; datasets: DatasetMeta[]
  onLabelChange: (label: string) => void
  onConfigChange: (patch: Partial<BuilderStepConfig>) => void
}) {
  const accent = opAccent(step.op)
  return (
    <div className="flex flex-col h-full">
      <div className="px-4 py-3.5 border-b border-chef-border shrink-0">
        <div className="flex items-center gap-2 mb-2">
          <span className={`p-1.5 rounded-lg border ${accent}`}><OpIcon op={step.op} className="w-3.5 h-3.5" /></span>
          <span className={`text-[10px] font-mono px-2 py-0.5 rounded border ${accent}`}>{step.op}</span>
          {step.invalidated && (
            <span className="text-[10px] font-medium px-2 py-0.5 rounded border border-amber-500/40 bg-amber-500/10 text-amber-300">
              invalidated
            </span>
          )}
        </div>
        <input
          className="w-full bg-transparent text-sm font-semibold text-chef-text placeholder:text-chef-muted focus:outline-none border-b border-transparent focus:border-chef-border pb-0.5 transition-colors"
          value={step.label} onChange={e => onLabelChange(e.target.value)} placeholder="Step name" />
      </div>

      <div className="flex-1 overflow-y-auto px-4 py-4">
        {step.invalidated && (
          <div className="mb-4 rounded-xl border border-amber-500/30 bg-amber-500/10 px-3 py-2.5 text-[11px] text-amber-200 leading-relaxed">
            {step.invalidationReason ?? 'This step depends on the source dataset and needs to be reconfigured.'}
          </div>
        )}
        {step.op === 'extract'   && <ExtractConfig   config={step.config} onChange={onConfigChange} />}
        {step.op === 'validate'  && <ValidateConfig  config={step.config} onChange={onConfigChange} schemaFields={schemaFields} />}
        {step.op === 'query'     && <QueryConfig     config={step.config} onChange={onConfigChange} datasets={datasets} schemaFields={schemaFields} />}
        {step.op === 'map'       && <MapConfig       config={step.config} onChange={onConfigChange} schemaFields={schemaFields} />}
        {step.op === 'coerce'    && <CoerceConfig    config={step.config} onChange={onConfigChange} schemaFields={schemaFields} />}
        {step.op === 'flatten'   && <FlattenConfig   config={step.config} onChange={onConfigChange} schemaFields={schemaFields} />}
        {step.op === 'enrich'    && <EnrichConfig    config={step.config} onChange={onConfigChange} schemaFields={schemaFields} />}
        {step.op === 'dedupe'    && <DedupeConfig    config={step.config} onChange={onConfigChange} schemaFields={schemaFields} />}
        {step.op === 'condition' && <ConditionConfig config={step.config} onChange={onConfigChange} schemaFields={schemaFields} />}
        {step.op === 'write'     && <WriteConfig     config={step.config} onChange={onConfigChange} datasets={datasets} />}
        <div className="mt-4 rounded-xl border border-chef-border bg-chef-card px-3 py-3 text-[11px] leading-relaxed">
          <div className="text-[10px] font-semibold uppercase tracking-widest text-chef-muted mb-2">How To Use This Node</div>
          <div className="text-chef-text mb-2">{NODE_GUIDES[step.op].purpose}</div>
          <div className="text-chef-muted mb-2">
            <span className="text-chef-text">Example:</span> {NODE_GUIDES[step.op].example}
          </div>
          <div className="space-y-1 text-chef-muted">
            {NODE_GUIDES[step.op].tips.map(tip => <div key={tip}>- {tip}</div>)}
          </div>
        </div>
      </div>
    </div>
  )
}

function SourcePanel({ builder, sourceName }: { builder: BuilderState; sourceName: string }) {
  return (
    <div className="flex flex-col h-full">
      <div className="px-4 py-3.5 border-b border-chef-border shrink-0">
        <div className="flex items-center gap-2 mb-2">
          <span className="p-1.5 rounded-lg border text-sky-400 border-sky-500/50 bg-sky-500/10"><Database className="w-3.5 h-3.5" /></span>
          <span className="text-[10px] font-mono px-2 py-0.5 rounded border text-sky-400 border-sky-500/50 bg-sky-500/10">source</span>
        </div>
        <div className="text-sm font-semibold text-chef-text">Pipeline source</div>
      </div>
      <div className="flex-1 overflow-y-auto px-4 py-4 space-y-3 text-[11px] leading-relaxed">
        <div className="rounded-xl border border-chef-border bg-chef-card px-3 py-2.5">
          <div className="text-[10px] uppercase tracking-widest text-chef-muted mb-1">Selected source</div>
          <div className="text-chef-text font-mono">{sourceName || 'Choose a dataset or connector in the header'}</div>
          <div className="text-chef-muted mt-1">{builder.sourceType === 'connector' ? 'Connector' : 'Dataset'}{builder.resource ? ` · resource: ${builder.resource}` : ''}</div>
        </div>
        <div className="text-chef-muted">
          The first node is always the source anchor. Pick the dataset or connector in the header, then add transform nodes after it.
        </div>
      </div>
    </div>
  )
}

function OutputPanel({ hasSteps }: { hasSteps: boolean }) {
  return (
    <div className="flex flex-col h-full">
      <div className="px-4 py-3.5 border-b border-chef-border shrink-0">
        <div className="flex items-center gap-2 mb-2">
          <span className="p-1.5 rounded-lg border text-emerald-400 border-emerald-500/50 bg-emerald-500/10"><Eye className="w-3.5 h-3.5" /></span>
          <span className="text-[10px] font-mono px-2 py-0.5 rounded border text-emerald-400 border-emerald-500/50 bg-emerald-500/10">output</span>
        </div>
        <div className="text-sm font-semibold text-chef-text">Final pipeline output</div>
      </div>
      <div className="flex-1 overflow-y-auto px-4 py-4 space-y-3 text-[11px] leading-relaxed">
        <div className="rounded-xl border border-chef-border bg-chef-card px-3 py-2.5 text-chef-muted">
          {hasSteps
            ? 'Selecting this node makes the preview panel render the full pipeline result and any final-step errors.'
            : 'Add at least one step to preview transformed output here.'}
        </div>
        <div className="text-chef-muted">
          Use this node when you want to validate the end-to-end pipeline output instead of inspecting one intermediate step.
        </div>
      </div>
    </div>
  )
}

/* ── Live preview helpers ──────────────────────────────────────────────────── */
type SampleSize = 10 | 25 | 50 | 100
type SourceRow   = Record<string, unknown>

function formatAge(fetchedAt: number): string {
  const s = Math.floor((Date.now() - fetchedAt) / 1000)
  return s < 60 ? `${s}s ago` : `${Math.floor(s / 60)}m ago`
}

/* ── Preview panel ─────────────────────────────────────────────────────────── */
function PreviewPanel({ preview, stepLabel, sampleSize, cacheInfo, onSampleSizeChange, onRefresh, onClose }: {
  preview: PreviewState
  stepLabel: string
  sampleSize: SampleSize
  cacheInfo: { size: number; fetchedAt: number } | null
  onSampleSizeChange: (n: SampleSize) => void
  onRefresh: () => void
  onClose: () => void
}) {
  return (
    <div className="flex flex-col h-full">
      {/* ── Header ── */}
      <div className="px-4 py-2 border-b border-chef-border flex items-center gap-2.5 shrink-0 flex-wrap">

        {/* Live indicator */}
        {preview.loading
          ? <Loader2 size={11} className="animate-spin text-indigo-400 shrink-0" />
          : (
            <span className="relative flex h-2 w-2 shrink-0">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-60" />
              <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
            </span>
          )
        }

        <span className="text-xs font-semibold text-chef-text">Live Preview</span>

        <span className="text-[11px] text-chef-muted">
          {stepLabel ? <>after <span className="text-chef-text/70">&quot;{stepLabel}&quot;</span></> : 'source data'}
        </span>

        {/* Row / col stats */}
        {!preview.loading && preview.rowCount > 0 && (
          <span className="text-[10px] text-chef-muted font-mono">
            {preview.rowCount} rows · {preview.columns.length} cols
            {preview.removed > 0 && (
              <span className="text-amber-400 ml-1.5">↓ {preview.removed} filtered</span>
            )}
          </span>
        )}

        {/* Sample size picker */}
        <div className="flex items-center gap-1 ml-auto">
          <span className="text-[10px] text-chef-muted mr-0.5">Sample:</span>
          {([10, 25, 50, 100] as const).map(n => (
            <button key={n}
              onMouseDown={e => { e.preventDefault(); onSampleSizeChange(n) }}
              className={`px-1.5 py-0.5 rounded text-[10px] font-mono border transition-colors ${
                sampleSize === n
                  ? 'bg-indigo-500/20 text-indigo-300 border-indigo-500/40'
                  : 'text-chef-muted border-chef-border/50 hover:text-chef-text hover:border-chef-border'
              }`}>
              {n}
            </button>
          ))}
        </div>

        {/* Cache info + refresh */}
        <button onClick={onRefresh}
          className="flex items-center gap-1.5 text-[10px] border border-chef-border/50 hover:border-indigo-500/40 hover:text-indigo-300 text-chef-muted px-2 py-1 rounded-lg transition-colors"
          title="Re-fetch source data (clears cache)">
          <RefreshCw size={9} className={preview.loading ? 'animate-spin' : ''} />
          {cacheInfo ? `${cacheInfo.size} cached · ${formatAge(cacheInfo.fetchedAt)}` : 'Fetch source'}
        </button>

        {/* Collapse */}
        <button onClick={onClose} className="p-1 text-chef-muted hover:text-chef-text transition-colors rounded" title="Collapse preview">
          <EyeOff size={12} />
        </button>
      </div>

      {/* ── Table ── */}
      <div className="flex-1 overflow-auto">
        {preview.loading ? (
          <div className="flex items-center justify-center h-full gap-2 text-xs text-chef-muted">
            <Loader2 size={14} className="animate-spin text-indigo-400" /> Running pipeline on {sampleSize} rows…
          </div>
        ) : preview.error ? (
          <div className="flex items-center justify-center h-full gap-2 text-xs text-rose-400">
            <AlertTriangle size={14} /> {preview.error}
          </div>
        ) : preview.columns.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full gap-2 text-xs text-chef-muted">
            <Database size={20} className="opacity-20" />
            Select a step or add one to see live data
          </div>
        ) : (
          <table className="w-full text-[10px] font-mono border-collapse">
            <thead className="sticky top-0 bg-chef-card z-10">
              <tr>
                {preview.columns.map(col => (
                  <th key={col} className="text-left px-3 py-1.5 text-chef-muted font-semibold border-b border-chef-border whitespace-nowrap">{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {preview.rows.map((row, ri) => (
                <tr key={ri} className={`border-b border-chef-border/40 transition-colors ${ri % 2 === 0 ? '' : 'bg-chef-bg/40'} hover:bg-indigo-500/5`}>
                  {row.map((cell, ci) => (
                    <td key={ci} className="px-3 py-1 text-chef-text-dim whitespace-nowrap max-w-[160px] overflow-hidden text-ellipsis">{cell}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

/* ── Toolbox ───────────────────────────────────────────────────────────────── */
function Toolbox({ onAdd }: { onAdd: (op: OpType) => void }) {
  return (
    <div className="flex flex-col h-full">
      <div className="px-4 py-3 border-b border-chef-border shrink-0">
        <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-widest">Operations</div>
        <div className="text-[10px] text-chef-muted mt-0.5">Click to add a step</div>
      </div>
      <div className="flex-1 overflow-y-auto p-3 space-y-2">
        {OP_META.map(meta => (
          <button key={meta.op} onClick={() => onAdd(meta.op)}
            className={`w-full text-left rounded-xl border p-3 transition-all hover:scale-[1.01] active:scale-[0.99] ${meta.color} ${meta.bg} hover:brightness-110`}>
            <div className={`flex items-center gap-2 mb-1 ${meta.iconColor}`}>
              <OpIcon op={meta.op} className="w-3.5 h-3.5" />
              <span className="text-xs font-semibold">{meta.label}</span>
            </div>
            <div className="text-[10px] text-chef-muted leading-tight">{meta.desc}</div>
          </button>
        ))}
      </div>
    </div>
  )
}

/* ── Main builder ──────────────────────────────────────────────────────────── */
function BuilderInner() {
  const router       = useRouter()
  const searchParams = useSearchParams()
  const editId       = searchParams.get('id')

  const [builder, setBuilder] = useState<BuilderState>({
    pipelineId: null, name: '', description: '', notes: '', sourceType: 'dataset', dataset: '', resource: '',
    status: 'draft', steps: [], selectedStepId: '__source__',
    previewOpen: true, saving: false, dirty: false,
  })
  const [datasets, setDatasets]         = useState<DatasetMeta[]>([])
  const [connectors, setConnectors]     = useState<ConnectorMeta[]>([])
  const [schemaFields, setSchemaFields]   = useState<SchemaField[]>([])
  const [preview, setPreview]           = useState<PreviewState>({ columns: [], rows: [], rowCount: 0, removed: 0, loading: false, error: null })
  const [saveError, setSaveError]       = useState<string | null>(null)
  const [sampleSize, setSampleSize]     = useState<SampleSize>(25)
  const [cacheInfo, setCacheInfo]       = useState<{ size: number; fetchedAt: number } | null>(null)
  const invalidatedCount                = builder.steps.filter(step => step.invalidated).length
  const sourceCacheRef                  = useRef<{ rows: SourceRow[]; dataset: string; size: SampleSize } | null>(null)
  const previewTimer                    = useRef<ReturnType<typeof setTimeout> | null>(null)
  const importFileRef                   = useRef<HTMLInputElement>(null)

  // ── Undo / redo history ────────────────────────────────────────────────────
  const MAX_HISTORY = 50
  type Snapshot = Pick<BuilderState, 'name' | 'description' | 'notes' | 'sourceType' | 'dataset' | 'resource' | 'status' | 'steps'>
  const historyRef    = useRef<Snapshot[]>([])
  const futureRef     = useRef<Snapshot[]>([])
  const builderRef    = useRef<BuilderState>(builder)
  const lastHistoryMs = useRef(0)
  const [historyLen, setHistoryLen] = useState(0)
  const [futureLen,  setFutureLen]  = useState(0)

  // Sync ref every render so undo/redo always capture the latest state
  builderRef.current = builder

  function snapshot(): Snapshot {
    const { name, description, notes, sourceType, dataset, resource, status, steps } = builderRef.current
    return { name, description, notes, sourceType, dataset, resource, status, steps }
  }

  /** Push current state onto the undo stack.
   *  immediate=true  → always push (structural changes like add/delete/reorder)
   *  immediate=false → throttled to once per 800 ms (text typing) */
  function pushHistory(immediate = true) {
    const now = Date.now()
    if (!immediate && now - lastHistoryMs.current < 800) return
    lastHistoryMs.current = now
    historyRef.current = [...historyRef.current.slice(-(MAX_HISTORY - 1)), snapshot()]
    futureRef.current  = []
    setHistoryLen(historyRef.current.length)
    setFutureLen(0)
  }

  const undo = useCallback(() => {
    if (!historyRef.current.length) return
    const past = [...historyRef.current]
    const prev = past.pop()!
    futureRef.current  = [snapshot(), ...futureRef.current]
    historyRef.current = past
    setBuilder(b => ({ ...b, ...prev, dirty: true }))
    setHistoryLen(past.length)
    setFutureLen(futureRef.current.length)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const redo = useCallback(() => {
    if (!futureRef.current.length) return
    const [next, ...rest] = futureRef.current
    futureRef.current  = rest
    historyRef.current = [...historyRef.current, snapshot()]
    setBuilder(b => ({ ...b, ...next, dirty: true }))
    setHistoryLen(historyRef.current.length)
    setFutureLen(rest.length)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const canUndo = historyLen > 0
  const canRedo  = futureLen  > 0

  // Load datasets
  useEffect(() => {
    fetch('/api/datasets')
      .then(r => r.json())
      .then((data: DatasetMeta[]) => {
        setDatasets(data)
        if (!builderRef.current.dataset && data[0]) {
          setBuilder(prev => ({ ...prev, dataset: data[0].id }))
        }
        // set initial schema from selected dataset
        const match = data.find(d => d.name === builder.dataset || d.id === builder.dataset)
        if (match?.schema) setSchemaFields(match.schema)
      })
      .catch(() => {})
    fetch('/api/connectors')
      .then(r => r.json())
      .then((data: ConnectorMeta[]) => {
        setConnectors(data.filter(connector =>
          ['http', 'postgresql', 'mysql', 'mongodb', 's3', 'sftp', 'bigquery', 'redis', 'appinsights', 'azuremonitor', 'elasticsearch', 'datadog', 'azureb2c', 'azureentraid'].includes(connector.type),
        ))
      })
      .catch(() => {})
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Update schema when dataset selection changes
  useEffect(() => {
    if (builder.sourceType !== 'dataset') {
      setSchemaFields([])
      return
    }
    const match = datasets.find(d => d.name === builder.dataset || d.id === builder.dataset)
    setSchemaFields(match?.schema ?? [])
  }, [builder.dataset, builder.sourceType, datasets])

  // Load pipeline for editing
  useEffect(() => {
    if (!editId) return
    fetch(`/api/pipelines/${editId}`)
      .then(r => r.json())
      .then((data: {
        id: string
        name: string
        description: string
        notes?: string
        dataset: string
        sourceType?: 'dataset' | 'connector'
        sourceId?: string
        resource?: string | null
        outputTarget?: { mode: 'none' | 'dataset'; datasetId?: string; datasetName?: string } | null
        status: 'active' | 'draft'
        uiSteps: { id: string; op: string; label: string }[]
        runtimeSteps?: RuntimeStepRecord[]
      }) => {
        const runtimeById = new Map((data.runtimeSteps ?? []).map(step => [step.id, step]))
        const nextSteps = data.uiSteps.map(s => {
          const runtime = runtimeById.get(s.id)
          const config = runtime ? { ...runtime.config } : defaultConfig(s.op as OpType)
          if (s.op === 'write' && data.outputTarget?.mode === 'dataset') {
            if (data.outputTarget.datasetId) {
              config.targetDatasetId = data.outputTarget.datasetId
              config.createDataset = false
            } else {
              config.createDataset = true
              config.newDatasetName = data.outputTarget.datasetName ?? ''
            }
          }
          return { id: s.id, op: s.op as OpType, label: s.label, config, invalidated: false }
        })
        setBuilder(prev => ({
          ...prev,
          pipelineId: data.id, name: data.name, description: data.description, notes: data.notes ?? '',
          sourceType: data.sourceType ?? 'dataset',
          dataset: data.sourceId ?? data.dataset,
          resource: data.resource ?? '',
          status: data.status,
          steps: nextSteps,
          selectedStepId: nextSteps[0]?.id ?? '__source__',
          dirty: false,
        }))
      })
      .catch(() => {})
  }, [editId])

  // Invalidate source cache when dataset or sample size changes
  useEffect(() => {
    sourceCacheRef.current = null
    setCacheInfo(null)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [builder.dataset, builder.sourceType, builder.resource, sampleSize])

  // Auto-preview: fires on step/config/dataset/sampleSize changes; debounced 300 ms
  useEffect(() => {
    if (!builder.previewOpen) return
    if (previewTimer.current) clearTimeout(previewTimer.current)
    previewTimer.current = setTimeout(doPreview, 300)
    return () => { if (previewTimer.current) clearTimeout(previewTimer.current) }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [builder.selectedStepId, builder.steps, builder.dataset, builder.sourceType, builder.resource, builder.previewOpen, sampleSize])

  // Keyboard undo / redo (Cmd+Z / Cmd+Shift+Z or Ctrl+Y)
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (!(e.metaKey || e.ctrlKey)) return
      if (e.key === 'z' && !e.shiftKey) { e.preventDefault(); undo() }
      if ((e.key === 'z' && e.shiftKey) || e.key === 'y') { e.preventDefault(); redo() }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [undo, redo])

  const doPreview = useCallback(async () => {
    if (!builderRef.current.previewOpen) return
    const b   = builderRef.current
    if (!b.dataset) {
      setPreview(p => ({ ...p, loading: false, error: `Select a ${b.sourceType} to preview the pipeline` }))
      return
    }
    const idx = b.selectedStepId === '__output__'
      ? b.steps.length - 1
      : b.steps.findIndex(s => s.id === b.selectedStepId)
    // -1 = no step selected → show raw source data (stepIndex -1 skips all transforms)
    const effectiveIdx = idx  // may be -1, which is fine

    setPreview(p => ({ ...p, loading: true, error: null }))

    // Check source cache
    const cache   = sourceCacheRef.current
    const cacheKey = `${b.sourceType}:${b.dataset}:${b.resource ?? ''}`
    const useCache = cache && cache.dataset === cacheKey && cache.size === sampleSize

    try {
      const res = await fetch('/api/pipelines/preview', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dataset:    b.dataset,
          sourceType: b.sourceType,
          sourceId:   b.dataset,
          resource:   b.resource,
          stepIndex:  effectiveIdx,
          steps:      b.steps.map(s => ({ op: s.op, config: s.config })),
          rowLimit:   sampleSize,
          cachedRows: useCache ? cache!.rows : undefined,
        }),
      })
      const data: {
        columns: string[]
        rows: string[][]
        rowCount: number
        removed: number
        sourceRows?: SourceRow[]
        error?: string
      } = await res.json()

      if (!res.ok) {
        throw new Error(data.error ?? `Preview failed (${res.status})`)
      }

      // Cache source rows returned by server (only on first/refresh fetch)
      if (data.sourceRows && data.sourceRows.length > 0) {
        sourceCacheRef.current = { rows: data.sourceRows, dataset: cacheKey, size: sampleSize }
        setCacheInfo({ size: data.sourceRows.length, fetchedAt: Date.now() })
      }

      setPreview({ columns: data.columns, rows: data.rows, rowCount: data.rowCount, removed: data.removed, loading: false, error: null })
    } catch (e) {
      setPreview(p => ({ ...p, loading: false, error: String(e) }))
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sampleSize])

  /** Force re-fetch from source, bypassing cache */
  function refreshSource() {
    sourceCacheRef.current = null
    setCacheInfo(null)
    doPreview()
  }

  function addStep(op: OpType) {
    pushHistory()  // snapshot before mutation
    const step: BuilderStep = { id: uid(), op, label: defaultLabel(op), config: defaultConfig(op) }
    setBuilder(prev => {
      let insertAt = prev.steps.length
      if (prev.selectedStepId === '__source__') insertAt = 0
      else if (prev.selectedStepId === '__output__' || !prev.selectedStepId) insertAt = prev.steps.length
      else {
        const currentIndex = prev.steps.findIndex(existing => existing.id === prev.selectedStepId)
        insertAt = currentIndex >= 0 ? currentIndex + 1 : prev.steps.length
      }
      const steps = [...prev.steps]
      steps.splice(insertAt, 0, step)
      return { ...prev, steps, selectedStepId: step.id, dirty: true }
    })
  }

  function reorderStep(from: number, dir: 'left' | 'right') {
    pushHistory()
    const to = dir === 'left' ? from - 1 : from + 1
    setBuilder(prev => {
      const steps = [...prev.steps];
      [steps[from], steps[to]] = [steps[to], steps[from]]
      return { ...prev, steps, dirty: true }
    })
  }

  function deleteStep(id: string) {
    pushHistory()
    setBuilder(prev => ({
      ...prev,
      steps: prev.steps.filter(s => s.id !== id),
      selectedStepId: prev.selectedStepId === id ? '__output__' : prev.selectedStepId,
      dirty: true,
    }))
  }

  function updateStepLabel(id: string, label: string) {
    pushHistory(false)  // debounced — groups rapid keystrokes into one undo point
    setBuilder(prev => ({ ...prev, steps: prev.steps.map(s => s.id === id ? { ...s, label } : s), dirty: true }))
  }

  function updateStepConfig(id: string, patch: Partial<BuilderStepConfig>) {
    pushHistory(false)
    setBuilder(prev => ({
      ...prev,
      steps: prev.steps.map(s => s.id === id
        ? { ...s, config: { ...s.config, ...patch }, invalidated: false, invalidationReason: undefined }
        : s),
      dirty: true,
    }))
  }

  function updateName(name: string) {
    pushHistory(false)
    setBuilder(prev => ({ ...prev, name, dirty: true }))
  }

  function updateDescription(description: string) {
    pushHistory(false)
    setBuilder(prev => ({ ...prev, description, dirty: true }))
  }

  function updateNotes(notes: string) {
    pushHistory(false)
    setBuilder(prev => ({ ...prev, notes, dirty: true }))
  }

  function updateDataset(dataset: string) {
    pushHistory()
    setBuilder(prev => {
      if (prev.dataset === dataset) return prev
      return {
        ...prev,
        dataset,
        selectedStepId: '__source__',
        steps: invalidateStepsForDatasetChange(prev.steps, prev.dataset, dataset),
        dirty: true,
      }
    })
  }

  function updateSourceType(sourceType: 'dataset' | 'connector') {
    pushHistory()
    setBuilder(prev => ({
      ...prev,
      sourceType,
      dataset: sourceType === 'dataset' ? (datasets[0]?.id ?? '') : (connectors[0]?.id ?? ''),
      resource: '',
      selectedStepId: '__source__',
      steps: invalidateStepsForDatasetChange(prev.steps, prev.dataset, sourceType === 'dataset' ? (datasets[0]?.id ?? '') : (connectors[0]?.id ?? '')),
      dirty: true,
    }))
  }

  function updateResource(resource: string) {
    pushHistory(false)
    setBuilder(prev => ({ ...prev, resource, dirty: true }))
  }

  function updateStatus() {
    pushHistory()
    setBuilder(prev => ({ ...prev, status: prev.status === 'active' ? 'draft' : 'active', dirty: true }))
  }

  function toPortableTemplate(): PipelineTemplate {
    const sourceName = builder.sourceType === 'dataset'
      ? (datasets.find(dataset => dataset.id === builder.dataset)?.name ?? builder.dataset)
      : (connectors.find(connector => connector.id === builder.dataset)?.name ?? builder.dataset)

    return {
      version: 1,
      name: builder.name || 'Untitled Pipeline',
      description: builder.description,
      notes: builder.notes,
      status: builder.status,
      source: {
        sourceType: builder.sourceType,
        sourceName,
        resource: builder.resource,
      },
      outputTarget: (() => {
        const writeStep = [...builder.steps].reverse().find(step => step.op === 'write')
        if (!writeStep) return null
        if (writeStep.config.createDataset) {
          return {
            mode: 'dataset' as const,
            datasetName: writeStep.config.newDatasetName?.trim() || `${builder.name || 'Pipeline'} output`,
            refreshMode: 'manual' as const,
          }
        }
        if (writeStep.config.targetDatasetId) {
          const dataset = datasets.find(entry => entry.id === writeStep.config.targetDatasetId)
          return {
            mode: 'dataset' as const,
            datasetName: dataset?.name ?? '',
            refreshMode: 'manual' as const,
          }
        }
        return { mode: 'none' as const }
      })(),
      steps: builder.steps.map(step => ({ id: step.id, op: step.op, label: step.label, config: step.config })),
    }
  }

  function applyImportedTemplate(template: PipelineTemplate) {
    const sourceType = template.source?.sourceType === 'connector' ? 'connector' : 'dataset'
    const matchedSource = sourceType === 'dataset'
      ? datasets.find(dataset => dataset.name === template.source.sourceName || dataset.id === template.source.sourceName)
      : connectors.find(connector => connector.name === template.source.sourceName || connector.id === template.source.sourceName)
    const nextSteps = template.steps.map(step => ({
      id: uid(),
      op: step.op as OpType,
      label: step.label,
      config: { ...defaultConfig(step.op as OpType), ...step.config },
      invalidated: false,
    }))

    if (!matchedSource) {
      setSaveError(`Imported pipeline source "${template.source.sourceName}" was not found locally. Source binding was cleared.`)
    } else {
      setSaveError(null)
    }

    setBuilder(prev => ({
      ...prev,
      pipelineId: null,
      name: template.name,
      description: template.description,
      notes: template.notes ?? '',
      sourceType,
      dataset: matchedSource?.id ?? '',
      resource: template.source.resource ?? '',
      status: template.status ?? 'draft',
      steps: nextSteps,
      selectedStepId: nextSteps[0]?.id ?? '__source__',
      dirty: true,
    }))
  }

  async function handleCopyJson() {
    await navigator.clipboard.writeText(JSON.stringify(toPortableTemplate(), null, 2))
    setSaveError(null)
  }

  function handleExportJson() {
    const blob = new Blob([JSON.stringify(toPortableTemplate(), null, 2)], { type: 'application/json' })
    const anchor = document.createElement('a')
    anchor.href = URL.createObjectURL(blob)
    anchor.download = `${(builder.name || 'pipeline').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || 'pipeline'}.json`
    anchor.click()
    URL.revokeObjectURL(anchor.href)
  }

  function handleImportJsonFile(file: File) {
    file.text()
      .then(raw => JSON.parse(raw) as PipelineTemplate)
      .then(applyImportedTemplate)
      .catch(error => setSaveError(`Import failed: ${error instanceof Error ? error.message : String(error)}`))
  }

  async function handleSave() {
    setSaveError(null)
    setBuilder(prev => ({ ...prev, saving: true }))
    try {
      const writeStep = [...builder.steps].reverse().find(step => step.op === 'write')
      const outputTarget = writeStep?.config.createDataset
        ? {
            mode: 'dataset' as const,
            datasetName: writeStep.config.newDatasetName?.trim() || `${builder.name || 'Pipeline'} output`,
            refreshMode: 'manual' as const,
          }
        : writeStep?.config.targetDatasetId
        ? {
            mode: 'dataset' as const,
            datasetId: writeStep.config.targetDatasetId,
            refreshMode: 'manual' as const,
          }
        : { mode: 'none' as const }

      const res = await fetch('/api/pipelines', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: builder.pipelineId,
          name: builder.name || 'Untitled Pipeline',
          description: builder.description,
          notes: builder.notes,
          dataset: builder.dataset,
          sourceType: builder.sourceType,
          sourceId: builder.dataset,
          resource: builder.resource,
          outputTarget,
          status: builder.status,
          steps: builder.steps,
        }),
      })
      if (!res.ok) { const e = await res.json(); throw new Error(e.error ?? 'Save failed') }
      const created = await res.json()
      setBuilder(prev => ({
        ...prev,
        pipelineId: created.id,
        saving: false,
        dirty: false,
      }))
      if (!editId || editId !== created.id) {
        router.replace(`/pipelines/builder?id=${created.id}`)
      }
    } catch (e) {
      setSaveError(String(e))
      setBuilder(prev => ({ ...prev, saving: false }))
    }
  }

  const selectedStep = builder.steps.find(s => s.id === builder.selectedStepId) ?? null
  const selectedSourceName = builder.sourceType === 'dataset'
    ? (datasets.find(dataset => dataset.id === builder.dataset)?.name ?? builder.dataset)
    : (connectors.find(connector => connector.id === builder.dataset)?.name ?? builder.dataset)
  const previewStepLabel = builder.selectedStepId === '__output__'
    ? 'final pipeline output'
    : builder.selectedStepId === '__source__' || !selectedStep
    ? 'source data'
    : selectedStep.label

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-5 py-3 border-b border-chef-border flex items-center gap-3 shrink-0 bg-chef-surface">
        <button onClick={() => router.push('/pipelines')}
          className="flex items-center gap-1.5 text-xs text-chef-muted hover:text-chef-text transition-colors mr-1">
          <ArrowLeft size={14} /> Pipelines
        </button>
        <div className="w-px h-5 bg-chef-border" />

        {/* Undo / Redo */}
        <div className="flex items-center gap-0.5">
          <button
            onClick={undo} disabled={!canUndo}
            title={`Undo (⌘Z)${historyLen ? ` · ${historyLen} step${historyLen !== 1 ? 's' : ''}` : ''}`}
            className="w-7 h-7 flex items-center justify-center rounded-lg text-chef-muted transition-colors
              disabled:opacity-25 disabled:cursor-not-allowed
              enabled:hover:text-chef-text enabled:hover:bg-chef-card enabled:hover:border-chef-border
              border border-transparent">
            <Undo2 size={13} />
          </button>
          <button
            onClick={redo} disabled={!canRedo}
            title={`Redo (⌘⇧Z)${futureLen ? ` · ${futureLen} step${futureLen !== 1 ? 's' : ''}` : ''}`}
            className="w-7 h-7 flex items-center justify-center rounded-lg text-chef-muted transition-colors
              disabled:opacity-25 disabled:cursor-not-allowed
              enabled:hover:text-chef-text enabled:hover:bg-chef-card enabled:hover:border-chef-border
              border border-transparent">
            <Redo2 size={13} />
          </button>
        </div>
        <div className="w-px h-5 bg-chef-border" />

        <input
          className="flex-1 min-w-0 bg-transparent text-sm font-semibold text-chef-text placeholder:text-chef-muted/60 focus:outline-none"
          value={builder.name} onChange={e => updateName(e.target.value)}
          placeholder="Pipeline name…" />

        <div className="flex items-center gap-1">
          <button
            onClick={() => void handleCopyJson()}
            className="px-2.5 py-1.5 rounded-lg text-[11px] border border-chef-border bg-chef-card text-chef-muted hover:text-chef-text hover:border-indigo-500/50 transition-colors"
          >
            Copy JSON
          </button>
          <button
            onClick={handleExportJson}
            className="px-2.5 py-1.5 rounded-lg text-[11px] border border-chef-border bg-chef-card text-chef-muted hover:text-chef-text hover:border-indigo-500/50 transition-colors"
          >
            Export JSON
          </button>
          <button
            onClick={() => importFileRef.current?.click()}
            className="px-2.5 py-1.5 rounded-lg text-[11px] border border-chef-border bg-chef-card text-chef-muted hover:text-chef-text hover:border-indigo-500/50 transition-colors"
          >
            Import JSON
          </button>
          <input
            ref={importFileRef}
            type="file"
            accept="application/json,.json"
            className="hidden"
            onChange={e => {
              const file = e.target.files?.[0]
              if (file) handleImportJsonFile(file)
              e.currentTarget.value = ''
            }}
          />
        </div>

        {/* Source picker */}
        <div className="flex items-center gap-1.5">
          <span className="text-[10px] text-chef-muted">Source:</span>
          <select
            className="bg-chef-card border border-chef-border rounded-lg px-2 py-1.5 text-xs text-chef-text focus:outline-none focus:border-indigo-500/60 transition-colors"
            value={builder.sourceType}
            onChange={e => updateSourceType(e.target.value as 'dataset' | 'connector')}>
            <option value="dataset">Dataset</option>
            <option value="connector">Connector</option>
          </select>
          <select
            className="bg-chef-card border border-chef-border rounded-lg px-2.5 py-1.5 text-xs text-chef-text focus:outline-none focus:border-indigo-500/60 transition-colors"
            value={builder.dataset}
            onChange={e => updateDataset(e.target.value)}>
            {builder.sourceType === 'dataset'
              ? datasets.map(d => <option key={d.id} value={d.id}>{d.name}</option>)
              : connectors.map(connector => <option key={connector.id} value={connector.id}>{connector.name}</option>)}
            {builder.sourceType === 'dataset' && datasets.length === 0 && <option value={builder.dataset}>{builder.dataset}</option>}
            {builder.sourceType === 'connector' && connectors.length === 0 && <option value={builder.dataset}>{builder.dataset}</option>}
          </select>
          {builder.sourceType === 'connector' && (
            <input
              className="bg-chef-card border border-chef-border rounded-lg px-2.5 py-1.5 text-xs text-chef-text placeholder:text-chef-muted/60 focus:outline-none focus:border-indigo-500/60 transition-colors w-40"
              value={builder.resource ?? ''}
              onChange={e => updateResource(e.target.value)}
              placeholder="resource / table / query"
            />
          )}
        </div>

        <button onClick={updateStatus}
          className={`px-2.5 py-1 rounded-full text-[10px] font-semibold border transition-colors ${
            builder.status === 'active' ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-400' : 'border-chef-border bg-chef-bg text-chef-muted'
          }`}>
          {builder.status === 'active' ? '● Active' : '● Draft'}
        </button>

        {saveError && <span className="text-[10px] text-rose-400 flex items-center gap-1"><AlertTriangle size={10} /> {saveError}</span>}
        {builder.dirty && !saveError && <span className="text-[10px] text-chef-muted">Unsaved changes</span>}
        {invalidatedCount > 0 && (
          <span className="text-[10px] text-amber-300 flex items-center gap-1">
            <AlertTriangle size={10} /> {invalidatedCount} step{invalidatedCount !== 1 ? 's' : ''} need review
          </span>
        )}

        <button onClick={handleSave} disabled={builder.saving || builder.steps.length === 0}
          className={`flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-xs font-semibold transition-colors ${
            builder.saving || builder.steps.length === 0
              ? 'bg-chef-card border border-chef-border text-chef-muted cursor-not-allowed'
              : 'bg-indigo-600 hover:bg-indigo-500 text-white border border-transparent'
          }`}>
          {builder.saving ? <><Loader2 size={12} className="animate-spin" /> Saving…</> : <><Save size={12} /> {builder.pipelineId ? 'Save changes' : 'Create pipeline'}</>}
        </button>
      </div>

      {/* 3-panel body */}
      <div className="flex flex-1 min-h-0 overflow-hidden">

        {/* Toolbox */}
        <div className="w-48 shrink-0 border-r border-chef-border bg-chef-bg">
          <Toolbox onAdd={addStep} />
        </div>

        {/* Canvas */}
        <div className="flex-1 overflow-auto bg-chef-surface"
          style={{ backgroundImage: 'radial-gradient(circle, rgb(var(--chef-border)) 1px, transparent 1px)', backgroundSize: '24px 24px' }}>
          <div className="p-8 min-h-full">
            <div className="mb-5">
              <input
                className="w-full max-w-lg bg-transparent text-xs text-chef-muted placeholder:text-chef-muted/40 focus:outline-none border-b border-transparent focus:border-chef-border pb-0.5 transition-colors"
                value={builder.description}
                onChange={e => updateDescription(e.target.value)}
                placeholder="Add a description…" />
            </div>

            <div className="mb-6">
              <textarea
                className="w-full max-w-3xl bg-chef-card border border-chef-border rounded-xl px-3 py-2.5 text-[11px] text-chef-text placeholder:text-chef-muted/50 focus:outline-none focus:border-indigo-500/50 min-h-24"
                value={builder.notes}
                onChange={e => updateNotes(e.target.value)}
                placeholder="Pipeline notes: explain intent, caveats, data assumptions, and how to read the output…" />
            </div>

            {builder.steps.length > 0 && (
              <div className="flex items-center gap-3 mb-5 text-[10px] text-chef-muted">
                <Zap size={10} className="text-indigo-400" />
                <span>{builder.steps.length} step{builder.steps.length !== 1 ? 's' : ''}</span>
                <span className="flex items-center gap-2">
                  {builder.steps.map((s, i) => (
                    <span key={s.id} className="flex items-center gap-1">
                      {i > 0 && <ChevronRight size={8} className="text-chef-border" />}
                      <span className={opAccent(s.op).split(' ')[0]}>{s.op}</span>
                    </span>
                  ))}
                </span>
              </div>
            )}

            <BuilderCanvas
              sourceType={builder.sourceType}
              sourceLabel={selectedSourceName}
              steps={builder.steps} selectedId={builder.selectedStepId}
              onSelect={id => setBuilder(prev => ({ ...prev, selectedStepId: id }))}
              onReorder={reorderStep} onDelete={deleteStep} />

            {builder.steps.length > 0 && (
              <div className="mt-10 text-[10px] text-chef-muted/60 flex items-center gap-1.5">
                <Plus size={10} /> Add more operations from the left panel
              </div>
            )}
          </div>
        </div>

        {/* Config panel */}
        <div className="w-72 shrink-0 border-l border-chef-border bg-chef-bg">
          {builder.selectedStepId === '__source__' ? (
            <SourcePanel builder={builder} sourceName={selectedSourceName} />
          ) : builder.selectedStepId === '__output__' ? (
            <OutputPanel hasSteps={builder.steps.length > 0} />
          ) : selectedStep ? (
            <ConfigPanel
              step={selectedStep}
              schemaFields={schemaFields}
              datasets={datasets}
              onLabelChange={label => updateStepLabel(selectedStep.id, label)}
              onConfigChange={patch => updateStepConfig(selectedStep.id, patch)} />
          ) : (
            <div className="flex flex-col items-center justify-center h-full text-center px-6">
              <Circle size={28} className="text-chef-muted opacity-30 mb-3" />
              <div className="text-xs font-semibold text-chef-text mb-1">No step selected</div>
              <div className="text-[11px] text-chef-muted leading-relaxed">
                Click a node in the canvas to configure it, or add a step from the left panel
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Preview panel */}
      {builder.previewOpen && (
        <div className="h-64 shrink-0 border-t border-chef-border bg-chef-bg">
          <PreviewPanel
            preview={preview}
            stepLabel={previewStepLabel}
            sampleSize={sampleSize}
            cacheInfo={cacheInfo}
            onSampleSizeChange={n => setSampleSize(n)}
            onRefresh={refreshSource}
            onClose={() => setBuilder(prev => ({ ...prev, previewOpen: false }))} />
        </div>
      )}

      {/* Saved indicator */}
      {!builder.dirty && builder.steps.length > 0 && !builder.saving && (
        <div className="absolute bottom-4 right-6 flex items-center gap-1.5 text-[10px] text-emerald-400 bg-emerald-500/10 border border-emerald-500/30 px-2.5 py-1.5 rounded-full pointer-events-none">
          <CheckCircle2 size={10} /> Saved
        </div>
      )}
    </div>
  )
}

export default function BuilderPage() {
  return (
    <Suspense fallback={
      <div className="flex items-center justify-center h-full gap-2 text-xs text-chef-muted">
        <Loader2 size={14} className="animate-spin text-indigo-400" /> Loading builder…
      </div>
    }>
      <BuilderInner />
    </Suspense>
  )
}
