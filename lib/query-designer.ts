import type { RecipeSourceBinding, RecipeVariableDefinition, RecipeVariableType, QueryRecipe } from '@/lib/query-recipes'

export type RecipeControlKind = 'text' | 'number' | 'boolean' | 'enum' | 'date' | 'datetime' | 'timeWindow'
export type RecipeWidgetWidth = 'full' | 'half' | 'third'
export type RecipeAccent = 'indigo' | 'cyan' | 'emerald' | 'amber'

export interface InferredVariable extends RecipeVariableDefinition {
  origin: 'inferred' | 'customized' | 'builtin'
  control: RecipeControlKind
  placeholderRefs: string[]
  stale?: boolean
}

export interface RecipeWidget {
  id: string
  variableName: string
  kind: RecipeControlKind
  width: RecipeWidgetWidth
  hidden?: boolean
  stale?: boolean
}

export interface RecipeRow {
  id: string
  widgetIds: string[]
}

export interface RecipeSection {
  id: string
  title: string
  rows: RecipeRow[]
}

export interface RecipeLayout {
  title?: string
  subtitle?: string
  accent?: RecipeAccent
  sections: RecipeSection[]
  widgets: RecipeWidget[]
  unplacedWidgetIds: string[]
}

export interface VariableInferenceResult {
  variables: InferredVariable[]
  layout: RecipeLayout
}

const BUILTIN_TIME_VARS = ['startTime', 'endTime', 'timespanIso', 'bucketHint'] as const
const BUILTIN_TIME_WIDGET = '__timeWindow__'

function uid(prefix: string) {
  return `${prefix}_${Math.random().toString(36).slice(2, 10)}`
}

function titleFromName(name: string) {
  return name
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase())
}

export function extractTemplateVariables(text: string): string[] {
  const matches = text.match(/\{\{\s*[A-Za-z_][A-Za-z0-9_]*\s*\}\}/g) ?? []
  return [...new Set(matches.map(match => match.replace(/[{}]/g, '').trim()))]
}

function inferControlKind(name: string, queryContext: string): { type: RecipeVariableType; control: RecipeControlKind; defaultValue?: string | number | boolean } {
  const lower = name.toLowerCase()
  const context = queryContext.toLowerCase()

  if (BUILTIN_TIME_VARS.includes(name as typeof BUILTIN_TIME_VARS[number])) {
    return { type: 'timeWindow', control: 'timeWindow' }
  }
  if (/^(is|has)[a-z_]/.test(lower) || /(enabled|disabled|active|inactive|flag)$/.test(lower)) {
    return { type: 'boolean', control: 'boolean', defaultValue: true }
  }
  if (/(date|day)$/.test(lower) || ['fromdate', 'todate'].includes(lower)) {
    return { type: 'date', control: 'date' }
  }
  if (/(time|timestamp|datetime|start|end|since|until)$/.test(lower)) {
    return { type: 'datetime', control: 'datetime' }
  }
  if (/(limit|count|min|max|status|size|age|qty|quantity|number|score)/.test(lower) || new RegExp(`\\b(limit|>=|<=|>|<|=)\\s+\\{\\{\\s*${name}\\s*\\}\\}`, 'i').test(context)) {
    return { type: 'number', control: 'number', defaultValue: /(limit|count)/.test(lower) ? 100 : undefined }
  }
  if (/(type|kind|mode|status|level|env|region|country)/.test(lower)) {
    return { type: 'enum', control: 'enum' }
  }
  return { type: 'string', control: 'text' }
}

function defaultLayoutFromVariables(variables: InferredVariable[], legacy?: QueryRecipe['cardLayout'] | RecipeLayout | null): RecipeLayout {
  const widgets: RecipeWidget[] = []
  const unplacedWidgetIds: string[] = []
  const rows: RecipeRow[] = []

  const hasBuiltins = variables.some(variable => BUILTIN_TIME_VARS.includes(variable.name as typeof BUILTIN_TIME_VARS[number]))
  if (hasBuiltins) {
    widgets.push({ id: uid('widget'), variableName: BUILTIN_TIME_WIDGET, kind: 'timeWindow', width: 'full' })
    rows.push({ id: uid('row'), widgetIds: [widgets[widgets.length - 1].id] })
  }

  for (const variable of variables) {
    if (BUILTIN_TIME_VARS.includes(variable.name as typeof BUILTIN_TIME_VARS[number])) continue
    const widget: RecipeWidget = {
      id: uid('widget'),
      variableName: variable.name,
      kind: variable.control,
      width: variable.control === 'boolean' ? 'third' : variable.control === 'number' ? 'half' : 'full',
    }
    widgets.push(widget)
    unplacedWidgetIds.push(widget.id)
  }

  const section: RecipeSection = {
    id: uid('section'),
    title: 'Inputs',
    rows,
  }

  return {
    title: legacy && 'title' in legacy ? legacy.title : undefined,
    subtitle: legacy && 'subtitle' in legacy ? legacy.subtitle : undefined,
    accent: legacy && 'accent' in legacy ? legacy.accent : 'indigo',
    sections: [section],
    widgets,
    unplacedWidgetIds,
  }
}

function cloneLayout(layout: RecipeLayout): RecipeLayout {
  return {
    ...layout,
    sections: layout.sections.map(section => ({
      ...section,
      rows: section.rows.map(row => ({ ...row, widgetIds: [...row.widgetIds] })),
    })),
    widgets: layout.widgets.map(widget => ({ ...widget })),
    unplacedWidgetIds: [...layout.unplacedWidgetIds],
  }
}

function ensureTimeWindowWidget(layout: RecipeLayout, variables: InferredVariable[]): RecipeLayout {
  const next = cloneLayout(layout)
  const builtinsPresent = variables.some(variable => BUILTIN_TIME_VARS.includes(variable.name as typeof BUILTIN_TIME_VARS[number]))
  const existing = next.widgets.find(widget => widget.variableName === BUILTIN_TIME_WIDGET)
  if (builtinsPresent && !existing) {
    const widget: RecipeWidget = { id: uid('widget'), variableName: BUILTIN_TIME_WIDGET, kind: 'timeWindow', width: 'full' }
    next.widgets.unshift(widget)
    if (!next.sections.length) next.sections.push({ id: uid('section'), title: 'Inputs', rows: [] })
    next.sections[0].rows.unshift({ id: uid('row'), widgetIds: [widget.id] })
  }
  if (!builtinsPresent && existing) {
    next.widgets = next.widgets.filter(widget => widget.id !== existing.id)
    next.unplacedWidgetIds = next.unplacedWidgetIds.filter(id => id !== existing.id)
    next.sections = next.sections.map(section => ({
      ...section,
      rows: section.rows.map(row => ({ ...row, widgetIds: row.widgetIds.filter(id => id !== existing.id) })).filter(row => row.widgetIds.length > 0),
    }))
  }
  return next
}

export function compactLayout(layout: RecipeLayout): RecipeLayout {
  const next = cloneLayout(layout)
  next.sections = next.sections.map(section => ({
    ...section,
    rows: section.rows.filter(row => row.widgetIds.length > 0),
  })).filter(section => section.rows.length > 0 || section.title || next.unplacedWidgetIds.length > 0)
  const placed = new Set(next.sections.flatMap(section => section.rows.flatMap(row => row.widgetIds)))
  next.unplacedWidgetIds = next.unplacedWidgetIds.filter(id => !placed.has(id))
  return next
}

export function inferVariablesFromRecipe(queryText: string, sources: RecipeSourceBinding[], previous?: QueryRecipe | null): VariableInferenceResult {
  const placeholders = new Map<string, string[]>()
  for (const match of extractTemplateVariables(queryText)) placeholders.set(match, ['query'])
  for (const source of sources) {
    for (const field of [source.resource, source.queryHint]) {
      for (const match of extractTemplateVariables(field ?? '')) {
        const refs = placeholders.get(match) ?? []
        refs.push(source.alias)
        placeholders.set(match, refs)
      }
    }
  }

  const previousVarMap = new Map((previous?.variables ?? []).map(variable => [variable.name, variable]))
  const variables: InferredVariable[] = Array.from(placeholders.entries()).map(([name, refs]) => {
    const existing = previousVarMap.get(name)
    const inferred = inferControlKind(name, queryText)
    return {
      name,
      type: existing?.type ?? inferred.type,
      label: existing?.label ?? titleFromName(name),
      description: existing?.description,
      required: existing?.required ?? true,
      defaultValue: existing?.defaultValue ?? inferred.defaultValue,
      options: existing?.options,
      validation: existing?.validation,
      origin: existing ? 'customized' : (BUILTIN_TIME_VARS.includes(name as typeof BUILTIN_TIME_VARS[number]) ? 'builtin' : 'inferred'),
      control: existing?.control ?? inferred.control,
      placeholderRefs: refs,
      stale: false,
    }
  })

  const priorVariables = previous?.variables ?? []
  for (const existing of priorVariables) {
    if (variables.some(variable => variable.name === existing.name)) continue
    variables.push({
      ...existing,
      origin: 'customized',
      control: (existing.control ?? inferControlKind(existing.name, queryText).control),
      placeholderRefs: [],
      stale: true,
    } as InferredVariable)
  }

  const layout = mergeInferenceWithExistingLayout(previous ?? null, { variables, layout: defaultLayoutFromVariables(variables, previous?.cardLayout ?? null) })
  return { variables, layout }
}

export function mergeInferenceWithExistingLayout(previous: QueryRecipe | null, nextInference: VariableInferenceResult): RecipeLayout {
  const existingLayout = previous?.cardLayout && 'sections' in previous.cardLayout
    ? cloneLayout(previous.cardLayout as RecipeLayout)
    : defaultLayoutFromVariables(nextInference.variables, previous?.cardLayout ?? null)
  const next = ensureTimeWindowWidget(existingLayout, nextInference.variables)
  const existingWidgetsByVariable = new Map(next.widgets.map(widget => [widget.variableName, widget]))
  const allowedNames = new Set(nextInference.variables.filter(variable => !BUILTIN_TIME_VARS.includes(variable.name as typeof BUILTIN_TIME_VARS[number])).map(variable => variable.name))
  const allWidgets: RecipeWidget[] = []

  const timeWidget = next.widgets.find(widget => widget.variableName === BUILTIN_TIME_WIDGET)
  if (timeWidget) allWidgets.push({ ...timeWidget })

  for (const variable of nextInference.variables) {
    if (BUILTIN_TIME_VARS.includes(variable.name as typeof BUILTIN_TIME_VARS[number])) continue
    const existing = existingWidgetsByVariable.get(variable.name)
    allWidgets.push(existing
      ? { ...existing, kind: variable.control, stale: variable.stale }
      : { id: uid('widget'), variableName: variable.name, kind: variable.control, width: variable.control === 'boolean' ? 'third' : variable.control === 'number' ? 'half' : 'full', stale: variable.stale })
  }

  for (const widget of next.widgets) {
    if (widget.variableName === BUILTIN_TIME_WIDGET) continue
    if (!allowedNames.has(widget.variableName)) {
      allWidgets.push({ ...widget, stale: true })
    }
  }

  const validWidgetIds = new Set(allWidgets.map(widget => widget.id))
  const sections = next.sections.map(section => ({
    ...section,
    rows: section.rows.map(row => ({
      ...row,
      widgetIds: row.widgetIds.filter(id => validWidgetIds.has(id)),
    })).filter(row => row.widgetIds.length > 0),
  }))
  const placed = new Set(sections.flatMap(section => section.rows.flatMap(row => row.widgetIds)))
  const unplacedWidgetIds = [
    ...next.unplacedWidgetIds.filter(id => validWidgetIds.has(id) && !placed.has(id)),
    ...allWidgets.filter(widget => !placed.has(widget.id) && !next.unplacedWidgetIds.includes(widget.id)).map(widget => widget.id),
  ]

  return compactLayout({
    title: next.title,
    subtitle: next.subtitle,
    accent: next.accent,
    sections: sections.length ? sections : [{ id: uid('section'), title: 'Inputs', rows: [] }],
    widgets: allWidgets,
    unplacedWidgetIds,
  })
}
