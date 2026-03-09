import { readJsonFile, writeJsonFile } from '@/lib/json-store'
import type { SourceType } from '@/lib/datasets'
import type { Lang } from '@/lib/query-types'
import type { TimeWindowPreset } from '@/lib/query-time'
import type { InferredVariable, RecipeAccent, RecipeControlKind, RecipeLayout } from '@/lib/query-designer'
import { inferVariablesFromRecipe } from '@/lib/query-designer'
import { invalidateSearchIndex } from '@/lib/search-cache'

export type RecipeVariableType = 'string' | 'number' | 'boolean' | 'date' | 'datetime' | 'enum' | 'timeWindow'

export interface RecipeSourceBinding {
  alias: string
  sourceType: SourceType
  sourceId: string
  resource?: string
  queryHint?: string
  rowLimit?: number
}

export interface RecipeVariableDefinition {
  name: string
  type: RecipeVariableType
  label: string
  description?: string
  required?: boolean
  defaultValue?: string | number | boolean
  options?: string[]
  validation?: {
    min?: number
    max?: number
    pattern?: string
  }
  origin?: 'inferred' | 'customized' | 'builtin'
  control?: RecipeControlKind
  placeholderRefs?: string[]
  stale?: boolean
}

export interface QueryRecipe {
  id: string
  name: string
  description: string
  lang: Lang
  queryText: string
  sources: RecipeSourceBinding[]
  variables: RecipeVariableDefinition[]
  timeWindowBinding?: {
    enabled: boolean
    defaultPreset: TimeWindowPreset
  } | null
  cardLayout?: RecipeLayout | {
    title?: string
    subtitle?: string
    accent?: RecipeAccent
  } | null
  createdAt: number
  updatedAt: number
}

interface RecipeStateFile {
  recipes: QueryRecipe[]
}

const RECIPE_STORE_FILE = 'query-recipes.json'
const NOW = Date.now()

function seedRecipes(): QueryRecipe[] {
  return [
    {
      id: 'recipe_b2c_identity_mailosaur',
      name: 'B2C Synthetic Identity Review',
      description: 'Filter B2C identities down to Mailosaur and synthetic sign-ins within a relative time window.',
      lang: 'sql',
      queryText: `SELECT users.id, users.displayName, users.userPrincipalName, users.createdDateTime, users.identities\nFROM users\nWHERE users.createdDateTime >= {{startTime}}\n  AND users.accountEnabled = {{accountEnabled}}\n  AND users.userPrincipalName LIKE {{mailosaurLike}}\nORDER BY users.createdDateTime DESC\nLIMIT {{limitRows}}`,
      sources: [
        { alias: 'users', sourceType: 'dataset', sourceId: 'demo_b2c_users', rowLimit: 500 },
      ],
      variables: [
        { name: 'accountEnabled', type: 'boolean', label: 'Account enabled', description: 'Only include enabled accounts', required: true, defaultValue: true },
        { name: 'mailosaurLike', type: 'string', label: 'Identity contains', description: 'Substring match for synthetic identities', required: true, defaultValue: '%mailosaur%' },
        { name: 'limitRows', type: 'number', label: 'Max rows', required: true, defaultValue: 100, validation: { min: 1, max: 500 } },
      ],
      timeWindowBinding: { enabled: true, defaultPreset: 'last_30d' },
      cardLayout: { title: 'Synthetic Identity Review', subtitle: 'Friendly recipe for B2C synthetic sign-in review', accent: 'cyan' },
      createdAt: NOW,
      updatedAt: NOW,
    },
    {
      id: 'recipe_nginx_checkout_triage',
      name: 'NGINX Checkout Failure Triage',
      description: 'Review failing or slow checkout and API requests in the chosen time window.',
      lang: 'sql',
      queryText: `SELECT logs.ts, logs.path, logs.status, logs.latencyMs, logs.country, logs.device, logs.orderId, logs.revenue\nFROM logs\nWHERE logs.ts >= {{startTime}}\n  AND logs.status >= {{minStatus}}\n  AND (logs.path LIKE '%checkout%' OR logs.path LIKE '%api%')\nORDER BY logs.latencyMs DESC\nLIMIT {{limitRows}}`,
      sources: [
        { alias: 'logs', sourceType: 'dataset', sourceId: 'demo_nginx_ecommerce_logs', rowLimit: 500 },
      ],
      variables: [
        { name: 'minStatus', type: 'number', label: 'Minimum status code', required: true, defaultValue: 500, validation: { min: 100, max: 599 } },
        { name: 'limitRows', type: 'number', label: 'Max rows', required: true, defaultValue: 100, validation: { min: 1, max: 500 } },
      ],
      timeWindowBinding: { enabled: true, defaultPreset: 'last_7d' },
      cardLayout: { title: 'Checkout Triage', subtitle: 'Operational failure review for ecommerce logs', accent: 'amber' },
      createdAt: NOW,
      updatedAt: NOW,
    },
  ]
}

function readState(): RecipeStateFile {
  const state = readJsonFile<RecipeStateFile>(RECIPE_STORE_FILE, { recipes: seedRecipes() })
  const merged = new Map<string, QueryRecipe>()
  for (const recipe of seedRecipes()) merged.set(recipe.id, normalizeRecipe(recipe))
  for (const recipe of state.recipes) merged.set(recipe.id, normalizeRecipe(recipe))
  return { recipes: Array.from(merged.values()) }
}

function writeState(recipes: QueryRecipe[]) {
  writeJsonFile<RecipeStateFile>(RECIPE_STORE_FILE, { recipes })
  invalidateSearchIndex()
}

export function listRecipes(): QueryRecipe[] {
  return readState().recipes.map(recipe => normalizeRecipe(recipe))
}

export function getRecipe(id: string): QueryRecipe | null {
  return listRecipes().find(recipe => recipe.id === id) ?? null
}

export function addRecipe(input: Omit<QueryRecipe, 'id' | 'createdAt' | 'updatedAt'> & { id?: string }): QueryRecipe {
  const recipes = listRecipes()
  const recipe = normalizeRecipe({
    ...input,
    id: input.id ?? `recipe_${Date.now().toString(36)}`,
    createdAt: Date.now(),
    updatedAt: Date.now(),
  })
  recipes.push(recipe)
  writeState(recipes)
  return recipe
}

export function updateRecipe(id: string, patch: Partial<Omit<QueryRecipe, 'id' | 'createdAt'>>): QueryRecipe | null {
  const recipes = listRecipes()
  const recipe = recipes.find(item => item.id === id)
  if (!recipe) return null
  Object.assign(recipe, patch, { updatedAt: Date.now() })
  Object.assign(recipe, normalizeRecipe(recipe))
  writeState(recipes)
  return recipe
}

export function deleteRecipe(id: string): boolean {
  const recipes = listRecipes()
  const index = recipes.findIndex(recipe => recipe.id === id)
  if (index === -1) return false
  recipes.splice(index, 1)
  writeState(recipes)
  return true
}

function normalizeRecipe(recipe: QueryRecipe): QueryRecipe {
  const normalizedSources = recipe.sources.map(source => ({ ...source }))
  const normalizedVariables = recipe.variables.map(variable => ({ ...variable }))
  const draft = {
    ...recipe,
    sources: normalizedSources,
    variables: normalizedVariables,
  }
  const inferred = inferVariablesFromRecipe(draft.queryText, draft.sources, draft)
  return {
    ...draft,
    variables: inferred.variables.map(variable => ({ ...variable })) as InferredVariable[],
    cardLayout: inferred.layout,
  }
}
