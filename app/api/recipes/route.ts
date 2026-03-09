import { NextRequest, NextResponse } from 'next/server'
import { addRecipe, listRecipes } from '@/lib/query-recipes'

export async function GET() {
  return NextResponse.json(listRecipes())
}

export async function POST(req: NextRequest) {
  const body = await req.json()
  const recipe = addRecipe({
    name: String(body.name ?? 'Untitled Recipe'),
    description: String(body.description ?? ''),
    lang: body.lang ?? 'sql',
    queryText: String(body.queryText ?? body.query ?? ''),
    sources: Array.isArray(body.sources) ? body.sources : [],
    variables: Array.isArray(body.variables) ? body.variables : [],
    timeWindowBinding: body.timeWindowBinding ?? null,
    cardLayout: body.cardLayout ?? null,
  })
  return NextResponse.json(recipe, { status: 201 })
}
