import { NextRequest, NextResponse } from 'next/server'
import { deleteRecipe, getRecipe, updateRecipe } from '@/lib/query-recipes'

export async function GET(_req: NextRequest, { params }: { params: { id: string } }) {
  const recipe = getRecipe(params.id)
  if (!recipe) return NextResponse.json({ error: 'Recipe not found' }, { status: 404 })
  return NextResponse.json(recipe)
}

export async function PUT(req: NextRequest, { params }: { params: { id: string } }) {
  const body = await req.json()
  const recipe = updateRecipe(params.id, {
    name: body.name,
    description: body.description,
    lang: body.lang,
    queryText: body.queryText ?? body.query,
    sources: body.sources,
    variables: body.variables,
    timeWindowBinding: body.timeWindowBinding,
    cardLayout: body.cardLayout,
  })
  if (!recipe) return NextResponse.json({ error: 'Recipe not found' }, { status: 404 })
  return NextResponse.json(recipe)
}

export async function DELETE(_req: NextRequest, { params }: { params: { id: string } }) {
  const ok = deleteRecipe(params.id)
  if (!ok) return NextResponse.json({ error: 'Recipe not found' }, { status: 404 })
  return NextResponse.json({ ok: true })
}
