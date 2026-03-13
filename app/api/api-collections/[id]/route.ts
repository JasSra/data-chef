import { NextRequest, NextResponse } from 'next/server'
import {
  getApiCollection,
  deleteApiCollection,
  renameApiCollection,
  addQueryToCollection,
  removeQueryFromCollection,
} from '@/lib/api-collections'

export async function GET(_req: NextRequest, { params }: { params: { id: string } }) {
  const col = getApiCollection(params.id)
  if (!col) return NextResponse.json({ error: 'Not found' }, { status: 404 })
  return NextResponse.json(col)
}

export async function PATCH(req: NextRequest, { params }: { params: { id: string } }) {
  try {
    const body = await req.json()

    // Add query to collection
    if (body.action === 'addQuery') {
      const { name, query } = body
      if (!name || !query) {
        return NextResponse.json({ error: 'name and query are required' }, { status: 400 })
      }
      const saved = addQueryToCollection(params.id, name, query)
      if (!saved) return NextResponse.json({ error: 'Collection not found' }, { status: 404 })
      return NextResponse.json(saved)
    }

    // Remove query from collection
    if (body.action === 'removeQuery') {
      const ok = removeQueryFromCollection(params.id, body.queryId)
      if (!ok) return NextResponse.json({ error: 'Not found' }, { status: 404 })
      return NextResponse.json({ ok: true })
    }

    // Rename collection
    if (body.name) {
      const ok = renameApiCollection(params.id, body.name)
      if (!ok) return NextResponse.json({ error: 'Not found' }, { status: 404 })
      return NextResponse.json({ ok: true })
    }

    return NextResponse.json({ error: 'No action specified' }, { status: 400 })
  } catch {
    return NextResponse.json({ error: 'Invalid request' }, { status: 400 })
  }
}

export async function DELETE(_req: NextRequest, { params }: { params: { id: string } }) {
  const ok = deleteApiCollection(params.id)
  if (!ok) return NextResponse.json({ error: 'Not found' }, { status: 404 })
  return NextResponse.json({ ok: true })
}
