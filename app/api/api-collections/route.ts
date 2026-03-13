import { NextRequest, NextResponse } from 'next/server'
import {
  getApiCollections,
  createApiCollection,
} from '@/lib/api-collections'

export async function GET(req: NextRequest) {
  const serviceId = req.nextUrl.searchParams.get('serviceId') ?? undefined
  return NextResponse.json(getApiCollections(serviceId))
}

export async function POST(req: NextRequest) {
  try {
    const body = await req.json()
    const { name, serviceId, description } = body
    if (!name || !serviceId) {
      return NextResponse.json({ error: 'name and serviceId are required' }, { status: 400 })
    }
    const collection = createApiCollection(name, serviceId, description ?? '')
    return NextResponse.json(collection, { status: 201 })
  } catch {
    return NextResponse.json({ error: 'Invalid request' }, { status: 400 })
  }
}
