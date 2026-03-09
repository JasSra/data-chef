import { NextResponse } from 'next/server'
import { getAppInfo } from '@/lib/app-info'

export const dynamic = 'force-dynamic'

export function GET() {
  return NextResponse.json(getAppInfo())
}
