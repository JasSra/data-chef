import { NextRequest, NextResponse } from 'next/server'

function parseIdentity(value: string) {
  const trimmed = value.trim()
  const normalizedEmail = trimmed.includes('@')
    ? trimmed
    : trimmed.includes('_at_')
    ? trimmed.replace('_at_', '@')
    : trimmed

  const [localPart = '', domain = ''] = normalizedEmail.split('@')
  const lower = normalizedEmail.toLowerCase()

  return {
    raw: trimmed,
    normalized: normalizedEmail,
    localPart,
    domain,
    tenant: domain.endsWith('.onmicrosoft.com') ? domain.replace(/\.onmicrosoft\.com$/i, '') : '',
    isMailosaur: lower.includes('mailosaur.net'),
    isSynthetic: /mailosaur|example|test|unknown/.test(lower),
    signInKindGuess: trimmed.includes('@') || trimmed.includes('_at_') ? 'email-like' : 'opaque-id',
    hasGuidPrefix: /^[0-9a-f]{8}-/i.test(trimmed),
  }
}

export function GET(req: NextRequest) {
  const value = req.nextUrl.searchParams.get('value') ?? ''
  if (!value.trim()) {
    return NextResponse.json({ error: 'Missing value' }, { status: 400 })
  }

  return NextResponse.json(parseIdentity(value))
}
