import { NextRequest, NextResponse } from 'next/server'

import { completeGitHubAuthTransaction, failGitHubAuthTransaction, getGitHubAuthTransactionByState } from '@/lib/github-auth'
import { getGitHubViewerForCredentials } from '@/lib/github'

export const dynamic = 'force-dynamic'

function popupResponse(origin: string, payload: Record<string, unknown>) {
  const html = `<!doctype html>
<html>
  <body>
    <script>
      (function () {
        var payload = ${JSON.stringify(payload)};
        if (window.opener) {
          window.opener.postMessage(payload, ${JSON.stringify(origin)});
        }
        window.close();
        document.body.innerText = payload.error ? "GitHub authorization failed." : "GitHub authorization complete. You can return to the app.";
      })();
    </script>
  </body>
</html>`
  return new NextResponse(html, {
    headers: { 'Content-Type': 'text/html; charset=utf-8' },
  })
}

export async function GET(req: NextRequest) {
  const code = req.nextUrl.searchParams.get('code') ?? ''
  const stateValue = req.nextUrl.searchParams.get('state') ?? ''
  const transaction = getGitHubAuthTransactionByState(stateValue)
  const origin = req.nextUrl.origin

  if (!transaction) {
    return popupResponse(origin, {
      source: 'datachef-github-auth',
      provider: 'oauth',
      error: 'Authorization state is invalid or expired.',
    })
  }

  const clientId = transaction.oauthConfig?.clientId ?? ''
  const clientSecret = transaction.oauthConfig?.clientSecret ?? ''
  if (!code || !clientId || !clientSecret) {
    failGitHubAuthTransaction(transaction.id, 'Missing OAuth callback inputs')
    return popupResponse(origin, {
      source: 'datachef-github-auth',
      provider: 'oauth',
      transactionId: transaction.id,
      error: 'GitHub OAuth is not configured correctly.',
    })
  }

  try {
    const redirectUri = new URL('/api/connectors/github/oauth/callback', origin).toString()
    const tokenResponse = await fetch('https://github.com/login/oauth/access_token', {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        client_id: clientId,
        client_secret: clientSecret,
        code,
        redirect_uri: redirectUri,
      }),
      signal: AbortSignal.timeout(15_000),
    })
    const tokenBody = await tokenResponse.json() as {
      access_token?: string
      refresh_token?: string
      expires_in?: number
      scope?: string
      token_type?: string
      error?: string
      error_description?: string
    }

    if (!tokenResponse.ok || !tokenBody.access_token) {
      throw new Error(tokenBody.error_description ?? tokenBody.error ?? 'GitHub OAuth token exchange failed')
    }

    const credentials = {
      mode: 'oauth' as const,
      clientId,
      clientSecret,
      accessToken: tokenBody.access_token,
      refreshToken: tokenBody.refresh_token ?? '',
      expiresAt: Date.now() + Number(tokenBody.expires_in ?? 28_800) * 1000,
      scope: tokenBody.scope ?? '',
      tokenType: tokenBody.token_type ?? 'bearer',
      accountLogin: '',
    }
    const viewer = await getGitHubViewerForCredentials(credentials)
    const completed = completeGitHubAuthTransaction(transaction.id, {
      credentials: {
        ...credentials,
        accountLogin: viewer.login,
      },
      accountLogin: viewer.login,
      accountType: viewer.type,
    })
    return popupResponse(origin, {
      source: 'datachef-github-auth',
      provider: 'oauth',
      transactionId: completed?.id ?? transaction.id,
      login: viewer.login,
      ok: true,
    })
  } catch (error) {
    failGitHubAuthTransaction(transaction.id, error instanceof Error ? error.message : String(error))
    return popupResponse(origin, {
      source: 'datachef-github-auth',
      provider: 'oauth',
      transactionId: transaction.id,
      error: error instanceof Error ? error.message : String(error),
    })
  }
}
