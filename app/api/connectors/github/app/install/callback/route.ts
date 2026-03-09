import { NextRequest, NextResponse } from 'next/server'

import { completeGitHubAuthTransaction, failGitHubAuthTransaction, getGitHubAuthTransactionByState } from '@/lib/github-auth'
import { validateGitHubCredentials } from '@/lib/github'

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
        document.body.innerText = payload.error ? "GitHub App installation failed." : "GitHub App installation complete. You can return to the app.";
      })();
    </script>
  </body>
</html>`
  return new NextResponse(html, {
    headers: { 'Content-Type': 'text/html; charset=utf-8' },
  })
}

export async function GET(req: NextRequest) {
  const stateValue = req.nextUrl.searchParams.get('state') ?? ''
  const installationId = Number(req.nextUrl.searchParams.get('installation_id') ?? 0)
  const transaction = getGitHubAuthTransactionByState(stateValue)
  const origin = req.nextUrl.origin

  if (!transaction) {
    return popupResponse(origin, {
      source: 'datachef-github-auth',
      provider: 'app',
      error: 'Installation state is invalid or expired.',
    })
  }

  const appSlug = transaction.appConfig?.appSlug ?? ''
  const appId = transaction.appConfig?.appId ?? ''
  const clientId = transaction.appConfig?.clientId ?? ''
  const clientSecret = transaction.appConfig?.clientSecret ?? ''
  const privateKey = transaction.appConfig?.privateKey ?? ''
  if (!appSlug || !appId || !clientId || !clientSecret || !privateKey || !installationId) {
    failGitHubAuthTransaction(transaction.id, 'GitHub App environment is incomplete')
    return popupResponse(origin, {
      source: 'datachef-github-auth',
      provider: 'app',
      transactionId: transaction.id,
      error: 'GitHub App is not configured correctly.',
    })
  }

  try {
    const credentials = {
      mode: 'app' as const,
      appId,
      clientId,
      clientSecret,
      privateKey: privateKey.replace(/\\n/g, '\n'),
      installationId,
    }
    const validation = await validateGitHubCredentials(credentials)
    const completed = completeGitHubAuthTransaction(transaction.id, {
      credentials: {
        ...credentials,
        accountLogin: validation.viewer.login,
      },
      accountLogin: validation.viewer.login,
      accountType: validation.viewer.type,
      installationId,
    })
    return popupResponse(origin, {
      source: 'datachef-github-auth',
      provider: 'app',
      transactionId: completed?.id ?? transaction.id,
      login: validation.viewer.login,
      ok: true,
    })
  } catch (error) {
    failGitHubAuthTransaction(transaction.id, error instanceof Error ? error.message : String(error))
    return popupResponse(origin, {
      source: 'datachef-github-auth',
      provider: 'app',
      transactionId: transaction.id,
      error: error instanceof Error ? error.message : String(error),
    })
  }
}
