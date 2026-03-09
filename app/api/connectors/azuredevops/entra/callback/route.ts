import { NextRequest, NextResponse } from 'next/server'

import {
  completeAzureDevOpsAuthTransaction,
  failAzureDevOpsAuthTransaction,
  getAzureDevOpsAuthTransactionByState,
} from '@/lib/azure-devops-auth'
import { listAzureDevOpsOrganizations } from '@/lib/azure-devops'

export const dynamic = 'force-dynamic'

const AZDO_SCOPE = '499b84ac-1321-427f-aa17-267ca6975798/user_impersonation offline_access openid profile'

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
        document.body.innerText = payload.error ? "Azure DevOps authorization failed." : "Azure DevOps authorization complete. You can return to the app.";
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
  const transaction = getAzureDevOpsAuthTransactionByState(stateValue)
  const origin = req.nextUrl.origin

  if (!transaction) {
    return popupResponse(origin, { source: 'datachef-azuredevops-auth', error: 'Authorization state is invalid or expired.' })
  }

  const tenantId = transaction.entraConfig?.tenantId ?? ''
  const clientId = transaction.entraConfig?.clientId ?? ''
  const clientSecret = transaction.entraConfig?.clientSecret ?? ''
  const organization = transaction.entraConfig?.organization ?? transaction.organization
  if (!code || !tenantId || !clientId || !clientSecret) {
    failAzureDevOpsAuthTransaction(transaction.id, 'Missing Entra callback inputs')
    return popupResponse(origin, {
      source: 'datachef-azuredevops-auth',
      transactionId: transaction.id,
      error: 'Azure DevOps delegated auth is not configured correctly.',
    })
  }

  try {
    const redirectUri = new URL('/api/connectors/azuredevops/entra/callback', origin).toString()
    const params = new URLSearchParams({
      client_id: clientId,
      client_secret: clientSecret,
      grant_type: 'authorization_code',
      code,
      redirect_uri: redirectUri,
      scope: AZDO_SCOPE,
    })
    const tokenResponse = await fetch(`https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: params.toString(),
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
      throw new Error(tokenBody.error_description ?? tokenBody.error ?? 'Azure DevOps token exchange failed')
    }

    const credentials = {
      mode: 'entra' as const,
      tenantId,
      clientId,
      clientSecret,
      organization,
      accessToken: tokenBody.access_token,
      refreshToken: tokenBody.refresh_token ?? '',
      expiresAt: Date.now() + Number(tokenBody.expires_in ?? 3600) * 1000,
      scope: tokenBody.scope ?? AZDO_SCOPE,
      tokenType: tokenBody.token_type ?? 'Bearer',
      accountName: '',
    }
    const orgs = await listAzureDevOpsOrganizations(credentials)
    const accountName = orgs.find(org => org.accountName.toLowerCase() === organization.toLowerCase())?.accountName ?? organization
    const completed = completeAzureDevOpsAuthTransaction(transaction.id, {
      credentials: {
        ...credentials,
        accountName,
      },
      accountName,
    })
    return popupResponse(origin, {
      source: 'datachef-azuredevops-auth',
      transactionId: completed?.id ?? transaction.id,
      organization: accountName,
      ok: true,
    })
  } catch (error) {
    failAzureDevOpsAuthTransaction(transaction.id, error instanceof Error ? error.message : String(error))
    return popupResponse(origin, {
      source: 'datachef-azuredevops-auth',
      transactionId: transaction.id,
      error: error instanceof Error ? error.message : String(error),
    })
  }
}
