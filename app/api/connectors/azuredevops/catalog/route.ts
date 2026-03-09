import { NextRequest, NextResponse } from 'next/server'

import { getAzureDevOpsAuthTransaction } from '@/lib/azure-devops-auth'
import {
  getAzureDevOpsCreds,
  type AzureDevOpsCredentials,
  type AzureDevOpsProjectSelection,
} from '@/lib/connectors'
import {
  listAzureDevOpsOrganizations,
  listAzureDevOpsProjects,
  listAzureDevOpsRepositories,
} from '@/lib/azure-devops'

export const dynamic = 'force-dynamic'

export async function GET(req: NextRequest) {
  const connectorId = req.nextUrl.searchParams.get('connectorId') ?? ''
  const transactionId = req.nextUrl.searchParams.get('transactionId') ?? ''
  const organization = req.nextUrl.searchParams.get('organization') ?? ''
  const projectsParam = req.nextUrl.searchParams.get('projects') ?? ''
  const patHeader = req.headers.get('x-datachef-azuredevops-pat') ?? ''
  const patOrganization = req.headers.get('x-datachef-azuredevops-organization') ?? organization

  let credentials: AzureDevOpsCredentials | null = null
  try {
    credentials = connectorId
      ? getAzureDevOpsCreds(connectorId)
      : transactionId
      ? getAzureDevOpsAuthTransaction(transactionId)?.credentials ?? null
      : patHeader && patOrganization
      ? { mode: 'pat' as const, organization: patOrganization, pat: patHeader }
      : null
  } catch (error) {
    return NextResponse.json({ error: error instanceof Error ? error.message : String(error) }, { status: 500 })
  }

  if (!credentials) {
    return NextResponse.json({ error: 'connectorId, transactionId, or PAT headers are required' }, { status: 400 })
  }

  try {
    if (!organization) {
      const organizations = await listAzureDevOpsOrganizations(credentials)
      return NextResponse.json({ organizations })
    }

    if (!projectsParam) {
      const projects = await listAzureDevOpsProjects(credentials, organization)
      return NextResponse.json({ projects })
    }

    const projectIds = new Set(projectsParam.split(',').map(value => value.trim()).filter(Boolean))
    const projects = (await listAzureDevOpsProjects(credentials, organization))
      .filter(project => projectIds.has(project.id))
      .map(project => ({
        id: project.id,
        name: project.name,
        description: project.description,
        visibility: project.visibility,
      })) as AzureDevOpsProjectSelection[]
    const repositories = await listAzureDevOpsRepositories(credentials, organization, projects)
    return NextResponse.json({ repositories })
  } catch (error) {
    return NextResponse.json({ error: error instanceof Error ? error.message : String(error) }, { status: 502 })
  }
}
