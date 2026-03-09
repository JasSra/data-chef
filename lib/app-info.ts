import 'server-only'

import { existsSync, statSync } from 'fs'
import { join } from 'path'
import packageJson from '@/package.json'
import { getAppSettings } from '@/lib/app-settings'

export interface AppInfo {
  name: string
  version: string
  builtAt: string
  tenant: {
    tenantId: string
    slug: string
    region: string
    timezone: string
  }
  branding: {
    productName: string
    logoUrl?: string
    faviconUrl?: string
    websiteUrl?: string
    supportUrl?: string
    parentCompanyLabel?: string
    aboutHeadline?: string
    aboutBody?: string
  }
  attribution: {
    parentCompany: string
    url: string
  }
}

function resolveBuiltAt(): string {
  const buildIdPath = join(process.cwd(), '.next', 'BUILD_ID')
  if (existsSync(buildIdPath)) {
    return statSync(buildIdPath).mtime.toISOString()
  }

  return statSync(join(process.cwd(), 'package.json')).mtime.toISOString()
}

export function getAppInfo(): AppInfo {
  const settings = getAppSettings()
  return {
    name: settings.branding.productName,
    version: packageJson.version,
    builtAt: resolveBuiltAt(),
    tenant: {
      tenantId: settings.tenant.tenantId,
      slug: settings.tenant.slug,
      region: settings.tenant.region,
      timezone: settings.tenant.timezone,
    },
    branding: {
      productName: settings.branding.productName,
      logoUrl: settings.branding.logoUrl,
      faviconUrl: settings.branding.faviconUrl,
      websiteUrl: settings.branding.websiteUrl,
      supportUrl: settings.branding.supportUrl,
      parentCompanyLabel: settings.branding.parentCompanyLabel,
      aboutHeadline: settings.branding.aboutHeadline,
      aboutBody: settings.branding.aboutBody,
    },
    attribution: {
      parentCompany: settings.branding.parentCompanyLabel || 'ThreatCo',
      url: settings.branding.websiteUrl || 'https://www.threatco.io',
    },
  }
}
