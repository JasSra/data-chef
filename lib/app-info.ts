import 'server-only'

import { existsSync, statSync } from 'fs'
import { join } from 'path'
import packageJson from '@/package.json'

export interface AppInfo {
  name: string
  version: string
  builtAt: string
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
  return {
    name: 'dataChef',
    version: packageJson.version,
    builtAt: resolveBuiltAt(),
    attribution: {
      parentCompany: 'ThreatCo',
      url: 'https://www.threatco.io',
    },
  }
}
