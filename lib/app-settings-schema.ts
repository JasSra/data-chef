import type { TenantContext } from '@/lib/tenant'

export interface WorkspaceSettings {
  workspaceName: string
  companyName: string
  region: string
  timezone: string
}

export interface BrandingSettings {
  productName: string
  logoMode: 'icon' | 'wordmark' | 'both'
  logoUrl?: string
  faviconUrl?: string
  primaryColor: string
  accentColor: string
  surfaceStyle: 'default' | 'glass' | 'contrast'
  supportUrl?: string
  websiteUrl?: string
  aboutHeadline?: string
  aboutBody?: string
  parentCompanyLabel?: string
}

export interface OwnerSettings {
  name: string
  email: string
  role: string
}

export interface QueryEngineSettings {
  maxRows: number
  timeoutSeconds: number
  defaultDataset: string
  autoExecuteOnOpen: boolean
}

export interface ApiKeysSettings {
  ingestKey: string
  queryKey: string
  webhookSecret: string
}

export interface NotificationSettings {
  pipelineFailure: boolean
  pipelineSuccess: boolean
  emailEnabled: boolean
  slackEnabled: boolean
  emailAddress: string
  slackChannel: string
}

export interface NetworkDiscoverySettings {
  enabled: boolean
  scanOnSetup: boolean
  backgroundRefreshEnabled: boolean
  refreshIntervalMinutes: number
  subnetMode: 'local-subnet'
  lastScanAt: number | null
}

export interface AppSettings {
  setupCompleted: boolean
  tenant: TenantContext
  workspace: WorkspaceSettings
  owner: OwnerSettings
  queryEngine: QueryEngineSettings
  apiKeys: ApiKeysSettings
  notifications: NotificationSettings
  networkDiscovery: NetworkDiscoverySettings
  branding: BrandingSettings
  createdAt: number
  updatedAt: number
}

export const REGION_OPTIONS = [
  { value: 'ap-southeast-2', label: 'Asia Pacific - Sydney (ap-southeast-2)' },
  { value: 'us-east-1', label: 'US East - N. Virginia (us-east-1)' },
  { value: 'eu-west-1', label: 'Europe - Ireland (eu-west-1)' },
] as const

export const TIMEZONE_OPTIONS = [
  { value: 'Australia/Brisbane', label: 'Australia/Brisbane (AEST +10)' },
  { value: 'Australia/Sydney', label: 'Australia/Sydney (AEDT +11)' },
  { value: 'America/New_York', label: 'America/New_York (EST -5)' },
  { value: 'Europe/London', label: 'Europe/London (GMT +0)' },
  { value: 'Asia/Tokyo', label: 'Asia/Tokyo (JST +9)' },
] as const

export const ROLE_OPTIONS = [
  { value: 'Owner', label: 'Owner' },
  { value: 'Admin', label: 'Admin' },
  { value: 'Data Builder', label: 'Data Builder' },
  { value: 'Analyst', label: 'Analyst' },
] as const
