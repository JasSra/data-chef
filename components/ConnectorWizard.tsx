'use client'

import { useState, useEffect, useRef } from 'react'
import {
  X, Globe, Database, Cloud, Server, Webhook as WebhookIcon,
  ArrowLeft, ChevronRight, CheckCircle2, Loader2, AlertCircle,
  Clock, Copy, Eye, EyeOff, Terminal, Zap, Key, HardDrive,
  AlertTriangle, Shield, Upload, FileText, BarChart2, Users,
} from 'lucide-react'
import BrandIcon from '@/components/BrandIcon'

/* ── Connector catalog ───────────────────────────────────────────── */
export type ConnectorId = 'http' | 'webhook' | 'postgresql' | 'mysql' | 'mongodb' | 'redis' | 'mssql' | 'rabbitmq' | 'mqtt' | 's3' | 'sftp' | 'bigquery' | 'file' | 'appinsights' | 'azuremonitor' | 'elasticsearch' | 'datadog' | 'azureb2c' | 'azureentraid' | 'github' | 'azuredevops'

interface ConnectorDef {
  id: ConnectorId; label: string; desc: string; Icon?: React.ElementType
  brandClass?: string
  color: string; bg: string; border: string
  category: 'API' | 'Database' | 'Storage' | 'Warehouse' | 'Monitoring' | 'Identity' | 'Developer Tools'
  badge?: string; noTest?: boolean
}
const CONNECTORS: ConnectorDef[] = [
  { id: 'http',       label: 'HTTP API',        desc: 'REST, GraphQL, JSON over HTTP(S)',      Icon: Globe,         color: 'text-sky-400',     bg: 'bg-sky-500/10',     border: 'border-sky-500/30',     category: 'API' },
  { id: 'github',     label: 'GitHub',          desc: 'Repos, pull requests, and issues',      brandClass: 'fa-brands fa-github', color: 'text-zinc-200', bg: 'bg-zinc-500/10', border: 'border-zinc-500/30', category: 'Developer Tools', badge: 'Code' },
  { id: 'azuredevops', label: 'Azure DevOps',   desc: 'Repos, commits, PRs, boards, and pipelines', brandClass: 'fa-brands fa-microsoft', color: 'text-cyan-200', bg: 'bg-cyan-500/10', border: 'border-cyan-500/30', category: 'Developer Tools', badge: 'Azure' },
  { id: 'webhook',    label: 'Inbound Webhook',  desc: 'Receive push events in real-time',      Icon: WebhookIcon,   color: 'text-amber-400',   bg: 'bg-amber-500/10',   border: 'border-amber-500/30',   category: 'API',       badge: 'Push', noTest: true },
  { id: 'postgresql', label: 'PostgreSQL',       desc: 'Tables, views, incremental / CDC',      Icon: Database,      color: 'text-blue-400',    bg: 'bg-blue-500/10',    border: 'border-blue-500/30',    category: 'Database' },
  { id: 'mysql',      label: 'MySQL / MariaDB',  desc: 'Tables or custom SQL queries',          Icon: Database,      color: 'text-orange-400',  bg: 'bg-orange-500/10',  border: 'border-orange-500/30',  category: 'Database' },
  { id: 'mongodb',    label: 'MongoDB',          desc: 'Collections, pipelines & aggregations', Icon: Database,      color: 'text-emerald-400', bg: 'bg-emerald-500/10', border: 'border-emerald-500/30', category: 'Database', badge: 'NoSQL' },
  { id: 'redis',      label: 'Redis',            desc: 'Keys, hashes, streams, RediSearch, RedisJSON', Icon: Database, color: 'text-red-400', bg: 'bg-red-500/10', border: 'border-red-500/30', category: 'Database', badge: 'Cache' },
  { id: 'mssql',     label: 'SQL Server / Azure SQL', desc: 'Tables, views, stored procedures, full SQL browser', Icon: Database, color: 'text-sky-300', bg: 'bg-sky-500/10', border: 'border-sky-500/30', category: 'Database', badge: 'Microsoft' },
  { id: 'rabbitmq',  label: 'RabbitMQ',         desc: 'Browse queues, exchanges, and messages via management API', Icon: Zap, color: 'text-orange-300', bg: 'bg-orange-500/10', border: 'border-orange-500/30', category: 'Database', badge: 'Queue' },
  { id: 'mqtt',      label: 'MQTT',             desc: 'Subscribe to topics, browse retained messages', Icon: Zap, color: 'text-emerald-300', bg: 'bg-emerald-500/10', border: 'border-emerald-500/30', category: 'Database', badge: 'IoT' },
  { id: 's3',         label: 'S3 / R2 / GCS',   desc: 'Object storage, JSON/CSV/Parquet',      Icon: Cloud,         color: 'text-violet-400',  bg: 'bg-violet-500/10',  border: 'border-violet-500/30',  category: 'Storage' },
  { id: 'sftp',       label: 'SFTP / FTP',       desc: 'Secure file transfer, remote exports',  Icon: Server,        color: 'text-slate-400',   bg: 'bg-slate-500/10',   border: 'border-slate-500/30',   category: 'Storage' },
  { id: 'bigquery',   label: 'BigQuery',         desc: 'Google BigQuery tables and SQL',        brandClass: 'fa-brands fa-google', color: 'text-rose-400',    bg: 'bg-rose-500/10',    border: 'border-rose-500/30',    category: 'Warehouse', badge: 'GCP' },
  { id: 'file',        label: 'File Upload',      desc: 'CSV, JSON, JSONL — upload directly',            Icon: FileText,   color: 'text-lime-400',    bg: 'bg-lime-500/10',    border: 'border-lime-500/30',    category: 'Storage',    badge: 'Local',  noTest: true },
  { id: 'appinsights', label: 'App Insights (API key)', desc: 'Application ID + API key for App Insights Analytics queries', brandClass: 'fa-brands fa-microsoft', color: 'text-cyan-400', bg: 'bg-cyan-500/10', border: 'border-cyan-500/30', category: 'Monitoring', badge: 'Azure' },
  { id: 'azuremonitor', label: 'Azure Monitor (OAuth)', desc: 'Workspace queries via Entra client credentials', brandClass: 'fa-brands fa-microsoft', color: 'text-cyan-300', bg: 'bg-cyan-500/10', border: 'border-cyan-500/30', category: 'Monitoring', badge: 'Azure' },
  { id: 'elasticsearch', label: 'Elasticsearch / OpenSearch', desc: 'Logs and events via KQL-compatible queries', Icon: Database, color: 'text-amber-300', bg: 'bg-amber-500/10', border: 'border-amber-500/30', category: 'Monitoring', badge: 'Logs' },
  { id: 'datadog', label: 'Datadog', desc: 'Logs and events with KQL-first observability queries', Icon: BarChart2, color: 'text-orange-300', bg: 'bg-orange-500/10', border: 'border-orange-500/30', category: 'Monitoring', badge: 'SaaS' },
  { id: 'azureb2c',    label: 'Azure AD B2C',    desc: 'Microsoft Graph users, user flows, and policies', brandClass: 'fa-brands fa-microsoft', color: 'text-teal-400',    bg: 'bg-teal-500/10',    border: 'border-teal-500/30',    category: 'Identity',   badge: 'Azure' },
  { id: 'azureentraid',label: 'Azure Entra ID',  desc: 'Microsoft Graph users, groups, and applications', brandClass: 'fa-brands fa-microsoft', color: 'text-sky-300', bg: 'bg-sky-500/10', border: 'border-sky-500/30', category: 'Identity', badge: 'Azure' },
]

/* ── Exported job / connector types ──────────────────────────────── */
export interface ConnectorJob {
  id: string; connectorId: string; connectorName: string; connectorType: ConnectorId
  jobType: 'test' | 'sync' | 'schema'
  status: 'queued' | 'running' | 'succeeded' | 'failed'
  progress: number
  logs: Array<{ level: 'info' | 'success' | 'warn' | 'error'; msg: string }>
  startedAt: number; duration?: number; error?: string
}
export interface NewConnector {
  id: string; name: string; type: ConnectorId; endpoint: string
  authMethod: string; syncInterval: string; description: string
  sourceDiscoveryId?: string
  existingConnectorId?: string
  aiCredentials?: {
    authMode: 'api_key' | 'entra_client_secret'
    appId: string
    apiKey: string
    connectionString?: string
    mode?: 'appinsights' | 'workspace'
    workspaceId?: string
    tenantId?: string
    clientId?: string
    clientSecret?: string
  }
  observabilityCredentials?: Record<string, unknown>
  azureB2cCredentials?: {
    tenantId: string
    clientId: string
    authMode: 'client_secret' | 'client_certificate'
    clientSecret: string
    certificatePem: string
    privateKeyPem: string
    thumbprint: string
    cloud: 'global'
  }
  azureEntraIdCredentials?: {
    tenantId: string
    clientId: string
    authMode: 'client_secret' | 'client_certificate'
    clientSecret: string
    certificatePem: string
    privateKeyPem: string
    thumbprint: string
    cloud: 'global'
  }
  githubCredentials?: {
    mode: 'pat'
    token: string
    username?: string
  }
  githubAuthTransactionId?: string
  azureDevOpsCredentials?: {
    mode: 'pat'
    organization: string
    pat: string
    username?: string
  }
  azureDevOpsAuthTransactionId?: string
  runtimeConfig?: Record<string, unknown>
}

export interface DiscoveryConnectorDraft {
  candidateId?: string
  existingConnectorId?: string
  type: ConnectorId
  name: string
  description: string
  endpoint: string
  runtimeConfig: Record<string, unknown>
  aiCredentials?: NewConnector['aiCredentials']
  observabilityCredentials?: NewConnector['observabilityCredentials']
  azureB2cCredentials?: NewConnector['azureB2cCredentials']
  azureEntraIdCredentials?: NewConnector['azureEntraIdCredentials']
  azureDevOpsCredentials?: NewConnector['azureDevOpsCredentials']
}

/* ── Form state types ────────────────────────────────────────────── */
interface HttpForm {
  name: string; description: string; url: string; method: 'GET' | 'POST'
  auth: 'none' | 'apikey' | 'bearer' | 'basic' | 'oauth2'
  apiKeyHeader: string; apiKeyValue: string; bearerToken: string
  basicUser: string; basicPass: string
  oauthTokenUrl: string; oauthClientId: string; oauthClientSecret: string; oauthScope: string
  responsePath: string; format: string; schedule: string
  paginationType: 'none' | 'cursor' | 'page' | 'offset'
  cursorParam: string; cursorPath: string; pageParam: string; totalPath: string
  limitParam: string; offsetParam: string; limitValue: string
}
interface WebhookForm {
  name: string; description: string; secret: string
  eventFilter: string; replayProtection: boolean; ttl: '24h' | '48h' | '7d'
}
interface DatabaseForm {
  name: string; description: string; host: string; port: string
  database: string; dbUser: string; dbPass: string; ssl: boolean
  sslMode: string; tableOrQuery: string; syncMode: 'full' | 'incremental'
  cursorColumn: string; cursorType: string; enableCdc: boolean; schedule: string
  useConnectionString: boolean; connectionString: string
  collection: string; filter: string
}
interface S3Form {
  name: string; description: string; provider: string; bucket: string
  region: string; endpoint: string; accessKeyId: string; secretAccessKey: string
  prefix: string; format: string; schedule: string
}
interface SftpForm {
  name: string; description: string; protocol: 'sftp' | 'ftp'
  host: string; port: string; sftpUser: string
  authType: 'password' | 'privatekey'; password: string; privateKey: string
  path: string; filePattern: string; format: string; schedule: string
}
interface BigQueryForm {
  name: string; description: string; project: string; dataset: string
  tableOrSql: string; serviceAccountJson: string; schedule: string
}
interface RedisForm {
  name: string; description: string
  connectionMode: 'fields' | 'connectionString'
  connectionString: string
  host: string; port: string
  username: string; password: string
  database: string
  tls: boolean
  defaultQueryMode: 'command' | 'search' | 'json' | 'timeseries' | 'stream' | 'catalog'
  defaultValueType: 'auto' | 'string' | 'hash' | 'list' | 'set' | 'zset' | 'json' | 'timeseries' | 'stream' | 'search'
  defaultKeyPattern: string
  defaultSearchIndex: string
  schedule: string
}
interface MssqlForm {
  name: string; description: string
  connectionMode: 'fields' | 'connectionString'
  connectionString: string
  host: string; port: string
  database: string; dbUser: string; dbPass: string
  instanceName: string
  encrypt: boolean; trustServerCertificate: boolean
  schema: string
  defaultCatalog: 'tables' | 'views' | 'columns' | 'procedures' | 'indexes' | 'databases'
  schedule: string
}
interface RabbitMQForm {
  name: string; description: string
  connectionMode: 'fields' | 'connectionString'
  connectionString: string
  host: string; port: string; managementPort: string
  vhost: string; username: string; password: string
  tls: boolean; managementTls: boolean
  defaultCatalog: 'queues' | 'exchanges' | 'bindings' | 'vhosts' | 'connections' | 'overview'
  schedule: string
}
interface MqttForm {
  name: string; description: string
  connectionMode: 'fields' | 'connectionString'
  connectionString: string
  host: string; port: string
  username: string; password: string
  tls: boolean
  protocol: 'mqtt' | 'mqtts' | 'ws' | 'wss'
  defaultTopic: string
  schedule: string
}
interface FileForm {
  name: string; description: string
  file: File | null; fileName: string; fileSize: number
  format: 'csv' | 'json' | 'jsonl' | ''; parsedRows: Record<string, unknown>[]
  detectedCols: string[]; parseError: string
}
interface AppInsightsForm {
  name: string; description: string
  authMode: 'api_key' | 'entra_client_secret'
  mode: 'appinsights' | 'workspace'
  connectionString: string
  appId: string; apiKey: string; workspaceId: string
  tenantId: string; clientId: string; clientSecret: string
}
interface AzureMonitorForm {
  name: string; description: string
  workspaceId: string
  tenantId: string; clientId: string; clientSecret: string
  defaultQuery: string
}
interface ElasticObservabilityForm {
  name: string; description: string
  endpoint: string
  authType: 'basic' | 'apikey'
  username: string; password: string; apiKey: string
  indexPattern: string
  defaultQuery: string
  schedule: string
}
interface DatadogForm {
  name: string; description: string
  site: string
  apiKey: string; applicationKey: string
  source: 'logs' | 'events'
  defaultQuery: string
  schedule: string
}
interface AzureB2CForm {
  name: string; description: string
  tenantId: string; clientId: string
  authMode: 'client_secret' | 'client_certificate'
  clientSecret: string
  certificatePem: string
  privateKeyPem: string
  thumbprint: string
  resource: 'users' | 'userFlows' | 'customPolicies'
  schedule: string
}
interface AzureEntraIdForm {
  name: string; description: string
  tenantId: string; clientId: string
  authMode: 'client_secret' | 'client_certificate'
  clientSecret: string
  certificatePem: string
  privateKeyPem: string
  thumbprint: string
  resource: 'users' | 'groups' | 'applications'
  schedule: string
}
interface GitHubRepoOption {
  id: number
  owner: string
  repo: string
  fullName: string
  private: boolean
  ownerType: 'User' | 'Organization'
  defaultBranch: string
  visibility: string
  archived: boolean
  url: string
}
interface GitHubForm {
  name: string
  description: string
  authMode: 'pat' | 'oauth' | 'app'
  token: string
  oauthClientId: string
  oauthClientSecret: string
  appSlug: string
  appId: string
  appClientId: string
  appClientSecret: string
  appPrivateKey: string
  transactionId: string
  accountLogin: string
  repoSearch: string
  reposLoaded: boolean
  selectedRepos: GitHubRepoOption[]
  availableRepos: GitHubRepoOption[]
  defaultResource: 'repos' | 'pullRequests' | 'issues'
  pullRequestState: 'open' | 'closed' | 'all'
  issueState: 'open' | 'closed' | 'all'
  schedule: string
}
interface AzureDevOpsOrganizationOption {
  accountId: string
  accountName: string
  accountUri: string
}
interface AzureDevOpsProjectOption {
  id: string
  name: string
  description?: string
  visibility?: string
}
interface AzureDevOpsRepoOption {
  projectId: string
  projectName: string
  repositoryId: string
  repositoryName: string
  fullName: string
  defaultBranch?: string
}
interface AzureDevOpsForm {
  name: string
  description: string
  authMode: 'pat' | 'entra'
  organization: string
  pat: string
  tenantId: string
  clientId: string
  clientSecret: string
  transactionId: string
  accountName: string
  authMessage: string
  organizationsLoaded: boolean
  availableOrganizations: AzureDevOpsOrganizationOption[]
  projectsLoaded: boolean
  availableProjects: AzureDevOpsProjectOption[]
  selectedProjects: AzureDevOpsProjectOption[]
  reposLoaded: boolean
  availableRepos: AzureDevOpsRepoOption[]
  selectedRepos: AzureDevOpsRepoOption[]
  repoSearch: string
  defaultResource: 'repositories' | 'commits' | 'pullRequests' | 'branches' | 'workItems' | 'pipelines' | 'pipelineRuns'
  pullRequestState: 'active' | 'completed' | 'abandoned' | 'all'
  workItemState: string
  branchFilter: string
  commitDays: string
  pipelineRunDays: string
  schedule: string
}
type AnyForm = HttpForm | WebhookForm | DatabaseForm | S3Form | SftpForm | BigQueryForm | RedisForm | MssqlForm | RabbitMQForm | MqttForm | FileForm | AppInsightsForm | AzureMonitorForm | ElasticObservabilityForm | DatadogForm | AzureB2CForm | AzureEntraIdForm | GitHubForm | AzureDevOpsForm

/* ── Validation ──────────────────────────────────────────────────── */
type FieldErrors = Record<string, string | undefined>
const validUrl = (s: string) => /^https?:\/\/.+\..+/.test(s)

function validateHttp(f: HttpForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (!f.url.trim()) e.url = 'URL is required'
  else if (!validUrl(f.url)) e.url = 'Must be a valid HTTP(S) URL'
  if (f.auth === 'apikey' && !f.apiKeyValue.trim()) e.apiKeyValue = 'API key is required'
  if (f.auth === 'bearer' && !f.bearerToken.trim()) e.bearerToken = 'Token is required'
  if (f.auth === 'basic') {
    if (!f.basicUser.trim()) e.basicUser = 'Username is required'
    if (!f.basicPass.trim()) e.basicPass = 'Password is required'
  }
  if (f.auth === 'oauth2') {
    if (!validUrl(f.oauthTokenUrl)) e.oauthTokenUrl = 'Valid token URL required'
    if (!f.oauthClientId.trim()) e.oauthClientId = 'Client ID required'
    if (!f.oauthClientSecret.trim()) e.oauthClientSecret = 'Client secret required'
  }
  return e
}
function validateWebhook(f: WebhookForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (!f.secret.trim()) e.secret = 'HMAC secret is required'
  return e
}
function validateDatabase(f: DatabaseForm, type: ConnectorId): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (type === 'mongodb' && f.useConnectionString) {
    if (!f.connectionString.trim()) e.connectionString = 'Connection string required'
    if (!f.collection.trim()) e.collection = 'Collection required'
  } else {
    if (!f.host.trim()) e.host = 'Host is required'
    if (!f.port || isNaN(Number(f.port))) e.port = 'Valid port required'
    if (!f.database.trim()) e.database = 'Database name required'
    if (!f.dbUser.trim()) e.dbUser = 'Username required'
    if (!f.dbPass.trim()) e.dbPass = 'Password required'
    if (type !== 'mongodb' && !f.tableOrQuery.trim()) e.tableOrQuery = 'Table or query required'
    if (type === 'mongodb' && !f.collection.trim()) e.collection = 'Collection required'
  }
  return e
}
function validateS3(f: S3Form): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (!f.bucket.trim()) e.bucket = 'Bucket is required'
  if (!f.accessKeyId.trim()) e.accessKeyId = 'Access key required'
  if (!f.secretAccessKey.trim()) e.secretAccessKey = 'Secret key required'
  if (['cloudflare', 'minio', 'other'].includes(f.provider) && !f.endpoint.trim()) e.endpoint = 'Endpoint URL required'
  return e
}
function validateSftp(f: SftpForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (!f.host.trim()) e.host = 'Host is required'
  if (!f.port || isNaN(Number(f.port))) e.port = 'Valid port required'
  if (!f.sftpUser.trim()) e.sftpUser = 'Username required'
  if (f.authType === 'password' && !f.password.trim()) e.password = 'Password required'
  if (f.authType === 'privatekey' && !f.privateKey.trim()) e.privateKey = 'Private key required'
  if (!f.path.trim()) e.path = 'Remote path required'
  return e
}
function validateBigQuery(f: BigQueryForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (!f.project.trim()) e.project = 'Project ID required'
  if (!f.dataset.trim()) e.dataset = 'Dataset required'
  if (!f.tableOrSql.trim()) e.tableOrSql = 'Table or SQL required'
  if (!f.serviceAccountJson.trim()) e.serviceAccountJson = 'Service account JSON required'
  else { try { JSON.parse(f.serviceAccountJson) } catch { e.serviceAccountJson = 'Must be valid JSON' } }
  return e
}
function validateRedis(f: RedisForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (f.connectionMode === 'connectionString') {
    if (!f.connectionString.trim()) e.connectionString = 'Connection string required'
    else if (!/^rediss?:\/\//.test(f.connectionString.trim())) e.connectionString = 'Must start with redis:// or rediss://'
  } else {
    if (!f.host.trim()) e.host = 'Host is required'
    if (!f.port || isNaN(Number(f.port))) e.port = 'Valid port required'
  }
  if (f.defaultQueryMode === 'search' && !f.defaultSearchIndex.trim()) e.defaultSearchIndex = 'Search index is required for search mode'
  return e
}
function validateMssql(f: MssqlForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (f.connectionMode === 'connectionString') {
    if (!f.connectionString.trim()) e.connectionString = 'Connection string required'
  } else {
    if (!f.host.trim()) e.host = 'Host is required'
    if (!f.port || isNaN(Number(f.port))) e.port = 'Valid port required'
    if (!f.dbUser.trim()) e.dbUser = 'Username required'
    if (!f.dbPass.trim()) e.dbPass = 'Password required'
  }
  return e
}
function validateRabbitMQ(f: RabbitMQForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (f.connectionMode === 'connectionString') {
    if (!f.connectionString.trim()) e.connectionString = 'Connection string required'
  } else {
    if (!f.host.trim()) e.host = 'Host is required'
    if (!f.port || isNaN(Number(f.port))) e.port = 'Valid AMQP port required'
    if (!f.managementPort || isNaN(Number(f.managementPort))) e.managementPort = 'Valid management port required'
  }
  return e
}
function validateMqtt(f: MqttForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (f.connectionMode === 'connectionString') {
    if (!f.connectionString.trim()) e.connectionString = 'Connection string required'
  } else {
    if (!f.host.trim()) e.host = 'Host is required'
    if (!f.port || isNaN(Number(f.port))) e.port = 'Valid port required'
  }
  return e
}
function validateFile(f: FileForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (!f.file) e.file = 'Please select a file'
  else if (f.parseError) e.file = f.parseError
  else if (f.parsedRows.length === 0) e.file = 'File appears to be empty or unreadable'
  return e
}
function validateAppInsights(f: AppInsightsForm): FieldErrors {
  const e: FieldErrors = {}
  const derived = parseAppInsightsConnectionString(f.connectionString)
  if (!f.name.trim())         e.name         = 'Name is required'
  if (!f.appId.trim() && !derived.applicationId) e.appId = 'Application ID or connection string with ApplicationId is required'
  if (f.authMode === 'api_key') {
    if (!f.apiKey.trim()) e.apiKey = 'API key is required'
  } else {
    if (f.mode === 'workspace' && !f.workspaceId.trim()) e.workspaceId = 'Workspace ID is required'
    if (!f.tenantId.trim())     e.tenantId     = 'Tenant ID is required'
    if (!f.clientId.trim())     e.clientId     = 'Client ID is required'
    if (!f.clientSecret.trim()) e.clientSecret = 'Client Secret is required'
  }
  return e
}
function validateAzureMonitor(f: AzureMonitorForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (!f.workspaceId.trim()) e.workspaceId = 'Workspace ID is required'
  if (!f.tenantId.trim()) e.tenantId = 'Tenant ID is required'
  if (!f.clientId.trim()) e.clientId = 'Client ID is required'
  if (!f.clientSecret.trim()) e.clientSecret = 'Client secret is required'
  return e
}
function validateElastic(f: ElasticObservabilityForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (!validUrl(f.endpoint)) e.endpoint = 'Valid endpoint URL required'
  if (!f.indexPattern.trim()) e.indexPattern = 'Index pattern is required'
  if (f.authType === 'basic') {
    if (!f.username.trim()) e.username = 'Username is required'
    if (!f.password.trim()) e.password = 'Password is required'
  }
  if (f.authType === 'apikey' && !f.apiKey.trim()) e.apiKey = 'API key is required'
  return e
}
function validateDatadog(f: DatadogForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (!f.site.trim()) e.site = 'Site is required'
  if (!f.apiKey.trim()) e.apiKey = 'API key is required'
  if (!f.applicationKey.trim()) e.applicationKey = 'Application key is required'
  return e
}
function validateAzureB2C(f: AzureB2CForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (!f.tenantId.trim()) e.tenantId = 'Tenant ID is required'
  if (!f.clientId.trim()) e.clientId = 'Client ID is required'
  if (f.authMode === 'client_secret' && !f.clientSecret.trim()) e.clientSecret = 'Client secret is required'
  if (f.authMode === 'client_certificate') {
    if (!f.certificatePem.trim()) e.certificatePem = 'Certificate PEM is required'
    if (!f.privateKeyPem.trim()) e.privateKeyPem = 'Private key PEM is required'
  }
  return e
}
function validateAzureEntraId(f: AzureEntraIdForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (!f.tenantId.trim()) e.tenantId = 'Tenant ID is required'
  if (!f.clientId.trim()) e.clientId = 'Client ID is required'
  if (f.authMode === 'client_secret' && !f.clientSecret.trim()) e.clientSecret = 'Client secret is required'
  if (f.authMode === 'client_certificate') {
    if (!f.certificatePem.trim()) e.certificatePem = 'Certificate PEM is required'
    if (!f.privateKeyPem.trim()) e.privateKeyPem = 'Private key PEM is required'
  }
  return e
}
function validateGitHub(f: GitHubForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (f.authMode === 'pat' && !f.token.trim()) e.token = 'Personal access token is required'
  if (f.authMode === 'oauth') {
    if (!f.oauthClientId.trim()) e.oauthClientId = 'OAuth client ID is required'
    if (!f.oauthClientSecret.trim()) e.oauthClientSecret = 'OAuth client secret is required'
  }
  if (f.authMode === 'app') {
    if (!f.appSlug.trim()) e.appSlug = 'GitHub App slug is required'
    if (!f.appId.trim()) e.appId = 'GitHub App ID is required'
    if (!f.appClientId.trim()) e.appClientId = 'GitHub App client ID is required'
    if (!f.appClientSecret.trim()) e.appClientSecret = 'GitHub App client secret is required'
    if (!f.appPrivateKey.trim()) e.appPrivateKey = 'GitHub App private key is required'
  }
  if (f.authMode !== 'pat' && !f.transactionId.trim()) e.transactionId = 'GitHub authorization is required'
  if (f.selectedRepos.length === 0) e.selectedRepos = 'Select at least one repository'
  return e
}
function validateAzureDevOps(f: AzureDevOpsForm): FieldErrors {
  const e: FieldErrors = {}
  if (!f.name.trim()) e.name = 'Name is required'
  if (!f.organization.trim()) e.organization = 'Organization is required'
  if (f.authMode === 'pat' && !f.pat.trim()) e.pat = 'Personal access token is required'
  if (f.authMode === 'entra') {
    if (!f.tenantId.trim()) e.tenantId = 'Tenant ID is required'
    if (!f.clientId.trim()) e.clientId = 'Client ID is required'
    if (!f.clientSecret.trim()) e.clientSecret = 'Client secret is required'
    if (!f.transactionId.trim()) e.transactionId = 'Azure DevOps authorization is required'
  }
  if (f.selectedProjects.length === 0) e.selectedProjects = 'Select at least one project'
  if (f.selectedRepos.length === 0) e.selectedRepos = 'Select at least one repository'
  return e
}

function parseAppInsightsConnectionString(connectionString: string): {
  applicationId: string
  instrumentationKey: string
  ingestionEndpoint: string
} {
  const parts = connectionString
    .split(';')
    .map(part => part.trim())
    .filter(Boolean)

  const map = new Map<string, string>()
  for (const part of parts) {
    const idx = part.indexOf('=')
    if (idx <= 0) continue
    map.set(part.slice(0, idx).trim().toLowerCase(), part.slice(idx + 1).trim())
  }

  return {
    applicationId: map.get('applicationid') ?? '',
    instrumentationKey: map.get('instrumentationkey') ?? '',
    ingestionEndpoint: map.get('ingestionendpoint') ?? '',
  }
}

/* ── Test log sequences ──────────────────────────────────────────── */
type LogEntry = { level: 'info' | 'success' | 'warn' | 'error'; msg: string; delay: number }
function getTestLogs(type: ConnectorId, form: Record<string, unknown>): LogEntry[] {
  switch (type) {
    case 'http': {
      let host = 'api.example.com'
      try { host = new URL(String(form.url || 'https://api.example.com')).hostname } catch {}
      return [
        { level: 'info',    msg: `Resolving hostname: ${host}`,                                     delay: 400 },
        { level: 'info',    msg: 'TCP connection established · 34ms',                               delay: 600 },
        { level: 'info',    msg: 'TLS 1.3 handshake · cipher: AES_256_GCM_SHA384',                 delay: 700 },
        { level: 'info',    msg: `${form.method || 'GET'} ${form.url || '/'} → 200 OK`,            delay: 500 },
        { level: 'info',    msg: 'Parsing response body (application/json)',                         delay: 400 },
        { level: 'info',    msg: 'Detected array root · sampling 100 records',                      delay: 500 },
        { level: 'success', msg: '7 fields inferred · SHA256:b3a8…f2c1',                           delay: 500 },
      ]
    }
    case 'postgresql':
    case 'mysql': {
      const port = String(form.port || (type === 'postgresql' ? '5432' : '3306'))
      const hasSSL = Boolean(form.ssl)
      return [
        { level: 'info',    msg: `Resolving ${form.host || 'localhost'}:${port}`,                   delay: 400 },
        { level: 'info',    msg: 'TCP connection established',                                       delay: 600 },
        { level: hasSSL ? 'info' : 'warn', msg: hasSSL ? 'TLS 1.3 negotiated · verify-full' : 'SSL disabled — unencrypted connection', delay: 500 },
        { level: 'info',    msg: `Authenticating as '${form.dbUser || 'user'}'`,                   delay: 400 },
        { level: 'info',    msg: `Connected to database '${form.database || 'mydb'}'`,              delay: 300 },
        { level: 'info',    msg: `SELECT COUNT(*) FROM ${String(form.tableOrQuery || 'table').split(/\s/)[0]}`,  delay: 600 },
        { level: 'info',    msg: '~150,421 rows detected',                                          delay: 400 },
        { level: 'info',    msg: 'Sampling schema (LIMIT 100)',                                     delay: 500 },
        { level: 'success', msg: '12 columns inferred · SHA256:a7c3…d9f2',                         delay: 500 },
      ]
    }
    case 'mongodb':
      return [
        { level: 'info',    msg: 'Resolving MongoDB host',                                          delay: 400 },
        { level: 'info',    msg: 'Connection pool established (min: 5)',                             delay: 600 },
        { level: 'info',    msg: 'Authenticating via SCRAM-SHA-256',                                delay: 500 },
        { level: 'info',    msg: `Database '${form.database || 'mydb'}' selected`,                 delay: 300 },
        { level: 'info',    msg: `Collection '${form.collection || 'events'}' · counting docs`,    delay: 500 },
        { level: 'info',    msg: '~2,341,820 documents detected',                                   delay: 400 },
        { level: 'info',    msg: 'Running schema inference on 500 samples',                         delay: 700 },
        { level: 'success', msg: '18 fields inferred · heterogeneous types detected',               delay: 500 },
      ]
    case 's3':
      return [
        { level: 'info',    msg: `Initializing ${String(form.provider || 'aws').toUpperCase()} SDK`, delay: 400 },
        { level: 'info',    msg: `Listing '${form.bucket || 'my-bucket'}/${form.prefix || ''}' ...`, delay: 700 },
        { level: 'info',    msg: '1,234 objects · 45.2 GB total',                                  delay: 400 },
        { level: 'info',    msg: 'Downloading sample for format detection',                          delay: 600 },
        { level: 'info',    msg: `Format: ${form.format === 'auto' ? 'Parquet (detected)' : String(form.format).toUpperCase()}`, delay: 300 },
        { level: 'info',    msg: 'Reading metadata (row groups: 24)',                               delay: 500 },
        { level: 'success', msg: '34 fields · ~12.1M records estimated',                           delay: 500 },
      ]
    case 'sftp':
      return [
        { level: 'info',    msg: `Connecting to ${form.host || 'sftp.example.com'}:${form.port || 22}`, delay: 500 },
        { level: form.protocol === 'ftp' ? 'warn' : 'info', msg: form.protocol === 'ftp' ? 'FTP is unencrypted — consider SFTP' : 'SSH handshake complete', delay: 600 },
        { level: 'info',    msg: `Authenticating as '${form.sftpUser || 'user'}'`,                 delay: 500 },
        { level: 'info',    msg: `Listing ${form.path || '/exports/'}`,                            delay: 600 },
        { level: 'info',    msg: `23 files found matching ${form.filePattern || '*'}`,             delay: 400 },
        { level: 'info',    msg: 'Reading latest file for schema detection',                        delay: 600 },
        { level: 'success', msg: '8 fields detected · CSV with headers',                           delay: 400 },
      ]
    case 'bigquery':
      return [
        { level: 'info',    msg: 'Loading service account credentials',                             delay: 400 },
        { level: 'info',    msg: 'Authenticating with Google APIs',                                 delay: 600 },
        { level: 'info',    msg: `Accessing project '${form.project || 'my-project'}'`,            delay: 400 },
        { level: 'info',    msg: `Opening dataset '${form.dataset || 'analytics'}'`,               delay: 300 },
        { level: 'info',    msg: `Dry-run: SELECT * FROM \`${form.dataset || 'ds'}.${String(form.tableOrSql || 'events').split(/\s/)[0]}\``, delay: 700 },
        { level: 'info',    msg: 'Billed: 0 bytes (dry run) · ~890M rows estimated',               delay: 400 },
        { level: 'success', msg: '22 fields inferred · ready to sync',                             delay: 500 },
      ]
    case 'appinsights': {
      const tenant   = String(form.tenantId    || 'xxxxxxxx').slice(0, 8)
      const isWs     = form.mode === 'workspace'
      const resource = isWs
        ? String(form.workspaceId || 'xxxxxxxx').slice(0, 8)
        : String(form.appId       || 'xxxxxxxx').slice(0, 8)
      const scope    = isWs ? 'api.loganalytics.io/.default' : 'api.applicationinsights.io/.default'
      const endpoint = isWs ? `loganalytics.azure.com/workspaces/${resource}…` : `applicationinsights.io/apps/${resource}…`
      return [
        { level: 'info',    msg: `Resolving login.microsoftonline.com`,                                          delay: 400 },
        { level: 'info',    msg: `Requesting OAuth2 token (tenant: ${tenant}…, scope: ${scope})`,                delay: 700 },
        { level: 'success', msg: 'Azure AD token acquired · client_credentials flow',                            delay: 500 },
        { level: 'info',    msg: `Probing ${endpoint}`,                                                          delay: 600 },
        { level: 'info',    msg: 'KQL: requests | limit 1 → 200 OK',                                            delay: 500 },
        { level: 'success', msg: `Connection verified · ${isWs ? 'Azure Monitor Workspace' : 'App Insights'} KQL engine ready`, delay: 400 },
      ]
    }
    case 'azuremonitor': {
      const workspace = String(form.workspaceId || 'xxxxxxxx').slice(0, 8)
      return [
        { level: 'info',    msg: 'Resolving login.microsoftonline.com', delay: 400 },
        { level: 'info',    msg: `Requesting OAuth2 token for workspace ${workspace}…`, delay: 650 },
        { level: 'success', msg: 'Azure AD token acquired · client_credentials flow', delay: 450 },
        { level: 'info',    msg: 'KQL: requests | take 1 via Log Analytics', delay: 500 },
        { level: 'success', msg: 'Workspace query endpoint verified', delay: 400 },
      ]
    }
    case 'elasticsearch':
      return [
        { level: 'info', msg: `Connecting to ${String(form.endpoint || 'https://elastic.example.com')}`, delay: 420 },
        { level: 'info', msg: `Probing index pattern ${String(form.indexPattern || 'logs-*')}`, delay: 580 },
        { level: 'info', msg: 'POST /_search size:1', delay: 480 },
        { level: 'success', msg: 'Cluster responded · KQL compatibility mode ready', delay: 420 },
      ]
    case 'datadog':
      return [
        { level: 'info', msg: `Connecting to api.${String(form.site || 'datadoghq.com')}`, delay: 420 },
        { level: 'info', msg: `Preparing ${String(form.source || 'logs')} search probe`, delay: 540 },
        { level: 'info', msg: 'POST /api/v2/logs/events/search', delay: 480 },
        { level: 'success', msg: 'Datadog API responded · observability query runtime ready', delay: 420 },
      ]
    case 'mssql': {
      const host = String(form.host || 'sqlserver.example.com')
      const db = String(form.database || 'master')
      const hasEncrypt = Boolean(form.encrypt)
      return [
        { level: 'info',    msg: `Resolving ${host}:${String(form.port || '1433')}`,       delay: 400 },
        { level: 'info',    msg: 'TCP connection established',                               delay: 600 },
        { level: hasEncrypt ? 'info' : 'warn', msg: hasEncrypt ? 'TLS encryption enabled' : 'Encryption disabled — use encrypt=true in production', delay: 500 },
        { level: 'info',    msg: `Authenticating as '${String(form.dbUser || 'sa')}'`,     delay: 400 },
        { level: 'info',    msg: `Connected to database '${db}'`,                           delay: 300 },
        { level: 'info',    msg: `SELECT TOP 1 @@VERSION — SQL Server version detected`,   delay: 600 },
        { level: 'info',    msg: `Listing INFORMATION_SCHEMA.TABLES WHERE schema = '${String(form.schema || 'dbo')}'`, delay: 500 },
        { level: 'success', msg: 'SQL Server reachable · full schema browser ready',        delay: 500 },
      ]
    }
    case 'rabbitmq': {
      const host = String(form.host || 'rabbitmq.example.com')
      const mgmtPort = String(form.managementPort || '15672')
      return [
        { level: 'info',    msg: `Resolving ${host}:${mgmtPort} (management API)`,         delay: 400 },
        { level: 'info',    msg: 'GET /api/overview · authenticating',                      delay: 600 },
        { level: 'info',    msg: `vhost: ${String(form.vhost || '/')}`,                    delay: 300 },
        { level: 'info',    msg: 'Fetching queue list · exchange catalog',                  delay: 500 },
        { level: 'info',    msg: '8 queues · 3 exchanges discovered',                      delay: 400 },
        { level: 'success', msg: 'RabbitMQ management API reachable · queue browser ready', delay: 500 },
      ]
    }
    case 'mqtt': {
      const host = String(form.host || 'mqtt.example.com')
      const port = String(form.port || '1883')
      return [
        { level: 'info',    msg: `Resolving ${host}:${port}`,                              delay: 400 },
        { level: 'info',    msg: `CONNECT packet sent (clientId: datachef-probe)`,         delay: 600 },
        { level: 'info',    msg: 'CONNACK received — connection accepted',                  delay: 500 },
        { level: 'info',    msg: `SUBSCRIBE ${String(form.defaultTopic || '#')} QoS 1`,   delay: 400 },
        { level: 'info',    msg: 'Sampling messages (5s window)…',                         delay: 600 },
        { level: 'success', msg: 'MQTT broker reachable · topic browser ready',            delay: 500 },
      ]
    }
    default:
      return []
  }
}

/* ── Helpers ─────────────────────────────────────────────────────── */
function genSecret() {
  const arr = typeof crypto !== 'undefined' ? crypto.getRandomValues(new Uint8Array(24)) : new Uint8Array(24)
  return Array.from(arr).map(b => b.toString(16).padStart(2, '0')).join('')
}
function genSlug(name: string) {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || 'my-connector'
}

/* ── Default form values ─────────────────────────────────────────── */
const INIT_HTTP: HttpForm = {
  name: '', description: '', url: '', method: 'GET', auth: 'none',
  apiKeyHeader: 'X-API-Key', apiKeyValue: '', bearerToken: '',
  basicUser: '', basicPass: '', oauthTokenUrl: '', oauthClientId: '', oauthClientSecret: '', oauthScope: '',
  responsePath: '', format: 'json', schedule: 'manual',
  paginationType: 'none', cursorParam: 'cursor', cursorPath: 'next_cursor',
  pageParam: 'page', totalPath: 'total_pages', limitParam: 'limit', offsetParam: 'offset', limitValue: '100',
}
const makeInitDb = (port: string): DatabaseForm => ({
  name: '', description: '', host: '', port, database: '', dbUser: '', dbPass: '', ssl: true, sslMode: 'verify-full',
  tableOrQuery: '', syncMode: 'incremental', cursorColumn: 'updated_at', cursorType: 'timestamp',
  enableCdc: false, schedule: '1h', useConnectionString: false, connectionString: '', collection: '', filter: '',
})
const INIT_S3: S3Form = {
  name: '', description: '', provider: 'aws', bucket: '', region: 'us-east-1',
  endpoint: '', accessKeyId: '', secretAccessKey: '', prefix: '', format: 'auto', schedule: '1h',
}
const INIT_SFTP: SftpForm = {
  name: '', description: '', protocol: 'sftp', host: '', port: '22',
  sftpUser: '', authType: 'password', password: '', privateKey: '',
  path: '/', filePattern: '*', format: 'auto', schedule: '24h',
}
const INIT_BQ: BigQueryForm = {
  name: '', description: '', project: '', dataset: '', tableOrSql: '', serviceAccountJson: '', schedule: '1h',
}
const INIT_REDIS: RedisForm = {
  name: '', description: '', connectionMode: 'fields', connectionString: '',
  host: '', port: '6379', username: '', password: '', database: '0', tls: false,
  defaultQueryMode: 'command', defaultValueType: 'auto', defaultKeyPattern: '*', defaultSearchIndex: '', schedule: 'on-demand',
}
const INIT_MSSQL: MssqlForm = {
  name: '', description: '', connectionMode: 'fields', connectionString: '',
  host: '', port: '1433', database: 'master', dbUser: 'sa', dbPass: '',
  instanceName: '', encrypt: true, trustServerCertificate: false, schema: 'dbo',
  defaultCatalog: 'tables', schedule: 'on-demand',
}
const INIT_RABBIT: RabbitMQForm = {
  name: '', description: '', connectionMode: 'fields', connectionString: '',
  host: '', port: '5672', managementPort: '15672', vhost: '/', username: 'guest', password: 'guest',
  tls: false, managementTls: false, defaultCatalog: 'queues', schedule: 'on-demand',
}
const INIT_MQTT: MqttForm = {
  name: '', description: '', connectionMode: 'fields', connectionString: '',
  host: '', port: '1883', username: '', password: '', tls: false, protocol: 'mqtt',
  defaultTopic: '#', schedule: 'on-demand',
}
const INIT_AI: AppInsightsForm = {
  name: '', description: '', authMode: 'api_key', mode: 'workspace', connectionString: '', appId: '', apiKey: '', workspaceId: '', tenantId: '', clientId: '', clientSecret: '',
}
const INIT_AZURE_MONITOR: AzureMonitorForm = {
  name: '', description: '', workspaceId: '', tenantId: '', clientId: '', clientSecret: '',
  defaultQuery: 'requests\n| where timestamp > ago(24h)\n| summarize count() by bin(timestamp, 1h)\n| order by timestamp asc',
}
const INIT_ELASTIC: ElasticObservabilityForm = {
  name: '', description: '', endpoint: '', authType: 'basic', username: '', password: '', apiKey: '',
  indexPattern: 'logs-*',
  defaultQuery: 'logs\n| where @timestamp > ago(24h)\n| limit 100',
  schedule: 'on-demand',
}
const INIT_DATADOG: DatadogForm = {
  name: '', description: '', site: 'datadoghq.com', apiKey: '', applicationKey: '', source: 'logs',
  defaultQuery: 'logs\n| where status:error\n| limit 100',
  schedule: 'on-demand',
}
const INIT_AZURE_B2C: AzureB2CForm = {
  name: '', description: '', tenantId: '', clientId: '', authMode: 'client_secret', clientSecret: '',
  certificatePem: '', privateKeyPem: '', thumbprint: '', resource: 'users', schedule: 'on-demand',
}
const INIT_AZURE_ENTRA_ID: AzureEntraIdForm = {
  name: '', description: '', tenantId: '', clientId: '', authMode: 'client_secret', clientSecret: '',
  certificatePem: '', privateKeyPem: '', thumbprint: '', resource: 'users', schedule: 'on-demand',
}
const INIT_GITHUB: GitHubForm = {
  name: '', description: '', authMode: 'pat', token: '', oauthClientId: '', oauthClientSecret: '', appSlug: '', appId: '', appClientId: '', appClientSecret: '', appPrivateKey: '', transactionId: '', accountLogin: '', repoSearch: '',
  reposLoaded: false, selectedRepos: [], availableRepos: [], defaultResource: 'repos', pullRequestState: 'open', issueState: 'open', schedule: '1h',
}
const INIT_AZURE_DEVOPS: AzureDevOpsForm = {
  name: '', description: '', authMode: 'pat', organization: '', pat: '', tenantId: '', clientId: '', clientSecret: '', transactionId: '', accountName: '', authMessage: '',
  organizationsLoaded: false, availableOrganizations: [], projectsLoaded: false, availableProjects: [], selectedProjects: [], reposLoaded: false, availableRepos: [], selectedRepos: [], repoSearch: '',
  defaultResource: 'repositories', pullRequestState: 'active', workItemState: 'Active', branchFilter: '', commitDays: '30', pipelineRunDays: '14', schedule: '1h',
}

/* ── Shared form widgets ─────────────────────────────────────────── */
function Label({ children }: { children: React.ReactNode }) {
  return <label className="block text-xs font-semibold text-chef-text mb-1.5">{children}</label>
}
function FieldErr({ msg }: { msg?: string }) {
  if (!msg) return null
  return <p className="mt-1 text-[11px] text-rose-400 flex items-center gap-1"><AlertCircle size={10} />{msg}</p>
}
function FInput({ value, onChange, placeholder, type = 'text', error, disabled, className = '' }: {
  value: string; onChange: (v: string) => void; placeholder?: string
  type?: string; error?: string; disabled?: boolean; className?: string
}) {
  const [show, setShow] = useState(false)
  const isPass = type === 'password'
  return (
    <div className="relative">
      <input
        type={isPass && !show ? 'password' : 'text'} value={value}
        onChange={e => onChange(e.target.value)} placeholder={placeholder} disabled={disabled}
        className={`w-full bg-chef-bg border text-chef-text text-sm rounded-lg px-3 py-2 placeholder-chef-muted focus:outline-none focus:ring-1 transition-colors disabled:opacity-50 ${error ? 'border-rose-500/60 focus:border-rose-500 focus:ring-rose-500/20' : 'border-chef-border focus:border-indigo-500 focus:ring-indigo-500/30'} ${isPass ? 'pr-9' : ''} ${className}`}
      />
      {isPass && (
        <button type="button" onClick={() => setShow(s => !s)} className="absolute right-2.5 top-2.5 text-chef-muted hover:text-chef-text">
          {show ? <EyeOff size={14} /> : <Eye size={14} />}
        </button>
      )}
    </div>
  )
}
function FSelect({ value, onChange, error, children }: { value: string; onChange: (v: string) => void; error?: string; children: React.ReactNode }) {
  return (
    <select value={value} onChange={e => onChange(e.target.value)}
      className={`w-full bg-chef-bg border text-chef-text text-sm rounded-lg px-3 py-2 focus:outline-none focus:ring-1 transition-colors ${error ? 'border-rose-500/60' : 'border-chef-border focus:border-indigo-500 focus:ring-indigo-500/30'}`}>
      {children}
    </select>
  )
}
function FTextarea({ value, onChange, placeholder, rows = 5, error }: {
  value: string; onChange: (v: string) => void; placeholder?: string; rows?: number; error?: string
}) {
  return (
    <textarea value={value} onChange={e => onChange(e.target.value)} placeholder={placeholder} rows={rows}
      className={`w-full bg-chef-bg border text-chef-text text-xs font-mono rounded-lg px-3 py-2 placeholder-chef-muted focus:outline-none focus:ring-1 transition-colors resize-none ${error ? 'border-rose-500/60 focus:border-rose-500 focus:ring-rose-500/20' : 'border-chef-border focus:border-indigo-500 focus:ring-indigo-500/30'}`} />
  )
}
function Toggle({ value, onChange, label }: { value: boolean; onChange: (v: boolean) => void; label: string }) {
  return (
    <div className="flex items-center gap-3">
      <button onClick={() => onChange(!value)} className={`relative w-9 h-5 rounded-full transition-colors ${value ? 'bg-indigo-500' : 'bg-chef-border'}`}>
        <span className={`absolute top-0.5 left-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform ${value ? 'translate-x-4' : ''}`} />
      </button>
      <span className="text-sm text-chef-text">{label}</span>
    </div>
  )
}
function FieldRow({ label, hint, error, children }: { label: string; hint?: string; error?: string; children: React.ReactNode }) {
  return (
    <div>
      <Label>{label}</Label>
      {hint && <div className="text-[9px] text-chef-muted mb-1 -mt-0.5">{hint}</div>}
      {children}
      <FieldErr msg={error} />
    </div>
  )
}
function Divider({ label }: { label: string }) {
  return (
    <div className="flex items-center gap-3 py-1">
      <div className="text-[10px] font-semibold text-chef-muted uppercase tracking-wider whitespace-nowrap">{label}</div>
      <div className="flex-1 h-px bg-chef-border" />
    </div>
  )
}
function ScheduleSelect({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  return (
    <FSelect value={value} onChange={onChange}>
      <option value="on-demand">On demand</option>
      <option value="manual">Manual only</option>
      <option value="5min">Every 5 minutes</option>
      <option value="15min">Every 15 minutes</option>
      <option value="1h">Every hour</option>
      <option value="6h">Every 6 hours</option>
      <option value="24h">Daily</option>
    </FSelect>
  )
}
function AuthTabs({ value, onChange, types }: { value: string; onChange: (v: string) => void; types: Array<{ id: string; label: string }> }) {
  return (
    <div className="flex flex-wrap gap-1 p-1 bg-chef-bg rounded-lg border border-chef-border w-fit">
      {types.map(t => (
        <button key={t.id} onClick={() => onChange(t.id)}
          className={`px-2.5 py-1 rounded-md text-[11px] font-medium transition-colors ${value === t.id ? 'bg-indigo-500 text-white' : 'text-chef-muted hover:text-chef-text'}`}>
          {t.label}
        </button>
      ))}
    </div>
  )
}

async function readJsonSafely(response: Response): Promise<Record<string, unknown>> {
  const text = await response.text()
  try {
    return text ? JSON.parse(text) as Record<string, unknown> : {}
  } catch {
    const snippet = text.slice(0, 120).trim()
    throw new Error(snippet ? `Unexpected response from server: ${snippet}` : 'Unexpected empty response from server')
  }
}

/* ── Step 0: Type Selection ──────────────────────────────────────── */
type Category = 'All' | 'API' | 'Database' | 'Storage' | 'Warehouse' | 'Monitoring' | 'Identity' | 'Developer Tools'
const CATS: Category[] = ['All', 'API', 'Developer Tools', 'Database', 'Storage', 'Warehouse', 'Monitoring', 'Identity']

function TypeStep({ selected, onSelect }: { selected: ConnectorId | null; onSelect: (id: ConnectorId) => void }) {
  const [cat, setCat] = useState<Category>('All')
  const shown = cat === 'All' ? CONNECTORS : CONNECTORS.filter(c => c.category === cat)
  return (
    <div className="animate-fade-in">
      <p className="text-sm text-chef-muted mb-4">Choose a connector type. Each has its own configuration flow and validation.</p>
      <div className="flex flex-wrap gap-1.5 mb-4">
        {CATS.map(c => (
          <button key={c} onClick={() => setCat(c)}
            className={`px-3 py-1 rounded-full text-[11px] font-medium border transition-colors ${cat === c ? 'bg-indigo-500/15 text-indigo-400 border-indigo-500/30' : 'text-chef-muted border-transparent hover:text-chef-text'}`}>
            {c}
          </button>
        ))}
      </div>
      <div className="grid grid-cols-2 gap-2.5">
        {shown.map(({ id, label, desc, Icon, brandClass, color, bg, border, badge }) => (
          <button key={id} onClick={() => onSelect(id)}
            className={`group relative flex items-center gap-3 p-3.5 rounded-xl border text-left transition-all ${selected === id ? `${border} ${bg} ring-1 ring-indigo-500/30` : 'border-chef-border bg-chef-card hover:border-indigo-500/30'}`}>
            <div className={`w-9 h-9 rounded-lg flex items-center justify-center shrink-0 ${bg} border ${border}`}>
              <BrandIcon icon={Icon} brandClass={brandClass} size={17} className={color} />
            </div>
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2">
                <span className="text-sm font-semibold text-chef-text truncate">{label}</span>
                {badge && <span className="text-[9px] font-bold px-1.5 py-0.5 rounded-full bg-chef-bg border border-chef-border text-chef-muted shrink-0">{badge}</span>}
              </div>
              <div className="text-[11px] text-chef-muted mt-0.5 leading-tight">{desc}</div>
            </div>
            {selected === id && <CheckCircle2 size={14} className="text-indigo-400 shrink-0" />}
          </button>
        ))}
      </div>
    </div>
  )
}

/* ── Step 1 Configure forms ──────────────────────────────────────── */
function HttpConfigure({ form, set, errors }: { form: HttpForm; set: (f: HttpForm) => void; errors: FieldErrors }) {
  const f = <K extends keyof HttpForm>(k: K, v: HttpForm[K]) => set({ ...form, [k]: v })
  const authTypes = [{ id: 'none', label: 'None' }, { id: 'apikey', label: 'API Key' }, { id: 'bearer', label: 'Bearer' }, { id: 'basic', label: 'Basic' }, { id: 'oauth2', label: 'OAuth 2.0' }]
  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}><FInput value={form.name} onChange={v => f('name', v)} placeholder="e.g. Commerce API" error={errors.name} /></FieldRow>
      <FieldRow label="Endpoint URL" error={errors.url}><FInput value={form.url} onChange={v => f('url', v)} placeholder="https://api.example.com/v1/data" error={errors.url} /></FieldRow>
      <div className="grid grid-cols-2 gap-4">
        <FieldRow label="HTTP Method">
          <FSelect value={form.method} onChange={v => f('method', v as 'GET' | 'POST')}>
            <option value="GET">GET</option><option value="POST">POST</option>
          </FSelect>
        </FieldRow>
        <FieldRow label="Format">
          <FSelect value={form.format} onChange={v => f('format', v)}>
            <option value="json">JSON (auto)</option><option value="jsonl">JSONL</option><option value="csv">CSV</option>
          </FSelect>
        </FieldRow>
      </div>
      <Divider label="Authentication" />
      <AuthTabs value={form.auth} onChange={v => f('auth', v as HttpForm['auth'])} types={authTypes} />
      {form.auth === 'apikey' && (
        <div className="grid grid-cols-2 gap-3">
          <FieldRow label="Header Name"><FInput value={form.apiKeyHeader} onChange={v => f('apiKeyHeader', v)} placeholder="X-API-Key" /></FieldRow>
          <FieldRow label="Key Value" error={errors.apiKeyValue}><FInput type="password" value={form.apiKeyValue} onChange={v => f('apiKeyValue', v)} placeholder="sk-••••••" error={errors.apiKeyValue} /></FieldRow>
        </div>
      )}
      {form.auth === 'bearer' && (
        <FieldRow label="Bearer Token" error={errors.bearerToken}><FInput type="password" value={form.bearerToken} onChange={v => f('bearerToken', v)} placeholder="eyJ..." error={errors.bearerToken} /></FieldRow>
      )}
      {form.auth === 'basic' && (
        <div className="grid grid-cols-2 gap-3">
          <FieldRow label="Username" error={errors.basicUser}><FInput value={form.basicUser} onChange={v => f('basicUser', v)} placeholder="user" error={errors.basicUser} /></FieldRow>
          <FieldRow label="Password" error={errors.basicPass}><FInput type="password" value={form.basicPass} onChange={v => f('basicPass', v)} placeholder="••••••" error={errors.basicPass} /></FieldRow>
        </div>
      )}
      {form.auth === 'oauth2' && (
        <div className="space-y-3">
          <FieldRow label="Token URL" error={errors.oauthTokenUrl}><FInput value={form.oauthTokenUrl} onChange={v => f('oauthTokenUrl', v)} placeholder="https://auth.example.com/token" error={errors.oauthTokenUrl} /></FieldRow>
          <div className="grid grid-cols-2 gap-3">
            <FieldRow label="Client ID" error={errors.oauthClientId}><FInput value={form.oauthClientId} onChange={v => f('oauthClientId', v)} placeholder="client_id" error={errors.oauthClientId} /></FieldRow>
            <FieldRow label="Client Secret" error={errors.oauthClientSecret}><FInput type="password" value={form.oauthClientSecret} onChange={v => f('oauthClientSecret', v)} placeholder="••••••" error={errors.oauthClientSecret} /></FieldRow>
          </div>
          <FieldRow label="Scope (optional)"><FInput value={form.oauthScope} onChange={v => f('oauthScope', v)} placeholder="read:data" /></FieldRow>
        </div>
      )}
      <Divider label="Pagination" />
      <FieldRow label="Type">
        <FSelect value={form.paginationType} onChange={v => f('paginationType', v as HttpForm['paginationType'])}>
          <option value="none">None (single page)</option>
          <option value="cursor">Cursor-based</option>
          <option value="page">Page number</option>
          <option value="offset">Limit / Offset</option>
        </FSelect>
      </FieldRow>
      {form.paginationType === 'cursor' && (
        <div className="grid grid-cols-2 gap-3">
          <FieldRow label="Cursor param"><FInput value={form.cursorParam} onChange={v => f('cursorParam', v)} placeholder="cursor" /></FieldRow>
          <FieldRow label="Next path"><FInput value={form.cursorPath} onChange={v => f('cursorPath', v)} placeholder="$.next_cursor" /></FieldRow>
        </div>
      )}
      {form.paginationType === 'page' && (
        <div className="grid grid-cols-2 gap-3">
          <FieldRow label="Page param"><FInput value={form.pageParam} onChange={v => f('pageParam', v)} placeholder="page" /></FieldRow>
          <FieldRow label="Total pages path"><FInput value={form.totalPath} onChange={v => f('totalPath', v)} placeholder="$.total_pages" /></FieldRow>
        </div>
      )}
      {form.paginationType === 'offset' && (
        <div className="grid grid-cols-3 gap-3">
          <FieldRow label="Limit param"><FInput value={form.limitParam} onChange={v => f('limitParam', v)} placeholder="limit" /></FieldRow>
          <FieldRow label="Offset param"><FInput value={form.offsetParam} onChange={v => f('offsetParam', v)} placeholder="offset" /></FieldRow>
          <FieldRow label="Limit value"><FInput value={form.limitValue} onChange={v => f('limitValue', v)} placeholder="100" /></FieldRow>
        </div>
      )}
      <Divider label="Schedule" />
      <FieldRow label="Refresh Interval"><ScheduleSelect value={form.schedule} onChange={v => f('schedule', v)} /></FieldRow>
    </div>
  )
}

function WebhookConfigure({ form, set, errors }: { form: WebhookForm; set: (f: WebhookForm) => void; errors: FieldErrors }) {
  const f = <K extends keyof WebhookForm>(k: K, v: WebhookForm[K]) => set({ ...form, [k]: v })
  const [copied, setCopied] = useState(false)
  const slug = genSlug(form.name)
  const endpoint = `https://api.datachef.io/ingest/webhook/${slug}`
  function copy(s: string) { navigator.clipboard.writeText(s).catch(() => {}); setCopied(true); setTimeout(() => setCopied(false), 2000) }
  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}><FInput value={form.name} onChange={v => f('name', v)} placeholder="e.g. Stripe Events" error={errors.name} /></FieldRow>
      <FieldRow label="Description"><FInput value={form.description} onChange={v => f('description', v)} placeholder="What events does this webhook receive?" /></FieldRow>
      <Divider label="Generated Endpoint" />
      <div className="p-3.5 bg-chef-bg rounded-xl border border-chef-border space-y-3">
        <div>
          <div className="text-[10px] text-chef-muted uppercase tracking-wider mb-1.5">Inbound URL</div>
          <div className="flex items-center gap-2">
            <div className="flex-1 font-mono text-[11px] text-sky-400 bg-chef-card px-3 py-2 rounded-lg border border-chef-border overflow-x-auto whitespace-nowrap">{endpoint}</div>
            <button onClick={() => copy(endpoint)} className="shrink-0 p-2 rounded-lg border border-chef-border hover:bg-chef-card text-chef-muted hover:text-chef-text transition-colors">
              {copied ? <CheckCircle2 size={13} className="text-emerald-400" /> : <Copy size={13} />}
            </button>
          </div>
        </div>
        <div>
          <div className="flex items-center justify-between mb-1.5">
            <div className="text-[10px] text-chef-muted uppercase tracking-wider">HMAC-SHA256 Secret</div>
            <button onClick={() => f('secret', genSecret())} className="text-[10px] text-indigo-400 hover:text-indigo-300 transition-colors">Regenerate</button>
          </div>
          <FInput type="password" value={form.secret} onChange={v => f('secret', v)} error={errors.secret} />
          <FieldErr msg={errors.secret} />
        </div>
      </div>
      <Divider label="Event Filtering" />
      <FieldRow label="Event Filter">
        <FInput value={form.eventFilter} onChange={v => f('eventFilter', v)} placeholder="* (all) or payment.succeeded, refund.created" />
      </FieldRow>
      <div className="flex items-center gap-2 text-[11px] text-chef-muted">
        <Zap size={11} className="text-amber-400 shrink-0" />
        Use <code className="font-mono text-chef-text px-1 bg-chef-card rounded">*</code> for all events, or comma-separate specific types.
      </div>
      <Divider label="Security" />
      <Toggle value={form.replayProtection} onChange={v => f('replayProtection', v)} label="Replay attack protection" />
      {form.replayProtection && (
        <FieldRow label="Deduplication Window">
          <FSelect value={form.ttl} onChange={v => f('ttl', v as WebhookForm['ttl'])}>
            <option value="24h">24 hours</option><option value="48h">48 hours</option><option value="7d">7 days</option>
          </FSelect>
        </FieldRow>
      )}
    </div>
  )
}

function DatabaseConfigure({ form, set, errors, type }: { form: DatabaseForm; set: (f: DatabaseForm) => void; errors: FieldErrors; type: ConnectorId }) {
  const f = <K extends keyof DatabaseForm>(k: K, v: DatabaseForm[K]) => set({ ...form, [k]: v })
  const isMongo = type === 'mongodb'; const isPg = type === 'postgresql'
  const defaultPort = type === 'mysql' ? '3306' : type === 'mongodb' ? '27017' : '5432'
  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}>
        <FInput value={form.name} onChange={v => f('name', v)} placeholder={`e.g. ${isPg ? 'PostgreSQL Prod' : isMongo ? 'MongoDB Atlas' : 'MySQL Analytics'}`} error={errors.name} />
      </FieldRow>
      {isMongo && <Toggle value={form.useConnectionString} onChange={v => f('useConnectionString', v)} label="Use connection string" />}
      {isMongo && form.useConnectionString ? (
        <FieldRow label="Connection String" error={errors.connectionString}>
          <FInput value={form.connectionString} onChange={v => f('connectionString', v)} placeholder="mongodb+srv://user:pass@cluster0.mongodb.net/mydb" error={errors.connectionString} />
        </FieldRow>
      ) : (
        <>
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2">
              <FieldRow label="Host" error={errors.host}><FInput value={form.host} onChange={v => f('host', v)} placeholder={isMongo ? 'cluster0.mongodb.net' : isPg ? 'pg.example.com' : 'mysql.example.com'} error={errors.host} /></FieldRow>
            </div>
            <FieldRow label="Port" error={errors.port}><FInput value={form.port || defaultPort} onChange={v => f('port', v)} placeholder={defaultPort} error={errors.port} /></FieldRow>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <FieldRow label="Database" error={errors.database}><FInput value={form.database} onChange={v => f('database', v)} placeholder="mydb" error={errors.database} /></FieldRow>
            <FieldRow label="Username" error={errors.dbUser}><FInput value={form.dbUser} onChange={v => f('dbUser', v)} placeholder={isPg ? 'postgres' : 'root'} error={errors.dbUser} /></FieldRow>
          </div>
          <FieldRow label="Password" error={errors.dbPass}><FInput type="password" value={form.dbPass} onChange={v => f('dbPass', v)} placeholder="••••••••" error={errors.dbPass} /></FieldRow>
        </>
      )}
      {!isMongo && (
        <div className="space-y-3">
          <Toggle value={form.ssl} onChange={v => f('ssl', v)} label="SSL / TLS" />
          {form.ssl && isPg && (
            <FieldRow label="SSL Mode">
              <FSelect value={form.sslMode} onChange={v => f('sslMode', v)}>
                <option value="require">require</option>
                <option value="verify-ca">verify-ca</option>
                <option value="verify-full">verify-full (recommended)</option>
              </FSelect>
            </FieldRow>
          )}
        </div>
      )}
      <Divider label={isMongo ? 'Collection' : 'Data Selection'} />
      {isMongo ? (
        <>
          <FieldRow label="Collection" error={errors.collection}><FInput value={form.collection} onChange={v => f('collection', v)} placeholder="events" error={errors.collection} /></FieldRow>
          <FieldRow label="Filter (optional)"><FInput value={form.filter} onChange={v => f('filter', v)} placeholder='{ "status": "active" }' /></FieldRow>
        </>
      ) : (
        <FieldRow label="Table or SQL Query" error={errors.tableOrQuery}>
          <FInput value={form.tableOrQuery} onChange={v => f('tableOrQuery', v)} placeholder="orders — or — SELECT id, amount FROM orders" error={errors.tableOrQuery} />
        </FieldRow>
      )}
      <Divider label="Sync Mode" />
      <FieldRow label="Strategy">
        <FSelect value={form.syncMode} onChange={v => f('syncMode', v as 'full' | 'incremental')}>
          <option value="incremental">Incremental (cursor-based)</option>
          <option value="full">Full refresh</option>
        </FSelect>
      </FieldRow>
      {form.syncMode === 'incremental' && (
        <div className="grid grid-cols-2 gap-3">
          <FieldRow label="Cursor Column"><FInput value={form.cursorColumn} onChange={v => f('cursorColumn', v)} placeholder="updated_at" /></FieldRow>
          <FieldRow label="Cursor Type">
            <FSelect value={form.cursorType} onChange={v => f('cursorType', v)}>
              <option value="timestamp">Timestamp</option><option value="integer">Integer (auto-increment)</option>
            </FSelect>
          </FieldRow>
        </div>
      )}
      {isPg && <Toggle value={form.enableCdc} onChange={v => f('enableCdc', v)} label="Enable CDC via WAL replication" />}
      <Divider label="Schedule" />
      <FieldRow label="Sync Interval"><ScheduleSelect value={form.schedule} onChange={v => f('schedule', v)} /></FieldRow>
    </div>
  )
}

function S3Configure({ form, set, errors }: { form: S3Form; set: (f: S3Form) => void; errors: FieldErrors }) {
  const f = <K extends keyof S3Form>(k: K, v: S3Form[K]) => set({ ...form, [k]: v })
  const needsEndpoint = ['cloudflare', 'minio', 'other'].includes(form.provider)
  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}><FInput value={form.name} onChange={v => f('name', v)} placeholder="e.g. Data Lake S3" error={errors.name} /></FieldRow>
      <FieldRow label="Storage Provider">
        <FSelect value={form.provider} onChange={v => f('provider', v)}>
          <option value="aws">Amazon S3</option><option value="cloudflare">Cloudflare R2</option>
          <option value="gcs">Google Cloud Storage</option><option value="minio">MinIO</option><option value="other">Other S3-compatible</option>
        </FSelect>
      </FieldRow>
      {needsEndpoint && (
        <FieldRow label="Custom Endpoint URL" error={errors.endpoint}>
          <FInput value={form.endpoint} onChange={v => f('endpoint', v)} placeholder="https://your.r2.cloudflarestorage.com" error={errors.endpoint} />
        </FieldRow>
      )}
      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="Bucket Name" error={errors.bucket}><FInput value={form.bucket} onChange={v => f('bucket', v)} placeholder="my-data-bucket" error={errors.bucket} /></FieldRow>
        {form.provider === 'aws' && (
          <FieldRow label="Region">
            <FSelect value={form.region} onChange={v => f('region', v)}>
              {['us-east-1','us-east-2','us-west-2','eu-west-1','eu-central-1','ap-southeast-1','ap-northeast-1'].map(r => <option key={r} value={r}>{r}</option>)}
            </FSelect>
          </FieldRow>
        )}
      </div>
      <Divider label="Credentials" />
      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="Access Key ID" error={errors.accessKeyId}><FInput value={form.accessKeyId} onChange={v => f('accessKeyId', v)} placeholder="AKIA..." error={errors.accessKeyId} /></FieldRow>
        <FieldRow label="Secret Access Key" error={errors.secretAccessKey}><FInput type="password" value={form.secretAccessKey} onChange={v => f('secretAccessKey', v)} placeholder="••••••••" error={errors.secretAccessKey} /></FieldRow>
      </div>
      <Divider label="Data Selection" />
      <FieldRow label="Path Prefix (optional)"><FInput value={form.prefix} onChange={v => f('prefix', v)} placeholder="data/events/2024/" /></FieldRow>
      <FieldRow label="File Format">
        <FSelect value={form.format} onChange={v => f('format', v)}>
          <option value="auto">Auto-detect</option><option value="json">JSON</option><option value="jsonl">JSONL</option><option value="csv">CSV</option><option value="parquet">Parquet</option>
        </FSelect>
      </FieldRow>
      <Divider label="Schedule" />
      <FieldRow label="Sync Interval"><ScheduleSelect value={form.schedule} onChange={v => f('schedule', v)} /></FieldRow>
    </div>
  )
}

function SftpConfigure({ form, set, errors }: { form: SftpForm; set: (f: SftpForm) => void; errors: FieldErrors }) {
  const f = <K extends keyof SftpForm>(k: K, v: SftpForm[K]) => set({ ...form, [k]: v })
  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}><FInput value={form.name} onChange={v => f('name', v)} placeholder="e.g. Supplier SFTP" error={errors.name} /></FieldRow>
      <div className="grid grid-cols-3 gap-3">
        <FieldRow label="Protocol">
          <FSelect value={form.protocol} onChange={v => { const p = v as 'sftp' | 'ftp'; f('protocol', p); f('port', p === 'ftp' ? '21' : '22') }}>
            <option value="sftp">SFTP (secure)</option><option value="ftp">FTP (legacy)</option>
          </FSelect>
        </FieldRow>
        <div className="col-span-2 grid grid-cols-3 gap-3">
          <div className="col-span-2"><FieldRow label="Host" error={errors.host}><FInput value={form.host} onChange={v => f('host', v)} placeholder="sftp.example.com" error={errors.host} /></FieldRow></div>
          <FieldRow label="Port" error={errors.port}><FInput value={form.port} onChange={v => f('port', v)} placeholder={form.protocol === 'ftp' ? '21' : '22'} error={errors.port} /></FieldRow>
        </div>
      </div>
      <FieldRow label="Username" error={errors.sftpUser}><FInput value={form.sftpUser} onChange={v => f('sftpUser', v)} placeholder="sftp-user" error={errors.sftpUser} /></FieldRow>
      {form.protocol === 'ftp' && (
        <div className="flex items-center gap-2 p-3 bg-amber-500/5 border border-amber-500/20 rounded-lg text-[11px] text-amber-400">
          <AlertTriangle size={12} className="shrink-0" /> FTP transmits data unencrypted — SFTP is strongly recommended.
        </div>
      )}
      <Divider label="Authentication" />
      <FieldRow label="Auth Method">
        <FSelect value={form.authType} onChange={v => f('authType', v as 'password' | 'privatekey')}>
          <option value="password">Password</option><option value="privatekey">SSH Private Key</option>
        </FSelect>
      </FieldRow>
      {form.authType === 'password' ? (
        <FieldRow label="Password" error={errors.password}><FInput type="password" value={form.password} onChange={v => f('password', v)} placeholder="••••••••" error={errors.password} /></FieldRow>
      ) : (
        <div><Label>Private Key (PEM)</Label>
          <FTextarea value={form.privateKey} onChange={v => f('privateKey', v)} placeholder={'-----BEGIN OPENSSH PRIVATE KEY-----\n...\n-----END OPENSSH PRIVATE KEY-----'} rows={5} error={errors.privateKey} />
          <FieldErr msg={errors.privateKey} />
        </div>
      )}
      <Divider label="File Selection" />
      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="Remote Path" error={errors.path}><FInput value={form.path} onChange={v => f('path', v)} placeholder="/exports/data/" error={errors.path} /></FieldRow>
        <FieldRow label="File Pattern"><FInput value={form.filePattern} onChange={v => f('filePattern', v)} placeholder="*.json" /></FieldRow>
      </div>
      <FieldRow label="Format">
        <FSelect value={form.format} onChange={v => f('format', v)}>
          <option value="auto">Auto-detect</option><option value="json">JSON</option><option value="jsonl">JSONL</option><option value="csv">CSV</option>
        </FSelect>
      </FieldRow>
      <Divider label="Schedule" />
      <FieldRow label="Sync Interval"><ScheduleSelect value={form.schedule} onChange={v => f('schedule', v)} /></FieldRow>
    </div>
  )
}

function BigQueryConfigure({ form, set, errors }: { form: BigQueryForm; set: (f: BigQueryForm) => void; errors: FieldErrors }) {
  const f = <K extends keyof BigQueryForm>(k: K, v: BigQueryForm[K]) => set({ ...form, [k]: v })
  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}><FInput value={form.name} onChange={v => f('name', v)} placeholder="e.g. BigQuery Analytics" error={errors.name} /></FieldRow>
      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="Project ID" error={errors.project}><FInput value={form.project} onChange={v => f('project', v)} placeholder="my-gcp-project" error={errors.project} /></FieldRow>
        <FieldRow label="Dataset" error={errors.dataset}><FInput value={form.dataset} onChange={v => f('dataset', v)} placeholder="analytics" error={errors.dataset} /></FieldRow>
      </div>
      <FieldRow label="Table or SQL Query" error={errors.tableOrSql}>
        <FInput value={form.tableOrSql} onChange={v => f('tableOrSql', v)} placeholder="events  — or —  SELECT * FROM analytics.events" error={errors.tableOrSql} />
      </FieldRow>
      <Divider label="Service Account" />
      <div className="flex items-center gap-2 p-3 bg-blue-500/5 border border-blue-500/20 rounded-lg text-[11px] text-blue-400">
        <Key size={12} className="shrink-0" />
        Create a service account with <strong>BigQuery Data Viewer</strong> role and paste the JSON key below.
      </div>
      <div>
        <Label>Service Account JSON</Label>
        <FTextarea value={form.serviceAccountJson} onChange={v => f('serviceAccountJson', v)} placeholder={'{ "type": "service_account", "project_id": "...", ... }'} rows={6} error={errors.serviceAccountJson} />
        <FieldErr msg={errors.serviceAccountJson} />
      </div>
      <Divider label="Schedule" />
      <FieldRow label="Sync Interval"><ScheduleSelect value={form.schedule} onChange={v => f('schedule', v)} /></FieldRow>
    </div>
  )
}

function RedisConfigure({ form, set, errors }: { form: RedisForm; set: (f: RedisForm) => void; errors: FieldErrors }) {
  const f = <K extends keyof RedisForm>(k: K, v: RedisForm[K]) => set({ ...form, [k]: v })
  const usesConnectionString = form.connectionMode === 'connectionString'
  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}>
        <FInput value={form.name} onChange={v => f('name', v)} placeholder="e.g. Redis Cache Prod" error={errors.name} />
      </FieldRow>
      <FieldRow label="Description (optional)">
        <FInput value={form.description} onChange={v => f('description', v)} placeholder="Keys, streams, and module-backed queries" />
      </FieldRow>

      <Divider label="Connection" />
      <AuthTabs
        value={form.connectionMode}
        onChange={v => f('connectionMode', v as RedisForm['connectionMode'])}
        types={[{ id: 'fields', label: 'Host / Port' }, { id: 'connectionString', label: 'Connection String' }]}
      />
      {usesConnectionString ? (
        <FieldRow label="Connection String" error={errors.connectionString}>
          <FInput value={form.connectionString} onChange={v => f('connectionString', v)} placeholder="redis://user:pass@localhost:6379/0 or rediss://..." error={errors.connectionString} />
        </FieldRow>
      ) : (
        <>
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2">
              <FieldRow label="Host" error={errors.host}>
                <FInput value={form.host} onChange={v => f('host', v)} placeholder="localhost" error={errors.host} />
              </FieldRow>
            </div>
            <FieldRow label="Port" error={errors.port}>
              <FInput value={form.port} onChange={v => f('port', v)} placeholder="6379" error={errors.port} />
            </FieldRow>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <FieldRow label="Username (optional)">
              <FInput value={form.username} onChange={v => f('username', v)} placeholder="default" />
            </FieldRow>
            <FieldRow label="Password">
              <FInput type="password" value={form.password} onChange={v => f('password', v)} placeholder="••••••••" />
            </FieldRow>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <FieldRow label="Database">
              <FInput value={form.database} onChange={v => f('database', v)} placeholder="0" />
            </FieldRow>
            <div className="pt-6">
              <Toggle value={form.tls} onChange={v => f('tls', v)} label="TLS / rediss" />
            </div>
          </div>
        </>
      )}

      <Divider label="Defaults" />
      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="Default Mode">
          <FSelect value={form.defaultQueryMode} onChange={v => f('defaultQueryMode', v as RedisForm['defaultQueryMode'])}>
            <option value="command">Command</option>
            <option value="search">RediSearch</option>
            <option value="json">RedisJSON</option>
            <option value="timeseries">TimeSeries</option>
            <option value="stream">Streams</option>
            <option value="catalog">Catalog</option>
          </FSelect>
        </FieldRow>
        <FieldRow label="Default Value Type">
          <FSelect value={form.defaultValueType} onChange={v => f('defaultValueType', v as RedisForm['defaultValueType'])}>
            {['auto', 'string', 'hash', 'list', 'set', 'zset', 'json', 'timeseries', 'stream', 'search'].map(option => (
              <option key={option} value={option}>{option}</option>
            ))}
          </FSelect>
        </FieldRow>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="Default Key Pattern">
          <FInput value={form.defaultKeyPattern} onChange={v => f('defaultKeyPattern', v)} placeholder="user:*" />
        </FieldRow>
        <FieldRow label="Default Search Index" error={errors.defaultSearchIndex}>
          <FInput value={form.defaultSearchIndex} onChange={v => f('defaultSearchIndex', v)} placeholder="idx:users" error={errors.defaultSearchIndex} />
        </FieldRow>
      </div>

      <Divider label="Schedule" />
      <FieldRow label="Sync Interval">
        <ScheduleSelect value={form.schedule} onChange={v => f('schedule', v)} />
      </FieldRow>
    </div>
  )
}

/* ── CSV parser (minimal, handles quoted fields) ─────────────────── */
function parseCSV(text: string): Record<string, unknown>[] {
  const lines = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim().split('\n')
  if (lines.length < 2) return []
  function splitRow(line: string): string[] {
    const cols: string[] = []; let cur = ''; let inQ = false
    for (let i = 0; i < line.length; i++) {
      const ch = line[i]
      if (ch === '"') { if (inQ && line[i+1] === '"') { cur += '"'; i++ } else inQ = !inQ }
      else if (ch === ',' && !inQ) { cols.push(cur.trim()); cur = '' }
      else cur += ch
    }
    cols.push(cur.trim())
    return cols
  }
  const headers = splitRow(lines[0])
  return lines.slice(1).filter(l => l.trim()).map(line => {
    const vals = splitRow(line)
    const obj: Record<string, unknown> = {}
    headers.forEach((h, i) => { obj[h] = vals[i] ?? '' })
    return obj
  })
}

/* ── MSSQL Configure step ────────────────────────────────────────── */
function MssqlConfigure({ form, set, errors }: { form: MssqlForm; set: (f: MssqlForm) => void; errors: FieldErrors }) {
  const f = <K extends keyof MssqlForm>(k: K, v: MssqlForm[K]) => set({ ...form, [k]: v })
  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}>
        <FInput value={form.name} onChange={v => f('name', v)} placeholder="e.g. Production SQL Server" error={errors.name} />
      </FieldRow>
      <Toggle value={form.connectionMode === 'connectionString'} onChange={v => f('connectionMode', v ? 'connectionString' : 'fields')} label="Use connection string" />
      {form.connectionMode === 'connectionString' ? (
        <FieldRow label="Connection String" error={errors.connectionString}>
          <FInput value={form.connectionString} onChange={v => f('connectionString', v)} placeholder="Server=host,1433;Database=mydb;User Id=sa;Password=..." error={errors.connectionString} />
        </FieldRow>
      ) : (
        <>
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2">
              <FieldRow label="Host / Server" error={errors.host}>
                <FInput value={form.host} onChange={v => f('host', v)} placeholder="sqlserver.example.com" error={errors.host} />
              </FieldRow>
            </div>
            <FieldRow label="Port" error={errors.port}>
              <FInput value={form.port || '1433'} onChange={v => f('port', v)} placeholder="1433" error={errors.port} />
            </FieldRow>
          </div>
          <FieldRow label="Instance Name (optional)">
            <FInput value={form.instanceName} onChange={v => f('instanceName', v)} placeholder="SQLEXPRESS" />
          </FieldRow>
          <div className="grid grid-cols-2 gap-3">
            <FieldRow label="Database" error={errors.database}>
              <FInput value={form.database} onChange={v => f('database', v)} placeholder="master" error={errors.database} />
            </FieldRow>
            <FieldRow label="Schema">
              <FInput value={form.schema} onChange={v => f('schema', v)} placeholder="dbo" />
            </FieldRow>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <FieldRow label="Username" error={errors.dbUser}>
              <FInput value={form.dbUser} onChange={v => f('dbUser', v)} placeholder="sa" error={errors.dbUser} />
            </FieldRow>
            <FieldRow label="Password" error={errors.dbPass}>
              <FInput type="password" value={form.dbPass} onChange={v => f('dbPass', v)} placeholder="••••••••" error={errors.dbPass} />
            </FieldRow>
          </div>
          <div className="space-y-2">
            <Toggle value={form.encrypt} onChange={v => f('encrypt', v)} label="Encrypt connection (recommended)" />
            <Toggle value={form.trustServerCertificate} onChange={v => f('trustServerCertificate', v)} label="Trust server certificate (dev/self-signed)" />
          </div>
        </>
      )}
      <Divider label="Default Browser View" />
      <FieldRow label="Default Catalog">
        <FSelect value={form.defaultCatalog} onChange={v => f('defaultCatalog', v as MssqlForm['defaultCatalog'])}>
          <option value="tables">Tables</option>
          <option value="views">Views</option>
          <option value="columns">Columns</option>
          <option value="procedures">Stored Procedures</option>
          <option value="indexes">Indexes</option>
          <option value="databases">Databases</option>
        </FSelect>
      </FieldRow>
      <Divider label="Schedule" />
      <FieldRow label="Sync Interval"><ScheduleSelect value={form.schedule} onChange={v => f('schedule', v)} /></FieldRow>
    </div>
  )
}

/* ── RabbitMQ Configure step ─────────────────────────────────────── */
function RabbitMQConfigure({ form, set, errors }: { form: RabbitMQForm; set: (f: RabbitMQForm) => void; errors: FieldErrors }) {
  const f = <K extends keyof RabbitMQForm>(k: K, v: RabbitMQForm[K]) => set({ ...form, [k]: v })
  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}>
        <FInput value={form.name} onChange={v => f('name', v)} placeholder="e.g. Production RabbitMQ" error={errors.name} />
      </FieldRow>
      <Toggle value={form.connectionMode === 'connectionString'} onChange={v => f('connectionMode', v ? 'connectionString' : 'fields')} label="Use AMQP connection string" />
      {form.connectionMode === 'connectionString' ? (
        <FieldRow label="AMQP URL" error={errors.connectionString}>
          <FInput value={form.connectionString} onChange={v => f('connectionString', v)} placeholder="amqp://user:pass@host:5672/vhost" error={errors.connectionString} />
        </FieldRow>
      ) : (
        <>
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2">
              <FieldRow label="Host" error={errors.host}>
                <FInput value={form.host} onChange={v => f('host', v)} placeholder="rabbitmq.example.com" error={errors.host} />
              </FieldRow>
            </div>
            <FieldRow label="AMQP Port" error={errors.port}>
              <FInput value={form.port || '5672'} onChange={v => f('port', v)} placeholder="5672" error={errors.port} />
            </FieldRow>
          </div>
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2">
              <FieldRow label="Virtual Host">
                <FInput value={form.vhost} onChange={v => f('vhost', v)} placeholder="/" />
              </FieldRow>
            </div>
            <FieldRow label="Mgmt Port" error={errors.managementPort}>
              <FInput value={form.managementPort || '15672'} onChange={v => f('managementPort', v)} placeholder="15672" error={errors.managementPort} />
            </FieldRow>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <FieldRow label="Username">
              <FInput value={form.username} onChange={v => f('username', v)} placeholder="guest" />
            </FieldRow>
            <FieldRow label="Password">
              <FInput type="password" value={form.password} onChange={v => f('password', v)} placeholder="••••••••" />
            </FieldRow>
          </div>
          <div className="space-y-2">
            <Toggle value={form.tls} onChange={v => f('tls', v)} label="AMQP TLS (amqps://)" />
            <Toggle value={form.managementTls} onChange={v => f('managementTls', v)} label="Management API HTTPS" />
          </div>
        </>
      )}
      <Divider label="Default Browser View" />
      <FieldRow label="Default Catalog">
        <FSelect value={form.defaultCatalog} onChange={v => f('defaultCatalog', v as RabbitMQForm['defaultCatalog'])}>
          <option value="queues">Queues</option>
          <option value="exchanges">Exchanges</option>
          <option value="bindings">Bindings</option>
          <option value="vhosts">Virtual Hosts</option>
          <option value="connections">Connections</option>
          <option value="overview">Overview</option>
        </FSelect>
      </FieldRow>
      <Divider label="Schedule" />
      <FieldRow label="Sync Interval"><ScheduleSelect value={form.schedule} onChange={v => f('schedule', v)} /></FieldRow>
    </div>
  )
}

/* ── MQTT Configure step ─────────────────────────────────────────── */
function MqttConfigure({ form, set, errors }: { form: MqttForm; set: (f: MqttForm) => void; errors: FieldErrors }) {
  const f = <K extends keyof MqttForm>(k: K, v: MqttForm[K]) => set({ ...form, [k]: v })
  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}>
        <FInput value={form.name} onChange={v => f('name', v)} placeholder="e.g. IoT MQTT Broker" error={errors.name} />
      </FieldRow>
      <Toggle value={form.connectionMode === 'connectionString'} onChange={v => f('connectionMode', v ? 'connectionString' : 'fields')} label="Use connection string" />
      {form.connectionMode === 'connectionString' ? (
        <FieldRow label="MQTT URL" error={errors.connectionString}>
          <FInput value={form.connectionString} onChange={v => f('connectionString', v)} placeholder="mqtt://user:pass@host:1883" error={errors.connectionString} />
        </FieldRow>
      ) : (
        <>
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2">
              <FieldRow label="Broker Host" error={errors.host}>
                <FInput value={form.host} onChange={v => f('host', v)} placeholder="mqtt.example.com" error={errors.host} />
              </FieldRow>
            </div>
            <FieldRow label="Port" error={errors.port}>
              <FInput value={form.port || '1883'} onChange={v => f('port', v)} placeholder="1883" error={errors.port} />
            </FieldRow>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <FieldRow label="Username">
              <FInput value={form.username} onChange={v => f('username', v)} placeholder="optional" />
            </FieldRow>
            <FieldRow label="Password">
              <FInput type="password" value={form.password} onChange={v => f('password', v)} placeholder="optional" />
            </FieldRow>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <FieldRow label="Protocol">
              <FSelect value={form.protocol} onChange={v => f('protocol', v as MqttForm['protocol'])}>
                <option value="mqtt">mqtt://</option>
                <option value="mqtts">mqtts:// (TLS)</option>
                <option value="ws">ws:// (WebSocket)</option>
                <option value="wss">wss:// (WebSocket TLS)</option>
              </FSelect>
            </FieldRow>
            <div className="flex items-end">
              <Toggle value={form.tls} onChange={v => { f('tls', v); if (v) f('protocol', 'mqtts') }} label="TLS / SSL" />
            </div>
          </div>
        </>
      )}
      <Divider label="Default Topic" />
      <FieldRow label="Default Topic Filter">
        <FInput value={form.defaultTopic} onChange={v => f('defaultTopic', v)} placeholder="# (all topics), sensors/+/temperature" />
      </FieldRow>
      <Divider label="Schedule" />
      <FieldRow label="Sync Interval"><ScheduleSelect value={form.schedule} onChange={v => f('schedule', v)} /></FieldRow>
    </div>
  )
}

/* ── File Upload Configure step ───────────────────────────────────── */
const FMT_COLOR: Record<string, string> = { csv: 'text-emerald-400', json: 'text-sky-400', jsonl: 'text-violet-400' }
const FMT_BG:    Record<string, string> = { csv: 'bg-emerald-500/10 border-emerald-500/20', json: 'bg-sky-500/10 border-sky-500/20', jsonl: 'bg-violet-500/10 border-violet-500/20' }

function fmtBytes(n: number) {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n/1024).toFixed(1)} KB`
  return `${(n/1024/1024).toFixed(1)} MB`
}

function FileUploadConfigure({ form, set, errors }: { form: FileForm; set: (f: FileForm) => void; errors: FieldErrors }) {
  const [dragging, setDragging] = useState(false)
  const [parsing, setParsing]   = useState(false)
  const inputRef                = useRef<HTMLInputElement>(null)
  const ff = <K extends keyof FileForm>(k: K, v: FileForm[K]) => set({ ...form, [k]: v })

  function handleFile(file: File) {
    const ext = file.name.split('.').pop()?.toLowerCase() ?? ''
    const fmt: FileForm['format'] = ext === 'csv' ? 'csv' : (ext === 'jsonl' || ext === 'ndjson') ? 'jsonl' : 'json'
    setParsing(true)
    const reader = new FileReader()
    reader.onload = e => {
      const text = e.target?.result as string
      let rows: Record<string, unknown>[] = []
      let parseError = ''
      try {
        if      (fmt === 'csv')   rows = parseCSV(text)
        else if (fmt === 'jsonl') rows = text.trim().split('\n').filter(Boolean).map(l => JSON.parse(l))
        else { const p = JSON.parse(text); rows = Array.isArray(p) ? p : [p] }
      } catch (err) { parseError = err instanceof Error ? err.message : 'Parse failed' }
      const cols = rows.length > 0 ? Object.keys(rows[0]) : []
      const autoName = form.name || file.name.replace(/\.[^.]+$/, '').replace(/[-_]/g, ' ')
      set({ ...form, file, fileName: file.name, fileSize: file.size, format: fmt,
            parsedRows: rows.slice(0, 1000), detectedCols: cols, parseError, name: autoName })
      setParsing(false)
    }
    reader.onerror = () => { set({ ...form, file, fileName: file.name, fileSize: file.size, format: fmt,
      parsedRows: [], detectedCols: [], parseError: 'Failed to read file', name: form.name }); setParsing(false) }
    reader.readAsText(file)
  }

  return (
    <div className="space-y-4 animate-fade-in">
      {/* Drop zone */}
      <div
        onDragOver={e => { e.preventDefault(); setDragging(true) }}
        onDragLeave={e => { if (!e.currentTarget.contains(e.relatedTarget as Node)) setDragging(false) }}
        onDrop={e => { e.preventDefault(); setDragging(false); const f = e.dataTransfer.files[0]; if (f) handleFile(f) }}
        onClick={() => !parsing && inputRef.current?.click()}
        className={`relative border-2 border-dashed rounded-xl p-8 text-center cursor-pointer transition-all ${
          dragging           ? 'border-indigo-400 bg-indigo-500/8 scale-[1.01]' :
          form.parseError    ? 'border-rose-500/40 bg-rose-500/5' :
          form.file && !parsing ? 'border-emerald-500/40 bg-emerald-500/5' :
          'border-chef-border hover:border-indigo-500/40 hover:bg-indigo-500/5'
        }`}
      >
        <input ref={inputRef} type="file" className="hidden" accept=".csv,.json,.jsonl,.ndjson"
          onChange={e => { const f = e.target.files?.[0]; if (f) handleFile(f); e.target.value = '' }} />

        {parsing ? (
          <div className="flex flex-col items-center gap-2.5">
            <Loader2 size={28} className="text-indigo-400 animate-spin" />
            <span className="text-sm text-chef-muted">Parsing file…</span>
          </div>
        ) : form.file && !form.parseError ? (
          <div className="flex flex-col items-center gap-2">
            <div className="w-12 h-12 rounded-full bg-emerald-500/10 border border-emerald-500/20 flex items-center justify-center">
              <FileText size={22} className="text-emerald-400" />
            </div>
            <div className="text-sm font-semibold text-chef-text">{form.fileName}</div>
            <div className="flex items-center gap-2 flex-wrap justify-center text-[11px]">
              {form.format && (
                <span className={`px-2 py-0.5 rounded-full border font-mono font-semibold uppercase ${FMT_BG[form.format] ?? ''} ${FMT_COLOR[form.format] ?? ''}`}>
                  {form.format}
                </span>
              )}
              <span className="text-chef-muted">{fmtBytes(form.fileSize)}</span>
              <span className="text-chef-muted">{form.parsedRows.length.toLocaleString()} rows</span>
              <span className="text-chef-muted">{form.detectedCols.length} columns</span>
            </div>
            <span className="text-[11px] text-chef-muted mt-0.5">Click or drop to replace</span>
          </div>
        ) : (
          <div className="flex flex-col items-center gap-2.5">
            <div className="w-12 h-12 rounded-full bg-chef-card border border-chef-border flex items-center justify-center">
              <Upload size={22} className="text-chef-muted" />
            </div>
            <div>
              <div className="text-sm font-medium text-chef-text">Drop a file here or <span className="text-indigo-400">browse</span></div>
              <div className="text-[11px] text-chef-muted mt-0.5">CSV, JSON, JSONL · up to 50 MB</div>
            </div>
          </div>
        )}
      </div>

      {errors.file && (
        <p className="text-[11px] text-rose-400 flex items-center gap-1.5"><AlertCircle size={11} />{errors.file}</p>
      )}

      {/* Data preview table */}
      {form.parsedRows.length > 0 && form.detectedCols.length > 0 && (
        <div className="rounded-xl border border-chef-border overflow-hidden">
          <div className="flex items-center gap-2 px-3 py-2 bg-chef-card border-b border-chef-border">
            <CheckCircle2 size={11} className="text-emerald-400 shrink-0" />
            <span className="text-[11px] font-semibold text-chef-text">Data Preview</span>
            <span className="text-[10px] text-chef-muted ml-auto font-mono">
              {form.detectedCols.length} cols · {form.parsedRows.length.toLocaleString()} rows parsed
            </span>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-[11px] font-mono">
              <thead>
                <tr className="border-b border-chef-border bg-chef-bg">
                  {form.detectedCols.slice(0, 7).map(col => (
                    <th key={col} className="px-3 py-1.5 text-left text-chef-muted font-semibold whitespace-nowrap max-w-[140px] truncate">{col}</th>
                  ))}
                  {form.detectedCols.length > 7 && <th className="px-3 py-1.5 text-chef-muted text-left">+{form.detectedCols.length - 7} more</th>}
                </tr>
              </thead>
              <tbody>
                {form.parsedRows.slice(0, 5).map((row, i) => (
                  <tr key={i} className={`border-b border-chef-border/40 ${i % 2 ? 'bg-chef-bg/50' : ''}`}>
                    {form.detectedCols.slice(0, 7).map(col => (
                      <td key={col} className="px-3 py-1.5 text-chef-text-dim max-w-[140px] truncate">
                        {String(row[col] ?? '')}
                      </td>
                    ))}
                    {form.detectedCols.length > 7 && <td className="px-3 py-1.5 text-chef-muted">…</td>}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <FieldRow label="Connector Name" error={errors.name}>
        <FInput value={form.name} onChange={v => ff('name', v)} placeholder="e.g. Customer Churn Data" error={errors.name} />
      </FieldRow>
      <FieldRow label="Description (optional)">
        <FInput value={form.description} onChange={v => ff('description', v)} placeholder="What does this dataset contain?" />
      </FieldRow>
    </div>
  )
}

/* ── Step 2: Test Connection ─────────────────────────────────────── */
const LOG_COLORS = { info: 'text-chef-muted', success: 'text-emerald-400', warn: 'text-amber-400', error: 'text-rose-400' }
const LOG_PREFIX = { info: '│', success: '✓', warn: '⚠', error: '✗' }

function TestStep({ type, form, onJobUpdate }: {
  type: ConnectorId; form: Record<string, unknown>
  onJobUpdate: (status: 'running' | 'succeeded' | 'failed', progress: number, logs: ConnectorJob['logs']) => void
}) {
  const [status, setStatus] = useState<'running' | 'succeeded' | 'failed'>('running')
  const [progress, setProgress] = useState(0)
  const [logs, setLogs] = useState<ConnectorJob['logs']>([])
  const logsEnd = useRef<HTMLDivElement>(null)

  useEffect(() => {
    // Reset state on each run (handles React Strict Mode double-invoke in dev)
    setStatus('running')
    setProgress(0)
    setLogs([])

    const ctrl = new AbortController()
    const allLogs: ConnectorJob['logs'] = []

    ;(async () => {
      try {
        const res = await fetch('/api/connectors/test', {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ connectorType: type, ...form }),
          signal:  ctrl.signal,
        })
        if (!res.body) throw new Error('No response body')

        const reader  = res.body.getReader()
        const decoder = new TextDecoder()
        let buf = ''
        let eventCount = 0

        while (true) {
          const { done, value } = await reader.read()
          if (done) break
          buf += decoder.decode(value, { stream: true })
          const chunks = buf.split('\n\n')
          buf = chunks.pop() ?? ''

          for (const chunk of chunks) {
            const dataLine = chunk.split('\n').find(l => l.startsWith('data: '))
            if (!dataLine) continue
            try {
              const event = JSON.parse(dataLine.slice(6))
              if (event.type === 'log') {
                const log = { level: event.level as ConnectorJob['logs'][number]['level'], msg: event.msg as string }
                allLogs.push(log)
                eventCount++
                setLogs([...allLogs])
                logsEnd.current?.scrollIntoView({ behavior: 'smooth' })
                const p = Math.min(90, Math.round(eventCount * 11))
                setProgress(p)
                onJobUpdate('running', p, [...allLogs])
              } else if (event.type === 'done') {
                const finalStatus: 'succeeded' | 'failed' = event.ok ? 'succeeded' : 'failed'
                setStatus(finalStatus)
                setProgress(100)
                onJobUpdate(finalStatus, 100, allLogs)
              }
            } catch { /* skip malformed events */ }
          }
        }
      } catch (err: unknown) {
        if (ctrl.signal.aborted) return
        const msg = err instanceof Error ? err.message : String(err)
        const errLog = { level: 'error' as const, msg: `Test error: ${msg}` }
        allLogs.push(errLog)
        setLogs([...allLogs])
        setStatus('failed')
        onJobUpdate('failed', 0, allLogs)
      }
    })()

    return () => ctrl.abort()
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const bannerClass = status === 'running'
    ? 'bg-indigo-500/5 border-indigo-500/20'
    : status === 'succeeded'
      ? 'bg-emerald-500/5 border-emerald-500/20'
      : 'bg-rose-500/5 border-rose-500/20'

  return (
    <div className="animate-fade-in space-y-4">
      <div className={`flex items-center gap-3 p-3.5 rounded-xl border ${bannerClass}`}>
        {status === 'running'
          ? <Loader2 size={16} className="text-indigo-400 animate-spin shrink-0" />
          : status === 'succeeded'
            ? <CheckCircle2 size={16} className="text-emerald-400 shrink-0" />
            : <AlertCircle size={16} className="text-rose-400 shrink-0" />}
        <div className="flex-1">
          <div className="text-sm font-medium text-chef-text">
            {status === 'running'
              ? 'Testing connection…'
              : status === 'succeeded'
                ? 'Connection verified successfully'
                : 'Connection test failed'}
          </div>
          {status === 'running' && (
            <div className="mt-1.5 h-1 bg-chef-border rounded-full overflow-hidden">
              <div className="h-full bg-indigo-500 rounded-full transition-all duration-500" style={{ width: `${progress}%` }} />
            </div>
          )}
          {status === 'failed' && (
            <div className="mt-1 text-[11px] text-rose-300">
              Review the worker log below. You can go back to fix the config or continue anyway.
            </div>
          )}
        </div>
        <div className="text-[10px] font-mono text-chef-muted">{progress}%</div>
      </div>

      <div className="rounded-xl border border-chef-border bg-[#080a0d] overflow-hidden">
        <div className="flex items-center gap-2 px-3 py-2 border-b border-chef-border bg-chef-card">
          <div className="flex gap-1.5">
            {['bg-rose-500/60','bg-amber-500/60','bg-emerald-500/60'].map(c => <div key={c} className={`w-2.5 h-2.5 rounded-full ${c}`} />)}
          </div>
          <span className="text-[10px] font-mono text-chef-muted flex-1 text-center">worker · connection-test · {type}</span>
          <Terminal size={11} className="text-chef-muted" />
        </div>
        <div className="p-3 h-52 overflow-y-auto font-mono text-[11px] space-y-1">
          {logs.length === 0 && <span className="text-chef-muted">Initializing worker…</span>}
          {logs.map((log, i) => (
            <div key={i} className={`flex items-start gap-2 ${LOG_COLORS[log.level]}`}>
              <span className="shrink-0 opacity-60">{LOG_PREFIX[log.level]}</span>
              <span>{log.msg}</span>
            </div>
          ))}
          {status === 'running' && <span className="text-chef-muted animate-pulse">▊</span>}
          <div ref={logsEnd} />
        </div>
      </div>
    </div>
  )
}

/* ── Step 3: Done ────────────────────────────────────────────────── */
function DoneStep({ type, name, onView }: { type: ConnectorId; name: string; onView: () => void }) {
  const def = CONNECTORS.find(c => c.id === type)!
  const isWebhook = type === 'webhook'
  const isFile    = type === 'file'
  const slug = genSlug(name)
  return (
    <div className="animate-fade-in flex flex-col items-center text-center py-6 gap-5">
      <div className="w-16 h-16 rounded-full bg-emerald-500/10 border border-emerald-500/30 flex items-center justify-center">
        <CheckCircle2 size={32} className="text-emerald-400" />
      </div>
      <div>
        <h3 className="text-lg font-bold text-chef-text mb-1">Connector Created</h3>
        <p className="text-sm text-chef-muted">
          <span className="font-mono text-indigo-400 font-semibold">{name || 'New Connector'}</span> is ready.{' '}
          {isWebhook ? 'Start sending events to your endpoint.'
            : isFile ? 'Your file has been parsed and is ready to query.'
            : 'First sync will begin shortly.'}
        </p>
      </div>
      {isWebhook && (
        <div className="w-full max-w-sm p-3 rounded-xl border border-amber-500/20 bg-amber-500/5 text-left">
          <div className="text-[10px] text-amber-400 uppercase tracking-wider mb-1.5">Your Endpoint</div>
          <div className="font-mono text-[11px] text-sky-400 break-all">
            https://api.datachef.io/ingest/webhook/{slug}
          </div>
        </div>
      )}
      <div className="w-full max-w-sm p-4 rounded-xl border border-chef-border bg-chef-card text-left space-y-2">
        {[
          ['Type', <span key="t" className={`font-medium ${def.color}`}>{def.label}</span>],
          ['Status', <span key="s" className="text-emerald-400 flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse inline-block" />Connected</span>],
          ['Worker', <span key="w" className="font-mono text-chef-text">wkr-{Math.random().toString(36).slice(2, 8)}</span>],
        ].map(([k, v]) => (
          <div key={String(k)} className="flex items-center justify-between text-[11px]">
            <span className="text-chef-muted">{k}</span>{v}
          </div>
        ))}
      </div>
      <button onClick={onView} className="flex items-center gap-1.5 bg-indigo-600 hover:bg-indigo-500 text-white text-sm font-semibold px-6 py-2.5 rounded-lg transition-colors">
        View Connector <ChevronRight size={14} />
      </button>
    </div>
  )
}

/* ── App Insights Configure ──────────────────────────────────────── */
function AppInsightsConfigure({
  form, set, errors,
}: { form: AppInsightsForm; set: (f: AppInsightsForm) => void; errors: FieldErrors }) {
  const [showGuide, setShowGuide] = useState(false)
  const ff = <K extends keyof AppInsightsForm>(k: K, v: AppInsightsForm[K]) =>
    set({ ...form, [k]: v })

  const isApiKey = form.authMode === 'api_key'
  const isWorkspace = form.mode === 'workspace'
  const parsedConnection = parseAppInsightsConnectionString(form.connectionString)

  const guideSteps = isApiKey ? [
    { n: 1, text: <>Open your <b>Application Insights</b> resource → <b>API Access</b>.</> },
    { n: 2, text: <>Choose <b>Create API key</b> and enable read/query permissions.</> },
    { n: 3, text: <>Copy the generated <b>API key</b> value immediately. Azure only shows it once.</> },
    { n: 4, text: <>Open <b>Properties</b> and copy the <b>Application ID</b> (GUID).</> },
    { n: 5, text: <>Optional: paste the App Insights connection string here to auto-fill the Application ID when it includes <code className="bg-chef-card px-1 rounded">ApplicationId</code>.</> },
  ] : isWorkspace ? [
    { n: 1, text: <>In <b>Azure Portal</b> → <b>Microsoft Entra ID</b> → <b>App registrations</b> → <b>New registration</b>.</> },
    { n: 2, text: <>Open the registration → <b>Certificates &amp; secrets</b> → <b>New client secret</b>. Copy the <b>Value</b>.</> },
    { n: 3, text: <>Copy <b>Application (client) ID</b> and <b>Directory (tenant) ID</b> from the registration Overview page.</> },
    { n: 4, text: <>Open the linked <b>Log Analytics workspace</b> → <b>Overview</b> → copy the <b>Workspace ID</b>.</> },
    { n: 5, text: <>Grant the app registration <code className="bg-chef-card px-1 rounded">Log Analytics Reader</code> on that workspace.</> },
  ] : [
    { n: 1, text: <>In <b>Azure Portal</b> → <b>Microsoft Entra ID</b> → <b>App registrations</b> → <b>New registration</b>.</> },
    { n: 2, text: <>Open the registration → <b>Certificates &amp; secrets</b> → <b>New client secret</b>. Copy the <b>Value</b>.</> },
    { n: 3, text: <>Copy <b>Application (client) ID</b> and <b>Directory (tenant) ID</b> from the registration Overview page.</> },
    { n: 4, text: <>Open your <b>Application Insights</b> resource → <b>Properties</b> → copy the <b>Application ID</b>.</> },
    { n: 5, text: <>Grant the app registration <code className="bg-chef-card px-1 rounded">Monitoring Reader</code> on the App Insights resource.</> },
  ]

  return (
    <div className="space-y-4 animate-fade-in">

      <div className="space-y-1.5">
        <div className="text-[10px] uppercase tracking-wider text-chef-muted">Auth Flow</div>
        <div className="grid grid-cols-2 gap-2">
          <button
            type="button"
            onClick={() => ff('authMode', 'api_key')}
            className={`p-3 rounded-xl border text-left transition-all ${isApiKey ? 'border-cyan-500/60 bg-cyan-500/10' : 'border-chef-border bg-chef-bg hover:border-chef-border'}`}
          >
            <div className={`text-[11px] font-semibold mb-0.5 ${isApiKey ? 'text-cyan-400' : 'text-chef-muted'}`}>
              App Insights API key <span className="text-[9px] font-normal bg-emerald-500/20 text-emerald-400 px-1.5 py-0.5 rounded ml-1">Recommended</span>
            </div>
            <div className="text-[10px] text-chef-muted leading-snug">Application ID + API key against the App Insights Analytics endpoint</div>
          </button>
          <button
            type="button"
            onClick={() => ff('authMode', 'entra_client_secret')}
            className={`p-3 rounded-xl border text-left transition-all ${!isApiKey ? 'border-cyan-500/60 bg-cyan-500/10' : 'border-chef-border bg-chef-bg hover:border-chef-border'}`}
          >
            <div className={`text-[11px] font-semibold mb-0.5 ${!isApiKey ? 'text-cyan-400' : 'text-chef-muted'}`}>
              Entra OAuth <span className="text-[9px] font-normal bg-amber-500/20 text-amber-400 px-1.5 py-0.5 rounded ml-1">Legacy</span>
            </div>
            <div className="text-[10px] text-chef-muted leading-snug">Retained for older App Insights OAuth setups</div>
          </button>
        </div>
      </div>

      <FieldRow label="Connector Name" error={errors.name}>
        <FInput value={form.name} onChange={v => ff('name', v)} placeholder="e.g. Production App Insights" error={errors.name} />
      </FieldRow>
      <FieldRow label="Description (optional)">
        <FInput value={form.description} onChange={v => ff('description', v)} placeholder="What does this resource monitor?" />
      </FieldRow>

      <div className="border-t border-chef-border pt-4 space-y-3">
        <div className="text-[10px] uppercase tracking-wider text-chef-muted flex items-center gap-1.5"><Key size={10} /> Connector Credentials</div>
        <FieldRow
          label="Connection String (optional)"
          hint="Paste an App Insights connection string to auto-fill Application ID when ApplicationId is present."
          error={errors.connectionString}
        >
          <div className="space-y-2">
            <FTextarea
              value={form.connectionString}
              onChange={v => ff('connectionString', v)}
              rows={3}
              placeholder="InstrumentationKey=...;IngestionEndpoint=https://...;ApplicationId=..."
              error={errors.connectionString}
            />
            <div className="flex flex-wrap items-center gap-2 text-[10px] text-chef-muted">
              <button
                type="button"
                onClick={() => {
                  const parsed = parseAppInsightsConnectionString(form.connectionString)
                  if (parsed.applicationId) ff('appId', parsed.applicationId)
                }}
                className="px-2 py-1 rounded-md border border-chef-border hover:border-cyan-500/40 hover:text-chef-text transition-colors"
              >
                Use Application ID
              </button>
              {parsedConnection.applicationId && (
                <span className="text-emerald-400">Detected ApplicationId {parsedConnection.applicationId.slice(0, 8)}…</span>
              )}
              {!parsedConnection.applicationId && form.connectionString.trim() && (
                <span className="text-amber-400">No ApplicationId found in the connection string.</span>
              )}
            </div>
          </div>
        </FieldRow>

        <FieldRow label="Application ID" hint="App Insights resource → Properties → Application ID" error={errors.appId}>
          <FInput value={form.appId} onChange={v => ff('appId', v)} placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" error={errors.appId} />
        </FieldRow>

        {isApiKey ? (
          <FieldRow label="API Key" hint="App Insights → API Access → Create API key" error={errors.apiKey}>
            <FInput type="password" value={form.apiKey} onChange={v => ff('apiKey', v)} placeholder="••••••••" error={errors.apiKey} />
          </FieldRow>
        ) : (
          <>
            <div className="space-y-1.5">
              <div className="text-[10px] uppercase tracking-wider text-chef-muted">Legacy OAuth Mode</div>
              <div className="grid grid-cols-2 gap-2">
                <button
                  type="button"
                  onClick={() => ff('mode', 'workspace')}
                  className={`p-3 rounded-xl border text-left transition-all ${isWorkspace ? 'border-cyan-500/60 bg-cyan-500/10' : 'border-chef-border bg-chef-bg hover:border-chef-border'}`}
                >
                  <div className={`text-[11px] font-semibold mb-0.5 ${isWorkspace ? 'text-cyan-400' : 'text-chef-muted'}`}>Workspace</div>
                  <div className="text-[10px] text-chef-muted leading-snug">Query the linked Log Analytics workspace</div>
                </button>
                <button
                  type="button"
                  onClick={() => ff('mode', 'appinsights')}
                  className={`p-3 rounded-xl border text-left transition-all ${!isWorkspace ? 'border-cyan-500/60 bg-cyan-500/10' : 'border-chef-border bg-chef-bg hover:border-chef-border'}`}
                >
                  <div className={`text-[11px] font-semibold mb-0.5 ${!isWorkspace ? 'text-cyan-400' : 'text-chef-muted'}`}>App ID</div>
                  <div className="text-[10px] text-chef-muted leading-snug">Use the legacy App Insights OAuth endpoint</div>
                </button>
              </div>
            </div>
            {isWorkspace && (
              <FieldRow label="Workspace ID" hint="Log Analytics workspace → Overview → Workspace ID" error={errors.workspaceId}>
                <FInput value={form.workspaceId} onChange={v => ff('workspaceId', v)} placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" error={errors.workspaceId} />
              </FieldRow>
            )}
            <FieldRow label="Tenant ID" hint="Microsoft Entra ID → Directory (tenant) ID" error={errors.tenantId}>
              <FInput value={form.tenantId} onChange={v => ff('tenantId', v)} placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" error={errors.tenantId} />
            </FieldRow>
            <div className="grid grid-cols-2 gap-3">
              <FieldRow label="Client ID" hint="App registration → Application (client) ID" error={errors.clientId}>
                <FInput value={form.clientId} onChange={v => ff('clientId', v)} placeholder="App registration client_id" error={errors.clientId} />
              </FieldRow>
              <FieldRow label="Client Secret" error={errors.clientSecret}>
                <FInput type="password" value={form.clientSecret} onChange={v => ff('clientSecret', v)} placeholder="••••••••" error={errors.clientSecret} />
              </FieldRow>
            </div>
          </>
        )}
      </div>

      {/* How-to guide (collapsible) */}
      <div className="rounded-xl border border-chef-border overflow-hidden">
        <button
          type="button"
          onClick={() => setShowGuide(v => !v)}
          className="w-full flex items-center justify-between px-3 py-2.5 bg-chef-bg hover:bg-chef-card/50 transition-colors text-left"
        >
          <span className="flex items-center gap-2 text-[11px] text-chef-muted">
            <BarChart2 size={11} className="text-cyan-400" />
            How to set this up in Azure Portal
          </span>
          <ChevronRight size={11} className={`text-chef-muted transition-transform ${showGuide ? 'rotate-90' : ''}`} />
        </button>
        {showGuide && (
          <div className="px-3 pb-3 pt-1 bg-chef-bg border-t border-chef-border/50 space-y-2">
            {guideSteps.map(s => (
              <div key={s.n} className="flex gap-2.5 text-[11px]">
                <span className="shrink-0 w-4 h-4 rounded-full bg-cyan-500/20 text-cyan-400 text-[10px] font-bold flex items-center justify-center mt-0.5">{s.n}</span>
                <span className="text-chef-muted leading-snug">{s.text}</span>
              </div>
            ))}
            <div className="mt-2 pt-2 border-t border-chef-border/50 text-[10px] text-chef-muted">
              Role required:{' '}
              <code className="bg-chef-card px-1 rounded text-cyan-400">
                {isApiKey ? 'API key with read/query access' : isWorkspace ? 'Log Analytics Reader' : 'Monitoring Reader'}
              </code>
              {!isApiKey && <> on the {isWorkspace ? 'Log Analytics workspace' : 'App Insights resource'}.</>}
            </div>
          </div>
        )}
      </div>

      <div className="p-3 rounded-xl border border-amber-500/20 bg-amber-500/5 text-[11px] text-amber-300 flex items-start gap-2">
        <Shield size={12} className="mt-0.5 shrink-0 text-amber-400" />
        <span><span className="font-semibold text-amber-400">Secure: </span>Credentials are stored server-side only and never returned to the browser after this step. New App Insights connectors use API-key auth by default; Entra OAuth is retained only for legacy compatibility.</span>
      </div>
    </div>
  )
}

function AzureMonitorConfigure({
  form, set, errors,
}: { form: AzureMonitorForm; set: (f: AzureMonitorForm) => void; errors: FieldErrors }) {
  const ff = <K extends keyof AzureMonitorForm>(k: K, v: AzureMonitorForm[K]) => set({ ...form, [k]: v })
  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}>
        <FInput value={form.name} onChange={v => ff('name', v)} placeholder="e.g. Azure Monitor Prod" error={errors.name} />
      </FieldRow>
      <FieldRow label="Description (optional)">
        <FInput value={form.description} onChange={v => ff('description', v)} placeholder="Workspace-backed logs and metrics" />
      </FieldRow>
      <Divider label="Workspace" />
      <FieldRow label="Workspace ID" hint="Log Analytics workspace GUID" error={errors.workspaceId}>
        <FInput value={form.workspaceId} onChange={v => ff('workspaceId', v)} placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" error={errors.workspaceId} />
      </FieldRow>
      <Divider label="Authentication" />
      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="Tenant ID" error={errors.tenantId}>
          <FInput value={form.tenantId} onChange={v => ff('tenantId', v)} placeholder="Directory (tenant) ID" error={errors.tenantId} />
        </FieldRow>
        <FieldRow label="Client ID" error={errors.clientId}>
          <FInput value={form.clientId} onChange={v => ff('clientId', v)} placeholder="Application (client) ID" error={errors.clientId} />
        </FieldRow>
      </div>
      <FieldRow label="Client Secret" error={errors.clientSecret}>
        <FInput type="password" value={form.clientSecret} onChange={v => ff('clientSecret', v)} placeholder="••••••••" error={errors.clientSecret} />
      </FieldRow>
      <Divider label="Default Query" />
      <FieldRow label="KQL">
        <FTextarea value={form.defaultQuery} onChange={v => ff('defaultQuery', v)} rows={5} />
      </FieldRow>
    </div>
  )
}

function ElasticObservabilityConfigure({
  form, set, errors,
}: { form: ElasticObservabilityForm; set: (f: ElasticObservabilityForm) => void; errors: FieldErrors }) {
  const ff = <K extends keyof ElasticObservabilityForm>(k: K, v: ElasticObservabilityForm[K]) => set({ ...form, [k]: v })
  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}>
        <FInput value={form.name} onChange={v => ff('name', v)} placeholder="e.g. Elastic Logs" error={errors.name} />
      </FieldRow>
      <FieldRow label="Description (optional)">
        <FInput value={form.description} onChange={v => ff('description', v)} placeholder="Elasticsearch or OpenSearch cluster" />
      </FieldRow>
      <FieldRow label="Endpoint URL" error={errors.endpoint}>
        <FInput value={form.endpoint} onChange={v => ff('endpoint', v)} placeholder="https://elastic.example.com:9200" error={errors.endpoint} />
      </FieldRow>
      <FieldRow label="Index Pattern" error={errors.indexPattern}>
        <FInput value={form.indexPattern} onChange={v => ff('indexPattern', v)} placeholder="logs-*" error={errors.indexPattern} />
      </FieldRow>
      <Divider label="Authentication" />
      <AuthTabs value={form.authType} onChange={v => ff('authType', v as ElasticObservabilityForm['authType'])} types={[{ id: 'basic', label: 'Basic' }, { id: 'apikey', label: 'API Key' }]} />
      {form.authType === 'basic' ? (
        <div className="grid grid-cols-2 gap-3">
          <FieldRow label="Username" error={errors.username}>
            <FInput value={form.username} onChange={v => ff('username', v)} placeholder="elastic" error={errors.username} />
          </FieldRow>
          <FieldRow label="Password" error={errors.password}>
            <FInput type="password" value={form.password} onChange={v => ff('password', v)} placeholder="••••••••" error={errors.password} />
          </FieldRow>
        </div>
      ) : (
        <FieldRow label="API Key" error={errors.apiKey}>
          <FInput type="password" value={form.apiKey} onChange={v => ff('apiKey', v)} placeholder="base64-api-key" error={errors.apiKey} />
        </FieldRow>
      )}
      <Divider label="Defaults" />
      <FieldRow label="Default KQL">
        <FTextarea value={form.defaultQuery} onChange={v => ff('defaultQuery', v)} rows={4} />
      </FieldRow>
      <FieldRow label="Sync Interval">
        <ScheduleSelect value={form.schedule} onChange={v => ff('schedule', v)} />
      </FieldRow>
    </div>
  )
}

function DatadogConfigure({
  form, set, errors,
}: { form: DatadogForm; set: (f: DatadogForm) => void; errors: FieldErrors }) {
  const ff = <K extends keyof DatadogForm>(k: K, v: DatadogForm[K]) => set({ ...form, [k]: v })
  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}>
        <FInput value={form.name} onChange={v => ff('name', v)} placeholder="e.g. Datadog Logs" error={errors.name} />
      </FieldRow>
      <FieldRow label="Description (optional)">
        <FInput value={form.description} onChange={v => ff('description', v)} placeholder="Datadog observability queries" />
      </FieldRow>
      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="Site" error={errors.site}>
          <FSelect value={form.site} onChange={v => ff('site', v)} error={errors.site}>
            <option value="datadoghq.com">datadoghq.com</option>
            <option value="us3.datadoghq.com">us3.datadoghq.com</option>
            <option value="us5.datadoghq.com">us5.datadoghq.com</option>
            <option value="datadoghq.eu">datadoghq.eu</option>
            <option value="ddog-gov.com">ddog-gov.com</option>
          </FSelect>
        </FieldRow>
        <FieldRow label="Source">
          <FSelect value={form.source} onChange={v => ff('source', v as DatadogForm['source'])}>
            <option value="logs">Logs</option>
            <option value="events">Events</option>
          </FSelect>
        </FieldRow>
      </div>
      <Divider label="Authentication" />
      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="API Key" error={errors.apiKey}>
          <FInput type="password" value={form.apiKey} onChange={v => ff('apiKey', v)} placeholder="••••••••" error={errors.apiKey} />
        </FieldRow>
        <FieldRow label="Application Key" error={errors.applicationKey}>
          <FInput type="password" value={form.applicationKey} onChange={v => ff('applicationKey', v)} placeholder="••••••••" error={errors.applicationKey} />
        </FieldRow>
      </div>
      <Divider label="Defaults" />
      <FieldRow label="Default KQL">
        <FTextarea value={form.defaultQuery} onChange={v => ff('defaultQuery', v)} rows={4} />
      </FieldRow>
      <FieldRow label="Sync Interval">
        <ScheduleSelect value={form.schedule} onChange={v => ff('schedule', v)} />
      </FieldRow>
    </div>
  )
}

function AzureB2CConfigure({
  form, set, errors,
}: { form: AzureB2CForm; set: (f: AzureB2CForm) => void; errors: FieldErrors }) {
  const ff = <K extends keyof AzureB2CForm>(k: K, v: AzureB2CForm[K]) => set({ ...form, [k]: v })
  const isCertificate = form.authMode === 'client_certificate'

  return (
    <div className="space-y-4 animate-fade-in">
      <div className="p-3 rounded-xl border border-amber-500/20 bg-amber-500/5 text-[11px] text-amber-300 flex items-start gap-2">
        <AlertTriangle size={12} className="mt-0.5 shrink-0 text-amber-400" />
        <span><span className="font-semibold text-amber-400">Lifecycle notice: </span>Azure AD B2C is not available for new customers after May 1, 2025. This connector targets existing B2C tenants and keeps the runtime reusable for a future Entra External ID variant.</span>
      </div>

      <FieldRow label="Connector Name" error={errors.name}>
        <FInput value={form.name} onChange={v => ff('name', v)} placeholder="e.g. Contoso B2C Directory" error={errors.name} />
      </FieldRow>
      <FieldRow label="Description (optional)">
        <FInput value={form.description} onChange={v => ff('description', v)} placeholder="Users, user flows, and custom policies" />
      </FieldRow>

      <div className="border-t border-chef-border pt-4 space-y-3">
        <div className="text-[10px] uppercase tracking-wider text-chef-muted flex items-center gap-1.5"><Key size={10} /> Microsoft Graph Credentials</div>
        <FieldRow label="Tenant ID" hint="Azure AD B2C tenant ID / directory tenant ID" error={errors.tenantId}>
          <FInput value={form.tenantId} onChange={v => ff('tenantId', v)} placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" error={errors.tenantId} />
        </FieldRow>
        <FieldRow label="Client ID" hint="App registration application (client) ID" error={errors.clientId}>
          <FInput value={form.clientId} onChange={v => ff('clientId', v)} placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" error={errors.clientId} />
        </FieldRow>

        <div className="space-y-1.5">
          <div className="text-[10px] uppercase tracking-wider text-chef-muted">Auth Mode</div>
          <div className="grid grid-cols-2 gap-2">
            <button
              type="button"
              onClick={() => ff('authMode', 'client_secret')}
              className={`p-3 rounded-xl border text-left transition-all ${!isCertificate ? 'border-teal-500/60 bg-teal-500/10' : 'border-chef-border bg-chef-bg hover:border-chef-border'}`}
            >
              <div className={`text-[11px] font-semibold mb-0.5 ${!isCertificate ? 'text-teal-400' : 'text-chef-muted'}`}>Client Secret</div>
              <div className="text-[10px] text-chef-muted leading-snug">Standard client credentials flow</div>
            </button>
            <button
              type="button"
              onClick={() => ff('authMode', 'client_certificate')}
              className={`p-3 rounded-xl border text-left transition-all ${isCertificate ? 'border-teal-500/60 bg-teal-500/10' : 'border-chef-border bg-chef-bg hover:border-chef-border'}`}
            >
              <div className={`text-[11px] font-semibold mb-0.5 ${isCertificate ? 'text-teal-400' : 'text-chef-muted'}`}>Client Certificate</div>
              <div className="text-[10px] text-chef-muted leading-snug">PEM certificate and PEM private key</div>
            </button>
          </div>
        </div>

        {!isCertificate ? (
          <FieldRow label="Client Secret" error={errors.clientSecret}>
            <FInput type="password" value={form.clientSecret} onChange={v => ff('clientSecret', v)} placeholder="••••••••" error={errors.clientSecret} />
          </FieldRow>
        ) : (
          <div className="space-y-3">
            <FieldRow label="Certificate PEM" error={errors.certificatePem}>
              <FTextarea value={form.certificatePem} onChange={v => ff('certificatePem', v)} rows={4} placeholder="-----BEGIN CERTIFICATE-----" error={errors.certificatePem} />
            </FieldRow>
            <FieldRow label="Private Key PEM" error={errors.privateKeyPem}>
              <FTextarea value={form.privateKeyPem} onChange={v => ff('privateKeyPem', v)} rows={5} placeholder="-----BEGIN PRIVATE KEY-----" error={errors.privateKeyPem} />
            </FieldRow>
            <FieldRow label="Thumbprint (optional)" hint="Hex SHA-1 thumbprint if your app registration expects x5t">
              <FInput value={form.thumbprint} onChange={v => ff('thumbprint', v)} placeholder="A1B2C3..." />
            </FieldRow>
          </div>
        )}
      </div>

      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="Default Resource">
          <FSelect value={form.resource} onChange={v => ff('resource', v as AzureB2CForm['resource'])}>
            <option value="users">users</option>
            <option value="userFlows">userFlows</option>
            <option value="customPolicies">customPolicies</option>
          </FSelect>
        </FieldRow>
        <FieldRow label="Sync Interval">
          <FSelect value={form.schedule} onChange={v => ff('schedule', v)}>
            <option value="on-demand">On demand</option>
            <option value="1h">Every hour</option>
            <option value="6h">Every 6 hours</option>
            <option value="24h">Daily</option>
          </FSelect>
        </FieldRow>
      </div>

      <div className="rounded-xl border border-chef-border bg-chef-bg p-3 space-y-2">
        <div className="text-[11px] font-semibold text-chef-text">Supported dataset resources</div>
        <div className="flex flex-wrap gap-2 text-[10px]">
          {['users', 'userFlows', 'customPolicies'].map(resource => (
            <span key={resource} className="px-2 py-1 rounded-md bg-teal-500/10 border border-teal-500/20 text-teal-300 font-mono">{resource}</span>
          ))}
        </div>
        <div className="text-[10px] text-chef-muted leading-snug">Advanced paths are also allowed for these families only: <code className="bg-chef-card px-1 rounded">/users</code>, <code className="bg-chef-card px-1 rounded">/identity/b2cUserFlows</code>, <code className="bg-chef-card px-1 rounded">/trustFramework/policies</code>.</div>
        <div className="text-[10px] text-chef-muted leading-snug">Required Microsoft Graph app permissions: <code className="bg-chef-card px-1 rounded">User.Read.All</code>, <code className="bg-chef-card px-1 rounded">IdentityUserFlow.Read.All</code>, and <code className="bg-chef-card px-1 rounded">Policy.Read.All</code>.</div>
      </div>

      <div className="p-3 rounded-xl border border-sky-500/20 bg-sky-500/5 text-[11px] text-sky-200 flex items-start gap-2">
        <Shield size={12} className="mt-0.5 shrink-0 text-sky-400" />
        <span><span className="font-semibold text-sky-300">Beta note: </span><code className="bg-chef-card px-1 rounded">userFlows</code> and <code className="bg-chef-card px-1 rounded">customPolicies</code> use Microsoft Graph beta endpoints in v1. <code className="bg-chef-card px-1 rounded">users</code> stays on Graph v1.0.</span>
      </div>
    </div>
  )
}

function AzureEntraIdConfigure({
  form, set, errors,
}: { form: AzureEntraIdForm; set: (f: AzureEntraIdForm) => void; errors: FieldErrors }) {
  const ff = <K extends keyof AzureEntraIdForm>(k: K, v: AzureEntraIdForm[K]) => set({ ...form, [k]: v })
  const isCertificate = form.authMode === 'client_certificate'

  return (
    <div className="space-y-4 animate-fade-in">
      <div className="p-3 rounded-xl border border-sky-500/20 bg-sky-500/5 text-[11px] text-sky-200 flex items-start gap-2">
        <Shield size={12} className="mt-0.5 shrink-0 text-sky-400" />
        <span><span className="font-semibold text-sky-300">Scope: </span>Azure Entra ID reads Microsoft Graph <code className="bg-chef-card px-1 rounded">users</code>, <code className="bg-chef-card px-1 rounded">groups</code>, and <code className="bg-chef-card px-1 rounded">applications</code> using stable v1.0 endpoints.</span>
      </div>

      <FieldRow label="Connector Name" error={errors.name}>
        <FInput value={form.name} onChange={v => ff('name', v)} placeholder="e.g. Corporate Entra Directory" error={errors.name} />
      </FieldRow>
      <FieldRow label="Description (optional)">
        <FInput value={form.description} onChange={v => ff('description', v)} placeholder="Directory users, groups, and app registrations" />
      </FieldRow>

      <div className="border-t border-chef-border pt-4 space-y-3">
        <div className="text-[10px] uppercase tracking-wider text-chef-muted flex items-center gap-1.5"><Key size={10} /> Microsoft Graph Credentials</div>
        <FieldRow label="Tenant ID" hint="Microsoft Entra ID directory tenant ID" error={errors.tenantId}>
          <FInput value={form.tenantId} onChange={v => ff('tenantId', v)} placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" error={errors.tenantId} />
        </FieldRow>
        <FieldRow label="Client ID" hint="App registration application (client) ID" error={errors.clientId}>
          <FInput value={form.clientId} onChange={v => ff('clientId', v)} placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" error={errors.clientId} />
        </FieldRow>

        <div className="space-y-1.5">
          <div className="text-[10px] uppercase tracking-wider text-chef-muted">Auth Mode</div>
          <div className="grid grid-cols-2 gap-2">
            <button
              type="button"
              onClick={() => ff('authMode', 'client_secret')}
              className={`p-3 rounded-xl border text-left transition-all ${!isCertificate ? 'border-sky-500/60 bg-sky-500/10' : 'border-chef-border bg-chef-bg hover:border-chef-border'}`}
            >
              <div className={`text-[11px] font-semibold mb-0.5 ${!isCertificate ? 'text-sky-300' : 'text-chef-muted'}`}>Client Secret</div>
              <div className="text-[10px] text-chef-muted leading-snug">Standard app-only Microsoft Graph auth</div>
            </button>
            <button
              type="button"
              onClick={() => ff('authMode', 'client_certificate')}
              className={`p-3 rounded-xl border text-left transition-all ${isCertificate ? 'border-sky-500/60 bg-sky-500/10' : 'border-chef-border bg-chef-bg hover:border-chef-border'}`}
            >
              <div className={`text-[11px] font-semibold mb-0.5 ${isCertificate ? 'text-sky-300' : 'text-chef-muted'}`}>Client Certificate</div>
              <div className="text-[10px] text-chef-muted leading-snug">PEM certificate and PEM private key</div>
            </button>
          </div>
        </div>

        {!isCertificate ? (
          <FieldRow label="Client Secret" error={errors.clientSecret}>
            <FInput type="password" value={form.clientSecret} onChange={v => ff('clientSecret', v)} placeholder="••••••••" error={errors.clientSecret} />
          </FieldRow>
        ) : (
          <div className="space-y-3">
            <FieldRow label="Certificate PEM" error={errors.certificatePem}>
              <FTextarea value={form.certificatePem} onChange={v => ff('certificatePem', v)} rows={4} placeholder="-----BEGIN CERTIFICATE-----" error={errors.certificatePem} />
            </FieldRow>
            <FieldRow label="Private Key PEM" error={errors.privateKeyPem}>
              <FTextarea value={form.privateKeyPem} onChange={v => ff('privateKeyPem', v)} rows={5} placeholder="-----BEGIN PRIVATE KEY-----" error={errors.privateKeyPem} />
            </FieldRow>
            <FieldRow label="Thumbprint (optional)">
              <FInput value={form.thumbprint} onChange={v => ff('thumbprint', v)} placeholder="A1B2C3..." />
            </FieldRow>
          </div>
        )}
      </div>

      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="Default Resource">
          <FSelect value={form.resource} onChange={v => ff('resource', v as AzureEntraIdForm['resource'])}>
            <option value="users">users</option>
            <option value="groups">groups</option>
            <option value="applications">applications</option>
          </FSelect>
        </FieldRow>
        <FieldRow label="Sync Interval">
          <FSelect value={form.schedule} onChange={v => ff('schedule', v)}>
            <option value="on-demand">On demand</option>
            <option value="1h">Every hour</option>
            <option value="6h">Every 6 hours</option>
            <option value="24h">Daily</option>
          </FSelect>
        </FieldRow>
      </div>

      <div className="rounded-xl border border-chef-border bg-chef-bg p-3 space-y-2">
        <div className="text-[11px] font-semibold text-chef-text">Supported dataset resources</div>
        <div className="flex flex-wrap gap-2 text-[10px]">
          {['users', 'groups', 'applications'].map(resource => (
            <span key={resource} className="px-2 py-1 rounded-md bg-sky-500/10 border border-sky-500/20 text-sky-200 font-mono">{resource}</span>
          ))}
        </div>
        <div className="text-[10px] text-chef-muted leading-snug">Advanced paths are also allowed for these families only: <code className="bg-chef-card px-1 rounded">/users</code>, <code className="bg-chef-card px-1 rounded">/groups</code>, <code className="bg-chef-card px-1 rounded">/applications</code>.</div>
        <div className="text-[10px] text-chef-muted leading-snug">Recommended Microsoft Graph application permissions: <code className="bg-chef-card px-1 rounded">User.Read.All</code>, <code className="bg-chef-card px-1 rounded">Group.Read.All</code>, and <code className="bg-chef-card px-1 rounded">Application.Read.All</code>.</div>
      </div>
    </div>
  )
}

function GitHubConfigure({
  form, set, errors,
}: { form: GitHubForm; set: (f: GitHubForm) => void; errors: FieldErrors }) {
  const [loadingRepos, setLoadingRepos] = useState(false)
  const [authBusy, setAuthBusy] = useState<'oauth' | 'app' | null>(null)
  const [authMessage, setAuthMessage] = useState<string>('')
  const ff = <K extends keyof GitHubForm>(k: K, v: GitHubForm[K]) => set({ ...form, [k]: v })

  useEffect(() => {
    function handleMessage(event: MessageEvent) {
      if (event.origin !== window.location.origin) return
      const data = event.data as Record<string, unknown> | null
      if (!data || data.source !== 'datachef-github-auth') return
      setAuthBusy(null)
      if (typeof data.error === 'string' && data.error) {
        setAuthMessage(data.error)
        return
      }
      set({
        ...form,
        transactionId: String(data.transactionId ?? ''),
        accountLogin: String(data.login ?? ''),
        reposLoaded: false,
        availableRepos: [],
        selectedRepos: [],
      })
      setAuthMessage(`Authorized as ${String(data.login ?? 'GitHub user')}`)
    }

    window.addEventListener('message', handleMessage)
    return () => window.removeEventListener('message', handleMessage)
  }, [form, set])

  async function loadRepos() {
    if (form.authMode === 'pat' && !form.token.trim()) {
      setAuthMessage('Enter a GitHub personal access token first.')
      return
    }
    if (form.authMode !== 'pat' && !form.transactionId.trim()) {
      setAuthMessage('Complete GitHub authorization first.')
      return
    }
    setLoadingRepos(true)
    setAuthMessage('')
    try {
      const url = new URL('/api/connectors/github/catalog', window.location.origin)
      if (form.authMode !== 'pat') {
        url.searchParams.set('transactionId', form.transactionId)
      }

      if (form.authMode === 'pat') {
        const response = await fetch('/api/connectors/github/catalog', {
          method: 'GET',
          headers: {
            'x-datachef-github-pat': form.token,
          },
        })
        const payload = await readJsonSafely(response)
        if (!response.ok) throw new Error(String(payload.error ?? 'Failed to load repositories'))
        const repos = Array.isArray(payload.repos) ? payload.repos as GitHubRepoOption[] : []
        set({ ...form, reposLoaded: true, availableRepos: repos, selectedRepos: form.selectedRepos.filter(selected => repos.some(repo => repo.fullName === selected.fullName)) })
        return
      }

      const response = await fetch(url.toString())
      const payload = await readJsonSafely(response)
      if (!response.ok) throw new Error(String(payload.error ?? 'Failed to load repositories'))
      const repos = Array.isArray(payload.repos) ? payload.repos as GitHubRepoOption[] : []
      set({ ...form, reposLoaded: true, availableRepos: repos, selectedRepos: form.selectedRepos.filter(selected => repos.some(repo => repo.fullName === selected.fullName)) })
    } catch (error) {
      setAuthMessage(error instanceof Error ? error.message : String(error))
    } finally {
      setLoadingRepos(false)
    }
  }

  async function launchAuth(provider: 'oauth' | 'app') {
    setAuthBusy(provider)
    setAuthMessage('')
    try {
      const path = provider === 'oauth'
        ? '/api/connectors/github/oauth/start'
        : '/api/connectors/github/app/install/start'
      const payload = provider === 'oauth'
        ? {
            connectorName: form.name || 'GitHub',
            connectorDescription: form.description,
            clientId: form.oauthClientId,
            clientSecret: form.oauthClientSecret,
          }
        : {
            connectorName: form.name || 'GitHub',
            connectorDescription: form.description,
            appSlug: form.appSlug,
            appId: form.appId,
            clientId: form.appClientId,
            clientSecret: form.appClientSecret,
            privateKey: form.appPrivateKey,
          }
      const response = await fetch(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      const body = await readJsonSafely(response)
      if (!response.ok) throw new Error(String(body.error ?? 'Failed to start GitHub authorization'))
      const targetUrl = String(body.authorizeUrl ?? body.installUrl ?? '')
      if (!targetUrl) throw new Error('GitHub authorization URL was not returned')
      window.open(targetUrl, 'github-auth', 'popup=yes,width=640,height=760')
    } catch (error) {
      setAuthBusy(null)
      setAuthMessage(error instanceof Error ? error.message : String(error))
    }
  }

  const filteredRepos = form.availableRepos.filter(repo =>
    !form.repoSearch.trim() || repo.fullName.toLowerCase().includes(form.repoSearch.toLowerCase()),
  )
  const selectedSet = new Set(form.selectedRepos.map(repo => repo.fullName))
  const allFilteredReposSelected = filteredRepos.length > 0 && filteredRepos.every(repo => selectedSet.has(repo.fullName))

  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}>
        <FInput value={form.name} onChange={v => ff('name', v)} placeholder="e.g. Engineering GitHub" error={errors.name} />
      </FieldRow>
      <FieldRow label="Description (optional)">
        <FInput value={form.description} onChange={v => ff('description', v)} placeholder="Repos, pull requests, and issues for selected repositories" />
      </FieldRow>

      <Divider label="Authentication" />
      <AuthTabs
        value={form.authMode}
        onChange={v => ff('authMode', v as GitHubForm['authMode'])}
        types={[{ id: 'pat', label: 'PAT' }, { id: 'oauth', label: 'OAuth App' }, { id: 'app', label: 'GitHub App' }]}
      />

      {form.authMode === 'pat' ? (
        <div className="space-y-3">
          <FieldRow label="Personal Access Token" error={errors.token}>
            <FInput type="password" value={form.token} onChange={v => ff('token', v)} placeholder="github_pat_..." error={errors.token} />
          </FieldRow>
          <div className="text-[10px] text-chef-muted leading-snug">
            Use a fine-grained PAT with read access to repository metadata, pull requests, and issues for the repos you plan to sync.
          </div>
        </div>
      ) : (
        <div className="rounded-xl border border-chef-border bg-chef-bg p-4 space-y-3">
          {form.authMode === 'oauth' ? (
            <div className="grid grid-cols-2 gap-3">
              <FieldRow label="OAuth Client ID" error={errors.oauthClientId}>
                <FInput value={form.oauthClientId} onChange={v => ff('oauthClientId', v)} placeholder="Iv1..." error={errors.oauthClientId} />
              </FieldRow>
              <FieldRow label="OAuth Client Secret" error={errors.oauthClientSecret}>
                <FInput type="password" value={form.oauthClientSecret} onChange={v => ff('oauthClientSecret', v)} placeholder="••••••••" error={errors.oauthClientSecret} />
              </FieldRow>
            </div>
          ) : (
            <div className="space-y-3">
              <div className="grid grid-cols-2 gap-3">
                <FieldRow label="App Slug" error={errors.appSlug}>
                  <FInput value={form.appSlug} onChange={v => ff('appSlug', v)} placeholder="my-github-app" error={errors.appSlug} />
                </FieldRow>
                <FieldRow label="App ID" error={errors.appId}>
                  <FInput value={form.appId} onChange={v => ff('appId', v)} placeholder="123456" error={errors.appId} />
                </FieldRow>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <FieldRow label="App Client ID" error={errors.appClientId}>
                  <FInput value={form.appClientId} onChange={v => ff('appClientId', v)} placeholder="Iv1..." error={errors.appClientId} />
                </FieldRow>
                <FieldRow label="App Client Secret" error={errors.appClientSecret}>
                  <FInput type="password" value={form.appClientSecret} onChange={v => ff('appClientSecret', v)} placeholder="••••••••" error={errors.appClientSecret} />
                </FieldRow>
              </div>
              <FieldRow label="App Private Key (PEM)" error={errors.appPrivateKey}>
                <FTextarea value={form.appPrivateKey} onChange={v => ff('appPrivateKey', v)} rows={5} placeholder="-----BEGIN RSA PRIVATE KEY-----" error={errors.appPrivateKey} />
              </FieldRow>
            </div>
          )}
          <div className="text-[11px] text-chef-muted leading-snug">
            {form.authMode === 'oauth'
              ? 'Authorize dataChef to access repositories on the user’s behalf.'
              : 'Install the GitHub App on the repositories or organizations you want this workspace to read.'}
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <button
              type="button"
              onClick={() => launchAuth(form.authMode === 'oauth' ? 'oauth' : 'app')}
              className="px-3 py-2 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white text-sm font-semibold transition-colors"
            >
              {authBusy === form.authMode ? 'Waiting for GitHub…' : form.authMode === 'oauth' ? 'Authorize with GitHub' : 'Install GitHub App'}
            </button>
            {form.accountLogin && <span className="text-[11px] text-emerald-400">Authorized as {form.accountLogin}</span>}
          </div>
          <FieldErr msg={errors.transactionId} />
        </div>
      )}

      <Divider label="Repository Scope" />
      <div className="flex flex-wrap items-center gap-3">
        <button
          type="button"
          onClick={() => void loadRepos()}
          className="px-3 py-2 rounded-lg border border-chef-border hover:border-indigo-500/30 hover:bg-chef-bg text-sm text-chef-text transition-colors"
        >
          {loadingRepos ? 'Loading repositories…' : form.reposLoaded ? 'Refresh repositories' : 'Load repositories'}
        </button>
        {form.availableRepos.length > 0 && (
          <span className="text-[11px] text-chef-muted">{form.availableRepos.length} repositories available</span>
        )}
      </div>
      {authMessage && (
        <div className={`text-[11px] ${authMessage.toLowerCase().includes('authorized') ? 'text-emerald-400' : 'text-amber-300'}`}>
          {authMessage}
        </div>
      )}

      {form.reposLoaded && (
        <div className="space-y-3">
          <FieldRow label="Search repositories">
            <FInput value={form.repoSearch} onChange={v => ff('repoSearch', v)} placeholder="owner/repo" />
          </FieldRow>
          <div className="flex flex-wrap items-center gap-2 text-[11px]">
            <button
              type="button"
              onClick={() => {
                const next = allFilteredReposSelected
                  ? form.selectedRepos.filter(item => !filteredRepos.some(repo => repo.fullName === item.fullName))
                  : [
                      ...form.selectedRepos,
                      ...filteredRepos.filter(repo => !selectedSet.has(repo.fullName)),
                    ]
                set({ ...form, selectedRepos: next })
              }}
              className="px-2.5 py-1 rounded-md border border-chef-border hover:border-indigo-500/30 hover:bg-chef-bg text-chef-text transition-colors"
            >
              {allFilteredReposSelected ? 'Clear shown' : 'Select all shown'}
            </button>
            <button
              type="button"
              onClick={() => set({ ...form, selectedRepos: [] })}
              className="px-2.5 py-1 rounded-md border border-chef-border hover:border-indigo-500/30 hover:bg-chef-bg text-chef-muted hover:text-chef-text transition-colors"
            >
              Clear all
            </button>
            <span className="text-chef-muted">{form.selectedRepos.length} selected</span>
          </div>
          <div className="rounded-xl border border-chef-border overflow-hidden">
            <div className="max-h-64 overflow-y-auto divide-y divide-chef-border">
              {filteredRepos.map(repo => {
                const checked = selectedSet.has(repo.fullName)
                return (
                  <label key={repo.id} className="flex items-center gap-3 px-3 py-2.5 text-sm text-chef-text hover:bg-chef-bg cursor-pointer">
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={event => {
                        const next = event.target.checked
                          ? [...form.selectedRepos, repo]
                          : form.selectedRepos.filter(item => item.fullName !== repo.fullName)
                        set({ ...form, selectedRepos: next })
                      }}
                    />
                    <div className="flex-1 min-w-0">
                      <div className="font-medium truncate">{repo.fullName}</div>
                      <div className="text-[10px] text-chef-muted">{repo.ownerType} · {repo.visibility} · {repo.defaultBranch || 'default branch n/a'}</div>
                    </div>
                  </label>
                )
              })}
              {filteredRepos.length === 0 && (
                <div className="px-3 py-4 text-[11px] text-chef-muted">No repositories match the current search.</div>
              )}
            </div>
          </div>
          <FieldErr msg={errors.selectedRepos} />
        </div>
      )}

      <Divider label="Defaults" />
      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="Default Resource">
          <FSelect value={form.defaultResource} onChange={v => ff('defaultResource', v as GitHubForm['defaultResource'])}>
            <option value="repos">repos</option>
            <option value="pullRequests">pullRequests</option>
            <option value="issues">issues</option>
          </FSelect>
        </FieldRow>
        <FieldRow label="Sync Interval">
          <ScheduleSelect value={form.schedule} onChange={v => ff('schedule', v)} />
        </FieldRow>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="Pull Request State">
          <FSelect value={form.pullRequestState} onChange={v => ff('pullRequestState', v as GitHubForm['pullRequestState'])}>
            <option value="open">open</option>
            <option value="closed">closed</option>
            <option value="all">all</option>
          </FSelect>
        </FieldRow>
        <FieldRow label="Issue State">
          <FSelect value={form.issueState} onChange={v => ff('issueState', v as GitHubForm['issueState'])}>
            <option value="open">open</option>
            <option value="closed">closed</option>
            <option value="all">all</option>
          </FSelect>
        </FieldRow>
      </div>
    </div>
  )
}

function AzureDevOpsConfigure({
  form, set, errors,
}: { form: AzureDevOpsForm; set: (f: AzureDevOpsForm) => void; errors: FieldErrors }) {
  const [loadingCatalog, setLoadingCatalog] = useState<'organizations' | 'projects' | 'repos' | null>(null)
  const [authBusy, setAuthBusy] = useState(false)
  const ff = <K extends keyof AzureDevOpsForm>(k: K, v: AzureDevOpsForm[K]) => set({ ...form, [k]: v })

  useEffect(() => {
    function handleMessage(event: MessageEvent) {
      if (event.origin !== window.location.origin) return
      const data = event.data as Record<string, unknown> | null
      if (!data || data.source !== 'datachef-azuredevops-auth') return
      setAuthBusy(false)
      if (typeof data.error === 'string' && data.error) {
        set({ ...form, authMessage: data.error })
        return
      }
      set({
        ...form,
        transactionId: String(data.transactionId ?? ''),
        accountName: String(data.organization ?? ''),
        authMessage: `Authorized for ${String(data.organization ?? 'Azure DevOps organization')}`,
        organizationsLoaded: false,
        projectsLoaded: false,
        reposLoaded: false,
        availableOrganizations: [],
        availableProjects: [],
        availableRepos: [],
        selectedProjects: [],
        selectedRepos: [],
      })
    }

    window.addEventListener('message', handleMessage)
    return () => window.removeEventListener('message', handleMessage)
  }, [form, set])

  async function loadOrganizations() {
    if (form.authMode === 'pat' && (!form.pat.trim() || !form.organization.trim())) {
      set({ ...form, authMessage: 'Enter an organization and PAT first.' })
      return
    }
    if (form.authMode === 'entra' && !form.transactionId.trim()) {
      set({ ...form, authMessage: 'Complete Microsoft authorization first.' })
      return
    }
    setLoadingCatalog('organizations')
    try {
      let response: Response
      if (form.authMode === 'pat') {
        response = await fetch('/api/connectors/azuredevops/catalog', {
          headers: {
            'x-datachef-azuredevops-pat': form.pat,
            'x-datachef-azuredevops-organization': form.organization,
          },
        })
      } else {
        const url = new URL('/api/connectors/azuredevops/catalog', window.location.origin)
        url.searchParams.set('transactionId', form.transactionId)
        response = await fetch(url.toString())
      }
      const payload = await readJsonSafely(response)
      if (!response.ok) throw new Error(String(payload.error ?? 'Failed to load organizations'))
      const orgs = Array.isArray(payload.organizations) ? payload.organizations as AzureDevOpsOrganizationOption[] : []
      const selectedOrg = orgs.find(org => org.accountName.toLowerCase() === form.organization.toLowerCase())
      set({
        ...form,
        availableOrganizations: orgs,
        organizationsLoaded: true,
        organization: selectedOrg?.accountName ?? form.organization,
        authMessage: orgs.length > 0 ? `${orgs.length} accessible organizations discovered` : form.authMessage,
      })
    } catch (error) {
      set({ ...form, authMessage: error instanceof Error ? error.message : String(error) })
    } finally {
      setLoadingCatalog(null)
    }
  }

  async function loadProjects() {
    if (!form.organization.trim()) {
      set({ ...form, authMessage: 'Enter or select an organization first.' })
      return
    }
    setLoadingCatalog('projects')
    try {
      const url = new URL('/api/connectors/azuredevops/catalog', window.location.origin)
      url.searchParams.set('organization', form.organization)
      let response: Response
      if (form.authMode === 'pat') {
        response = await fetch(url.toString(), {
          headers: {
            'x-datachef-azuredevops-pat': form.pat,
            'x-datachef-azuredevops-organization': form.organization,
          },
        })
      } else {
        if (!form.transactionId.trim()) {
          set({ ...form, authMessage: 'Complete Microsoft authorization first.' })
          return
        }
        url.searchParams.set('transactionId', form.transactionId)
        response = await fetch(url.toString())
      }
      const payload = await readJsonSafely(response)
      if (!response.ok) throw new Error(String(payload.error ?? 'Failed to load projects'))
      const projects = Array.isArray(payload.projects) ? payload.projects as AzureDevOpsProjectOption[] : []
      set({
        ...form,
        projectsLoaded: true,
        availableProjects: projects,
        selectedProjects: form.selectedProjects.filter(selected => projects.some(project => project.id === selected.id)),
        reposLoaded: false,
        availableRepos: [],
        selectedRepos: [],
      })
    } catch (error) {
      set({ ...form, authMessage: error instanceof Error ? error.message : String(error) })
    } finally {
      setLoadingCatalog(null)
    }
  }

  async function loadRepos() {
    if (form.selectedProjects.length === 0) {
      set({ ...form, authMessage: 'Select at least one project first.' })
      return
    }
    setLoadingCatalog('repos')
    try {
      const url = new URL('/api/connectors/azuredevops/catalog', window.location.origin)
      url.searchParams.set('organization', form.organization)
      url.searchParams.set('projects', form.selectedProjects.map(project => project.id).join(','))
      let response: Response
      if (form.authMode === 'pat') {
        response = await fetch(url.toString(), {
          headers: {
            'x-datachef-azuredevops-pat': form.pat,
            'x-datachef-azuredevops-organization': form.organization,
          },
        })
      } else {
        if (!form.transactionId.trim()) {
          set({ ...form, authMessage: 'Complete Microsoft authorization first.' })
          return
        }
        url.searchParams.set('transactionId', form.transactionId)
        response = await fetch(url.toString())
      }
      const payload = await readJsonSafely(response)
      if (!response.ok) throw new Error(String(payload.error ?? 'Failed to load repositories'))
      const repos = Array.isArray(payload.repositories) ? payload.repositories as AzureDevOpsRepoOption[] : []
      set({
        ...form,
        reposLoaded: true,
        availableRepos: repos,
        selectedRepos: form.selectedRepos.filter(selected => repos.some(repo => repo.repositoryId === selected.repositoryId)),
      })
    } catch (error) {
      set({ ...form, authMessage: error instanceof Error ? error.message : String(error) })
    } finally {
      setLoadingCatalog(null)
    }
  }

  async function launchEntraAuth() {
    setAuthBusy(true)
    try {
      const response = await fetch('/api/connectors/azuredevops/entra/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          connectorName: form.name || 'Azure DevOps',
          connectorDescription: form.description,
          tenantId: form.tenantId,
          clientId: form.clientId,
          clientSecret: form.clientSecret,
          organization: form.organization,
        }),
      })
      const payload = await readJsonSafely(response)
      if (!response.ok) throw new Error(String(payload.error ?? 'Failed to start Azure DevOps authorization'))
      window.open(String(payload.authorizeUrl ?? ''), 'azuredevops-auth', 'popup=yes,width=640,height=760')
    } catch (error) {
      setAuthBusy(false)
      set({ ...form, authMessage: error instanceof Error ? error.message : String(error) })
    }
  }

  const filteredRepos = form.availableRepos.filter(repo =>
    !form.repoSearch.trim() || repo.fullName.toLowerCase().includes(form.repoSearch.toLowerCase()),
  )
  const selectedProjects = new Set(form.selectedProjects.map(project => project.id))
  const selectedRepos = new Set(form.selectedRepos.map(repo => repo.repositoryId))
  const allProjectsSelected = form.availableProjects.length > 0 && form.availableProjects.every(project => selectedProjects.has(project.id))
  const allFilteredReposSelected = filteredRepos.length > 0 && filteredRepos.every(repo => selectedRepos.has(repo.repositoryId))

  return (
    <div className="space-y-4 animate-fade-in">
      <FieldRow label="Connector Name" error={errors.name}>
        <FInput value={form.name} onChange={v => ff('name', v)} placeholder="e.g. Azure DevOps Engineering" error={errors.name} />
      </FieldRow>
      <FieldRow label="Description (optional)">
        <FInput value={form.description} onChange={v => ff('description', v)} placeholder="Repositories, pull requests, work items, and pipelines" />
      </FieldRow>

      <Divider label="Authentication" />
      <AuthTabs value={form.authMode} onChange={v => ff('authMode', v as AzureDevOpsForm['authMode'])} types={[{ id: 'pat', label: 'PAT' }, { id: 'entra', label: 'Microsoft Entra' }]} />
      <FieldRow label="Organization" hint="Organization name or https://dev.azure.com/{org}" error={errors.organization}>
        <FInput value={form.organization} onChange={v => ff('organization', v)} placeholder="contoso" error={errors.organization} />
      </FieldRow>
      <div className="flex flex-wrap items-center gap-3">
        <button type="button" onClick={() => void loadOrganizations()} className="px-3 py-2 rounded-lg border border-chef-border hover:border-indigo-500/30 hover:bg-chef-bg text-sm text-chef-text transition-colors">
          {loadingCatalog === 'organizations' ? 'Loading organizations…' : 'Discover organizations'}
        </button>
        {form.availableOrganizations.length > 0 && <span className="text-[11px] text-chef-muted">{form.availableOrganizations.length} organizations available</span>}
      </div>
      {form.organizationsLoaded && form.availableOrganizations.length > 0 && (
        <FieldRow label="Authorized Organizations">
          <FSelect value={form.organization} onChange={v => ff('organization', v)}>
            <option value="">Select organization</option>
            {form.availableOrganizations.map(org => <option key={org.accountId} value={org.accountName}>{org.accountName}</option>)}
          </FSelect>
        </FieldRow>
      )}

      {form.authMode === 'pat' ? (
        <FieldRow label="Personal Access Token" error={errors.pat}>
          <FInput type="password" value={form.pat} onChange={v => ff('pat', v)} placeholder="azdovspat..." error={errors.pat} />
        </FieldRow>
      ) : (
        <div className="rounded-xl border border-chef-border bg-chef-bg p-4 space-y-3">
          <div className="grid grid-cols-2 gap-3">
            <FieldRow label="Tenant ID" error={errors.tenantId}>
              <FInput value={form.tenantId} onChange={v => ff('tenantId', v)} placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" error={errors.tenantId} />
            </FieldRow>
            <FieldRow label="Client ID" error={errors.clientId}>
              <FInput value={form.clientId} onChange={v => ff('clientId', v)} placeholder="Application (client) ID" error={errors.clientId} />
            </FieldRow>
          </div>
          <FieldRow label="Client Secret" error={errors.clientSecret}>
            <FInput type="password" value={form.clientSecret} onChange={v => ff('clientSecret', v)} placeholder="••••••••" error={errors.clientSecret} />
          </FieldRow>
          <div className="flex flex-wrap items-center gap-3">
            <button type="button" onClick={() => void launchEntraAuth()} className="px-3 py-2 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white text-sm font-semibold transition-colors">
              {authBusy ? 'Waiting for Microsoft…' : 'Authorize with Microsoft'}
            </button>
            {form.accountName && <span className="text-[11px] text-emerald-400">Authorized for {form.accountName}</span>}
          </div>
          <FieldErr msg={errors.transactionId} />
        </div>
      )}

      {form.authMessage && <div className={`text-[11px] ${form.authMessage.toLowerCase().includes('authorized') ? 'text-emerald-400' : 'text-amber-300'}`}>{form.authMessage}</div>}

      <Divider label="Project Scope" />
      <div className="flex flex-wrap items-center gap-3">
        <button type="button" onClick={() => void loadProjects()} className="px-3 py-2 rounded-lg border border-chef-border hover:border-indigo-500/30 hover:bg-chef-bg text-sm text-chef-text transition-colors">
          {loadingCatalog === 'projects' ? 'Loading projects…' : form.projectsLoaded ? 'Refresh projects' : 'Load projects'}
        </button>
        {form.availableProjects.length > 0 && <span className="text-[11px] text-chef-muted">{form.availableProjects.length} projects available</span>}
      </div>
      {form.projectsLoaded && (
        <div className="space-y-3">
          <div className="flex flex-wrap items-center gap-2 text-[11px]">
            <button
              type="button"
              onClick={() => {
                const next = allProjectsSelected ? [] : form.availableProjects
                set({ ...form, selectedProjects: next, reposLoaded: false, availableRepos: [], selectedRepos: [] })
              }}
              className="px-2.5 py-1 rounded-md border border-chef-border hover:border-indigo-500/30 hover:bg-chef-bg text-chef-text transition-colors"
            >
              {allProjectsSelected ? 'Clear all projects' : 'Select all projects'}
            </button>
            <span className="text-chef-muted">{form.selectedProjects.length} selected</span>
          </div>
          <div className="rounded-xl border border-chef-border overflow-hidden">
            <div className="max-h-48 overflow-y-auto divide-y divide-chef-border">
              {form.availableProjects.map(project => (
                <label key={project.id} className="flex items-center gap-3 px-3 py-2.5 text-sm text-chef-text hover:bg-chef-bg cursor-pointer">
                  <input
                    type="checkbox"
                    checked={selectedProjects.has(project.id)}
                    onChange={event => {
                      const next = event.target.checked
                        ? [...form.selectedProjects, project]
                        : form.selectedProjects.filter(item => item.id !== project.id)
                      set({ ...form, selectedProjects: next, reposLoaded: false, availableRepos: [], selectedRepos: [] })
                    }}
                  />
                  <div className="flex-1 min-w-0">
                    <div className="font-medium truncate">{project.name}</div>
                    <div className="text-[10px] text-chef-muted">{project.visibility ?? 'visibility n/a'}{project.description ? ` · ${project.description}` : ''}</div>
                  </div>
                </label>
              ))}
            </div>
          </div>
        </div>
      )}
      <FieldErr msg={errors.selectedProjects} />

      <Divider label="Repository Scope" />
      <div className="flex flex-wrap items-center gap-3">
        <button type="button" onClick={() => void loadRepos()} className="px-3 py-2 rounded-lg border border-chef-border hover:border-indigo-500/30 hover:bg-chef-bg text-sm text-chef-text transition-colors">
          {loadingCatalog === 'repos' ? 'Loading repositories…' : form.reposLoaded ? 'Refresh repositories' : 'Load repositories'}
        </button>
        {form.availableRepos.length > 0 && <span className="text-[11px] text-chef-muted">{form.availableRepos.length} repositories available</span>}
      </div>
      {form.reposLoaded && (
        <div className="space-y-3">
          <FieldRow label="Search repositories">
            <FInput value={form.repoSearch} onChange={v => ff('repoSearch', v)} placeholder="project/repository" />
          </FieldRow>
          <div className="flex flex-wrap items-center gap-2 text-[11px]">
            <button
              type="button"
              onClick={() => {
                const next = allFilteredReposSelected
                  ? form.selectedRepos.filter(item => !filteredRepos.some(repo => repo.repositoryId === item.repositoryId))
                  : [
                      ...form.selectedRepos,
                      ...filteredRepos.filter(repo => !selectedRepos.has(repo.repositoryId)),
                    ]
                set({ ...form, selectedRepos: next })
              }}
              className="px-2.5 py-1 rounded-md border border-chef-border hover:border-indigo-500/30 hover:bg-chef-bg text-chef-text transition-colors"
            >
              {allFilteredReposSelected ? 'Clear shown' : 'Select all shown'}
            </button>
            <button
              type="button"
              onClick={() => set({ ...form, selectedRepos: [] })}
              className="px-2.5 py-1 rounded-md border border-chef-border hover:border-indigo-500/30 hover:bg-chef-bg text-chef-muted hover:text-chef-text transition-colors"
            >
              Clear all
            </button>
            <span className="text-chef-muted">{form.selectedRepos.length} selected</span>
          </div>
          <div className="rounded-xl border border-chef-border overflow-hidden">
            <div className="max-h-64 overflow-y-auto divide-y divide-chef-border">
              {filteredRepos.map(repo => (
                <label key={repo.repositoryId} className="flex items-center gap-3 px-3 py-2.5 text-sm text-chef-text hover:bg-chef-bg cursor-pointer">
                  <input
                    type="checkbox"
                    checked={selectedRepos.has(repo.repositoryId)}
                    onChange={event => {
                      const next = event.target.checked
                        ? [...form.selectedRepos, repo]
                        : form.selectedRepos.filter(item => item.repositoryId !== repo.repositoryId)
                      set({ ...form, selectedRepos: next })
                    }}
                  />
                  <div className="flex-1 min-w-0">
                    <div className="font-medium truncate">{repo.fullName}</div>
                    <div className="text-[10px] text-chef-muted">{repo.projectName} · {repo.defaultBranch || 'default branch n/a'}</div>
                  </div>
                </label>
              ))}
              {filteredRepos.length === 0 && (
                <div className="px-3 py-4 text-[11px] text-chef-muted">No repositories match the current search.</div>
              )}
            </div>
          </div>
        </div>
      )}
      <FieldErr msg={errors.selectedRepos} />

      <Divider label="Defaults" />
      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="Default Resource">
          <FSelect value={form.defaultResource} onChange={v => ff('defaultResource', v as AzureDevOpsForm['defaultResource'])}>
            <option value="repositories">repositories</option>
            <option value="commits">commits</option>
            <option value="pullRequests">pullRequests</option>
            <option value="branches">branches</option>
            <option value="workItems">workItems</option>
            <option value="pipelines">pipelines</option>
            <option value="pipelineRuns">pipelineRuns</option>
          </FSelect>
        </FieldRow>
        <FieldRow label="Sync Interval">
          <ScheduleSelect value={form.schedule} onChange={v => ff('schedule', v)} />
        </FieldRow>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <FieldRow label="Pull Request State">
          <FSelect value={form.pullRequestState} onChange={v => ff('pullRequestState', v as AzureDevOpsForm['pullRequestState'])}>
            <option value="active">active</option>
            <option value="completed">completed</option>
            <option value="abandoned">abandoned</option>
            <option value="all">all</option>
          </FSelect>
        </FieldRow>
        <FieldRow label="Work Item State">
          <FInput value={form.workItemState} onChange={v => ff('workItemState', v)} placeholder="Active" />
        </FieldRow>
      </div>
      <div className="grid grid-cols-3 gap-3">
        <FieldRow label="Branch Filter">
          <FInput value={form.branchFilter} onChange={v => ff('branchFilter', v)} placeholder="main" />
        </FieldRow>
        <FieldRow label="Commit Days">
          <FInput value={form.commitDays} onChange={v => ff('commitDays', v)} placeholder="30" />
        </FieldRow>
        <FieldRow label="Pipeline Run Days">
          <FInput value={form.pipelineRunDays} onChange={v => ff('pipelineRunDays', v)} placeholder="14" />
        </FieldRow>
      </div>
    </div>
  )
}

/* ── Stepper ─────────────────────────────────────────────────────── */
function Stepper({ steps, current }: { steps: string[]; current: number }) {
  return (
    <div className="flex items-center gap-0 px-6 py-3 border-b border-chef-border shrink-0">
      {steps.map((label, i) => (
        <div key={i} className="flex items-center flex-1 last:flex-none">
          <div className="flex flex-col items-center gap-1">
            <div className={`w-6 h-6 rounded-full flex items-center justify-center text-[11px] font-bold border-2 transition-all ${i < current ? 'bg-emerald-500 border-emerald-500 text-white' : i === current ? 'bg-indigo-500 border-indigo-500 text-white' : 'border-chef-border text-chef-muted'}`}>
              {i < current ? <CheckCircle2 size={12} /> : i + 1}
            </div>
            <span className={`text-[10px] font-medium whitespace-nowrap ${i === current ? 'text-indigo-400' : i < current ? 'text-emerald-400' : 'text-chef-muted'}`}>{label}</span>
          </div>
          {i < steps.length - 1 && <div className={`flex-1 h-px mx-2 mb-4 transition-colors ${i < current ? 'bg-emerald-500/40' : 'bg-chef-border'}`} />}
        </div>
      ))}
    </div>
  )
}

/* ── Main ConnectorWizard ────────────────────────────────────────── */
interface WizardProps {
  onClose: () => void
  onCreated: (conn: NewConnector, job: ConnectorJob) => void
  initialDraft?: DiscoveryConnectorDraft | null
}

export default function ConnectorWizard({ onClose, onCreated, initialDraft = null }: WizardProps) {
  const [step, setStep] = useState(0)
  const [type, setType] = useState<ConnectorId | null>(null)
  const [touched, setTouched] = useState(false)

  const [httpForm, setHttpForm]       = useState<HttpForm>(INIT_HTTP)
  const [webhookForm, setWebhookForm] = useState<WebhookForm>({ name: '', description: '', secret: '', eventFilter: '*', replayProtection: true, ttl: '24h' })
  const [dbForm, setDbForm]           = useState<DatabaseForm>(makeInitDb('5432'))
  const [s3Form, setS3Form]           = useState<S3Form>(INIT_S3)
  const [sftpForm, setSftpForm]       = useState<SftpForm>(INIT_SFTP)
  const [bqForm, setBqForm]           = useState<BigQueryForm>(INIT_BQ)
  const [redisForm, setRedisForm]     = useState<RedisForm>(INIT_REDIS)
  const [mssqlForm, setMssqlForm]     = useState<MssqlForm>(INIT_MSSQL)
  const [rabbitForm, setRabbitForm]   = useState<RabbitMQForm>(INIT_RABBIT)
  const [mqttForm, setMqttForm]       = useState<MqttForm>(INIT_MQTT)
  const [fileForm, setFileForm]       = useState<FileForm>({ name: '', description: '', file: null, fileName: '', fileSize: 0, format: '', parsedRows: [], detectedCols: [], parseError: '' })
  const [aiForm, setAiForm]           = useState<AppInsightsForm>(INIT_AI)
  const [azureMonitorForm, setAzureMonitorForm] = useState<AzureMonitorForm>(INIT_AZURE_MONITOR)
  const [elasticForm, setElasticForm] = useState<ElasticObservabilityForm>(INIT_ELASTIC)
  const [datadogForm, setDatadogForm] = useState<DatadogForm>(INIT_DATADOG)
  const [azureB2cForm, setAzureB2cForm] = useState<AzureB2CForm>(INIT_AZURE_B2C)
  const [azureEntraIdForm, setAzureEntraIdForm] = useState<AzureEntraIdForm>(INIT_AZURE_ENTRA_ID)
  const [githubForm, setGitHubForm] = useState<GitHubForm>(INIT_GITHUB)
  const [azureDevOpsForm, setAzureDevOpsForm] = useState<AzureDevOpsForm>(INIT_AZURE_DEVOPS)

  // Lazy-init secret on mount
  useEffect(() => { setWebhookForm(f => ({ ...f, secret: genSecret() })) }, [])

  useEffect(() => {
    if (!initialDraft) return

    setTouched(false)
    setType(initialDraft.type)
    setStep(1)

    if (initialDraft.type === 'postgresql') {
      setDbForm({
        ...makeInitDb('5432'),
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<DatabaseForm>),
      })
      return
    }

    if (initialDraft.type === 'mysql') {
      setDbForm({
        ...makeInitDb('3306'),
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<DatabaseForm>),
      })
      return
    }

    if (initialDraft.type === 'mongodb') {
      setDbForm({
        ...makeInitDb('27017'),
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<DatabaseForm>),
      })
      return
    }

    if (initialDraft.type === 'sftp') {
      setSftpForm({
        ...INIT_SFTP,
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<SftpForm>),
      })
      return
    }

    if (initialDraft.type === 'redis') {
      setRedisForm({
        ...INIT_REDIS,
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<RedisForm>),
      })
      return
    }

    if (initialDraft.type === 's3') {
      setS3Form({
        ...INIT_S3,
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<S3Form>),
      })
      return
    }

    if (initialDraft.type === 'http') {
      setHttpForm({
        ...INIT_HTTP,
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<HttpForm>),
      })
      return
    }

    if (initialDraft.type === 'bigquery') {
      setBqForm({
        ...INIT_BQ,
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<BigQueryForm>),
      })
      return
    }

    if (initialDraft.type === 'appinsights') {
      const creds = initialDraft.aiCredentials
      setAiForm({
        ...INIT_AI,
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<AppInsightsForm>),
        authMode: creds?.authMode === 'api_key' ? 'api_key' : 'entra_client_secret',
        mode: creds?.mode === 'workspace' ? 'workspace' : 'appinsights',
        appId: creds?.appId ?? '',
        apiKey: creds?.apiKey ?? '',
        workspaceId: creds?.workspaceId ?? '',
        tenantId: creds?.tenantId ?? '',
        clientId: creds?.clientId ?? '',
        clientSecret: creds?.clientSecret ?? '',
        connectionString: creds?.connectionString ?? '',
      })
      return
    }

    if (initialDraft.type === 'azuremonitor') {
      const creds = initialDraft.observabilityCredentials as Partial<{
        workspaceId: string
        tenantId: string
        clientId: string
        clientSecret: string
      }> | undefined
      setAzureMonitorForm({
        ...INIT_AZURE_MONITOR,
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<AzureMonitorForm>),
        workspaceId: creds?.workspaceId ?? String((initialDraft.runtimeConfig as Record<string, unknown> | undefined)?.workspaceId ?? ''),
        tenantId: creds?.tenantId ?? '',
        clientId: creds?.clientId ?? '',
        clientSecret: creds?.clientSecret ?? '',
      })
      return
    }

    if (initialDraft.type === 'elasticsearch') {
      const creds = initialDraft.observabilityCredentials as Partial<{
        endpoint: string
        authType: 'basic' | 'apikey'
        username: string
        password: string
        apiKey: string
        indexPattern: string
      }> | undefined
      setElasticForm({
        ...INIT_ELASTIC,
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<ElasticObservabilityForm>),
        endpoint: creds?.endpoint ?? initialDraft.endpoint ?? '',
        authType: creds?.authType === 'apikey' ? 'apikey' : 'basic',
        username: creds?.username ?? '',
        password: creds?.password ?? '',
        apiKey: creds?.apiKey ?? '',
        indexPattern: creds?.indexPattern ?? String((initialDraft.runtimeConfig as Record<string, unknown> | undefined)?.indexPattern ?? INIT_ELASTIC.indexPattern),
      })
      return
    }

    if (initialDraft.type === 'datadog') {
      const creds = initialDraft.observabilityCredentials as Partial<{
        site: string
        apiKey: string
        applicationKey: string
        source: 'logs' | 'events'
        defaultQuery: string
      }> | undefined
      setDatadogForm({
        ...INIT_DATADOG,
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<DatadogForm>),
        site: creds?.site ?? INIT_DATADOG.site,
        apiKey: creds?.apiKey ?? '',
        applicationKey: creds?.applicationKey ?? '',
        source: creds?.source === 'events' ? 'events' : 'logs',
        defaultQuery: creds?.defaultQuery ?? String((initialDraft.runtimeConfig as Record<string, unknown> | undefined)?.defaultQuery ?? INIT_DATADOG.defaultQuery),
      })
      return
    }

    if (initialDraft.type === 'azureb2c') {
      const creds = initialDraft.azureB2cCredentials
      setAzureB2cForm({
        ...INIT_AZURE_B2C,
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<AzureB2CForm>),
        tenantId: creds?.tenantId ?? '',
        clientId: creds?.clientId ?? '',
        authMode: creds?.authMode === 'client_certificate' ? 'client_certificate' : 'client_secret',
        clientSecret: creds?.clientSecret ?? '',
        certificatePem: creds?.certificatePem ?? '',
        privateKeyPem: creds?.privateKeyPem ?? '',
        thumbprint: creds?.thumbprint ?? '',
      })
      return
    }

    if (initialDraft.type === 'azureentraid') {
      const creds = initialDraft.azureEntraIdCredentials
      setAzureEntraIdForm({
        ...INIT_AZURE_ENTRA_ID,
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<AzureEntraIdForm>),
        tenantId: creds?.tenantId ?? '',
        clientId: creds?.clientId ?? '',
        authMode: creds?.authMode === 'client_certificate' ? 'client_certificate' : 'client_secret',
        clientSecret: creds?.clientSecret ?? '',
        certificatePem: creds?.certificatePem ?? '',
        privateKeyPem: creds?.privateKeyPem ?? '',
        thumbprint: creds?.thumbprint ?? '',
      })
      return
    }

    if (initialDraft.type === 'github') {
      setGitHubForm({
        ...INIT_GITHUB,
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<GitHubForm>),
      })
      return
    }

    if (initialDraft.type === 'azuredevops') {
      setAzureDevOpsForm({
        ...INIT_AZURE_DEVOPS,
        name: initialDraft.name,
        description: initialDraft.description,
        ...(initialDraft.runtimeConfig as Partial<AzureDevOpsForm>),
      })
    }
  }, [initialDraft])

  const jobRef = useRef<ConnectorJob>({
    id: `job-${Date.now()}`, connectorId: '', connectorName: '', connectorType: 'http',
    jobType: 'test', status: 'running', progress: 0, logs: [], startedAt: Date.now(),
  })

  const noTestTypes: (ConnectorId | null)[] = ['webhook', 'file']
  const steps = type
    ? noTestTypes.includes(type) ? ['Type', 'Configure', 'Done'] : ['Type', 'Configure', 'Test', 'Done']
    : ['Type', 'Configure', 'Test', 'Done']
  const isDone = step === steps.length - 1
  const isTest = !isDone && step === steps.length - 2 && !noTestTypes.includes(type)

  function currentForm(): AnyForm {
    switch (type) {
      case 'http': return httpForm
      case 'webhook': return webhookForm
      case 'postgresql': case 'mysql': case 'mongodb': return dbForm
      case 'redis': return redisForm
      case 'mssql': return mssqlForm
      case 'rabbitmq': return rabbitForm
      case 'mqtt': return mqttForm
      case 's3': return s3Form
      case 'sftp': return sftpForm
      case 'bigquery': return bqForm
      case 'file': return fileForm
      case 'appinsights': return aiForm
      case 'azuremonitor': return azureMonitorForm
      case 'elasticsearch': return elasticForm
      case 'datadog': return datadogForm
      case 'azureb2c': return azureB2cForm
      case 'azureentraid': return azureEntraIdForm
      case 'github': return githubForm
      case 'azuredevops': return azureDevOpsForm
      default: return httpForm
    }
  }

  function validate(): FieldErrors {
    if (!type) return {}
    switch (type) {
      case 'http': return validateHttp(httpForm)
      case 'webhook': return validateWebhook(webhookForm)
      case 'postgresql': case 'mysql': case 'mongodb': return validateDatabase(dbForm, type)
      case 'redis': return validateRedis(redisForm)
      case 'mssql': return validateMssql(mssqlForm)
      case 'rabbitmq': return validateRabbitMQ(rabbitForm)
      case 'mqtt': return validateMqtt(mqttForm)
      case 's3': return validateS3(s3Form)
      case 'sftp': return validateSftp(sftpForm)
      case 'bigquery': return validateBigQuery(bqForm)
      case 'file': return validateFile(fileForm)
      case 'appinsights': return validateAppInsights(aiForm)
      case 'azuremonitor': return validateAzureMonitor(azureMonitorForm)
      case 'elasticsearch': return validateElastic(elasticForm)
      case 'datadog': return validateDatadog(datadogForm)
      case 'azureb2c': return validateAzureB2C(azureB2cForm)
      case 'azureentraid': return validateAzureEntraId(azureEntraIdForm)
      case 'github': return validateGitHub(githubForm)
      case 'azuredevops': return validateAzureDevOps(azureDevOpsForm)
      default: return {}
    }
  }

  const errors = touched ? validate() : {}
  const hasErrors = Object.keys(validate()).length > 0

  async function advance() {
    if (step === 1) { // configure step
      setTouched(true)
      if (hasErrors) return
      // Register file as a queryable dataset before proceeding to Done
      if (type === 'file' && fileForm.parsedRows.length > 0) {
        try {
          await fetch('/api/datasets', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              name: fileForm.name,
              source: 'file',
              format: fileForm.format ? fileForm.format.toUpperCase() : 'JSON',
              schema: fileForm.detectedCols.map(field => {
                const sample = fileForm.parsedRows.slice(0, 20).map(r => r[field]).find(v => v != null)
                let colType = 'string'
                if (typeof sample === 'number') colType = Number.isInteger(sample) ? 'integer' : 'float'
                else if (typeof sample === 'boolean') colType = 'boolean'
                else if (typeof sample === 'string') {
                  if (/^\d{4}-\d{2}-\d{2}T/.test(sample)) colType = 'timestamp'
                  else if (/^\d{4}-\d{2}-\d{2}$/.test(sample)) colType = 'date'
                }
                return { field, type: colType, nullable: true, example: String(sample ?? '') }
              }),
              sampleRows: fileForm.parsedRows.slice(0, 5),
              totalRows: fileForm.parsedRows.length,
              description: fileForm.description || `File upload: ${fileForm.fileName}`,
            }),
          })
        } catch { /* non-fatal: connector still created */ }
      }
    }
    if (step === 0 && type) {
      // Reset db port on type change
      if (type === 'mysql') setDbForm(f => ({ ...f, port: '3306' }))
      if (type === 'mongodb') setDbForm(f => ({ ...f, port: '27017' }))
    }
    setStep(s => s + 1)
  }

  function handleJobUpdate(status: 'running' | 'succeeded' | 'failed', progress: number, logs: ConnectorJob['logs']) {
    jobRef.current = { ...jobRef.current, status, progress, logs }
  }

  function handleDone() {
    const f = currentForm() as unknown as Record<string, unknown>
    const name = String(f.name || 'New Connector')
    const ff = type === 'file' ? fileForm : null
    const parsedAiConnection = type === 'appinsights'
      ? parseAppInsightsConnectionString(aiForm.connectionString)
      : { applicationId: '', instrumentationKey: '', ingestionEndpoint: '' }
    const resolvedAiAppId = type === 'appinsights'
      ? (aiForm.appId.trim() || parsedAiConnection.applicationId)
      : ''
    const newConn: NewConnector = {
      id: `c-${Date.now()}`,
      name,
      type: type!,
      ...(initialDraft?.candidateId ? { sourceDiscoveryId: initialDraft.candidateId } : {}),
      ...(initialDraft?.existingConnectorId ? { existingConnectorId: initialDraft.existingConnectorId } : {}),
      endpoint: type === 'file'
        ? String(f.fileName || 'upload')
        : type === 'github'
        ? `github.com/${githubForm.selectedRepos[0]?.fullName ?? githubForm.accountLogin ?? 'selected-repos'}`
        : type === 'azuredevops'
        ? `dev.azure.com/${azureDevOpsForm.organization}/${azureDevOpsForm.selectedRepos[0]?.fullName ?? azureDevOpsForm.selectedProjects[0]?.name ?? 'selected-scope'}`
        : type === 'redis'
        ? redisForm.connectionMode === 'connectionString'
          ? redisForm.connectionString
          : `${redisForm.tls ? 'rediss' : 'redis'}://${redisForm.host}:${redisForm.port}/${redisForm.database || '0'}`
        : type === 'mssql'
        ? mssqlForm.connectionMode === 'connectionString'
          ? mssqlForm.connectionString
          : `${mssqlForm.host}:${mssqlForm.port || 1433}/${mssqlForm.database || 'master'}`
        : type === 'rabbitmq'
        ? rabbitForm.connectionMode === 'connectionString'
          ? rabbitForm.connectionString
          : `${rabbitForm.tls ? 'amqps' : 'amqp'}://${rabbitForm.host}:${rabbitForm.port || 5672}/${rabbitForm.vhost || '/'}`
        : type === 'mqtt'
        ? mqttForm.connectionMode === 'connectionString'
          ? mqttForm.connectionString
          : `${mqttForm.protocol || 'mqtt'}://${mqttForm.host}:${mqttForm.port || 1883}`
        : type === 'appinsights'
        ? aiForm.authMode === 'api_key'
          ? `api.applicationinsights.io/v1/apps/${resolvedAiAppId.slice(0, 8)}…`
          : aiForm.mode === 'workspace'
          ? `api.loganalytics.azure.com/v1/workspaces/${aiForm.workspaceId.slice(0, 8)}…`
          : `api.applicationinsights.io/v1/apps/${resolvedAiAppId.slice(0, 8)}…`
        : type === 'azuremonitor'
        ? `api.loganalytics.azure.com/v1/workspaces/${azureMonitorForm.workspaceId.slice(0, 8)}…`
        : type === 'elasticsearch'
        ? elasticForm.endpoint
        : type === 'datadog'
        ? `api.${datadogForm.site}/${datadogForm.source}`
        : type === 'azureb2c'
        ? `graph.microsoft.com/${azureB2cForm.resource}`
        : type === 'azureentraid'
        ? `graph.microsoft.com/${azureEntraIdForm.resource}`
        : String(f.url || f.host || f.bucket || f.project || (type === 'webhook' ? `wh://${genSlug(name)}` : '')),
      authMethod: String(
        type === 'webhook'    ? 'HMAC-SHA256' :
        type === 'http'       ? (f.auth || 'None') :
        type === 'file'       ? 'Direct upload' :
        type === 'github'     ? (githubForm.authMode === 'pat' ? 'Personal access token' : githubForm.authMode === 'oauth' ? 'GitHub OAuth' : 'GitHub App installation') :
        type === 'azuredevops'? (azureDevOpsForm.authMode === 'pat' ? 'Personal access token' : 'Microsoft Entra delegated auth') :
        type === 'redis'      ? 'Redis auth' :
        type === 'mssql'      ? (mssqlForm.encrypt ? 'TLS + password' : 'password') :
        type === 'rabbitmq'   ? 'username/password' :
        type === 'mqtt'       ? (mqttForm.username ? 'username/password' : 'anonymous') :
        type === 'appinsights'? (aiForm.authMode === 'api_key' ? 'App Insights API key' : 'OAuth2 client_credentials') :
        type === 'azuremonitor' ? 'OAuth2 client_credentials' :
        type === 'elasticsearch' ? (elasticForm.authType === 'apikey' ? 'API key' : 'Basic auth') :
        type === 'datadog' ? 'API key + application key' :
        type === 'azureb2c'   ? (azureB2cForm.authMode === 'client_certificate' ? 'OAuth2 client_credentials + certificate' : 'OAuth2 client_credentials + secret') :
        type === 'azureentraid' ? (azureEntraIdForm.authMode === 'client_certificate' ? 'OAuth2 client_credentials + certificate' : 'OAuth2 client_credentials + secret') :
        (type === 'postgresql' || type === 'mysql' || type === 'mongodb') ? (f.ssl ? 'TLS + password' : 'password') :
        type === 'sftp'       ? (f.authType === 'privatekey' ? 'SSH private key' : 'password') :
        type === 'bigquery'   ? 'Service account' :
        f.auth || f.authType || 'N/A'
      ),
      syncInterval: String(f.schedule || (type === 'webhook' ? 'real-time' : type === 'file' ? 'manual' : type === 'github' ? githubForm.schedule : type === 'azuredevops' ? azureDevOpsForm.schedule : type === 'appinsights' || type === 'azuremonitor' ? 'on-demand' : type === 'azureb2c' ? azureB2cForm.schedule : type === 'azureentraid' ? azureEntraIdForm.schedule : 'manual')),
      description: String(f.description || (
        ff ? `${String(ff.format).toUpperCase()} · ${ff.parsedRows.length.toLocaleString()} rows · ${ff.detectedCols.length} columns` :
        type === 'github'
          ? `GitHub ${githubForm.defaultResource} · ${githubForm.selectedRepos.length} selected repos`
        : type === 'azuredevops'
          ? `Azure DevOps ${azureDevOpsForm.defaultResource} · ${azureDevOpsForm.selectedProjects.length} projects · ${azureDevOpsForm.selectedRepos.length} repos`
        :
        type === 'redis'
          ? `Redis · ${redisForm.defaultQueryMode} · ${redisForm.defaultKeyPattern || '*'}`
        : type === 'mssql'
          ? `SQL Server · ${mssqlForm.database || 'master'} · schema: ${mssqlForm.schema || 'dbo'}`
        : type === 'rabbitmq'
          ? `RabbitMQ · vhost: ${rabbitForm.vhost || '/'} · default: ${rabbitForm.defaultCatalog}`
        : type === 'mqtt'
          ? `MQTT · topic: ${mqttForm.defaultTopic || '#'}`
        : type === 'appinsights'
          ? aiForm.authMode === 'api_key'
            ? `App Insights API key · app: ${resolvedAiAppId.slice(0, 8)}…`
            : aiForm.mode === 'workspace'
            ? `Azure Monitor · workspace: ${aiForm.workspaceId.slice(0, 8)}…`
            : `Azure App Insights · app: ${resolvedAiAppId.slice(0, 8)}…`
          : type === 'azuremonitor'
            ? `Azure Monitor · workspace: ${azureMonitorForm.workspaceId.slice(0, 8)}…`
          : type === 'elasticsearch'
            ? `Observability logs · ${elasticForm.indexPattern}`
          : type === 'datadog'
            ? `Datadog ${datadogForm.source} · ${datadogForm.site}`
          : type === 'azureb2c'
            ? `Azure AD B2C · default resource: ${azureB2cForm.resource}`
          : type === 'azureentraid'
            ? `Azure Entra ID · default resource: ${azureEntraIdForm.resource}`
          : ''
      )),
      runtimeConfig: type === 'redis'
        ? {
            connectionMode: redisForm.connectionMode,
            connectionString: redisForm.connectionString,
            host: redisForm.host,
            port: Number(redisForm.port || 6379),
            username: redisForm.username,
            password: redisForm.password,
            database: Number(redisForm.database || 0),
            tls: redisForm.tls,
            defaultQueryMode: redisForm.defaultQueryMode,
            defaultValueType: redisForm.defaultValueType,
            defaultKeyPattern: redisForm.defaultKeyPattern,
            defaultSearchIndex: redisForm.defaultSearchIndex,
            schedule: redisForm.schedule,
          }
        : type === 'mssql'
        ? {
            connectionMode: mssqlForm.connectionMode,
            connectionString: mssqlForm.connectionString,
            host: mssqlForm.host,
            port: Number(mssqlForm.port || 1433),
            database: mssqlForm.database || 'master',
            dbUser: mssqlForm.dbUser,
            dbPass: mssqlForm.dbPass,
            instanceName: mssqlForm.instanceName,
            encrypt: mssqlForm.encrypt,
            trustServerCertificate: mssqlForm.trustServerCertificate,
            schema: mssqlForm.schema || 'dbo',
            defaultCatalog: mssqlForm.defaultCatalog,
            schedule: mssqlForm.schedule,
          }
        : type === 'rabbitmq'
        ? {
            connectionMode: rabbitForm.connectionMode,
            connectionString: rabbitForm.connectionString,
            host: rabbitForm.host,
            port: Number(rabbitForm.port || 5672),
            managementPort: Number(rabbitForm.managementPort || 15672),
            vhost: rabbitForm.vhost || '/',
            username: rabbitForm.username,
            password: rabbitForm.password,
            tls: rabbitForm.tls,
            managementTls: rabbitForm.managementTls,
            defaultCatalog: rabbitForm.defaultCatalog,
            schedule: rabbitForm.schedule,
          }
        : type === 'mqtt'
        ? {
            connectionMode: mqttForm.connectionMode,
            connectionString: mqttForm.connectionString,
            host: mqttForm.host,
            port: Number(mqttForm.port || 1883),
            username: mqttForm.username,
            password: mqttForm.password,
            tls: mqttForm.tls,
            protocol: mqttForm.protocol || 'mqtt',
            defaultTopic: mqttForm.defaultTopic || '#',
            schedule: mqttForm.schedule,
          }
        : type === 'azureb2c'
        ? {
            resource: azureB2cForm.resource,
            schedule: azureB2cForm.schedule,
            authMode: azureB2cForm.authMode,
          }
        : type === 'azureentraid'
        ? {
            resource: azureEntraIdForm.resource,
            schedule: azureEntraIdForm.schedule,
            authMode: azureEntraIdForm.authMode,
          }
        : type === 'azuremonitor'
        ? {
            defaultQuery: azureMonitorForm.defaultQuery,
            workspaceId: azureMonitorForm.workspaceId,
            provider: 'azuremonitor',
          }
        : type === 'github'
        ? {
            selectedRepos: githubForm.selectedRepos.map(repo => ({
              owner: repo.owner,
              repo: repo.repo,
              fullName: repo.fullName,
              private: repo.private,
              ownerType: repo.ownerType,
            })),
            defaultResource: githubForm.defaultResource,
            pullRequestState: githubForm.pullRequestState,
            issueState: githubForm.issueState,
            schedule: githubForm.schedule,
          }
        : type === 'azuredevops'
        ? {
            organization: azureDevOpsForm.organization,
            selectedProjects: azureDevOpsForm.selectedProjects.map(project => ({
              id: project.id,
              name: project.name,
              description: project.description,
              visibility: project.visibility,
            })),
            selectedRepos: azureDevOpsForm.selectedRepos.map(repo => ({
              projectId: repo.projectId,
              projectName: repo.projectName,
              repositoryId: repo.repositoryId,
              repositoryName: repo.repositoryName,
              fullName: repo.fullName,
              defaultBranch: repo.defaultBranch,
            })),
            defaultResource: azureDevOpsForm.defaultResource,
            pullRequestState: azureDevOpsForm.pullRequestState,
            workItemState: azureDevOpsForm.workItemState,
            branchFilter: azureDevOpsForm.branchFilter,
            commitDays: Number(azureDevOpsForm.commitDays || 30),
            pipelineRunDays: Number(azureDevOpsForm.pipelineRunDays || 14),
            schedule: azureDevOpsForm.schedule,
          }
        : type === 'elasticsearch'
        ? {
            defaultQuery: elasticForm.defaultQuery,
            indexPattern: elasticForm.indexPattern,
            provider: 'elasticsearch',
          }
        : type === 'datadog'
        ? {
            defaultQuery: datadogForm.defaultQuery,
            source: datadogForm.source,
            site: datadogForm.site,
            provider: 'datadog',
          }
        : type === 'appinsights'
        ? {
            authMode: aiForm.authMode,
            mode: aiForm.mode,
            appId: resolvedAiAppId,
            workspaceId: aiForm.workspaceId,
          }
        : { ...(currentForm() as unknown as Record<string, unknown>) },
      ...(type === 'appinsights' ? { aiCredentials: {
        authMode: aiForm.authMode,
        mode: aiForm.mode,
        appId: resolvedAiAppId,
        apiKey: aiForm.apiKey,
        workspaceId: aiForm.workspaceId,
        tenantId: aiForm.tenantId,
        clientId: aiForm.clientId,
        clientSecret: aiForm.clientSecret,
        connectionString: aiForm.connectionString,
      } } : {}),
      ...(type === 'appinsights' ? { observabilityCredentials: {
        provider: 'appinsights',
        authMode: aiForm.authMode,
        mode: aiForm.mode,
        appId: resolvedAiAppId,
        apiKey: aiForm.apiKey,
        workspaceId: aiForm.workspaceId,
        tenantId: aiForm.tenantId,
        clientId: aiForm.clientId,
        clientSecret: aiForm.clientSecret,
        connectionString: aiForm.connectionString,
      } } : {}),
      ...(type === 'azuremonitor' ? { observabilityCredentials: {
        provider: 'azuremonitor',
        workspaceId: azureMonitorForm.workspaceId,
        tenantId: azureMonitorForm.tenantId,
        clientId: azureMonitorForm.clientId,
        clientSecret: azureMonitorForm.clientSecret,
      } } : {}),
      ...(type === 'elasticsearch' ? { observabilityCredentials: {
        provider: 'elasticsearch',
        endpoint: elasticForm.endpoint,
        authType: elasticForm.authType,
        username: elasticForm.username,
        password: elasticForm.password,
        apiKey: elasticForm.apiKey,
        indexPattern: elasticForm.indexPattern,
      } } : {}),
      ...(type === 'datadog' ? { observabilityCredentials: {
        provider: 'datadog',
        site: datadogForm.site,
        apiKey: datadogForm.apiKey,
        applicationKey: datadogForm.applicationKey,
        source: datadogForm.source,
        defaultQuery: datadogForm.defaultQuery,
      } } : {}),
      ...(type === 'azureb2c' ? { azureB2cCredentials: {
        tenantId: azureB2cForm.tenantId,
        clientId: azureB2cForm.clientId,
        authMode: azureB2cForm.authMode,
        clientSecret: azureB2cForm.clientSecret,
        certificatePem: azureB2cForm.certificatePem,
        privateKeyPem: azureB2cForm.privateKeyPem,
        thumbprint: azureB2cForm.thumbprint,
        cloud: 'global' as const,
      } } : {}),
      ...(type === 'azureentraid' ? { azureEntraIdCredentials: {
        tenantId: azureEntraIdForm.tenantId,
        clientId: azureEntraIdForm.clientId,
        authMode: azureEntraIdForm.authMode,
        clientSecret: azureEntraIdForm.clientSecret,
        certificatePem: azureEntraIdForm.certificatePem,
        privateKeyPem: azureEntraIdForm.privateKeyPem,
        thumbprint: azureEntraIdForm.thumbprint,
        cloud: 'global' as const,
      } } : {}),
      ...(type === 'github' && githubForm.authMode === 'pat' ? { githubCredentials: {
        mode: 'pat' as const,
        token: githubForm.token,
        username: githubForm.accountLogin || undefined,
      } } : {}),
      ...(type === 'github' && githubForm.authMode !== 'pat' ? { githubAuthTransactionId: githubForm.transactionId } : {}),
      ...(type === 'azuredevops' && azureDevOpsForm.authMode === 'pat' ? { azureDevOpsCredentials: {
        mode: 'pat' as const,
        organization: azureDevOpsForm.organization,
        pat: azureDevOpsForm.pat,
        username: azureDevOpsForm.accountName || undefined,
      } } : {}),
      ...(type === 'azuredevops' && azureDevOpsForm.authMode !== 'pat' ? { azureDevOpsAuthTransactionId: azureDevOpsForm.transactionId } : {}),
    }
    const finalJob: ConnectorJob = {
      ...jobRef.current,
      connectorId: newConn.id,
      connectorName: newConn.name,
      connectorType: type!,
      status: (type === 'webhook' || type === 'file') ? 'succeeded' : jobRef.current.status,
      progress: 100,
      duration: Date.now() - jobRef.current.startedAt,
    }
    onCreated(newConn, finalJob)
    onClose()
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />
      <div className="relative w-full max-w-[700px] bg-chef-surface rounded-2xl border border-chef-border shadow-2xl flex flex-col max-h-[90vh] animate-fade-in">
        <div className="flex items-center justify-between px-6 pt-5 pb-0 shrink-0">
          <div>
            <h2 className="text-base font-bold text-chef-text">New Connector</h2>
            <p className="text-[11px] text-chef-muted mt-0.5">Step {step + 1} of {steps.length}</p>
          </div>
          <button onClick={onClose} className="p-1.5 text-chef-muted hover:text-chef-text hover:bg-chef-card rounded-lg transition-colors"><X size={16} /></button>
        </div>

        <Stepper steps={steps} current={step} />

        <div className="flex-1 overflow-y-auto px-6 py-5">
          {step === 0 && (
            <TypeStep selected={type} onSelect={id => { setType(id); setTouched(false) }} />
          )}
          {step === 1 && type === 'http' && <HttpConfigure form={httpForm} set={setHttpForm} errors={errors} />}
          {step === 1 && type === 'webhook' && <WebhookConfigure form={webhookForm} set={setWebhookForm} errors={errors} />}
          {step === 1 && (type === 'postgresql' || type === 'mysql' || type === 'mongodb') && <DatabaseConfigure form={dbForm} set={setDbForm} errors={errors} type={type} />}
          {step === 1 && type === 'redis' && <RedisConfigure form={redisForm} set={setRedisForm} errors={errors} />}
          {step === 1 && type === 'mssql' && <MssqlConfigure form={mssqlForm} set={setMssqlForm} errors={errors} />}
          {step === 1 && type === 'rabbitmq' && <RabbitMQConfigure form={rabbitForm} set={setRabbitForm} errors={errors} />}
          {step === 1 && type === 'mqtt' && <MqttConfigure form={mqttForm} set={setMqttForm} errors={errors} />}
          {step === 1 && type === 's3' && <S3Configure form={s3Form} set={setS3Form} errors={errors} />}
          {step === 1 && type === 'sftp' && <SftpConfigure form={sftpForm} set={setSftpForm} errors={errors} />}
          {step === 1 && type === 'bigquery' && <BigQueryConfigure form={bqForm} set={setBqForm} errors={errors} />}
          {step === 1 && type === 'file' && <FileUploadConfigure form={fileForm} set={setFileForm} errors={errors} />}
          {step === 1 && type === 'appinsights' && <AppInsightsConfigure form={aiForm} set={setAiForm} errors={errors} />}
          {step === 1 && type === 'azuremonitor' && <AzureMonitorConfigure form={azureMonitorForm} set={setAzureMonitorForm} errors={errors} />}
          {step === 1 && type === 'elasticsearch' && <ElasticObservabilityConfigure form={elasticForm} set={setElasticForm} errors={errors} />}
          {step === 1 && type === 'datadog' && <DatadogConfigure form={datadogForm} set={setDatadogForm} errors={errors} />}
          {step === 1 && type === 'azureb2c' && <AzureB2CConfigure form={azureB2cForm} set={setAzureB2cForm} errors={errors} />}
          {step === 1 && type === 'azureentraid' && <AzureEntraIdConfigure form={azureEntraIdForm} set={setAzureEntraIdForm} errors={errors} />}
          {step === 1 && type === 'github' && <GitHubConfigure form={githubForm} set={setGitHubForm} errors={errors} />}
          {step === 1 && type === 'azuredevops' && <AzureDevOpsConfigure form={azureDevOpsForm} set={setAzureDevOpsForm} errors={errors} />}
          {isTest && type && (
            <TestStep
              type={type}
              form={currentForm() as unknown as Record<string, unknown>}
              onJobUpdate={handleJobUpdate}
            />
          )}
          {isDone && type && (
            <DoneStep
              type={type}
              name={String((currentForm() as unknown as Record<string, unknown>).name || '')}
              onView={handleDone}
            />
          )}
        </div>

        {!isDone && (
          <div className="px-6 py-4 border-t border-chef-border flex items-center justify-between shrink-0">
            <button
              onClick={() => step > 0 ? setStep(s => s - 1) : onClose()}
              className="flex items-center gap-1.5 text-sm text-chef-muted hover:text-chef-text px-3 py-1.5 rounded-lg hover:bg-chef-card border border-transparent hover:border-chef-border transition-colors"
            >
              <ArrowLeft size={13} /> {step === 0 ? 'Cancel' : 'Back'}
            </button>
            {!isTest && (
              <button
                onClick={advance}
                disabled={step === 0 ? !type : false}
                className={`flex items-center gap-1.5 text-sm font-semibold px-4 py-2 rounded-lg transition-colors ${(step === 0 ? !!type : true) ? 'bg-indigo-600 hover:bg-indigo-500 text-white' : 'bg-chef-card text-chef-muted cursor-not-allowed border border-chef-border'}`}
              >
                {step === 1 ? (noTestTypes.includes(type) ? 'Create Connector' : 'Test Connection') : 'Continue'}
                <ChevronRight size={14} />
              </button>
            )}
            {isTest && (
              <button onClick={() => setStep(s => s + 1)}
                className="flex items-center gap-1.5 text-sm font-semibold px-4 py-2 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white transition-colors">
                Create Connector <ChevronRight size={14} />
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
