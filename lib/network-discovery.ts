import 'server-only'

import os from 'node:os'
import net from 'node:net'
import dns from 'node:dns/promises'
import { exec } from 'node:child_process'
import util from 'node:util'
import type { ConnectorId } from '@/components/ConnectorWizard'
import { readJsonFile, removeJsonFile, writeJsonFile } from '@/lib/json-store'
import { getAppSettings, saveAppSettings } from '@/lib/app-settings'
import { relativeTime } from '@/lib/connectors'
import { workerEnd, workerStart } from '@/lib/pipelines'

type DiscoverableConnectorType = Extract<ConnectorId, 'postgresql' | 'mysql' | 'mongodb' | 'redis' | 's3' | 'sftp' | 'elasticsearch'>

export interface DiscoveryCandidate {
  id: string
  groupId: string
  type: DiscoverableConnectorType
  host: string
  port: number
  displayName: string
  confidence: number
  matchReason: string
  status: 'new' | 'dismissed' | 'added'
  lastSeenAt: number
  connectorId?: string | null
}

interface DiscoveryStateFile {
  candidates: DiscoveryCandidate[]
  lastScanAt: number | null
  lastScanDurationMs: number | null
}

export interface DiscoveryCandidateResponse extends DiscoveryCandidate {
  lastSeen: string
}

export interface DiscoveryOverview {
  enabled: boolean
  running: boolean
  lastScanAt: number | null
  lastScan: string
  lastScanDurationMs: number | null
  candidates: DiscoveryCandidateResponse[]
}

export interface DiscoveryGroupedCandidate {
  groupId: string
  type: DiscoverableConnectorType
  port: number
  displayName: string
  hosts: Array<{
    host: string
    confidence: number
    matchReason: string
    status: 'new' | 'dismissed' | 'added'
    lastSeen: string
    candidateId: string
  }>
  totalCount: number
  highestConfidence: number
}

export interface DiscoveryConnectorDraft {
  candidateId: string
  type: DiscoverableConnectorType
  name: string
  description: string
  endpoint: string
  runtimeConfig: Record<string, unknown>
}

interface ProbeResult {
  ok: boolean
  confidence: number
  matchReason: string
}

interface ScanResult {
  ok: boolean
  startedAt: number
  finishedAt: number
  durationMs: number
  scannedHosts: number
  found: number
  skipped?: boolean
  logs?: string[]
  layerSummary?: Array<{ layer: string; count: number; subnets?: string[] }>
  typeSummary?: Record<string, number>
}

interface ProbeSpec {
  type: DiscoverableConnectorType
  port: number
  allowHeuristicOnly?: boolean
  verify: (host: string, port: number) => Promise<ProbeResult>
}

const DISCOVERY_STORE_FILE = 'discoveries.json'
const COMMON_HOSTNAMES = [
  'localhost',
  'host.docker.internal',
  'db',
  'postgres',
  'mysql',
  'mongo',
  'mongodb',
  'redis',
  'minio',
  'sftp',
  'elasticsearch',
  'opensearch',
  'storage',
  'fileserver',
  'nas',
] as const
const CONNECT_TIMEOUT_MS = 220
const HTTP_TIMEOUT_MS = 450
const GLOBAL_SCAN_BUDGET = 300

const SPECS: ProbeSpec[] = [
  {
    type: 'postgresql',
    port: 5432,
    verify: verifyPostgreSql,
  },
  {
    type: 'mysql',
    port: 3306,
    verify: verifyMySql,
  },
  {
    type: 'mongodb',
    port: 27017,
    allowHeuristicOnly: true,
    verify: verifyMongoDb,
  },
  {
    type: 'redis',
    port: 6379,
    allowHeuristicOnly: true,
    verify: verifyRedis,
  },
  {
    type: 'redis',
    port: 6380,
    allowHeuristicOnly: true,
    verify: verifyRedis,
  },
  {
    type: 'sftp',
    port: 22,
    verify: verifySftp,
  },
  {
    type: 's3',
    port: 9000,
    allowHeuristicOnly: true,
    verify: verifyMinio,
  },
  {
    type: 's3',
    port: 9001,
    allowHeuristicOnly: true,
    verify: verifyMinio,
  },
  {
    type: 'elasticsearch',
    port: 9200,
    allowHeuristicOnly: true,
    verify: verifyElastic,
  },
]

declare global {
  // eslint-disable-next-line no-var
  var __datachefDiscoverySchedulerStarted: boolean | undefined
  // eslint-disable-next-line no-var
  var __datachefDiscoveryRunning: boolean | undefined
}

function seedState(): DiscoveryStateFile {
  return {
    candidates: [],
    lastScanAt: null,
    lastScanDurationMs: null,
  }
}

function readState(): DiscoveryStateFile {
  const state = readJsonFile<DiscoveryStateFile>(DISCOVERY_STORE_FILE, seedState())
  return {
    lastScanAt: state.lastScanAt ?? null,
    lastScanDurationMs: state.lastScanDurationMs ?? null,
    candidates: (state.candidates ?? []).map(candidate => ({
      ...candidate,
      connectorId: candidate.connectorId ?? null,
      groupId: candidate.groupId ?? `grp-${candidate.type}-${candidate.port}`,
    })),
  }
}

function writeState(state: DiscoveryStateFile): void {
  writeJsonFile(DISCOVERY_STORE_FILE, state)
}

function makeCandidateId(type: DiscoverableConnectorType, host: string, port: number): string {
  return `disc-${type}-${host.replace(/[^a-z0-9]+/gi, '-')}-${port}`
}

function isPrivateIpv4(value: string): boolean {
  const parts = value.split('.').map(part => Number(part))
  if (parts.length !== 4 || parts.some(part => Number.isNaN(part) || part < 0 || part > 255)) return false
  if (parts[0] === 10) return true
  if (parts[0] === 127) return true
  if (parts[0] === 192 && parts[1] === 168) return true
  if (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) return true
  return false
}

async function resolveHostToPrivateIpv4(host: string): Promise<string[]> {
  if (net.isIP(host) === 4) {
    return isPrivateIpv4(host) ? [host] : []
  }

  try {
    const records = await dns.lookup(host, { all: true, family: 4 })
    return records
      .map(record => record.address)
      .filter(address => isPrivateIpv4(address))
  } catch {
    return []
  }
}

function collectLocalTargets(): string[] {
  const targets = new Set<string>(['127.0.0.1'])
  const interfaces = os.networkInterfaces()

  for (const entries of Object.values(interfaces)) {
    for (const entry of entries ?? []) {
      if (entry.family !== 'IPv4' || !isPrivateIpv4(entry.address)) continue
      targets.add(entry.address)

      const octets = entry.address.split('.').map(Number)
      if (octets.length !== 4) continue
      const prefix = `${octets[0]}.${octets[1]}.${octets[2]}`
      targets.add(`${prefix}.1`)
      for (let offset = -8; offset <= 8; offset++) {
        const candidate = octets[3] + offset
        if (candidate >= 1 && candidate <= 254) {
          targets.add(`${prefix}.${candidate}`)
        }
      }
    }
  }

  return [...targets]
}

const execAsync = util.promisify(exec)

async function getPublicIp(): Promise<string | null> {
  try {
    const res = await fetch('https://api.ipify.org?format=json', { signal: AbortSignal.timeout(3000) })
    return ((await res.json()) as { ip: string }).ip
  } catch {
    return null
  }
}

async function getTracerouteHops(targetIp: string): Promise<string[]> {
  try {
    const cmd = os.platform() === 'win32' ? `tracert -d -w 500 -h 10 ${targetIp}` : `traceroute -n -w 1 -m 10 ${targetIp}`
    const { stdout } = await execAsync(cmd, { timeout: 15000 })
    
    // Match anything that looks like an IP address in the output
    const match = stdout.match(/\b(?:\d{1,3}\.){3}\d{1,3}\b/g)
    const privateHops = match ? [...new Set(match)].filter(ip => ip !== targetIp && isPrivateIpv4(ip)) : []
    console.log('[Discovery] Traceroute hops found:', privateHops)
    return privateHops
  } catch (err) {
    console.error('[Discovery] Traceroute failed:', err)
    return []
  }
}

async function buildScanTargets(): Promise<{ targets: string[]; logs: string[]; layers: Array<{ layer: string; count: number; subnets?: string[] }> }> {
  const logs: string[] = []
  const layers: Array<{ layer: string; count: number; subnets?: string[] }> = []
  
  logs.push('═══ Building Scan Targets ═══')
  console.log('\n[Discovery] ═══ Building Scan Targets ═══')
  
  const localTargets = collectLocalTargets()
  const resolved = new Set<string>(localTargets)
  const intermediateTargets = new Set<string>()

  logs.push(`Layer 1: Local Docker/Container IPs (${localTargets.length} targets)`)
  console.log(`[Discovery] Layer 1: Local Docker/Container IPs (${localTargets.length} targets)`)
  layers.push({ layer: 'Local Docker/Container', count: localTargets.length })

  // First add common hostnames which are likely local
  const hostnamesBefore = resolved.size
  for (const host of COMMON_HOSTNAMES) {
    const addresses = await resolveHostToPrivateIpv4(host)
    for (const address of addresses) resolved.add(address)
  }
  const hostnamesAdded = resolved.size - hostnamesBefore
  if (hostnamesAdded > 0) {
    logs.push(`Layer 2: Common Hostnames (+${hostnamesAdded} targets)`)
    console.log(`[Discovery] Layer 2: Common Hostnames (+${hostnamesAdded} targets)`)
    layers.push({ layer: 'Common Hostnames', count: hostnamesAdded })
  }

  // Add all local network interfaces to ensure we scan their subnets
  const interfaces = os.networkInterfaces()
  const hostSubnets = new Set<string>()
  for (const entries of Object.values(interfaces)) {
    for (const entry of entries ?? []) {
      if (entry.family !== 'IPv4' || !isPrivateIpv4(entry.address)) continue
      const octets = entry.address.split('.').map(Number)
      if (octets.length !== 4) continue
      const prefix = `${octets[0]}.${octets[1]}.${octets[2]}`
      hostSubnets.add(prefix)
      
      // Scan wider range for host machine subnet
      for (let i = 1; i <= 254; i++) {
        intermediateTargets.add(`${prefix}.${i}`)
      }
    }
  }
  if (hostSubnets.size > 0) {
    const subnetList = [...hostSubnets].map(s => `${s}.*`)
    const msg = `Layer 3: Host Machine Subnets (${subnetList.join(', ')}) = ${intermediateTargets.size} targets`
    logs.push(msg)
    console.log(`[Discovery] ${msg}`)
    layers.push({ layer: 'Host Machine Subnets', count: intermediateTargets.size, subnets: subnetList })
  }

  // Then trace route to public IP to find gateway networks
  try {
    const publicIp = await getPublicIp()
    const msg = `Layer 4: Traceroute Analysis (Public IP: ${publicIp || 'N/A'})`
    logs.push(msg)
    console.log(`[Discovery] ${msg}`)
    if (publicIp) {
      const intermediateBefore = intermediateTargets.size
      const hops = await getTracerouteHops(publicIp)
      const gatewaySubnets = new Set<string>()
      for (const hop of hops) {
        const octets = hop.split('.').map(Number)
        if (octets.length !== 4) continue
        const prefix = `${octets[0]}.${octets[1]}.${octets[2]}`
        gatewaySubnets.add(prefix)
        
        // Add gateway and surrounding IPs
        intermediateTargets.add(`${prefix}.1`)
        for (let offset = -8; offset <= 8; offset++) {
          const candidate = octets[3] + offset
          if (candidate >= 1 && candidate <= 254) {
            intermediateTargets.add(`${prefix}.${candidate}`)
          }
        }
      }
      const gatewayAdded = intermediateTargets.size - intermediateBefore
      if (gatewayAdded > 0) {
        const subnetList = [...gatewaySubnets].map(s => `${s}.*`)
        const msg2 = `Layer 4: Gateway Networks (${subnetList.join(', ')}) +${gatewayAdded} targets`
        logs.push(msg2)
        console.log(`[Discovery] ${msg2}`)
        layers.push({ layer: 'Gateway Networks', count: gatewayAdded, subnets: subnetList })
      }
    }
  } catch (err) {
    console.error('[Discovery] Traceroute error:', err)
    logs.push(`Traceroute error: ${err}`)
  }

  // Priority: local targets first, then intermediate networks
  const orderedTargets = [...resolved, ...intermediateTargets]
  const finalCount = Math.min(orderedTargets.length, GLOBAL_SCAN_BUDGET)
  
  const summary = [
    '═══ Scan Summary ═══',
    `Total targets: ${orderedTargets.length}, Budget: ${GLOBAL_SCAN_BUDGET}, Will scan: ${finalCount}`,
    `Breakdown: Local=${resolved.size}, Host Subnets+Gateways=${intermediateTargets.size}`
  ]
  
  logs.push(...summary)
  summary.forEach(s => console.log(`[Discovery] ${s}`))
  
  return {
    targets: orderedTargets.slice(0, GLOBAL_SCAN_BUDGET),
    logs,
    layers
  }
}

function makeDisplayName(type: DiscoverableConnectorType, host: string, port: number): string {
  const label = {
    postgresql: 'PostgreSQL',
    mysql: 'MySQL',
    mongodb: 'MongoDB',
    redis: 'Redis',
    sftp: 'SFTP',
    s3: 'S3-compatible storage',
    elasticsearch: 'Elasticsearch / OpenSearch',
  }[type]
  return `${label} on ${host}:${port}`
}

function toCandidateResponse(candidate: DiscoveryCandidate): DiscoveryCandidateResponse {
  return {
    ...candidate,
    lastSeen: relativeTime(candidate.lastSeenAt),
  }
}

export function clearDiscoveryCandidates(): void {
  removeJsonFile(DISCOVERY_STORE_FILE)
}

export function getDiscoveryOverview(options: { includeDismissed?: boolean; includeAdded?: boolean } = {}): DiscoveryOverview {
  const settings = getAppSettings().networkDiscovery
  const state = readState()
  const candidates = state.candidates.filter(candidate => {
    if (!options.includeDismissed && candidate.status === 'dismissed') return false
    if (!options.includeAdded && candidate.status === 'added') return false
    return true
  })

  return {
    enabled: settings.enabled,
    running: Boolean(globalThis.__datachefDiscoveryRunning),
    lastScanAt: state.lastScanAt,
    lastScan: relativeTime(state.lastScanAt),
    lastScanDurationMs: state.lastScanDurationMs,
    candidates: candidates
      .sort((left, right) => right.lastSeenAt - left.lastSeenAt)
      .map(toCandidateResponse),
  }
}

export function getDiscoveryCandidate(id: string): DiscoveryCandidate | null {
  return readState().candidates.find(candidate => candidate.id === id) ?? null
}

export function getGroupedDiscoveryCandidates(options: { includeDismissed?: boolean; includeAdded?: boolean } = {}): DiscoveryGroupedCandidate[] {
  const state = readState()
  const candidates = state.candidates.filter(candidate => {
    if (!options.includeDismissed && candidate.status === 'dismissed') return false
    if (!options.includeAdded && candidate.status === 'added') return false
    return true
  })

  const groups = new Map<string, DiscoveryGroupedCandidate>()

  for (const candidate of candidates) {
    let group = groups.get(candidate.groupId)
    if (!group) {
      const label = {
        postgresql: 'PostgreSQL',
        mysql: 'MySQL',
        mongodb: 'MongoDB',
        redis: 'Redis',
        sftp: 'SFTP',
        s3: 'S3-compatible storage',
        elasticsearch: 'Elasticsearch / OpenSearch',
      }[candidate.type]
      group = {
        groupId: candidate.groupId,
        type: candidate.type,
        port: candidate.port,
        displayName: `${label} (Port ${candidate.port})`,
        hosts: [],
        totalCount: 0,
        highestConfidence: 0,
      }
      groups.set(candidate.groupId, group)
    }

    group.hosts.push({
      host: candidate.host,
      confidence: candidate.confidence,
      matchReason: candidate.matchReason,
      status: candidate.status,
      lastSeen: relativeTime(candidate.lastSeenAt),
      candidateId: candidate.id,
    })
    group.totalCount++
    group.highestConfidence = Math.max(group.highestConfidence, candidate.confidence)
  }

  return [...groups.values()]
    .sort((a, b) => b.highestConfidence - a.highestConfidence)
}

export function setDiscoveryCandidateStatus(id: string, status: 'new' | 'dismissed'): DiscoveryCandidate | null {
  const state = readState()
  const candidate = state.candidates.find(item => item.id === id)
  if (!candidate) return null
  candidate.status = status
  writeState(state)
  return candidate
}

export function markDiscoveryCandidateAdded(id: string, connectorId: string): DiscoveryCandidate | null {
  const state = readState()
  const candidate = state.candidates.find(item => item.id === id)
  if (!candidate) return null
  candidate.status = 'added'
  candidate.connectorId = connectorId
  writeState(state)
  return candidate
}

export function clearDiscoveryCandidateConnector(connectorId: string): number {
  const state = readState()
  let updated = 0
  for (const candidate of state.candidates) {
    if (candidate.connectorId !== connectorId) continue
    candidate.connectorId = null
    candidate.status = 'new'
    updated++
  }
  if (updated > 0) writeState(state)
  return updated
}

export function buildDiscoveryDraft(candidate: DiscoveryCandidate): DiscoveryConnectorDraft {
  const endpoint = candidate.type === 'sftp'
    ? `sftp://${candidate.host}:${candidate.port}`
    : candidate.type === 'redis'
      ? `redis://${candidate.host}:${candidate.port}/0`
    : candidate.type === 's3'
      ? `http://${candidate.host}:${candidate.port}`
      : `${candidate.host}:${candidate.port}`

  const runtimeConfig: Record<string, unknown> = candidate.type === 'postgresql' || candidate.type === 'mysql'
    ? {
        host: candidate.host,
        port: String(candidate.port),
        database: '',
        dbUser: '',
        dbPass: '',
        ssl: true,
        sslMode: 'verify-full',
        tableOrQuery: '',
        syncMode: 'incremental',
        cursorColumn: 'updated_at',
        cursorType: 'timestamp',
        enableCdc: candidate.type === 'postgresql',
        schedule: '1h',
        useConnectionString: false,
        connectionString: '',
        collection: '',
        filter: '',
      }
    : candidate.type === 'mongodb'
      ? {
          host: candidate.host,
          port: String(candidate.port),
          database: '',
          dbUser: '',
          dbPass: '',
          ssl: true,
          sslMode: 'verify-full',
          tableOrQuery: '',
          syncMode: 'incremental',
          cursorColumn: 'updated_at',
          cursorType: 'timestamp',
          enableCdc: false,
          schedule: '1h',
          useConnectionString: false,
          connectionString: '',
          collection: '',
          filter: '',
        }
      : candidate.type === 'redis'
        ? {
            connectionMode: 'fields',
            connectionString: '',
            host: candidate.host,
            port: String(candidate.port),
            username: '',
            password: '',
            database: '0',
            tls: candidate.port === 6380,
            defaultQueryMode: 'command',
            defaultValueType: 'auto',
            defaultKeyPattern: '*',
            defaultSearchIndex: '',
            schedule: 'on-demand',
          }
      : candidate.type === 'sftp'
        ? {
            protocol: 'sftp',
            host: candidate.host,
            port: String(candidate.port),
            sftpUser: '',
            authType: 'password',
            password: '',
            privateKey: '',
            path: '/',
            filePattern: '*',
            format: 'auto',
            schedule: '24h',
          }
        : candidate.type === 'elasticsearch'
          ? {
              endpoint: `http://${candidate.host}:${candidate.port}`,
              authType: 'basic',
              username: '',
              password: '',
              apiKey: '',
              indexPattern: 'logs-*',
              defaultQuery: 'logs\n| where @timestamp > ago(24h)\n| limit 100',
              schedule: 'on-demand',
            }
        : {
            provider: 'other',
            bucket: '',
            region: 'us-east-1',
            endpoint,
            accessKeyId: '',
            secretAccessKey: '',
            prefix: '',
            format: 'auto',
            schedule: '1h',
          }

  return {
    candidateId: candidate.id,
    type: candidate.type,
    name: makeDisplayName(candidate.type, candidate.host, candidate.port),
    description: `${candidate.matchReason}. Complete credentials and source selection before saving.`,
    endpoint,
    runtimeConfig,
  }
}

async function openPort(host: string, port: number, timeoutMs = CONNECT_TIMEOUT_MS): Promise<boolean> {
  return new Promise(resolve => {
    const socket = new net.Socket()
    const done = (ok: boolean) => {
      socket.removeAllListeners()
      socket.destroy()
      resolve(ok)
    }

    socket.setTimeout(timeoutMs)
    socket.once('connect', () => done(true))
    socket.once('timeout', () => done(false))
    socket.once('error', () => done(false))
    socket.connect(port, host)
  })
}

async function verifyPostgreSql(host: string, port: number): Promise<ProbeResult> {
  return new Promise(resolve => {
    const socket = new net.Socket()
    const request = Buffer.alloc(8)
    request.writeInt32BE(8, 0)
    request.writeInt32BE(80877103, 4)

    const finish = (result: ProbeResult) => {
      socket.removeAllListeners()
      socket.destroy()
      resolve(result)
    }

    socket.setTimeout(CONNECT_TIMEOUT_MS)
    socket.once('connect', () => {
      socket.write(request)
    })
    socket.once('data', chunk => {
      const marker = chunk.toString('utf8', 0, 1)
      if (marker === 'S' || marker === 'N') {
        finish({ ok: true, confidence: 0.94, matchReason: 'PostgreSQL SSL handshake responded on port 5432' })
        return
      }
      finish({ ok: false, confidence: 0.42, matchReason: 'Port 5432 was open but did not answer like PostgreSQL' })
    })
    socket.once('timeout', () => finish({ ok: false, confidence: 0.42, matchReason: 'Port 5432 was open but PostgreSQL verification timed out' }))
    socket.once('error', () => finish({ ok: false, confidence: 0.42, matchReason: 'Port 5432 was reachable but PostgreSQL verification failed' }))
    socket.connect(port, host)
  })
}

async function verifyMySql(host: string, port: number): Promise<ProbeResult> {
  return new Promise(resolve => {
    const socket = new net.Socket()
    const finish = (result: ProbeResult) => {
      socket.removeAllListeners()
      socket.destroy()
      resolve(result)
    }

    socket.setTimeout(CONNECT_TIMEOUT_MS)
    socket.once('data', chunk => {
      const protocolVersion = chunk.at(4) ?? 0
      if (protocolVersion > 0 && protocolVersion < 32) {
        finish({ ok: true, confidence: 0.93, matchReason: 'MySQL handshake packet detected on port 3306' })
        return
      }
      finish({ ok: false, confidence: 0.4, matchReason: 'Port 3306 was open but did not look like MySQL' })
    })
    socket.once('timeout', () => finish({ ok: false, confidence: 0.4, matchReason: 'Port 3306 was open but MySQL handshake timed out' }))
    socket.once('error', () => finish({ ok: false, confidence: 0.4, matchReason: 'Port 3306 was reachable but MySQL verification failed' }))
    socket.connect(port, host)
  })
}

async function verifyMongoDb(host: string, port: number): Promise<ProbeResult> {
  const open = await openPort(host, port)
  return open
    ? { ok: true, confidence: 0.61, matchReason: 'Port 27017 accepted a TCP connection' }
    : { ok: false, confidence: 0.38, matchReason: 'Port 27017 was unreachable during verification' }
}

async function verifyRedis(host: string, port: number): Promise<ProbeResult> {
  return new Promise(resolve => {
    const socket = new net.Socket()
    const finish = (result: ProbeResult) => {
      socket.removeAllListeners()
      socket.destroy()
      resolve(result)
    }

    socket.setTimeout(CONNECT_TIMEOUT_MS)
    socket.once('connect', () => {
      socket.write('*1\r\n$4\r\nPING\r\n')
    })
    socket.once('data', chunk => {
      const data = chunk.toString('utf8')
      if (data.startsWith('+PONG') || data.includes('NOAUTH') || data.includes('PONG')) {
        finish({ ok: true, confidence: 0.9, matchReason: `Redis protocol response detected on port ${port}` })
        return
      }
      finish({ ok: false, confidence: 0.44, matchReason: `Port ${port} was open but did not respond like Redis` })
    })
    socket.once('timeout', () => finish({ ok: false, confidence: 0.44, matchReason: `Port ${port} was open but Redis verification timed out` }))
    socket.once('error', () => finish({ ok: false, confidence: 0.44, matchReason: `Port ${port} was reachable but Redis verification failed` }))
    socket.connect(port, host)
  })
}

async function verifySftp(host: string, port: number): Promise<ProbeResult> {
  return new Promise(resolve => {
    const socket = new net.Socket()
    const finish = (result: ProbeResult) => {
      socket.removeAllListeners()
      socket.destroy()
      resolve(result)
    }

    socket.setTimeout(CONNECT_TIMEOUT_MS)
    socket.once('data', chunk => {
      const banner = chunk.toString('utf8')
      if (banner.startsWith('SSH-')) {
        finish({ ok: true, confidence: 0.96, matchReason: 'SSH banner detected on port 22' })
        return
      }
      finish({ ok: false, confidence: 0.45, matchReason: 'Port 22 was open but no SSH banner was returned' })
    })
    socket.once('timeout', () => finish({ ok: false, confidence: 0.45, matchReason: 'Port 22 was open but SSH banner timed out' }))
    socket.once('error', () => finish({ ok: false, confidence: 0.45, matchReason: 'Port 22 was reachable but SSH verification failed' }))
    socket.connect(port, host)
  })
}

async function verifyMinio(host: string, port: number): Promise<ProbeResult> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), HTTP_TIMEOUT_MS)

  try {
    const res = await fetch(`http://${host}:${port}/minio/health/live`, {
      method: 'GET',
      signal: controller.signal,
      cache: 'no-store',
    })
    const server = res.headers.get('server') ?? ''
    if (res.ok || /minio/i.test(server)) {
      return { ok: true, confidence: 0.84, matchReason: 'MinIO-compatible HTTP health endpoint responded' }
    }
  } catch {
    // fall through to lightweight port heuristic
  } finally {
    clearTimeout(timer)
  }

  const open = await openPort(host, port)
  return open
    ? { ok: true, confidence: 0.58, matchReason: `Port ${port} accepted a storage probe` }
    : { ok: false, confidence: 0.32, matchReason: `Port ${port} did not respond to storage verification` }
}

async function verifyElastic(host: string, port: number): Promise<ProbeResult> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), HTTP_TIMEOUT_MS)

  try {
    const res = await fetch(`http://${host}:${port}/`, {
      method: 'GET',
      signal: controller.signal,
      cache: 'no-store',
    })
    const text = await res.text()
    const server = res.headers.get('server') ?? ''
    if (/elasticsearch|opensearch/i.test(text) || /elasticsearch|opensearch/i.test(server)) {
      return { ok: true, confidence: 0.86, matchReason: 'Elasticsearch/OpenSearch HTTP endpoint responded on port 9200' }
    }
    if (res.ok) {
      return { ok: true, confidence: 0.62, matchReason: 'Port 9200 returned an HTTP response consistent with a search endpoint' }
    }
  } catch {
    // fall through
  } finally {
    clearTimeout(timer)
  }

  const open = await openPort(host, port)
  return open
    ? { ok: true, confidence: 0.52, matchReason: 'Port 9200 accepted a TCP connection' }
    : { ok: false, confidence: 0.34, matchReason: 'Port 9200 did not respond during verification' }
}

async function probeHost(host: string): Promise<DiscoveryCandidate[]> {
  const candidates: Array<DiscoveryCandidate | null> = await Promise.all(SPECS.map(async spec => {
    const portOpen = await openPort(host, spec.port)
    if (!portOpen) return null

    const heuristicReason = `Matched default ${spec.type} port ${spec.port} on a private host`
    const verification = await spec.verify(host, spec.port)
    if (!verification.ok && !spec.allowHeuristicOnly) return null

    const confidence = verification.ok ? verification.confidence : 0.46
    const matchReason = verification.ok ? verification.matchReason : heuristicReason
    return {
      id: makeCandidateId(spec.type, host, spec.port),
      groupId: `grp-${spec.type}-${spec.port}`,
      type: spec.type,
      host,
      port: spec.port,
      displayName: makeDisplayName(spec.type, host, spec.port),
      confidence,
      matchReason,
      status: 'new',
      lastSeenAt: Date.now(),
      connectorId: null,
    } satisfies DiscoveryCandidate
  }))

  return candidates.filter((candidate): candidate is DiscoveryCandidate => candidate !== null)
}

function mergeDiscoveredCandidates(discovered: DiscoveryCandidate[], startedAt: number, finishedAt: number): void {
  const state = readState()
  const existing = new Map(state.candidates.map(candidate => [candidate.id, candidate]))

  for (const candidate of discovered) {
    const prior = existing.get(candidate.id)
    if (prior) {
      prior.displayName = candidate.displayName
      prior.confidence = candidate.confidence
      prior.matchReason = candidate.matchReason
      prior.lastSeenAt = candidate.lastSeenAt
      prior.host = candidate.host
      prior.port = candidate.port
      prior.type = candidate.type
      prior.groupId = candidate.groupId
      continue
    }
    existing.set(candidate.id, candidate)
  }

  const nextState: DiscoveryStateFile = {
    candidates: [...existing.values()],
    lastScanAt: finishedAt,
    lastScanDurationMs: finishedAt - startedAt,
  }
  writeState(nextState)
}

export async function runNetworkDiscoveryScan(options: { force?: boolean } = {}): Promise<ScanResult> {
  const settings = getAppSettings().networkDiscovery
  const startedAt = Date.now()

  if (!settings.enabled && !options.force) {
    return {
      ok: true,
      skipped: true,
      startedAt,
      finishedAt: Date.now(),
      durationMs: 0,
      scannedHosts: 0,
      found: 0,
    }
  }

  if (globalThis.__datachefDiscoveryRunning) {
    return {
      ok: true,
      skipped: true,
      startedAt,
      finishedAt: Date.now(),
      durationMs: 0,
      scannedHosts: 0,
      found: 0,
    }
  }

  globalThis.__datachefDiscoveryRunning = true
  workerStart()
  try {
    const { targets: hosts, logs, layers } = await buildScanTargets()
    const discoveredMap = new Map<string, DiscoveryCandidate>()
    
    const scanLogs = [...logs]
    scanLogs.push('\n═══ Starting Scan ═══')
    scanLogs.push(`Scanning ${hosts.length} hosts...`)
    console.log(`\n[Discovery] ═══ Starting Scan ═══`)
    console.log(`[Discovery] Scanning ${hosts.length} hosts...`)
    
    let scanned = 0
    let lastLogTime = Date.now()
    
    for (const host of hosts) {
      const discovered = await probeHost(host)
      for (const candidate of discovered) {
        discoveredMap.set(candidate.id, candidate)
        const msg = `✓ Found: ${candidate.type} on ${candidate.host}:${candidate.port} (confidence: ${(candidate.confidence * 100).toFixed(0)}%)`
        scanLogs.push(msg)
        console.log(`[Discovery] ${msg}`)
      }
      scanned++
      
      // Log progress every 50 hosts or every 10 seconds
      const now = Date.now()
      if (scanned % 50 === 0 || now - lastLogTime > 10000) {
        const msg = `Progress: ${scanned}/${hosts.length} hosts scanned, ${discoveredMap.size} services found`
        scanLogs.push(msg)
        console.log(`[Discovery] ${msg}`)
        lastLogTime = now
      }
    }

    const finishedAt = Date.now()
    const discovered = [...discoveredMap.values()]
    
    scanLogs.push('\n═══ Scan Complete ═══')
    scanLogs.push(`Duration: ${((finishedAt - startedAt) / 1000).toFixed(1)}s`)
    scanLogs.push(`Hosts scanned: ${hosts.length}`)
    scanLogs.push(`Services found: ${discovered.length}`)
    
    console.log(`\n[Discovery] ═══ Scan Complete ═══`)
    console.log(`[Discovery] Duration: ${((finishedAt - startedAt) / 1000).toFixed(1)}s`)
    console.log(`[Discovery] Hosts scanned: ${hosts.length}`)
    console.log(`[Discovery] Services found: ${discovered.length}`)
    
    // Group by type for summary
    const byType = discovered.reduce((acc, c) => {
      acc[c.type] = (acc[c.type] || 0) + 1
      return acc
    }, {} as Record<string, number>)
    
    if (discovered.length > 0) {
      scanLogs.push('Breakdown by type:')
      console.log(`[Discovery] Breakdown by type:`)
      Object.entries(byType).forEach(([type, count]) => {
        scanLogs.push(`  - ${type}: ${count}`)
        console.log(`[Discovery]   - ${type}: ${count}`)
      })
    }
    
    mergeDiscoveredCandidates(discovered, startedAt, finishedAt)
    saveAppSettings({
      networkDiscovery: {
        ...settings,
        lastScanAt: finishedAt,
      },
    })

    return {
      ok: true,
      startedAt,
      finishedAt,
      durationMs: finishedAt - startedAt,
      scannedHosts: hosts.length,
      found: discovered.length,
      logs: scanLogs,
      layerSummary: layers,
      typeSummary: byType,
    }
  } finally {
    globalThis.__datachefDiscoveryRunning = false
    workerEnd()
  }
}

async function runDueDiscoveryScans(): Promise<void> {
  const settings = getAppSettings().networkDiscovery
  if (!settings.enabled || !settings.backgroundRefreshEnabled) return

  const now = Date.now()
  const lastScanAt = settings.lastScanAt ?? 0
  const intervalMs = Math.max(15, settings.refreshIntervalMinutes) * 60_000
  if (now - lastScanAt < intervalMs) return
  await runNetworkDiscoveryScan()
}

export function ensureNetworkDiscoverySchedulerStarted(): void {
  if (globalThis.__datachefDiscoverySchedulerStarted) return
  globalThis.__datachefDiscoverySchedulerStarted = true

  setInterval(() => {
    void runDueDiscoveryScans()
  }, 30_000)
}
