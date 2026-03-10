/**
 * Bootstrap script to auto-start background workers when the server starts.
 * This ensures workers run immediately in containerized environments.
 */

import { ensureConnectorSchedulerStarted } from './connector-sync'
import { ensureNetworkDiscoverySchedulerStarted } from './network-discovery'

let bootstrapped = false

export function bootstrapWorkers(): void {
  if (bootstrapped) return
  bootstrapped = true

  console.log('[DataChef Bootstrap] Starting background workers...')
  
  try {
    ensureConnectorSchedulerStarted()
    console.log('[DataChef Bootstrap] ✓ Connector sync scheduler started')
  } catch (err) {
    console.error('[DataChef Bootstrap] ✗ Failed to start connector scheduler:', err)
  }

  try {
    ensureNetworkDiscoverySchedulerStarted()
    console.log('[DataChef Bootstrap] ✓ Network discovery scheduler started')
  } catch (err) {
    console.error('[DataChef Bootstrap] ✗ Failed to start network discovery scheduler:', err)
  }

  console.log('[DataChef Bootstrap] Background workers initialized')
}

// Auto-start workers on server boot (in production/containerized environments)
if (typeof window === 'undefined' && process.env.NODE_ENV === 'production') {
  bootstrapWorkers()
}
