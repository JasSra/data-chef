/**
 * OpenTelemetry instrumentation for Data Chef
 * 
 * This file is loaded by Next.js instrumentation hook.
 * It dynamically imports the server-only OpenTelemetry setup.
 */

// Next.js instrumentation hook - only runs on the server
export async function register() {
  if (process.env.NEXT_RUNTIME === 'nodejs') {
    await import('./instrumentation.server').then((mod) => mod.initializeOpenTelemetry())
  }
}
