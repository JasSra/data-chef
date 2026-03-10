/**
 * OpenTelemetry instrumentation for Data Chef - Server Only
 * 
 * Provides automatic instrumentation for:
 * - HTTP requests (incoming/outgoing)
 * - Database queries (PostgreSQL, MySQL, MongoDB, Redis)
 * - Next.js routing and API handlers
 * - Custom application traces
 * 
 * Configuration via environment variables:
 * - OTEL_ENABLED=true (default: false)
 * - OTEL_SERVICE_NAME=data-chef
 * - OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318
 * - OTEL_EXPORTER_OTLP_HEADERS=x-api-key=...
 */

import { NodeSDK } from '@opentelemetry/sdk-node'
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http'
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http'
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics'

let sdk: NodeSDK | null = null

export function initializeOpenTelemetry() {
  // Only initialize in production and if explicitly enabled
  const enabled = process.env.OTEL_ENABLED === 'true'
  const isProduction = process.env.NODE_ENV === 'production'
  
  if (!enabled) {
    console.log('[OpenTelemetry] Disabled (set OTEL_ENABLED=true to enable)')
    return
  }

  if (sdk) {
    console.log('[OpenTelemetry] Already initialized')
    return
  }

  try {
    const serviceName = process.env.OTEL_SERVICE_NAME || 'data-chef'
    const serviceVersion = process.env.npm_package_version || '0.1.0'
    const otlpEndpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT || 'http://localhost:4318'

    console.log(`[OpenTelemetry] Initializing with endpoint: ${otlpEndpoint}`)

    // Configure trace exporter
    const traceExporter = new OTLPTraceExporter({
      url: `${otlpEndpoint}/v1/traces`,
      headers: parseOtelHeaders(),
    })

    // Configure metric exporter
    const metricExporter = new OTLPMetricExporter({
      url: `${otlpEndpoint}/v1/metrics`,
      headers: parseOtelHeaders(),
    })

    const metricReader = new PeriodicExportingMetricReader({
      exporter: metricExporter,
      exportIntervalMillis: 60000, // Export every 60 seconds
    })

    // Initialize SDK
    sdk = new NodeSDK({
      serviceName,
      resource: {
        'service.name': serviceName,
        'service.version': serviceVersion,
        'deployment.environment': isProduction ? 'production' : 'development',
      } as any,
      traceExporter,
      metricReader,
      instrumentations: [
        getNodeAutoInstrumentations({
          // Automatically instrument these libraries
          '@opentelemetry/instrumentation-http': { enabled: true },
          '@opentelemetry/instrumentation-express': { enabled: true },
          '@opentelemetry/instrumentation-pg': { enabled: true },           // PostgreSQL
          '@opentelemetry/instrumentation-mysql': { enabled: true },        // MySQL
          '@opentelemetry/instrumentation-mongodb': { enabled: true },      // MongoDB
          '@opentelemetry/instrumentation-ioredis': { enabled: true },      // Redis
          '@opentelemetry/instrumentation-dns': { enabled: false },         // Too noisy
          '@opentelemetry/instrumentation-net': { enabled: false },         // Too noisy
        }),
      ],
    })

    sdk.start()
    console.log('[OpenTelemetry] ✓ Initialized successfully')

    // Graceful shutdown
    process.on('SIGTERM', () => {
      sdk?.shutdown()
        .then(() => console.log('[OpenTelemetry] Shut down successfully'))
        .catch((error) => console.error('[OpenTelemetry] Error shutting down', error))
        .finally(() => process.exit(0))
    })
  } catch (error) {
    console.error('[OpenTelemetry] Failed to initialize:', error)
  }
}

function parseOtelHeaders(): Record<string, string> {
  const headers = process.env.OTEL_EXPORTER_OTLP_HEADERS || ''
  const parsed: Record<string, string> = {}
  
  headers.split(',').forEach((pair) => {
    const [key, value] = pair.split('=')
    if (key && value) {
      parsed[key.trim()] = value.trim()
    }
  })
  
  return parsed
}
