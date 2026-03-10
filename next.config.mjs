import { PHASE_DEVELOPMENT_SERVER } from 'next/constants.js'

/** @type {import('next').NextConfig} */
const createNextConfig = (phase) => ({
  // Keep dev and production artifacts isolated so a `next build` run
  // does not invalidate the chunk/manifest set used by a live `next dev`.
  distDir: phase === PHASE_DEVELOPMENT_SERVER ? '.next-dev' : '.next',
  output: process.env.NEXT_OUTPUT_STANDALONE === '1' ? 'standalone' : undefined,
  
  // Enable OpenTelemetry instrumentation
  experimental: {
    instrumentationHook: true,
    serverComponentsExternalPackages: [
      '@aws-sdk/client-s3',
      '@google-cloud/bigquery',
      'mongodb',
      'mysql2',
      'pg',
      'ssh2',
      'ssh2-sftp-client',
      '@opentelemetry/sdk-node',
      '@opentelemetry/auto-instrumentations-node',
      '@opentelemetry/exporter-trace-otlp-http',
      '@opentelemetry/exporter-metrics-otlp-http',
      '@grpc/grpc-js',
    ],
  },

  webpack: (config, { isServer }) => {
    if (!isServer) {
      // alasql ships a browser-safe bundle at alasql.min.js but its `main`
      // field points to the Node/FS version.  Force webpack to use the browser build.
      config.resolve.alias = {
        ...config.resolve.alias,
        'alasql': 'alasql/dist/alasql.min.js',
      }
      config.resolve.fallback = {
        ...config.resolve.fallback,
        fs:            false,
        net:           false,
        tls:           false,
        child_process: false,
        path:          false,
        stream:        false,
        zlib:          false,
      }
    }
    return config
  },
})

export default createNextConfig
