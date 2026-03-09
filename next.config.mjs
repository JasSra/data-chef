/** @type {import('next').NextConfig} */
const nextConfig = {
  output: process.env.NEXT_OUTPUT_STANDALONE === '1' ? 'standalone' : undefined,
  experimental: {
    serverComponentsExternalPackages: [
      '@aws-sdk/client-s3',
      '@google-cloud/bigquery',
      'mongodb',
      'mysql2',
      'pg',
      'ssh2',
      'ssh2-sftp-client',
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
      }
    }
    return config
  },
}

export default nextConfig
