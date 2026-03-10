# Data Chef

**Enterprise data integration platform** for querying, transforming, and orchestrating operational data from multiple sources.

Query databases, APIs, cloud services, and observability platforms through a unified interface with built-in pipeline automation, network discovery, and distributed tracing.

---

## 🚀 Quick Start

### Prerequisites

- **Docker** (recommended) or Node.js 20+
- A secure encryption key for credential storage

### Option 1: Docker (Recommended)

**1. Pull the latest image:**
```bash
docker pull jassra/datachef:latest
```

**2. Generate an encryption key:**
```bash
export CONNECTOR_SECRET_KEY=$(openssl rand -base64 32)
echo "Save this key: $CONNECTOR_SECRET_KEY"
```

**3. Run the container:**
```bash
docker run -d \
  --name data-chef \
  --restart always \
  -p 8080:3000 \
  -e NODE_ENV=production \
  -e CONNECTOR_SECRET_KEY="$CONNECTOR_SECRET_KEY" \
  -v datachef-data:/app/.datachef \
  jassra/datachef:latest
```

**4. Access the application:**
```
http://localhost:8080
```

**5. Check health status:**
```bash
curl http://localhost:8080/api/health | jq
```

### Option 2: Docker Compose

**1. Create a `.env` file:**
```bash
cp .env.example .env
# Edit .env and add your CONNECTOR_SECRET_KEY
```

**2. Start the stack:**
```bash
docker compose up -d
```

**3. View logs:**
```bash
docker compose logs -f
```

### Option 3: Local Development

**1. Install dependencies:**
```bash
npm install
```

**2. Set environment variables:**
```bash
export CONNECTOR_SECRET_KEY=$(openssl rand -base64 32)
```

**3. Run development server:**
```bash
npm run dev
```

**4. Open browser:**
```
http://localhost:3333
```

---

## 📦 What's Included

### Core Features

- ✅ **Unified Query Interface** - SQL, KQL, JSONPath, JMESPath, Redis queries
- ✅ **Multi-Source Connectors** - PostgreSQL, MySQL, MongoDB, Redis, HTTP APIs, S3, SFTP
- ✅ **Observability Integration** - Azure Monitor, App Insights, Elasticsearch, Datadog
- ✅ **Pipeline Automation** - Visual pipeline builder with 40+ node types
- ✅ **Network Discovery** - Automatic discovery of network services and APIs
- ✅ **GitHub & Azure DevOps** - Source code and work item analysis
- ✅ **OpenTelemetry** - Built-in distributed tracing and metrics
- ✅ **Background Workers** - Auto-running connector syncs and discovery scans

### Production Ready

- 🔐 AES-256-GCM encrypted credential storage
- 🏥 Comprehensive health checks
- 📊 OpenTelemetry auto-instrumentation
- 🔄 Auto-restarting background workers
- 💾 Persistent data storage
- 🐳 Multi-arch Docker images
- 🚦 Load balancer ready

---

## 🔧 Configuration

### Required Environment Variables

| Variable | Description | How to Generate |
|----------|-------------|-----------------|
| `CONNECTOR_SECRET_KEY` | Encryption key for credentials | `openssl rand -base64 32` |

### Optional Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OTEL_ENABLED` | `false` | Enable OpenTelemetry tracing |
| `OTEL_SERVICE_NAME` | `data-chef` | Service name in traces |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTEL collector endpoint |
| `OTEL_EXPORTER_OTLP_HEADERS` | - | Custom headers (e.g., API keys) |
| `NODE_ENV` | `development` | Runtime environment |
| `PORT` | `3000` | HTTP port (in container) |

See [`.env.example`](.env.example) for complete configuration options.

---

## 📚 Documentation

- **[Deployment Guide](docs/DEPLOYMENT.md)** - Production deployment, security, backups
- **[Observability Guide](docs/OBSERVABILITY.md)** - OpenTelemetry setup with Jaeger, Tempo, etc.
- **[Pipeline Node Guide](docs/pipeline-node-guide.md)** - Available pipeline transformation nodes
- **[Network Discovery](docs/network-discovery-enhancements.md)** - Network scanning and service discovery

---

## 🐳 Docker Images

### Pull from Docker Hub

```bash
docker pull jassra/datachef:latest
docker pull jassra/datachef:main        # Latest from main branch
docker pull jassra/datachef:v1.0.0      # Specific version tag
```

### Tags

- `latest` - Latest stable release from main branch
- `main` - Latest commit to main branch
- `v*.*.*` - Semantic version tags

### Multi-Architecture

Images support:
- `linux/amd64` (x86_64)
- `linux/arm64` (Apple Silicon, ARM servers)

---

## 🔐 Security Best Practices

### Production Checklist

- [ ] Generate strong `CONNECTOR_SECRET_KEY` (32+ bytes)
- [ ] Store secrets in a vault (Azure Key Vault, AWS Secrets Manager)
- [ ] Use the **same key** across all container instances
- [ ] Mount persistent volume at `/app/.datachef`
- [ ] Enable HTTPS/TLS via reverse proxy (nginx, traefik, cloud LB)
- [ ] Run containers as non-root user (already configured)
- [ ] Enable health checks
- [ ] Regular backups of data volume
- [ ] Enable OpenTelemetry for monitoring
- [ ] Restrict network access with firewalls

### Minimal Production Example

```bash
# Generate and save your encryption key
openssl rand -base64 32 > /secure/vault/datachef-key.txt

# Run with production settings
docker run -d \
  --name data-chef \
  --restart unless-stopped \
  -p 8080:3000 \
  -e NODE_ENV=production \
  -e CONNECTOR_SECRET_KEY="$(cat /secure/vault/datachef-key.txt)" \
  -e OTEL_ENABLED=true \
  -e OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.internal:4318 \
  -v datachef-data:/app/.datachef \
  --health-cmd="wget -qO- http://localhost:3000/api/health || exit 1" \
  --health-interval=30s \
  --health-retries=3 \
  jassra/datachef:latest
```

---

## 🏥 Health Monitoring

### Health Check Endpoint

```bash
curl http://localhost:8080/api/health
```

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2026-03-10T12:34:56.789Z",
  "version": "0.1.0",
  "system": {
    "uptime": 12345,
    "uptimeFormatted": "3h 25m 45s",
    "nodeVersion": "v20.x.x",
    "platform": "linux"
  },
  "memory": {
    "heapUsed": 125,
    "heapTotal": 256,
    "rss": 312,
    "unit": "MB"
  },
  "data": {
    "exists": true,
    "writable": true,
    "files": 42,
    "tenants": 2
  },
  "workers": {
    "connectorScheduler": true,
    "discoveryScheduler": true
  },
  "environment": {
    "nodeEnv": "production",
    "hasSecretKey": true,
    "otelEnabled": true
  }
}
```

### Worker Status

```bash
curl http://localhost:8080/api/workers
```

---

## 🔄 CI/CD

### Automated Docker Builds

Every push to `main` triggers:
1. Docker image build
2. Push to Docker Hub as `jassra/datachef:latest`
3. Tag semantic versions (e.g., `v1.0.0`)

See [`.github/workflows/docker-publish.yml`](.github/workflows/docker-publish.yml)

### GitHub Secrets Required

Configure in **Settings > Secrets and variables > Actions**:

- `DOCKERHUB_USERNAME` - Your Docker Hub username
- `DOCKERHUB_TOKEN` - Docker Hub access token

---

## 🌐 Multi-Instance Deployment

For load-balanced, high-availability setups:

### Requirements

1. **Shared storage** for `/app/.datachef` (Azure Files, NFS, EFS)
2. **Same `CONNECTOR_SECRET_KEY`** across all instances
3. **Load balancer** (nginx, cloud LB)

### Example Architecture

```
        ┌─────────────┐
        │ Load Balancer│
        └──────┬───────┘
               │
       ┌───────┼───────┐
       ▼       ▼       ▼
   ┌────┐  ┌────┐  ┌────┐
   │ DC1│  │ DC2│  │ DC3│  (Data Chef instances)
   └──┬─┘  └──┬─┘  └──┬─┘
      └───────┼───────┘
              │
      ┌───────▼────────┐
      │ Shared Storage │ (/app/.datachef)
      └────────────────┘
```

See [Deployment Guide](docs/DEPLOYMENT.md) for detailed multi-instance setup.

---

## 🔭 Observability

### Built-in OpenTelemetry

Data Chef automatically instruments:
- HTTP requests (incoming/outgoing)
- Database queries (PostgreSQL, MySQL, MongoDB, Redis)
- Next.js routing and API handlers
- Background worker executions

### Quick OTEL Setup

```bash
docker run -d \
  -e OTEL_ENABLED=true \
  -e OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318 \
  jassra/datachef:latest
```

**Supported backends:**
- Jaeger
- Grafana Tempo
- Honeycomb
- New Relic
- Datadog
- Azure Monitor
- Elastic APM

See [Observability Guide](docs/OBSERVABILITY.md) for complete setup examples.

---

## 📁 Data Storage

All configuration is stored in `/app/.datachef/`:

```
/app/.datachef/
├── platform/
│   └── tenants.json              # Tenant registry
└── tenant_local/
    ├── connectors.json           # Connector configs (encrypted credentials)
    ├── datasets.json             # Dataset metadata
    ├── pipelines.json            # Pipeline definitions
    ├── recipes.json              # Saved query recipes
    └── discoveries.json          # Network discovery results
```

### Backup

```bash
docker run --rm \
  -v datachef-data:/data \
  -v $(pwd):/backup \
  alpine tar czf /backup/datachef-backup-$(date +%Y%m%d).tar.gz -C /data .
```

### Restore

```bash
docker run --rm \
  -v datachef-data:/data \
  -v $(pwd):/backup \
  alpine tar xzf /backup/datachef-backup-20260310.tar.gz -C /data
```

---

## 🛠️ Development

### Local Setup

```bash
# Clone the repository
git clone https://github.com/JasSra/data-chef.git
cd data-chef

# Install dependencies
npm install

# Set encryption key
export CONNECTOR_SECRET_KEY=$(openssl rand -base64 32)

# Run development server
npm run dev
```

### Build Docker Image Locally

```bash
docker build -t datachef:local .
```

### Run Tests

```bash
npm test
```

---

## 📝 License

Private repository - All rights reserved

---

## 🤝 Support

- **Issues:** [GitHub Issues](https://github.com/JasSra/data-chef/issues)
- **Documentation:** [`/docs`](docs/)
- **Health Check:** `http://localhost:8080/api/health`

---

## 🎯 Quick Links

| Resource | URL |
|----------|-----|
| **Application** | `http://localhost:8080` |
| **Health Check** | `http://localhost:8080/api/health` |
| **Worker Status** | `http://localhost:8080/api/workers` |
| **Docker Hub** | `https://hub.docker.com/r/jassra/datachef` |
| **GitHub Actions** | `https://github.com/JasSra/data-chef/actions` |

---

**Built with:** Next.js 14, TypeScript, TailwindCSS, OpenTelemetry, Docker
