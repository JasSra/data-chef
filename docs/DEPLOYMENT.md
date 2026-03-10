# Data Chef - Secure Production Deployment Guide

This guide covers secure deployment of Data Chef in production, including data persistence, secrets management, and proper environment configuration.

---

## 🔐 Security Requirements

### 1. Required Secret: Encryption Key

Data Chef encrypts sensitive connector credentials (API keys, passwords, tokens) using AES-256-GCM encryption. You **MUST** set a secret key:

```bash
CONNECTOR_SECRET_KEY=<your-random-32-char-string>
```

**How to generate a secure key:**

```bash
# Linux/Mac
openssl rand -base64 32

# PowerShell
[Convert]::ToBase64String((1..32 | ForEach-Object { Get-Random -Maximum 256 }))

# Node.js
node -e "console.log(require('crypto').randomBytes(32).toString('base64'))"
```

⚠️ **CRITICAL:** 
- Never commit this key to git
- Use the same key across all container instances
- If you lose this key, all encrypted credentials become unrecoverable
- Store it in a secure vault (Azure Key Vault, AWS Secrets Manager, etc.)

---

## 📦 Data Persistence

### What Data is Stored?

Data Chef stores all configuration in the **`.datachef/`** directory:

- `/app/.datachef/platform/tenants.json` - Tenant configuration
- `/app/.datachef/tenant_*/connectors.json` - Connector definitions (with encrypted credentials)
- `/app/.datachef/tenant_*/datasets.json` - Dataset metadata
- `/app/.datachef/tenant_*/pipelines.json` - Pipeline configurations
- `/app/.datachef/tenant_*/recipes.json` - Query recipes
- `/app/.datachef/tenant_*/discoveries.json` - Network discovery scans

### Volume Mounting Strategy

**Option 1: Docker Named Volume (Recommended for single-host)**
```bash
docker run -d \
  -v datachef-data:/app/.datachef \
  -p 8080:3000 \
  --name data-chef \
  jassra/datachef:latest
```

**Option 2: Bind Mount (For backups/versioning)**
```bash
docker run -d \
  -v /opt/datachef/storage:/app/.datachef \
  -p 8080:3000 \
  --name data-chef \
  jassra/datachef:latest
```

**Option 3: Cloud Storage (Production multi-instance)**
For shared storage across multiple containers, mount network storage:
- Azure Files Share
- AWS EFS
- NFS mount

---

## 🔒 Secure Environment Configuration

### Create a `.env` file

**DO NOT** commit this file to source control. Add `.env` to your `.gitignore`.

```bash
# .env - NEVER COMMIT TO GIT

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# REQUIRED: Encryption key for connector credentials
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONNECTOR_SECRET_KEY=<your-generated-key-from-above>

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OPTIONAL: Pre-configured Azure App Insights connector
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# APPINSIGHTS_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
# APPINSIGHTS_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
# APPINSIGHTS_CLIENT_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
# APPINSIGHTS_WORKSPACE_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OPTIONAL: External shared Redis (if not using local connector)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# REDIS_HOST=redis.threadcode.internal
# REDIS_PORT=6379
# REDIS_PASSWORD=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
# REDIS_TLS=true

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OPTIONAL: OpenTelemetry (OTEL) Distributed Tracing & Metrics
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OTEL_ENABLED=true
# OTEL_SERVICE_NAME=data-chef
# OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
# OTEL_EXPORTER_OTLP_HEADERS=x-api-key=your-observability-api-key
```

---

## 🚀 Production Deployment Examples

### Single Container (Docker CLI)

```bash
# Pull the latest image
docker pull jassra/datachef:latest

# Run with environment file and persistent volume
docker run -d \
  --name data-chef \
  --restart always \
  -p 8080:3000 \
  --env-file /secure/path/.env \
  -v datachef-data:/app/.datachef \
  --health-cmd="wget -qO- http://localhost:3000/api/health || exit 1" \
  --health-interval=30s \
  --health-retries=3 \
  jassra/datachef:latest
```

### Docker Compose (Recommended)

Create `docker-compose.production.yml`:

```yaml
version: '3.8'

services:
  data-chef:
    image: jassra/datachef:latest
    container_name: data-chef
    restart: unless-stopped
    
    ports:
      - "8080:3000"
    
    # Mount persistent storage
    volumes:
      - datachef-data:/app/.datachef
      # Optional: for backups
      # - ./backups:/app/.datachef:ro
    
    # Load secrets from .env file (NEVER commit this file)
    env_file:
      - .env  # Contains CONNECTOR_SECRET_KEY and other secrets
    
    environment:
      NODE_ENV: production
      # Optionally override specific vars here
    
    # Health check (built into image)
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:3000/api/health"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 40s
    
    # Security: run as non-root user
    user: "1001:1001"
    
    # Resource limits (adjust based on workload)
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '0.5'
          memory: 512M

volumes:
  datachef-data:
    driver: local
```

**Deploy:**
```bash
docker compose -f docker-compose.production.yml up -d
```

---

## 🌐 Multi-Instance Deployment (Load Balanced)

For high availability with multiple instances sharing data:

### Requirements:
1. **Shared storage** for `/app/.datachef` (Azure Files, NFS, EFS)
2. **Same `CONNECTOR_SECRET_KEY`** across all instances
3. **Load balancer** (nginx, traefik, cloud LB)

### Example with Azure Files:

```yaml
version: '3.8'

services:
  data-chef-1:
    image: jassra/datachef:latest
    restart: unless-stopped
    env_file: .env
    volumes:
      - type: volume
        source: datachef-shared
        target: /app/.datachef
    ports:
      - "8081:3000"

  data-chef-2:
    image: jassra/datachef:latest
    restart: unless-stopped
    env_file: .env
    volumes:
      - type: volume
        source: datachef-shared
        target: /app/.datachef
    ports:
      - "8082:3000"

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      - data-chef-1
      - data-chef-2

volumes:
  datachef-shared:
    driver: local
    driver_opts:
      type: cifs
      o: addr=threadcode.file.core.windows.net,username=threadcode,password=${AZURE_STORAGE_KEY},vers=3.0
      device: //threadcode.file.core.windows.net/datachef
```

---

## 🔑 Secrets Management Best Practices

### Development
```bash
# Use .env file (gitignored)
cp .env.example .env
# Edit .env and add your CONNECTOR_SECRET_KEY
```

### Production Options

**Option 1: Docker Secrets (Swarm/Kubernetes)**
```bash
echo "your-secret-key" | docker secret create connector_secret_key -

# In docker-compose:
secrets:
  - connector_secret_key

environment:
  CONNECTOR_SECRET_KEY_FILE: /run/secrets/connector_secret_key
```

**Option 2: Environment Variable (from vault)**
```bash
# Azure Key Vault
export CONNECTOR_SECRET_KEY=$(az keyvault secret show --vault-name myVault --name datachef-key --query value -o tsv)

# AWS Secrets Manager
export CONNECTOR_SECRET_KEY=$(aws secretsmanager get-secret-value --secret-id datachef/connector-key --query SecretString --output text)

docker run --env CONNECTOR_SECRET_KEY ...
```

**Option 3: CI/CD Pipeline Injection**
In GitHub Actions, Azure DevOps, etc., set secrets as pipeline variables and inject at runtime.

---

## 📋 Health & Monitoring

### Health Check Endpoint
```bash
curl http://localhost:8080/api/health
```

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2026-03-10T12:34:56.789Z",
  "workers": {
    "connectorScheduler": true,
    "discoveryScheduler": true
  },
  "uptime": 12345.67
}
```

### Monitoring in Production

- Health endpoint: `/api/health`
- Worker status: `/api/workers`
- Docker health checks run every 30s automatically

---

## 🛡️ Security Checklist

- [ ] Generate a strong `CONNECTOR_SECRET_KEY` (32+ bytes)
- [ ] Never commit `.env` to git
- [ ] Use Docker secrets or vault for production
- [ ] Mount `/app/.datachef` for persistence
- [ ] Enable HTTPS/TLS in front of the container (nginx/traefik)
- [ ] Run container as non-root user (already configured as `1001:1001`)
- [ ] Restrict network access (firewall/security groups)
- [ ] Regular backups of `/app/.datachef` volume
- [ ] Rotate secrets periodically
- [ ] Monitor health checks and logs

---

## 📞 Quick Start Command

```bash
# 1. Generate encryption key
export CONNECTOR_SECRET_KEY=$(openssl rand -base64 32)

# 2. Run container
docker run -d \
  --name data-chef \
  --restart always \
  -p 8080:3000 \
  -e CONNECTOR_SECRET_KEY="$CONNECTOR_SECRET_KEY" \
  -v datachef-data:/app/.datachef \
  jassra/datachef:latest

# 3. Verify
curl http://localhost:8080/api/health

# 4. Access UI
open http://localhost:8080
```

**⚠️ SAVE YOUR ENCRYPTION KEY!** Store it securely - you'll need it for all future deployments.

---

## 🔄 Backup & Recovery

### Backup
```bash
# Stop container
docker stop data-chef

# Backup volume
docker run --rm -v datachef-data:/data -v $(pwd):/backup alpine tar czf /backup/datachef-backup-$(date +%Y%m%d).tar.gz -C /data .

# Restart
docker start data-chef
```

### Restore
```bash
docker run --rm -v datachef-data:/data -v $(pwd):/backup alpine sh -c "cd /data && tar xzf /backup/datachef-backup-20260310.tar.gz"
```

---

## 📖 Additional Resources

- GitHub Actions CI/CD: [`.github/workflows/docker-publish.yml`](.github/workflows/docker-publish.yml)
- Docker Hub: `jassra/datachef:latest`
- Health Check: `/api/health`
- Worker Status: `/api/workers`
