# OpenTelemetry Observability Guide

Data Chef includes comprehensive OpenTelemetry (OTEL) instrumentation for distributed tracing, metrics collection, and observability.

---

## 🔭 What is Instrumented?

The application automatically traces:

- **HTTP Requests** - All incoming API calls and outgoing HTTP requests
- **Database Queries** - PostgreSQL, MySQL, MongoDB operations
- **Redis Commands** - All Redis interactions
- **Next.js Routing** - Server-side rendering and API routes
- **Background Workers** - Connector syncs and network discovery jobs

---

## 🚀 Quick Start

### 1. Enable OpenTelemetry

Add to your `.env` file:

```bash
OTEL_ENABLED=true
OTEL_SERVICE_NAME=data-chef
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
```

### 2. Run with OTEL Collector

```bash
docker pull jassra/datachef:latest

docker run -d \
  --name data-chef \
  -p 8080:3000 \
  -e OTEL_ENABLED=true \
  -e OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318 \
  --env-file .env \
  -v datachef-data:/app/.datachef \
  jassra/datachef:latest
```

---

## 📊 Supported Observability Backends

OpenTelemetry uses the standard OTLP (OpenTelemetry Protocol), compatible with:

| Backend | Setup |
|---------|-------|
| **Jaeger** | `http://jaeger:4318` |
| **Grafana Tempo** | `http://tempo:4318` |
| **Honeycomb** | `https://api.honeycomb.io` + `x-honeycomb-team` header |
| **New Relic** | `https://otlp.nr-data.net:4318` + `api-key` header |
| **Datadog** | `http://datadog-agent:4318` |
| **Azure Monitor** | Use Azure Monitor OpenTelemetry Distro |
| **Elastic APM** | `http://apm-server:8200` |
| **Self-hosted Collector** | `http://otel-collector:4318` |

---

## 🐳 Example: Docker Compose with Jaeger

Complete observability stack with Jaeger UI:

```yaml
version: '3.8'

services:
  # OpenTelemetry Collector
  otel-collector:
    image: otel/opentelemetry-collector:latest
    command: ["--config=/etc/otel-config.yaml"]
    volumes:
      - ./otel-config.yaml:/etc/otel-config.yaml
    ports:
      - "4318:4318"  # OTLP HTTP receiver
      - "4317:4317"  # OTLP gRPC receiver

  # Jaeger - Distributed tracing UI
  jaeger:
    image: jaegertracing/all-in-one:latest
    environment:
      COLLECTOR_OTLP_ENABLED: true
    ports:
      - "16686:16686"  # Jaeger UI
      - "14250:14250"  # Jaeger gRPC

  # Data Chef with OTEL enabled
  data-chef:
    image: jassra/datachef:latest
    ports:
      - "8080:3000"
    environment:
      OTEL_ENABLED: "true"
      OTEL_SERVICE_NAME: data-chef
      OTEL_EXPORTER_OTLP_ENDPOINT: http://otel-collector:4318
      CONNECTOR_SECRET_KEY: ${CONNECTOR_SECRET_KEY}
    volumes:
      - datachef-data:/app/.datachef
    depends_on:
      - otel-collector

volumes:
  datachef-data:
```

### OTEL Collector Config (`otel-config.yaml`)

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
      grpc:
        endpoint: 0.0.0.0:4317

processors:
  batch:
    timeout: 10s

exporters:
  jaeger:
    endpoint: jaeger:14250
    tls:
      insecure: true
  
  logging:
    loglevel: info

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [jaeger, logging]
    
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [logging]
```

**Access Jaeger UI:** http://localhost:16686

---

## 🌐 Production Examples

### Honeycomb

```yaml
environment:
  OTEL_ENABLED: "true"
  OTEL_SERVICE_NAME: data-chef
  OTEL_EXPORTER_OTLP_ENDPOINT: https://api.honeycomb.io
  OTEL_EXPORTER_OTLP_HEADERS: x-honeycomb-team=YOUR_API_KEY
```

### New Relic

```yaml
environment:
  OTEL_ENABLED: "true"
  OTEL_SERVICE_NAME: data-chef
  OTEL_EXPORTER_OTLP_ENDPOINT: https://otlp.nr-data.net:4318
  OTEL_EXPORTER_OTLP_HEADERS: api-key=YOUR_INGEST_KEY
```

### Datadog

```yaml
services:
  datadog-agent:
    image: gcr.io/datadoghq/agent:latest
    environment:
      DD_API_KEY: ${DD_API_KEY}
      DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_HTTP_ENDPOINT: 0.0.0.0:4318
    ports:
      - "4318:4318"

  data-chef:
    environment:
      OTEL_ENABLED: "true"
      OTEL_EXPORTER_OTLP_ENDPOINT: http://datadog-agent:4318
```

### Azure Monitor (App Insights)

```yaml
environment:
  OTEL_ENABLED: "true"
  OTEL_SERVICE_NAME: data-chef
  APPLICATIONINSIGHTS_CONNECTION_STRING: InstrumentationKey=...;IngestionEndpoint=https://...
```

Or use the Azure Monitor OpenTelemetry Distro for auto-configuration.

---

## 📈 What You'll See

### Traces

- **HTTP Requests**: `/api/pipelines`, `/api/datasets`, etc.
- **Database Queries**: PostgreSQL SELECT, INSERT, UPDATE
- **Redis Commands**: GET, SET, HGETALL
- **External API Calls**: GitHub API, Azure DevOps, etc.
- **Background Jobs**: Connector sync runs, network discovery scans

### Metrics (Automatic)

- HTTP request count & duration
- Database query count & duration
- Redis operation count
- Worker execution times
- Memory usage, CPU time

### Custom Instrumentation (Advanced)

You can add custom spans in your code:

```typescript
import { trace } from '@opentelemetry/api'

const tracer = trace.getTracer('data-chef')

export async function myFunction() {
  return tracer.startActiveSpan('custom-operation', async (span) => {
    try {
      span.setAttribute('custom.attribute', 'value')
      // Your code here
      return result
    } finally {
      span.end()
    }
  })
}
```

---

## 🔍 Troubleshooting

### OTEL not working?

**Check logs:**
```bash
docker logs data-chef | grep OpenTelemetry
```

Expected output:
```
[OpenTelemetry] Initializing with endpoint: http://otel-collector:4318
[OpenTelemetry] ✓ Initialized successfully
```

**Common issues:**

1. **"Disabled" message**: Set `OTEL_ENABLED=true`
2. **Connection refused**: Ensure OTEL collector is reachable
3. **No traces**: Check collector config and exporter endpoints
4. **Missing spans**: Some operations may be filtered by instrumentation config

### Test OTEL endpoint

```bash
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/json" \
  -d '{"resourceSpans":[]}'
```

Should return `200 OK` or similar success response.

---

## 🎯 Performance Impact

- **Overhead**: ~2-5% CPU, ~10-20MB memory
- **Network**: Batched exports every 60 seconds
- **Sampling**: Configure in OTEL collector for high-traffic scenarios

---

## 📋 Environment Variables Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OTEL_ENABLED` | No | `false` | Enable OpenTelemetry instrumentation |
| `OTEL_SERVICE_NAME` | No | `data-chef` | Service name in traces |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | No | `http://localhost:4318` | OTLP collector endpoint |
| `OTEL_EXPORTER_OTLP_HEADERS` | No | - | Custom headers (e.g., API keys) |

---

## 🛠️ Architecture

```
┌─────────────┐
│  Data Chef  │ (Next.js app with auto-instrumentation)
└──────┬──────┘
       │ OTLP (HTTP/gRPC)
       │ Traces + Metrics
       ▼
┌──────────────────┐
│ OTEL Collector   │ (Optional - batching, filtering, routing)
└────────┬─────────┘
         │
    ┌────┼────┬──────────┬───────────┐
    ▼    ▼    ▼          ▼           ▼
  Jaeger Tempo Honeycomb Datadog Azure Monitor
```

---

## 🔗 Resources

- [OpenTelemetry Docs](https://opentelemetry.io/docs/)
- [OTLP Specification](https://opentelemetry.io/docs/reference/specification/protocol/)
- [Collector Configuration](https://opentelemetry.io/docs/collector/configuration/)
- [Jaeger Getting Started](https://www.jaegertracing.io/docs/getting-started/)
