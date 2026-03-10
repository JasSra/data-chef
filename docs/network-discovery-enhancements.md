# Network Discovery Enhancements

## Overview

The network discovery system has been enhanced to scan beyond the local Docker network and group discovered services by type and port.

## Key Features

### 1. Multi-Layer Network Scanning

The discovery process now scans networks in a bottom-up approach:

1. **Local Networks** (Priority 1):
   - Docker container interfaces
   - Host machine interfaces  
   - Common hostnames (localhost, host.docker.internal, db, postgres, etc.)

2. **Intermediate Networks** (Priority 2):
   - Gateway IPs discovered via traceroute
   - Subnet ranges around intermediate hops
   - Host machine subnet (one level up from Docker network)

### 2. Traceroute Integration

- Fetches public IP via `https://api.ipify.org`
- Runs traceroute to discover intermediate network hops
- Extracts private IP addresses from the route
- Scans surrounding IPs in discovered subnets

### 3. Service Grouping

Services are now grouped by `type` and `port`:

- **groupId**: `grp-{type}-{port}` (e.g., `grp-postgresql-5432`)
- Multiple instances of the same service across different networks are grouped together
- Users can see all IP addresses/hostnames where a service is accessible

### 4. New API Endpoint

**GET /api/discovery/grouped**

Returns discovered services grouped by type and port:

```json
[
  {
    "groupId": "grp-postgresql-5432",
    "type": "postgresql",
    "port": 5432,
    "displayName": "PostgreSQL (Port 5432)",
    "totalCount": 3,
    "highestConfidence": 0.94,
    "hosts": [
      {
        "host": "172.17.0.2",
        "confidence": 0.94,
        "matchReason": "PostgreSQL SSL handshake responded",
        "status": "new",
        "lastSeen": "2 minutes ago",
        "candidateId": "disc-postgresql-172-17-0-2-5432"
      },
      {
        "host": "localhost",
        "confidence": 0.94,
        "matchReason": "PostgreSQL SSL handshake responded",
        "status": "new",
        "lastSeen": "2 minutes ago",
        "candidateId": "disc-postgresql-localhost-5432"
      },
      {
        "host": "192.168.1.10",
        "confidence": 0.94,
        "matchReason": "PostgreSQL SSL handshake responded",
        "status": "new",
        "lastSeen": "2 minutes ago",
        "candidateId": "disc-postgresql-192-168-1-10-5432"
      }
    ]
  }
]
```

## Configuration

### Scan Budget

The maximum number of hosts scanned has been increased from 60 to 150 to accommodate multi-layer scanning.

### Docker Requirements

The Docker image now includes traceroute utilities:
- `busybox-extras` (provides traceroute on Alpine Linux)
- `iputils` (additional network utilities)

## Implementation Details

### Functions Added/Modified

1. **`getPublicIp()`**: Fetches public IP from ipify.org API
2. **`getTracerouteHops(targetIp)`**: Runs traceroute and extracts private IPs
3. **`buildScanTargets()`**: Enhanced to include intermediate networks
4. **`getGroupedDiscoveryCandidates()`**: Returns grouped view of candidates
5. **`DiscoveryCandidate`**: Now includes `groupId` field

### Backward Compatibility

- Existing `DiscoveryCandidate` records without `groupId` automatically get one assigned
- Original `/api/discovery` endpoint still works with flat list view
- New `/api/discovery/grouped` endpoint provides grouped view

## Use Cases

### 1. Docker Service Accessibility

When a PostgreSQL container is accessible via:
- `localhost:5432` (Docker port forwarding)
- `172.17.0.2:5432` (Docker network)
- `192.168.1.10:5432` (Host machine IP)

All three are grouped together, allowing users to:
- Choose which network path to use
- Verify service is accessible from desired networks
- Debug connectivity issues across network layers

### 2. Network Security Analysis

Users can verify:
- Which services are exposed beyond Docker network
- Which services are accessible from host machine subnet
- Network segmentation is working correctly

## Future Enhancements

- Network topology visualization
- Automatic network reachability testing
- Custom subnet scanning rules
- Integration with network security policies
