# Pipeline Node Guide

This guide documents the pipeline nodes available in the builder and provides examples that match the current runtime behavior.

## Source

The first node is always the source node. Select a dataset or connector in the builder header before adding transform nodes.

Examples:
- `Demo B2C Users`
- `Demo NGINX Ecommerce Logs`

## Validate

Use this node to assert required fields and types before downstream steps rely on them.

Example schema:

```text
id: string
createdDateTime: timestamp
identities: array
```

## Query / Filter

Use SQL, KQL, JSONPath, or JMESPath to reshape upstream rows.

Examples:

```sql
SELECT id, userPrincipalName
FROM upstream
WHERE accountEnabled = true
ORDER BY createdDateTime DESC
LIMIT 100
```

```text
$[*].identities[*]
```

```text
[].{userId: id, upn: userPrincipalName, identityCount: length(identities)}
```

## Transform / Map

Use this node to rename or curate fields after flattening or enrichment.

Example:
- `$.identities_issuerAssignedId -> identityKey`
- `$.enrich_domain -> identityDomain`

## Coerce Types

Normalize one field into a known type for later comparison or writing.

Examples:
- `$.createdDateTime -> timestamp`
- `$.status -> integer`

## Flatten

Expand nested JSON.

Examples:
- Array mode on `$.identities` creates one row per identity.
- Object mode on `$.identities` creates fields such as `identities_signInType` and `identities_issuerAssignedId`.

## Enrich

Call an HTTP endpoint to derive extra metadata and merge it into each row.

Example:
- Join key: `$.signInId`
- URL: `http://localhost:3333/api/pipelines/demo-enrich`
- Fields: `normalized,domain,tenant,isMailosaur,isSynthetic`

## Deduplicate

Keep one row per logical key.

Examples:
- `$.signInId`
- `$.orderId`

## Branch / Condition

Use a readable row-level gate.

Examples:
- `$.accountEnabled == true`
- `$.status >= 500`
- `$.identities_issuerAssignedId contains mailosaur`

## Write / Project

Mark the final result shape for the run page and optionally prepare it for a downstream sink.

Examples:
- Final identity-gold shaped output in memory
- Final checkout-triage shaped output in memory

## Seeded Demo Pipelines

The app seeds demo pipelines for:
- `Demo · B2C Identity Expansion Gold`
- `Demo · NGINX Checkout Triage`

These are intended to be opened step-by-step so users can inspect the preview panel after each node.
