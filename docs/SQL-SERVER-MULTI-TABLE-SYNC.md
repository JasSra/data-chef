# SQL Server Multi-Table Sync Guide

## Overview

DataChef supports syncing multiple SQL Server tables into separate datasets using a single connector. This guide explains how to set up and manage multi-table synchronization.

## Quick Start

### 1. Create the SQL Server Connector

1. Navigate to **Connections** page
2. Click **Add Connector**
3. Select **SQL Server / Azure SQL**
4. Configure connection:
   - **Host**: Your SQL Server hostname (use `host.docker.internal` when running in Docker to reach host machine)
   - **Port**: `1433` (default)
   - **Instance Name**: Leave empty for default instance (only use if you have a named instance like SQLEXPRESS)
   - **Database**: Your database name
   - **Schema**: `dbo` (or your schema)
   - **Username/Password**: SQL Server authentication credentials
   - **Encrypt**: ON (recommended)
   - **Trust Server Certificate**: ON for development/self-signed certificates

5. Click **Test Connection** to verify
6. Save the connector

### 2. Create Datasets for Each Table

For each table you want to sync:

1. Go to **Datasets** page
2. Click **New Dataset**
3. Select **Existing Connector**
4. Choose your SQL Server connector
5. In the **Resource** field, enter the table name (e.g., `Customers`, `Orders`, `Products`)
6. Give the dataset a descriptive name
7. Save the dataset

### 3. Sync Your Data

When you click **Sync** on the SQL Server connector, it will automatically:
- Query each linked dataset's table
- Infer the schema for each table
- Update each dataset with fresh data
- Show consolidated statistics

## Docker Networking

### Common Issue: "Cannot reach SQL Server from container"

When DataChef runs in a Docker container, `localhost` refers to the container itself, not your host machine.

**Solution Options:**

1. **Use `host.docker.internal`** (Docker Desktop):
   - Set `MSSQL_DEFAULT_HOST=host.docker.internal` in `.env` or docker-compose.yml
   - Or manually enter `host.docker.internal` as the host in the connector wizard

2. **Use host IP directly**:
   - Set `MSSQL_DEFAULT_HOST=192.168.4.102` (your machine's IP)
   - Or enter the IP in the connector wizard

3. **Use Docker host network mode** (Linux only):
   ```yaml
   services:
     app:
       network_mode: "host"
   ```

### Environment Variable Configuration

Add to `.env` or `docker-compose.yml`:

```env
# Default host for SQL Server connectors when running in Docker
MSSQL_DEFAULT_HOST=host.docker.internal
```

## Advanced Usage

### Using SQL Queries Instead of Tables

Instead of a table name, you can use SQL queries in the Resource field:

```sql
SELECT id, name, amount, created_at 
FROM orders 
WHERE status = 'active'
```

### Schema Selection

If your tables are in a non-default schema:

1. Set the **Schema** field in the connector (e.g., `sales`, `reporting`)
2. Or use fully qualified names in the Resource field: `sales.Orders`

### Sync Scheduling

- **On-demand**: Manual sync only
- **1h**: Sync every hour
- **6h**: Sync every 6 hours
- **24h**: Sync daily

Set this in the connector configuration under **Schedule**.

## Troubleshooting

### Error: "Invalid object name 'dbo.dbo_sample'"

This error occurred in older versions when no table was specified. Update to the latest version - the connector now automatically discovers the first available table.

### Error: "No tables found in schema 'dbo'"

Your database has no tables in the specified schema. Solutions:
- Check if tables exist: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo'`
- Verify you're connected to the correct database
- Try a different schema name

### Connection Timeout

If you get "Failed to connect in 10000ms":
- Verify SQL Server is running
- Check firewall rules (port 1433)
- For named instances, verify SQL Server Browser service is running
- Ensure TCP/IP protocol is enabled in SQL Server Configuration Manager

### Named Instance Issues

SQL Server named instances (like SQLEXPRESS) use dynamic ports. Two options:

1. **Use the default instance** (leave Instance Name blank)
2. **Configure static port** in SQL Server Configuration Manager and use that port number

## Best Practices

1. **Start simple**: Create the connector first, test it, then add datasets
2. **Name clearly**: Use descriptive dataset names like "CRM Customers" instead of generic "Table1"
3. **Monitor syncs**: Check the Jobs panel to see sync progress and errors
4. **Use incremental sync**: For large tables, configure incremental sync with a cursor column
5. **Secure credentials**: Always use encrypted connections in production

## Example Workflow

```
1. Create SQL Server connector "Production SQL"
   ↓
2. Create dataset "Customers" → resource: Customers
   ↓
3. Create dataset "Orders" → resource: Orders
   ↓
4. Create dataset "Products" → resource: Products
   ↓
5. Click "Sync" on "Production SQL"
   ↓
6. All three datasets update automatically!
```

## Architecture Notes

- **One connector, many datasets**: A single SQL Server connector can serve multiple datasets
- **Automatic schema inference**: DataChef automatically detects columns and types
- **Pipeline integration**: Synced datasets can be used as sources in pipeline nodes
- **Incremental updates**: The connector remembers last sync time for each dataset
