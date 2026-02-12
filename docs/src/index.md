# Trino Lance Connector

The Trino Lance Connector allows [Trino](https://trino.io/) to efficiently query and modify datasets stored in [Lance](https://lancedb.github.io/lance/) format.

## Quick Start

1. [Install](install.md) the connector
2. [Configure](config.md) a catalog
3. Start querying your Lance data

## Example

```sql
-- Query a Lance dataset
SELECT * FROM lance.default.my_dataset
WHERE value > 100
ORDER BY id
LIMIT 10;
```
