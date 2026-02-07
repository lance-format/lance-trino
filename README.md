# Trino Lance Connector

A Trino connector for reading Lance format data.
This connector allows Trino to query Lance datasets directly.

## Configuration

Create a catalog properties file (e.g., `etc/catalog/lance.properties`):

### Configuration Properties

| Property                       | Description                                                                           | Default |
|--------------------------------|---------------------------------------------------------------------------------------|---------|
| `lance.impl`                   | Namespace implementation: `dir`, `rest`, `glue`, `hive2`, `hive3`, or full class name | `dir`   |
| `lance.connection_timeout`     | Connection timeout duration                                                           | `1m`    |
| `lance.connection_retry_count` | Number of retries for failed connections                                              | `5`     |

Any other `lance.*` properties are stripped of the `lance.` prefix and passed to the namespace implementation.

#### Directory Namespace Properties

```properties
connector.name=lance
lance.impl=dir
lance.root=/path/to/warehouse
lance.storage.timeout=30s
lance.storage.retry-count=3
```

#### REST Namespace Properties

```properties
connector.name=lance
lance.impl=rest
lance.uri=https://api.lancedb.com
lance.tls.enabled=true
lance.header.Authorization=Bearer <token>
```

## Usage

Once configured, you can query Lance data using SQL:

```sql
-- List schemas
SHOW SCHEMAS FROM lance;

-- List tables
SHOW TABLES FROM lance.default;

-- Describe table schema
DESCRIBE lance.default.my_dataset;

-- Query a Lance dataset
SELECT * FROM lance.default.my_dataset;

-- Query with filtering and projection
SELECT id, name, value
FROM lance.default.my_dataset
WHERE value > 100
ORDER BY id
LIMIT 10;

-- Aggregations
SELECT category, COUNT(*), AVG(value)
FROM lance.default.my_dataset
GROUP BY category;
```
