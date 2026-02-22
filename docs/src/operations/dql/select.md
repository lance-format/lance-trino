# SELECT

Query data from Lance tables using standard SQL.

## Syntax

```sql
SELECT [DISTINCT] <select_list>
FROM <table_name>
[WHERE <condition>]
[GROUP BY <columns>]
[HAVING <condition>]
[ORDER BY <columns> [ASC|DESC]]
[LIMIT <n>]
[OFFSET <n>]
```

## Clauses

### SELECT list

Specify columns to retrieve:

```sql
-- All columns
SELECT * FROM lance.default.users;

-- Specific columns
SELECT id, name FROM lance.default.users;

-- Expressions
SELECT id, name, price * quantity AS total FROM lance.default.orders;

-- Distinct values
SELECT DISTINCT category FROM lance.default.products;
```

### WHERE

Filter rows based on conditions:

```sql
-- Comparison operators
SELECT * FROM lance.default.products WHERE price > 100;

-- Logical operators
SELECT * FROM lance.default.users
WHERE age >= 18 AND status = 'active';

-- IN operator
SELECT * FROM lance.default.orders
WHERE status IN ('pending', 'processing');

-- LIKE pattern matching
SELECT * FROM lance.default.users
WHERE email LIKE '%@gmail.com';

-- NULL checks
SELECT * FROM lance.default.users
WHERE phone IS NOT NULL;

-- BETWEEN
SELECT * FROM lance.default.orders
WHERE order_date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31';
```

### GROUP BY

Group rows for aggregation:

```sql
SELECT category, COUNT(*) as count
FROM lance.default.products
GROUP BY category;

SELECT
    region,
    COUNT(*) as orders,
    SUM(amount) as total,
    AVG(amount) as average
FROM lance.default.orders
GROUP BY region;
```

### HAVING

Filter groups after aggregation:

```sql
SELECT category, COUNT(*) as count
FROM lance.default.products
GROUP BY category
HAVING COUNT(*) > 10;
```

### ORDER BY

Sort results:

```sql
-- Ascending (default)
SELECT * FROM lance.default.users ORDER BY name;

-- Descending
SELECT * FROM lance.default.orders ORDER BY created_at DESC;

-- Multiple columns
SELECT * FROM lance.default.products
ORDER BY category ASC, price DESC;
```

### LIMIT and OFFSET

Limit result set:

```sql
-- First 10 rows
SELECT * FROM lance.default.orders LIMIT 10;

-- Pagination (skip 20, take 10)
SELECT * FROM lance.default.orders
ORDER BY id
LIMIT 10 OFFSET 20;
```

## Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Count rows |
| `COUNT(column)` | Count non-null values |
| `COUNT(DISTINCT column)` | Count distinct values |
| `SUM(column)` | Sum of values |
| `AVG(column)` | Average of values |
| `MIN(column)` | Minimum value |
| `MAX(column)` | Maximum value |

## Examples

### Basic query

```sql
SELECT * FROM lance.default.customers;
```

### Filtered query with projection

```sql
SELECT id, name, email
FROM lance.default.users
WHERE status = 'active'
ORDER BY name
LIMIT 100;
```

### Aggregation with grouping

```sql
SELECT
    category,
    COUNT(*) as product_count,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM lance.default.products
GROUP BY category
HAVING COUNT(*) > 5
ORDER BY product_count DESC;
```

### Join tables

```sql
SELECT
    o.id,
    o.order_date,
    c.name as customer_name,
    o.amount
FROM lance.default.orders o
JOIN lance.default.customers c ON o.customer_id = c.id
WHERE o.amount > 1000;
```

### Subquery

```sql
SELECT *
FROM lance.default.products
WHERE category IN (
    SELECT category
    FROM lance.default.products
    GROUP BY category
    HAVING AVG(price) > 50
);
```

### Common Table Expression (CTE)

```sql
WITH high_value_orders AS (
    SELECT customer_id, SUM(amount) as total
    FROM lance.default.orders
    GROUP BY customer_id
    HAVING SUM(amount) > 10000
)
SELECT c.name, h.total
FROM high_value_orders h
JOIN lance.default.customers c ON h.customer_id = c.id;
```

## Time Travel

Lance supports time travel queries, allowing you to query historical versions of your data. This is useful for auditing, debugging, or recovering data from a specific point in time.

### Query by Version Number

Use `FOR VERSION AS OF` to query a specific version:

```sql
-- Query version 1 of a table
SELECT * FROM lance.default.orders FOR VERSION AS OF 1;

-- Query version 5 of a table
SELECT * FROM lance.default.orders FOR VERSION AS OF 5;

-- Compare data between versions
SELECT 'current' AS version, COUNT(*) FROM lance.default.orders
UNION ALL
SELECT 'v1' AS version, COUNT(*) FROM lance.default.orders FOR VERSION AS OF 1;
```

Supported version types:

- `TINYINT`
- `SMALLINT`
- `INTEGER`
- `BIGINT`

### Query by Timestamp

Use `FOR TIMESTAMP AS OF` to query data as of a specific timestamp:

```sql
-- Query data as of a specific date
SELECT * FROM lance.default.orders
FOR TIMESTAMP AS OF DATE '2024-01-15';

-- Query data as of a specific timestamp
SELECT * FROM lance.default.orders
FOR TIMESTAMP AS OF TIMESTAMP '2024-06-01 12:00:00';

-- Query data with timezone
SELECT * FROM lance.default.orders
FOR TIMESTAMP AS OF TIMESTAMP '2024-06-01 12:00:00 America/New_York';
```

Supported timestamp types:

- `DATE` - Resolves to start of day in session timezone
- `TIMESTAMP` - Uses session timezone
- `TIMESTAMP WITH TIME ZONE` - Uses specified timezone

### How It Works

Lance stores data in immutable fragments. Each write operation (INSERT, UPDATE, DELETE) creates a new version of the table. Time travel queries read from the version that was current at the specified version number or timestamp.

When querying by timestamp, Lance finds the latest version whose creation timestamp is at or before the requested timestamp.

### Limitations

- Only `FOR ... AS OF` is supported (end version). Start version (`FOR ... BETWEEN`) is not supported.
- Version numbers must be positive integers.
- Timestamps must be in the past.
- Versions may be cleaned up by compaction operations, making old versions unavailable.

### Examples

```sql
-- Track changes over time
WITH current_data AS (
    SELECT COUNT(*) as cnt FROM lance.default.metrics
),
historical_data AS (
    SELECT COUNT(*) as cnt FROM lance.default.metrics FOR VERSION AS OF 1
)
SELECT
    (SELECT cnt FROM current_data) - (SELECT cnt FROM historical_data) as new_rows;

-- Restore accidentally deleted data
INSERT INTO lance.default.orders
SELECT * FROM lance.default.orders FOR VERSION AS OF 10
WHERE id NOT IN (SELECT id FROM lance.default.orders);
```

## Blob Virtual Columns

Tables with blob-encoded columns automatically expose virtual columns for accessing blob metadata. These columns are hidden from `DESCRIBE TABLE` but can be selected in queries.

### Virtual Column Naming

For each blob column, two virtual columns are available:

| Virtual Column | Type | Description |
|----------------|------|-------------|
| `<column>__blob_pos` | BIGINT | Byte offset of the blob data in the blob file |
| `<column>__blob_size` | BIGINT | Size of the blob data in bytes |

### Querying Blob Metadata

```sql
-- Get blob position and size
SELECT id, content__blob_pos, content__blob_size
FROM lance.default.documents;

-- Filter by blob size
SELECT id, title
FROM lance.default.documents
WHERE content__blob_size > 1000000;  -- Blobs larger than 1MB

-- Aggregate blob statistics
SELECT
    COUNT(*) as total_blobs,
    SUM(image__blob_size) as total_bytes,
    AVG(image__blob_size) as avg_size,
    MAX(image__blob_size) as largest_blob
FROM lance.default.media;
```

### Multiple Blob Columns

When a table has multiple blob columns, each has its own virtual columns:

```sql
-- Table created with: blob_columns = 'image, video'
SELECT
    id,
    image__blob_pos,
    image__blob_size,
    video__blob_pos,
    video__blob_size
FROM lance.default.media;
```

!!! note
    Blob virtual columns return metadata about where the blob is stored, not the blob content itself. The actual blob data is stored out-of-line and not materialized during queries.
