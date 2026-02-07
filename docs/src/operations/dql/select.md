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
