# INSERT

Insert data into a Lance table.

## Syntax

### Insert values

```sql
INSERT INTO <table_name> VALUES
    (<value>, ...),
    (<value>, ...),
    ...
```

### Insert from query

```sql
INSERT INTO <table_name>
SELECT ...
```

### Insert with column list

```sql
INSERT INTO <table_name> (<column>, ...)
VALUES (<value>, ...),
       (<value>, ...)
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `table_name` | Fully qualified table name: `catalog.schema.table`. |
| `column` | Column name for explicit column mapping. |
| `value` | Value to insert. |

## Examples

### Insert single row

```sql
INSERT INTO lance.default.users VALUES
    (1, 'Alice', 'alice@example.com');
```

### Insert multiple rows

```sql
INSERT INTO lance.default.products VALUES
    (1, 'Widget', 9.99),
    (2, 'Gadget', 19.99),
    (3, 'Gizmo', 29.99);
```

### Insert with explicit columns

```sql
INSERT INTO lance.default.users (id, name)
VALUES (2, 'Bob');
```

### Insert from SELECT

```sql
INSERT INTO lance.default.us_orders
SELECT * FROM lance.default.orders
WHERE country = 'US';
```

### Insert aggregated data

```sql
INSERT INTO lance.default.daily_summary
SELECT
    current_date as report_date,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM lance.default.orders
WHERE order_date = current_date;
```

## Limitations

- NULL values are inserted for unspecified columns
- DEFAULT values are not supported
- Concurrent inserts to the same table may cause conflicts
