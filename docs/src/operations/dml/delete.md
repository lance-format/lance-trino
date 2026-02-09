# DELETE

Delete rows from a Lance table using a predicate.

## Syntax

```sql
DELETE FROM <table_name>
WHERE <condition>
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `table_name` | Fully qualified table name: `catalog.schema.table`. |
| `condition` | Boolean expression to identify rows to delete. |

## Examples

### Delete by primary key

```sql
DELETE FROM lance.default.users
WHERE id = 42;
```

### Delete with multiple conditions

```sql
DELETE FROM lance.default.orders
WHERE status = 'cancelled' AND created_at < DATE '2024-01-01';
```

### Delete with IN clause

```sql
DELETE FROM lance.default.products
WHERE category IN ('deprecated', 'discontinued');
```

### Delete with subquery

```sql
DELETE FROM lance.default.orders
WHERE customer_id IN (
    SELECT id FROM lance.default.customers
    WHERE status = 'inactive'
);
```

### Delete all rows (use with caution)

```sql
DELETE FROM lance.default.temp_data
WHERE true;
```

## Implementation

The Lance connector implements DELETE using a **merge-on-read** approach with deletion vectors:

1. Rows matching the WHERE clause are identified by their row addresses
2. A deletion vector is created marking these rows as deleted
3. The deletion vector is committed to the dataset
4. Subsequent reads automatically filter out deleted rows

This approach provides:

- Fast delete operations (no data rewriting)
- Efficient storage (only deletion markers are stored)
- Consistent reads (deleted rows are immediately invisible)

## Limitations

- Concurrent deletes to the same table may cause conflicts
- DELETE without WHERE clause is not recommended; use DROP TABLE instead
- Very large deletion vectors may impact read performance over time
