# UPDATE

Update existing rows in a Lance table.

## Syntax

```sql
UPDATE <table_name>
SET <column> = <value> [, <column> = <value> ...]
WHERE <condition>
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `table_name` | Fully qualified table name: `catalog.schema.table`. |
| `column` | Column name to update. |
| `value` | New value for the column. Can be a literal, expression, or subquery. |
| `condition` | Boolean expression to identify rows to update. |

## Examples

### Update single column

```sql
UPDATE lance.default.users
SET status = 'active'
WHERE id = 42;
```

### Update multiple columns

```sql
UPDATE lance.default.orders
SET
    status = 'shipped',
    shipped_at = CURRENT_TIMESTAMP
WHERE id = 1001;
```

### Update with expression

```sql
UPDATE lance.default.products
SET price = price * 1.10
WHERE category = 'electronics';
```

### Update with CASE expression

```sql
UPDATE lance.default.users
SET tier = CASE
    WHEN total_purchases > 10000 THEN 'gold'
    WHEN total_purchases > 1000 THEN 'silver'
    ELSE 'bronze'
END
WHERE tier IS NULL;
```

### Update with subquery

```sql
UPDATE lance.default.orders
SET discount = (
    SELECT discount_rate
    FROM lance.default.promotions
    WHERE code = 'SUMMER2024'
)
WHERE promo_code = 'SUMMER2024';
```

## Implementation

The Lance connector implements UPDATE using a **merge-on-read** approach:

1. Rows matching the WHERE clause are identified by their row addresses
2. These rows are marked as deleted using deletion vectors
3. New rows with updated values are written as new fragments
4. The operation is committed atomically

This approach provides:

- Atomic updates (all-or-nothing semantics)
- No in-place modifications (immutable data files)
- Efficient handling of sparse updates

## Limitations

- Concurrent updates to the same table may cause conflicts
- UPDATE without WHERE clause updates all rows (use with caution)
- Very frequent updates may lead to data fragmentation over time
