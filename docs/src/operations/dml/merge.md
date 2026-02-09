# MERGE

Perform upsert operations (insert, update, or delete) on a Lance table based on matching conditions.

## Syntax

```sql
MERGE INTO <target_table> [AS <alias>]
USING <source> [AS <alias>]
ON <condition>
WHEN MATCHED [AND <condition>] THEN
    UPDATE SET <column> = <value> [, ...]
    | DELETE
WHEN NOT MATCHED [AND <condition>] THEN
    INSERT [(<column>, ...)] VALUES (<value>, ...)
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `target_table` | Fully qualified table name to merge into. |
| `source` | Source table, subquery, or VALUES clause. |
| `condition` | Join condition to match source and target rows. |
| `column` | Column name for UPDATE SET or INSERT. |
| `value` | Value expression for the column. |

## Examples

### Upsert (update existing, insert new)

```sql
MERGE INTO lance.default.products AS target
USING lance.default.product_updates AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET
        name = source.name,
        price = source.price,
        updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (id, name, price, created_at)
    VALUES (source.id, source.name, source.price, CURRENT_TIMESTAMP);
```

### Conditional update or delete

```sql
MERGE INTO lance.default.inventory AS target
USING lance.default.stock_changes AS source
ON target.sku = source.sku
WHEN MATCHED AND source.quantity = 0 THEN
    DELETE
WHEN MATCHED THEN
    UPDATE SET quantity = target.quantity + source.quantity;
```

### Insert only (no updates)

```sql
MERGE INTO lance.default.events AS target
USING lance.default.new_events AS source
ON target.event_id = source.event_id
WHEN NOT MATCHED THEN
    INSERT (event_id, event_type, timestamp, data)
    VALUES (source.event_id, source.event_type, source.timestamp, source.data);
```

### MERGE with VALUES clause

```sql
MERGE INTO lance.default.config AS target
USING (VALUES ('theme', 'dark'), ('language', 'en')) AS source(key, value)
ON target.key = source.key
WHEN MATCHED THEN
    UPDATE SET value = source.value
WHEN NOT MATCHED THEN
    INSERT (key, value) VALUES (source.key, source.value);
```

### Multiple WHEN clauses

```sql
MERGE INTO lance.default.accounts AS target
USING lance.default.account_updates AS source
ON target.id = source.id
WHEN MATCHED AND source.action = 'deactivate' THEN
    DELETE
WHEN MATCHED AND source.action = 'update' THEN
    UPDATE SET
        balance = source.balance,
        status = source.status
WHEN NOT MATCHED AND source.action = 'create' THEN
    INSERT (id, balance, status, created_at)
    VALUES (source.id, source.balance, 'active', CURRENT_TIMESTAMP);
```

## Implementation

The Lance connector implements MERGE using a **merge-on-read** approach:

1. Source and target rows are joined based on the ON condition
2. Each matched/unmatched row is processed according to the WHEN clauses
3. Deletions are recorded using deletion vectors
4. Updates are processed as delete + insert operations
5. New rows are written to new fragments
6. All changes are committed atomically

This provides:

- Atomic multi-row operations
- Efficient batch processing
- Consistent semantics with standard SQL MERGE

## Limitations

- Each source row must match at most one target row
- Concurrent MERGE operations to the same table may cause conflicts
- Complex MERGE operations may be slower than equivalent separate operations
