# TRUNCATE TABLE

Remove all rows from a Lance table.

## Syntax

```sql
TRUNCATE TABLE <table_name>
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `table_name` | Fully qualified table name: `catalog.schema.table`. |

## Examples

```sql
TRUNCATE TABLE lance.default.users;
```

## Limitations

- Truncating a table through a named ref (branch or tag) is not supported.
