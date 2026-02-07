# DROP TABLE

Drop a table from the Lance catalog.

## Syntax

```sql
DROP TABLE [IF EXISTS] <table_name>
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `IF EXISTS` | Optional. Prevents error if the table does not exist. |
| `table_name` | Fully qualified table name: `catalog.schema.table`. |

## Examples

Drop a table:

```sql
DROP TABLE lance.default.old_data;
```

Drop a table only if it exists:

```sql
DROP TABLE IF EXISTS lance.default.temp_results;
```
