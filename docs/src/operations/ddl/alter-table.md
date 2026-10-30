# ALTER TABLE

Modify an existing Lance table.

## Syntax

```sql
ALTER TABLE <table_name> ALTER COLUMN <column_name> DROP NOT NULL
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `table_name` | Fully qualified table name: `catalog.schema.table`. |
| `column_name` | Name of a column declared `NOT NULL`. |

## Examples

Allow `NULL` values in a column:

```sql
ALTER TABLE lance.default.accounts ALTER COLUMN id DROP NOT NULL;
```

## Limitations

- Only `DROP NOT NULL` is supported. Adding a `NOT NULL` constraint to an existing column is not supported by Trino.
- Adding, dropping and renaming columns and changing column types are not supported.
