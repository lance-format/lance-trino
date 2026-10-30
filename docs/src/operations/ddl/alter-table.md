# ALTER TABLE

Modify the schema of an existing Lance table.

## Syntax

```sql
ALTER TABLE <table_name> ADD COLUMN [IF NOT EXISTS] <column_name> <data_type>
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `table_name` | Fully qualified table name: `catalog.schema.table`. |
| `column_name` | Name of the new column. |
| `data_type` | Any type supported for [CREATE TABLE](create-table.md). |

## Examples

Add a column:

```sql
ALTER TABLE lance.default.users ADD COLUMN age INTEGER;
```

## Behavior

The new column is appended at the end of the schema. Existing data files are not rewritten,
and existing rows read `NULL` for the new column.

## Limitations

- Only `ADD COLUMN` is supported. Dropping, renaming columns and changing column types are not supported.
- Columns are always added last. `FIRST` and `AFTER` are not supported.
- Column comments and `NOT NULL` constraints are not supported.
