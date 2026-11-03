# ALTER TABLE

Modify an existing Lance table.

## Syntax

```sql
ALTER TABLE <table_name> ADD COLUMN [IF NOT EXISTS] <column_name> <data_type>
ALTER TABLE <table_name> ALTER COLUMN <column_name> DROP NOT NULL
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `table_name` | Fully qualified table name: `catalog.schema.table`. |
| `column_name` | Name of the new column, or of a column declared `NOT NULL`. |
| `data_type` | Any type supported for [CREATE TABLE](create-table.md). |

## Examples

Add a column:

```sql
ALTER TABLE lance.default.users ADD COLUMN age INTEGER;
```

Allow `NULL` values in a column:

```sql
ALTER TABLE lance.default.accounts ALTER COLUMN id DROP NOT NULL;
```

## Behavior

The new column is appended at the end of the schema. Existing data files are not rewritten,
and existing rows read `NULL` for the new column.

## Limitations

- Only `ADD COLUMN` and `DROP NOT NULL` are supported. Dropping, renaming columns and changing column types are not supported.
- Adding a `NOT NULL` constraint to an existing column is not supported by Trino.
- `ADD COLUMN` is not supported on tables using the `legacy` (`0.1`) file format. Tables created with `2.0` or later are supported.
- Columns are always added last. `FIRST` and `AFTER` are not supported.
- Column comments and `NOT NULL` constraints are not supported in `ADD COLUMN`.
