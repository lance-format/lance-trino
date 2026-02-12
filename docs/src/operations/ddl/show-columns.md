# SHOW COLUMNS

Display the columns of a table.

## Syntax

```sql
SHOW COLUMNS FROM <catalog>.<schema>.<table>
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `catalog` | The Lance catalog name. |
| `schema` | The schema name. |
| `table` | The table name. |

## Example

```sql
SHOW COLUMNS FROM lance.default.users;
```

## Output

| Column | Type | Extra | Comment |
|--------|------|-------|---------|
| id | bigint | | |
| name | varchar | | |
| email | varchar | | |
