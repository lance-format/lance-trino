# SHOW TABLES

List all tables in a schema.

## Syntax

```sql
SHOW TABLES FROM <catalog>.<schema>
```

```sql
SHOW TABLES FROM <catalog>.<schema> LIKE '<pattern>'
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `catalog` | The Lance catalog name. |
| `schema` | The schema name. |
| `pattern` | Optional SQL LIKE pattern to filter table names. |

## Examples

List all tables in a schema:

```sql
SHOW TABLES FROM lance.default;
```

Filter tables by pattern:

```sql
SHOW TABLES FROM lance.default LIKE 'user%';
```

```sql
SHOW TABLES FROM lance.analytics LIKE '%_daily';
```

## Output

Returns a single column `Table` containing the table names.

| Table |
|-------|
| users |
| orders |
| products |
