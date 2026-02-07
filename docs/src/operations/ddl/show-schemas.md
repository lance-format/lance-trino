# SHOW SCHEMAS

List all schemas (namespaces) in the Lance catalog.

## Syntax

```sql
SHOW SCHEMAS FROM <catalog>
```

```sql
SHOW SCHEMAS FROM <catalog> LIKE '<pattern>'
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `catalog` | The Lance catalog name. |
| `pattern` | Optional SQL LIKE pattern to filter schema names. |

## Examples

List all schemas:

```sql
SHOW SCHEMAS FROM lance;
```

Filter schemas by pattern:

```sql
SHOW SCHEMAS FROM lance LIKE 'prod%';
```

```sql
SHOW SCHEMAS FROM lance LIKE '%_staging';
```

## Output

Returns a single column `Schema` containing the schema names.

| Schema |
|--------|
| default |
| analytics |
| staging |
