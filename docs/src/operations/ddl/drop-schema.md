# DROP SCHEMA

Drop a schema (namespace) from the Lance catalog.

!!! note
    DROP SCHEMA is only supported in multi-level namespace mode. In single-level namespace mode, only the virtual `default` schema exists and cannot be dropped.

## Syntax

```sql
DROP SCHEMA [IF EXISTS] <catalog>.<schema>
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `IF EXISTS` | Optional. Prevents error if the schema does not exist. |
| `catalog` | The Lance catalog name. |
| `schema` | The name of the schema to drop. |

## Examples

Drop a schema:

```sql
DROP SCHEMA lance.my_schema;
```

Drop a schema only if it exists:

```sql
DROP SCHEMA IF EXISTS lance.old_schema;
```

## Limitations

- The schema must be empty (contain no tables) before it can be dropped
- CASCADE option is not supported
