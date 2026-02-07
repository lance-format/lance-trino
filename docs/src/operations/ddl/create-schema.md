# CREATE SCHEMA

Create a new schema (namespace) in the Lance catalog.

!!! note
    CREATE SCHEMA is only supported in multi-level namespace mode. In single-level namespace mode, only the virtual `default` schema exists and schema creation is not allowed.

## Syntax

```sql
CREATE SCHEMA [IF NOT EXISTS] <catalog>.<schema>
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `IF NOT EXISTS` | Optional. Prevents error if the schema already exists. |
| `catalog` | The Lance catalog name. |
| `schema` | The name of the schema to create. |

## Examples

Create a new schema:

```sql
CREATE SCHEMA lance.my_schema;
```

Create a schema only if it doesn't exist:

```sql
CREATE SCHEMA IF NOT EXISTS lance.analytics;
```

## Limitations

- Schema properties and comments are not supported
- CASCADE option for dropping schemas with tables is not supported
