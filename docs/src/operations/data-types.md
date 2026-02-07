# Data Types

Lance supports a subset of Trino data types. This page documents the supported types and their mappings.

## Supported Types

| Trino Type | Lance Type | Description |
|------------|------------|-------------|
| `BIGINT` | Int64 | 64-bit signed integer |
| `INTEGER` | Int32 | 32-bit signed integer |
| `DOUBLE` | Float64 | 64-bit IEEE 754 floating point |
| `REAL` | Float32 | 32-bit IEEE 754 floating point |
| `VARCHAR` | Utf8 | Variable-length Unicode string |
| `BOOLEAN` | Boolean | True or false |
| `DATE` | Date32 | Calendar date (days since epoch) |
| `VARBINARY` | Binary | Variable-length binary data |
| `ARRAY<T>` | List | Array of elements of type T |

## Type Examples

### Numeric types

```sql
CREATE TABLE lance.default.numbers (
    big_num BIGINT,
    int_num INTEGER,
    float_num REAL,
    double_num DOUBLE
);

INSERT INTO lance.default.numbers VALUES
    (9223372036854775807, 2147483647, 3.14, 3.141592653589793);
```

### String types

```sql
CREATE TABLE lance.default.strings (
    name VARCHAR,
    description VARCHAR
);

INSERT INTO lance.default.strings VALUES
    ('Alice', 'A user named Alice'),
    ('Bob', 'Another user');
```

### Boolean type

```sql
CREATE TABLE lance.default.flags (
    id BIGINT,
    is_active BOOLEAN,
    is_verified BOOLEAN
);

INSERT INTO lance.default.flags VALUES
    (1, true, false),
    (2, false, true);
```

### Date type

```sql
CREATE TABLE lance.default.events (
    id BIGINT,
    event_date DATE
);

INSERT INTO lance.default.events VALUES
    (1, DATE '2024-01-15'),
    (2, DATE '2024-06-30');
```

### Binary type

```sql
CREATE TABLE lance.default.blobs (
    id BIGINT,
    data VARBINARY
);

INSERT INTO lance.default.blobs VALUES
    (1, X'48454C4C4F');
```

### Array type

```sql
CREATE TABLE lance.default.lists (
    id BIGINT,
    tags ARRAY(VARCHAR),
    scores ARRAY(BIGINT)
);

-- Note: Array writes have limited support
```

## Unsupported Types

The following Trino types are **not supported**:

| Type | Alternative |
|------|-------------|
| `TINYINT` | Use `INTEGER` or `BIGINT` |
| `SMALLINT` | Use `INTEGER` or `BIGINT` |
| `DECIMAL` | Use `DOUBLE` |
| `CHAR(n)` | Use `VARCHAR` |
| `TIME` | Store as `VARCHAR` or epoch `BIGINT` |
| `TIMESTAMP` | Store as `DATE` or epoch `BIGINT` |
| `MAP<K,V>` | Not supported |
| `ROW(...)` | Not supported for writes |

## Type Casting

Use CAST to convert between types:

```sql
SELECT
    CAST(id AS VARCHAR) as id_string,
    CAST(price AS BIGINT) as price_int
FROM lance.default.products;
```

## NULL Values

All columns support NULL values:

```sql
INSERT INTO lance.default.users (id, name, email)
VALUES (1, 'Alice', NULL);

SELECT * FROM lance.default.users
WHERE email IS NULL;
```
