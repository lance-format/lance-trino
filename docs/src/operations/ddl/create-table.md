# CREATE TABLE / REPLACE TABLE

Create a new table or replace an existing table in the Lance catalog.

## Syntax

```sql
CREATE TABLE [IF NOT EXISTS] <table_name> (
    <column_name> <data_type> [, ...]
) [WITH (<property_name> = <value> [, ...])]
```

```sql
CREATE TABLE [IF NOT EXISTS] <table_name>
[WITH (<property_name> = <value> [, ...])]
AS <query>
```

```sql
CREATE OR REPLACE TABLE <table_name>
[WITH (<property_name> = <value> [, ...])]
AS <query>
```

```sql
REPLACE TABLE <table_name> (
    <column_name> <data_type> [, ...]
) [WITH (<property_name> = <value> [, ...])]
```

```sql
REPLACE TABLE <table_name>
[WITH (<property_name> = <value> [, ...])]
AS <query>
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `IF NOT EXISTS` | Optional. Prevents error if the table already exists. |
| `OR REPLACE` | Optional. Replaces the table if it already exists, or creates it if it doesn't. |
| `REPLACE TABLE` | Replaces an existing table. The table must already exist. |
| `table_name` | Fully qualified table name: `catalog.schema.table`. |
| `column_name` | Name of the column. |
| `data_type` | Data type of the column. See [Data Types](../data-types.md). |
| `query` | A SELECT query whose results populate the table. |

## Table Properties

| Property | Description |
|----------|-------------|
| `blob_columns` | Comma-separated list of VARBINARY columns to use blob encoding (out-of-line storage for large binary data). Example: `'image, video'` |
| `vector_columns` | Comma-separated list of vector columns with dimensions. Format: `'column1:dim1, column2:dim2'`. Columns must be `ARRAY(REAL)` or `ARRAY(DOUBLE)`. Example: `'embedding:768'` |
| `file_format_version` | Lance file format version for new tables. Valid values: `'legacy'`, `'0.1'`, `'2.0'`, `'2.1'`, `'2.2'`, `'stable'`, `'next'`. Default uses the Lance SDK's default version. |

## Examples

### Create an empty table

```sql
CREATE TABLE lance.default.users (
    id BIGINT,
    name VARCHAR,
    email VARCHAR,
    created_at DATE
);
```

### Create table with INSERT

After creating an empty table, you must insert data before querying:

```sql
CREATE TABLE lance.default.products (
    product_id BIGINT,
    name VARCHAR,
    price DOUBLE
);

INSERT INTO lance.default.products VALUES
    (1, 'Widget', 9.99),
    (2, 'Gadget', 19.99);
```

### Create table from query (CTAS)

```sql
CREATE TABLE lance.default.us_customers
AS SELECT * FROM lance.default.customers
WHERE country = 'US';
```

### Create table with row count

```sql
CREATE TABLE lance.default.summary
AS SELECT region, COUNT(*) as cnt
FROM lance.default.orders
GROUP BY region;
```

### Replace existing table (CREATE OR REPLACE)

Use `CREATE OR REPLACE` to replace a table if it exists, or create it if it doesn't:

```sql
CREATE OR REPLACE TABLE lance.default.daily_stats
AS SELECT
    current_date as report_date,
    COUNT(*) as total_orders
FROM lance.default.orders;
```

### Replace table with new data (REPLACE TABLE AS SELECT)

Use `REPLACE TABLE` to completely replace an existing table's data and schema:

```sql
-- Replace table with new data (same schema)
REPLACE TABLE lance.default.user_stats
AS SELECT user_id, COUNT(*) as order_count
FROM lance.default.orders
GROUP BY user_id;
```

Replace with a different schema:

```sql
-- Original table had: (id BIGINT, name VARCHAR, value DOUBLE)
-- Replace with completely different schema
REPLACE TABLE lance.default.metrics
AS SELECT
    VARCHAR 'metric_1' as metric_name,
    VARBINARY X'0102030405' as data;
```

### Replace table with empty schema

Replace a table with a new schema but no data:

```sql
REPLACE TABLE lance.default.staging (
    new_id BIGINT,
    description VARCHAR,
    created_at DATE
);
```

### Create table with blob columns

Blob encoding stores large binary data out-of-line, which is efficient for storing images, documents, or other large binary content:

```sql
CREATE TABLE lance.default.documents (
    id BIGINT,
    title VARCHAR,
    content VARBINARY
) WITH (blob_columns = 'content');
```

Multiple blob columns:

```sql
CREATE TABLE lance.default.media (
    id BIGINT,
    image VARBINARY,
    video VARBINARY
) WITH (blob_columns = 'image, video');
```

#### Blob Virtual Columns

When a table has blob columns, virtual columns are automatically available to retrieve blob metadata:

- `<column>__blob_pos` - The byte position of the blob data in the blob file
- `<column>__blob_size` - The size of the blob data in bytes

```sql
-- Query blob metadata
SELECT id, content__blob_pos, content__blob_size
FROM lance.default.documents;
```

These virtual columns are hidden from `INSERT` statements and `DESCRIBE TABLE` but are selectable in queries.

### Create table with vector columns

Vector columns use FixedSizeList encoding, optimized for ML embeddings and similarity search:

```sql
CREATE TABLE lance.default.embeddings (
    id BIGINT,
    text VARCHAR,
    embedding ARRAY(REAL)
) WITH (vector_columns = 'embedding:768');
```

Multiple vector columns with different dimensions:

```sql
CREATE TABLE lance.default.multi_modal (
    id BIGINT,
    text_embedding ARRAY(REAL),
    image_embedding ARRAY(DOUBLE)
) WITH (vector_columns = 'text_embedding:768, image_embedding:512');
```

### Combine blob and vector columns

```sql
CREATE TABLE lance.default.image_search (
    id BIGINT,
    image VARBINARY,
    embedding ARRAY(REAL)
) WITH (
    blob_columns = 'image',
    vector_columns = 'embedding:512'
);
```

### Create table with specific file format version

Specify the Lance file format version for the table:

```sql
CREATE TABLE lance.default.legacy_table (
    id BIGINT,
    name VARCHAR
) WITH (file_format_version = 'legacy');
```

Create a table with the latest stable format:

```sql
CREATE TABLE lance.default.modern_table (
    id BIGINT,
    data VARCHAR
) WITH (file_format_version = '2.1');
```

Combine with other table properties:

```sql
CREATE TABLE lance.default.ml_data (
    id BIGINT,
    embedding ARRAY(REAL)
) WITH (
    vector_columns = 'embedding:768',
    file_format_version = '2.0'
);
```

!!! note "File Format Version"
    The `file_format_version` property only applies when creating new tables. When inserting data into existing tables, the connector automatically uses the table's existing format version to ensure compatibility.

## Supported Data Types

| Type | Description |
|------|-------------|
| `BIGINT` | 64-bit signed integer |
| `INTEGER` | 32-bit signed integer |
| `DOUBLE` | 64-bit floating point |
| `REAL` | 32-bit floating point |
| `VARCHAR` | Variable-length string |
| `BOOLEAN` | Boolean (true/false) |
| `DATE` | Calendar date |
| `VARBINARY` | Variable-length binary |
| `ARRAY<type>` | Array of elements |

## Limitations

- Table and column comments are not supported
- NOT NULL constraints are not supported
- Default column values are not supported
- CHAR type is not supported (use VARCHAR)
- TIME and TIMESTAMP types are not supported
- DECIMAL type is not supported (use DOUBLE)
- MAP and ROW types are not supported for writes
- Column names cannot contain dots or special characters
- Blob columns return empty bytes on read (data is stored out-of-line and not materialized)
- Vector columns must be `ARRAY(REAL)` or `ARRAY(DOUBLE)` type
