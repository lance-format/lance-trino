# CREATE TABLE

Create a new table in the Lance catalog.

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

## Parameters

| Parameter | Description |
|-----------|-------------|
| `IF NOT EXISTS` | Optional. Prevents error if the table already exists. |
| `OR REPLACE` | Optional. Replaces the table if it already exists. |
| `table_name` | Fully qualified table name: `catalog.schema.table`. |
| `column_name` | Name of the column. |
| `data_type` | Data type of the column. See [Data Types](../data-types.md). |
| `query` | A SELECT query whose results populate the table. |

## Table Properties

| Property | Description |
|----------|-------------|
| `blob_columns` | Comma-separated list of VARBINARY columns to use blob encoding (out-of-line storage for large binary data). Example: `'image, video'` |
| `vector_columns` | Comma-separated list of vector columns with dimensions. Format: `'column1:dim1, column2:dim2'`. Columns must be `ARRAY(REAL)` or `ARRAY(DOUBLE)`. Example: `'embedding:768'` |

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

### Replace existing table

```sql
CREATE OR REPLACE TABLE lance.default.daily_stats
AS SELECT
    current_date as report_date,
    COUNT(*) as total_orders
FROM lance.default.orders;
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
