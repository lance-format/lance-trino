# CREATE TABLE

Create a new table in the Lance catalog.

## Syntax

```sql
CREATE TABLE [IF NOT EXISTS] <table_name> (
    <column_name> <data_type> [, ...]
)
```

```sql
CREATE TABLE [IF NOT EXISTS] <table_name>
AS <query>
```

```sql
CREATE OR REPLACE TABLE <table_name>
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
