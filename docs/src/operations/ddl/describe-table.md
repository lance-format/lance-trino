# DESCRIBE

Display the schema of a table.

## Syntax

```sql
DESCRIBE <catalog>.<schema>.<table>
```

```sql
DESC <catalog>.<schema>.<table>
```

```sql
SHOW COLUMNS FROM <catalog>.<schema>.<table>
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `catalog` | The Lance catalog name. |
| `schema` | The schema name. |
| `table` | The table name. |

## Examples

Describe a table:

```sql
DESCRIBE lance.default.users;
```

Using short form:

```sql
DESC lance.default.orders;
```

Using SHOW COLUMNS:

```sql
SHOW COLUMNS FROM lance.default.products;
```

## Output

Returns table column information:

| Column | Type | Extra | Comment |
|--------|------|-------|---------|
| id | bigint | | |
| name | varchar | | |
| email | varchar | | |
| created_at | date | | |

## Related

See also:

- [SHOW CREATE TABLE](#show-create-table) - Display the CREATE TABLE statement

## SHOW CREATE TABLE

Display the CREATE TABLE statement for a table.

### Syntax

```sql
SHOW CREATE TABLE <catalog>.<schema>.<table>
```

### Example

```sql
SHOW CREATE TABLE lance.default.users;
```

### Output

```sql
CREATE TABLE lance.default.users (
   id bigint,
   name varchar,
   email varchar
)
```
