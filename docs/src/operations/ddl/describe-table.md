# DESCRIBE

Display the schema of a table.

## Syntax

```sql
DESCRIBE <catalog>.<schema>.<table>
```

```sql
DESC <catalog>.<schema>.<table>
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `catalog` | The Lance catalog name. |
| `schema` | The schema name. |
| `table` | The table name. |

## Example

```sql
DESCRIBE lance.default.users;
```

Using short form:

```sql
DESC lance.default.orders;
```

## Output

| Column | Type | Extra | Comment |
|--------|------|-------|---------|
| id | bigint | | |
| name | varchar | | |
| email | varchar | | |

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
