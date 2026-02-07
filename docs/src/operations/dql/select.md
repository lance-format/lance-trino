# SELECT

Query data from Lance tables using standard SQL SELECT statements.

## Syntax

```sql
SELECT <columns>
FROM <catalog>.<schema>.<table>
[WHERE <condition>]
[GROUP BY <columns>]
[ORDER BY <columns>]
[LIMIT <n>]
```

## Examples

### Basic Query

```sql
SELECT * FROM lance.default.my_dataset;
```

### Filtering and Projection

```sql
SELECT id, name, value
FROM lance.default.my_dataset
WHERE value > 100
ORDER BY id
LIMIT 10;
```

### Aggregations

```sql
SELECT category, COUNT(*), AVG(value)
FROM lance.default.my_dataset
GROUP BY category;
```
