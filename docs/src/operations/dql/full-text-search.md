# Full-text search

Search text columns using Lance FTS (inverted) indexes with the `full_text_search` table function.
The query is pushed down to Lance and ranked results carry a BM25 `_score`.

## Syntax

```sql
SELECT ...
FROM TABLE(lance.system.full_text_search(
    schema_name => '<schema>',
    table_name => '<table>',
    columns => DESCRIPTOR(<column> [, ...]),
    query => '<text>'
    [, options => '<key>=<value>[,...]']))
```

## Arguments

| Argument      | Description                                                      |
|---------------|------------------------------------------------------------------|
| `schema_name` | Schema of the table                                              |
| `table_name`  | Table to search                                                  |
| `columns`     | Descriptor of `varchar` columns. Each needs an inverted index    |
| `query`       | Search text                                                      |
| `options`     | Optional comma-separated `key=value` list                        |

One column runs a match query. Two or more columns run a multi-match query.

### Options

| Option           | Default | Description                                               |
|------------------|---------|-----------------------------------------------------------|
| `operator`       | `OR`    | `AND` requires all terms, `OR` requires any               |
| `fuzziness`      | auto    | Maximum edit distance, at least `0`                       |
| `boost`          | `1.0`   | Affects ranking only                                      |
| `prefix_length`  | `0`     | Leading characters that must match exactly                |
| `max_expansions` | `50`    | Maximum fuzzy term expansions, at least `1`               |

Only `operator` is supported with multiple columns.

## Result

All table columns plus `_score` (`real`). Tables that already have a `_score` column are rejected.

## Examples

```sql
SELECT id, _score
FROM TABLE(lance.system.full_text_search(
    schema_name => 'default',
    table_name => 'documents',
    columns => DESCRIPTOR(body),
    query => 'machin lerning',
    options => 'fuzziness=1,operator=AND'))
WHERE year >= 2024
ORDER BY _score DESC
LIMIT 10;

-- Multiple columns
SELECT id
FROM TABLE(lance.system.full_text_search('default', 'documents', DESCRIPTOR(title, body), 'deep learning'));
```

## Limitations

- Columns must have an inverted index. Check with the `$indexes` system table.
- Scalar predicates are applied as post-filters. Combined with `LIMIT`, fewer rows may be returned than exist.
- `ORDER BY _score DESC LIMIT n` is not pushed down. Trino sorts all matches.
- Phrase search is not supported.
