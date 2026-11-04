# VACUUM

Remove old versions of a Lance table and free the storage they use.

## Syntax

```sql
ALTER TABLE <table_name> EXECUTE vacuum
ALTER TABLE <table_name> EXECUTE vacuum(before_version => <version>)
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `table_name` | Fully qualified table name: `catalog.schema.table`. |
| `before_version` | Optional. Remove versions older than this version number. If omitted, all versions except the latest are removed. |

## Examples

### Remove versions older than a given version

```sql
ALTER TABLE lance.default.users
EXECUTE vacuum(before_version => 42);
```

Find version numbers with the [`$versions`](../dql/select.md#versions-table) table.

## Implementation

The procedure runs on the coordinator and calls Lance's dataset cleanup with a cleanup policy built from the given parameters.
Cached dataset metadata for the table is invalidated afterwards.

The latest version and tagged versions are never removed by age alone.
Files that no version references are removed only when they are at least 7 days old, to protect in-progress writes.

## Limitations

- Removed versions can no longer be read, so time travel to them stops working
- Fails if a version to be removed is tagged
