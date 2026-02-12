# Configuration

Create a catalog properties file (e.g., `etc/catalog/lance.properties`) to configure the Lance connector.

## Configuration Properties

| Property | Description | Default |
|----------|-------------|---------|
| `connector.name` | Must be set to `lance` | - |
| `lance.impl` | Namespace implementation: `dir`, `rest`, `glue`, `hive2`, `hive3`, or full class name | `dir` |

Any other `lance.*` properties are stripped of the `lance.` prefix and passed to the namespace implementation.

## Namespace Levels

Lance namespaces support different access modes depending on the underlying namespace structure. These settings apply to all namespace implementations.

### Default Mode

Direct schema-to-namespace mapping. Schemas map directly to namespaces in the catalog.

```properties
connector.name=lance
lance.impl=dir
lance.root=s3://my-bucket/lance-warehouse
```

```sql
CREATE SCHEMA lance.myschema;
CREATE TABLE lance.myschema.users (id BIGINT, name VARCHAR);
```

### Single Level Mode

Uses a virtual `default` schema. All tables are stored at the root level. `CREATE SCHEMA` is not supported.

```properties
connector.name=lance
lance.impl=dir
lance.root=s3://my-bucket/lance-warehouse
lance.single_level_ns=true
```

```sql
-- Only the "default" schema is available
CREATE TABLE lance.default.users (id BIGINT, name VARCHAR);
```

### Parent Prefix Mode

For namespaces with deeper hierarchies (3+ levels). The parent prefix is prepended to all namespace operations.

```properties
connector.name=lance
lance.impl=dir
lance.root=s3://my-bucket/lance-warehouse
lance.parent=org$team
```

```sql
-- Schema "myschema" maps to namespace ["org", "team", "myschema"]
CREATE SCHEMA lance.myschema;
CREATE TABLE lance.myschema.users (id BIGINT, name VARCHAR);
```

| Property | Description | Default |
|----------|-------------|---------|
| `lance.single_level_ns` | Enable single-level mode with virtual `default` schema | `false` |
| `lance.parent` | Parent namespace prefix (levels separated by `$`) | - |

## Examples

### Directory Namespace

The directory namespace allows operating against Lance datasets from any storage.

```properties
connector.name=lance
lance.impl=dir
lance.root=s3://my-bucket/lance-warehouse
lance.storage.region=us-east-1
lance.storage.access_key_id=YOUR_ACCESS_KEY
lance.storage.secret_access_key=YOUR_SECRET_KEY
```

### REST Namespace

The REST namespace connects to a Lance REST catalog server.

```properties
connector.name=lance
lance.impl=rest
lance.uri=http://localhost:8080
lance.header.Authorization=Bearer your-token
```
