# Configuration

Create a catalog properties file (e.g., `etc/catalog/lance.properties`) to configure the Lance connector.

## Configuration Properties

| Property     | Description                                                                           | Default |
|--------------|---------------------------------------------------------------------------------------|---------|
| `lance.impl` | Namespace implementation: `dir`, `rest`, `glue`, `hive2`, `hive3`, or full class name | `dir`   |

Any other `lance.*` properties are stripped of the `lance.` prefix and passed to the namespace implementation.

## Directory Namespace

The directory namespace reads Lance datasets from a local or cloud filesystem.

```properties
connector.name=lance
lance.impl=dir
lance.root=/path/to/warehouse
lance.storage.timeout=30s
lance.storage.retry-count=3
```

### Properties

| Property                  | Description                        | Default |
|---------------------------|------------------------------------|---------|
| `lance.root`              | Root directory for Lance datasets  | -       |
| `lance.storage.timeout`   | Storage operation timeout          | `30s`   |
| `lance.storage.retry-count` | Number of storage operation retries | `3`     |

## REST Namespace

The REST namespace connects to a Lance REST catalog server.

```properties
connector.name=lance
lance.impl=rest
lance.uri=https://api.lancedb.com
lance.tls.enabled=true
lance.header.Authorization=Bearer <token>
```

### Properties

| Property                    | Description                          | Default |
|-----------------------------|--------------------------------------|---------|
| `lance.uri`                 | REST catalog server URI              | -       |
| `lance.tls.enabled`         | Enable TLS for connections           | `false` |
| `lance.header.<name>`       | Custom headers to send with requests | -       |
