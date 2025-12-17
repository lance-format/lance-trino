# Trino Lance Connector

A Trino connector for reading Lance format data.
This connector allows Trino to query Lance datasets directly.

## Configuration

Create a catalog properties file (e.g., `etc/catalog/lance.properties`):

### Configuration Properties

| Property                       | Description                                                                           | Default |
|--------------------------------|---------------------------------------------------------------------------------------|---------|
| `lance.impl`                   | Namespace implementation: `dir`, `rest`, `glue`, `hive2`, `hive3`, or full class name | `dir`   |
| `lance.connection-timeout`     | Connection timeout duration                                                           | `1m`    |
| `lance.connection-retry-count` | Number of retries for failed connections                                              | `5`     |

Any other `lance.*` properties are stripped of the `lance.` prefix and passed to the namespace implementation.

#### Directory Namespace Properties

```properties
connector.name=lance
lance.impl=dir
lance.root=/path/to/warehouse
lance.storage.timeout=30s
lance.storage.retry-count=3
```

#### REST Namespace Properties

```properties
connector.name=lance
lance.impl=rest
lance.uri=https://api.lancedb.com
lance.tls.enabled=true
lance.header.Authorization=Bearer <token>
```

## Usage

Once configured, you can query Lance data using SQL:

```sql
-- List schemas
SHOW SCHEMAS FROM lance;

-- List tables
SHOW TABLES FROM lance.default;

-- Describe table schema
DESCRIBE lance.default.my_dataset;

-- Query a Lance dataset
SELECT * FROM lance.default.my_dataset;

-- Query with filtering and projection
SELECT id, name, value
FROM lance.default.my_dataset
WHERE value > 100
ORDER BY id
LIMIT 10;

-- Aggregations
SELECT category, COUNT(*), AVG(value)
FROM lance.default.my_dataset
GROUP BY category;
```

## Building

Build the connector:

```bash
./mvnw clean install
```

## Installation

1. Build the connector using Maven
2. Copy the generated plugin directory from `plugin/trino-lance/target/trino-lance-<version>/` to your Trino plugins directory (e.g., `/usr/lib/trino/plugin/lance/`)
3. Configure the connector in your Trino catalog properties
4. Restart Trino

## Development Guide

### Project Structure

Everything in this repository apart from the `plugin/trino-lance` mimics the layout of the Trino project.
For example, the root pom file is a copy of the Trino root pom file with exactly the same content.
We periodically upgrade the Trino version to stay up to date with the latest Trino features.

### Running Tests

```bash
./mvnw test -pl plugin/trino-lance
```

### Running the Query Runner (Development Server)

You can run a local Trino server for development:

```bash
cd plugin/trino-lance
mvn exec:java -Dexec.mainClass="io.trino.plugin.lance.LanceQueryRunner"
```

This starts a Trino server on port 8080 with the Lance connector configured.

## Requirements

- Java 23 or later
- Trino 476 or compatible version

## License

Apache License 2.0
