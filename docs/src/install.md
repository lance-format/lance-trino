# Installation

## Requirements

- Java 23 or later
- Trino 476 or compatible version

## Building from Source

Clone the repository and build:

```bash
git clone https://github.com/lancedb/lance-trino.git
cd lance-trino
make install
```

## Installing the Plugin

1. Build the connector using Maven:

    ```bash
    make install
    ```

2. Copy the generated plugin directory to your Trino plugins directory:

    ```bash
    cp -r plugin/trino-lance/target/trino-lance-<version>/ /usr/lib/trino/plugin/lance/
    ```

3. [Configure](config.md) the connector in your Trino catalog properties

4. Restart Trino

## Development Server

You can run a local Trino server for development:

```bash
make run
```

This starts a Trino server on port 8080 with the Lance connector configured.
