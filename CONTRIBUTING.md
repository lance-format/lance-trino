# Contributing to Trino Lance Connector

Thank you for your interest in contributing to the Trino Lance Connector!

## Development Guide

### Project Structure

Everything in this repository apart from the `plugin/trino-lance` mimics the layout of the Trino project.
For example, the root pom file is a copy of the Trino root pom file with exactly the same content.
We periodically upgrade the Trino version to stay up to date with the latest Trino features.

### Building

Build the connector:

```bash
make install
```

Other available commands:

```bash
make help      # Show all available commands
make build     # Compile and package
make test      # Run tests
make compile   # Compile only
make package   # Package without tests
make clean     # Clean build artifacts
make verify    # Full verification
make run       # Run development server
make lint      # Run code style checks (checkstyle, modernizer, sortpom)
make format    # Format pom.xml files
make check     # Run all checks without tests
```

### Installation

1. Build the connector using Maven
2. Copy the generated plugin directory from `plugin/trino-lance/target/trino-lance-<version>/` to your Trino plugins directory (e.g., `/usr/lib/trino/plugin/lance/`)
3. Configure the connector in your Trino catalog properties
4. Restart Trino

### Running Tests

```bash
make test
```

### Running the Query Runner (Development Server)

You can run a local Trino server for development:

```bash
make run
```

This starts a Trino server on port 8080 with the Lance connector configured.

### Requirements

- Java 23 or later
- Trino 476 or compatible version
