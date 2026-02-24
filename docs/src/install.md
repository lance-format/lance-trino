# Installation

## Requirements

- Java 23 or later
- Trino 476 or compatible version

## Download from GitHub Releases

Each release includes a `trino-lance-<version>-trino<trino_version>.tar.gz` archive containing all required JARs. Download from the [releases page](https://github.com/lancedb/lance-trino/releases).

### Quick Installation

=== "Linux/macOS"
    ```bash
    # Set variables
    VERSION="0.1.0-beta.3"
    TRINO_VERSION="476"
    PLUGIN_DIR="/usr/lib/trino/plugin"

    # Download and extract
    wget "https://github.com/lancedb/lance-trino/releases/download/v${VERSION}/trino-lance-${VERSION}-trino${TRINO_VERSION}.tar.gz"
    tar -xzf "trino-lance-${VERSION}-trino${TRINO_VERSION}.tar.gz" -C "${PLUGIN_DIR}/"
    mv "${PLUGIN_DIR}/trino-lance-${TRINO_VERSION}" "${PLUGIN_DIR}/lance"
    ```

=== "Docker"
    ```dockerfile
    FROM trinodb/trino:476

    # Download and install Lance connector
    ARG VERSION=0.1.0-beta.3
    ARG TRINO_VERSION=476

    RUN curl -fsSL "https://github.com/lancedb/lance-trino/releases/download/v${VERSION}/trino-lance-${VERSION}-trino${TRINO_VERSION}.tar.gz" \
        | tar -xz -C /usr/lib/trino/plugin/ \
        && mv "/usr/lib/trino/plugin/trino-lance-${TRINO_VERSION}" /usr/lib/trino/plugin/lance
    ```
