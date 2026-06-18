# Installation

## Requirements

- Java 25 or later
- Trino 481 or compatible version

## Download from GitHub Releases

Each release includes a `lance-trino-<version>-trino<trino_version>.tar.gz` archive containing all required JARs. Download from the [releases page](https://github.com/lancedb/lance-trino/releases).

### Quick Installation

=== "Linux/macOS"
    ```bash
    # Set variables
    VERSION="0.3.2"
    TRINO_VERSION="481"
    PLUGIN_DIR="/usr/lib/trino/plugin"

    # Download and extract
    wget "https://github.com/lancedb/lance-trino/releases/download/v${VERSION}/lance-trino-${VERSION}-trino${TRINO_VERSION}.tar.gz"
    tar -xzf "lance-trino-${VERSION}-trino${TRINO_VERSION}.tar.gz" -C "${PLUGIN_DIR}/"
    mv "${PLUGIN_DIR}/lance-trino-${VERSION}" "${PLUGIN_DIR}/lance"
    ```

=== "Docker"
    ```dockerfile
    FROM trinodb/trino:481

    # Download and install Lance connector
    ARG VERSION=0.3.2
    ARG TRINO_VERSION=481

    RUN curl -fsSL "https://github.com/lancedb/lance-trino/releases/download/v${VERSION}/lance-trino-${VERSION}-trino${TRINO_VERSION}.tar.gz" \
        | tar -xz -C /usr/lib/trino/plugin/ \
        && mv "/usr/lib/trino/plugin/lance-trino-${VERSION}" /usr/lib/trino/plugin/lance
    ```
