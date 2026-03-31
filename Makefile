.PHONY: build test clean install compile package help run lint format check serve-docs

# Compute a safe Surefire forkCount based on system thread limits.
# Each test fork starts a Trino DistributedQueryRunner that needs ~500 threads.
# Use 50% of the per-user process limit (ulimit -u) as headroom, capped at CPU count.
_THREAD_LIMIT := $(shell ulimit -u 2>/dev/null || echo 4096)
_CPUS         := $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
_SAFE_FORKS   := $(shell expr $(_THREAD_LIMIT) / 2 / 500 2>/dev/null || echo 1)
_SAFE_FORKS   := $(shell [ "0$(_SAFE_FORKS)" -gt 0 ] 2>/dev/null && echo $(_SAFE_FORKS) || echo 1)
SUREFIRE_FORK_COUNT ?= $(shell [ "$(_SAFE_FORKS)" -lt "$(_CPUS)" ] 2>/dev/null && echo $(_SAFE_FORKS) || echo $(_CPUS))

# Default target
help:
	@echo "Available targets:"
	@echo "  build     - Build the project (compile + package)"
	@echo "  test      - Run tests"
	@echo "  install   - Build and install to local Maven repository"
	@echo "  compile   - Compile source code"
	@echo "  package   - Package the plugin"
	@echo "  clean     - Clean build artifacts"
	@echo "  verify    - Run full verification (compile, test, package)"
	@echo "  run       - Run the development query runner server"
	@echo "  lint      - Run all code style checks (checkstyle, modernizer, sortpom)"
	@echo "  format    - Format pom.xml files"
	@echo "  check     - Run lint checks without tests"
	@echo "  serve-docs - Serve documentation locally"

# Build the project
build: compile package

# Run tests
test:
	./mvnw test -Dair.check.skip-enforcer=true -Dsurefire.forkCount=$(SUREFIRE_FORK_COUNT)

# Install to local repository
install:
	./mvnw clean install -Dair.check.skip-enforcer=true -Dsurefire.forkCount=$(SUREFIRE_FORK_COUNT)

# Compile source code
compile:
	./mvnw compile -Dair.check.skip-enforcer=true

# Package the plugin
package:
	./mvnw package -Dair.check.skip-enforcer=true -DskipTests

# Clean build artifacts
clean:
	./mvnw clean

# Full verification
verify:
	./mvnw verify -Dair.check.skip-enforcer=true -Dsurefire.forkCount=$(SUREFIRE_FORK_COUNT)

# Run the development query runner server
run:
	./mvnw exec:java -pl plugin/trino-lance -Dexec.mainClass="io.trino.plugin.lance.LanceQueryRunner" -Dair.check.skip-enforcer=true

# Run all code style checks (Trino standard)
lint:
	./mvnw checkstyle:check sortpom:verify modernizer:modernizer javadoc:jar -Dair.check.skip-enforcer=true -DskipTests

# Format pom.xml files
format:
	./mvnw sortpom:sort

# Run all checks without tests
check:
	./mvnw compile checkstyle:check sortpom:verify modernizer:modernizer -Dair.check.skip-enforcer=true -DskipTests

# Serve documentation locally
serve-docs:
	cd docs && uv pip install --system -r requirements.txt && mkdocs serve
