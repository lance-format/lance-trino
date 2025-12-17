.PHONY: build test clean install compile package help run lint format check

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

# Build the project
build: compile package

# Run tests
test:
	./mvnw test -Dair.check.skip-enforcer=true

# Install to local repository
install:
	./mvnw clean install -Dair.check.skip-enforcer=true

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
	./mvnw verify -Dair.check.skip-enforcer=true

# Run the development query runner server
run:
	./mvnw exec:java -pl plugin/trino-lance -Dexec.mainClass="io.trino.plugin.lance.LanceQueryRunner" -Dair.check.skip-enforcer=true

# Run all code style checks (Trino standard)
lint:
	./mvnw checkstyle:check sortpom:verify modernizer:modernizer -Dair.check.skip-enforcer=true

# Format pom.xml files
format:
	./mvnw sortpom:sort

# Run all checks without tests
check:
	./mvnw compile checkstyle:check sortpom:verify modernizer:modernizer -Dair.check.skip-enforcer=true -DskipTests
