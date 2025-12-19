/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.lance;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.log.Logger;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * Add '--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED' to JVM options
 */
public final class LanceQueryRunner
{
    private static final Logger log = Logger.get(LanceQueryRunner.class);

    public static final String LANCE_CATALOG = "lance";

    private LanceQueryRunner() {}

    /**
     * Create a builder with the default example database (read-only test data).
     * Uses single-level mode since example_db is a flat directory structure.
     */
    public static Builder builder()
    {
        return new Builder()
                .addConnectorProperty("lance.root", Resources.getResource(LanceQueryRunner.class, "/example_db").toString())
                .addConnectorProperty("lance.single_level_ns", "true");
    }

    /**
     * Create a builder that uses a temporary directory for write operations.
     * Use this when you need to test CREATE TABLE, INSERT, etc.
     */
    public static Builder builderForWriteTests()
    {
        return new Builder()
                .setUseTempDirectory(true);
    }

    /**
     * Create a builder for a specific namespace test configuration.
     *
     * @param config the namespace test configuration
     * @return a builder configured for the specified namespace mode
     */
    public static Builder builderForConfig(LanceNamespaceTestConfig config)
    {
        return new Builder()
                .setUseTempDirectory(true)
                .setNamespaceTestConfig(config);
    }

    /**
     * Create a builder for S3/LocalStack tests with pre-configured S3 endpoint.
     *
     * @param config the namespace test configuration (should be S3_*)
     * @param s3Endpoint the S3 endpoint URL
     * @param bucketName the S3 bucket name
     * @return a builder configured for S3 namespace mode
     */
    public static Builder builderForS3(LanceNamespaceTestConfig config, String s3Endpoint, String bucketName)
    {
        Builder builder = new Builder()
                .setNamespaceTestConfig(config);

        // Set S3 connector properties
        Map<String, String> s3Props = config.buildS3ConnectorProperties(s3Endpoint, bucketName);
        for (Map.Entry<String, String> entry : s3Props.entrySet()) {
            builder.addConnectorProperty(entry.getKey(), entry.getValue());
        }

        return builder;
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private Set<TpchTable<?>> initialTables = ImmutableSet.of();
        private boolean useTempDirectory;
        private Optional<LanceNamespaceTestConfig> namespaceTestConfig = Optional.empty();
        private Optional<Closeable> externalResource = Optional.empty();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(LANCE_CATALOG)
                    .setSchema("default")
                    .build());
        }

        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        public Builder addConnectorProperties(Map<String, String> properties)
        {
            this.connectorProperties.putAll(properties);
            return this;
        }

        public Builder setUseTempDirectory(boolean useTempDirectory)
        {
            this.useTempDirectory = useTempDirectory;
            return this;
        }

        public Builder setNamespaceTestConfig(LanceNamespaceTestConfig config)
        {
            this.namespaceTestConfig = Optional.of(config);
            return this;
        }

        public Builder setExternalResource(Closeable resource)
        {
            this.externalResource = Optional.of(resource);
            return this;
        }

        public Builder setInitialTables(TpchTable<?>... initialTables)
        {
            return setInitialTables(ImmutableList.copyOf(initialTables));
        }

        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableSet.copyOf(initialTables);
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            // Apply namespace test config if specified
            if (namespaceTestConfig.isPresent()) {
                LanceNamespaceTestConfig config = namespaceTestConfig.get();
                // Only apply if not S3 config (S3 properties are set separately in builderForS3)
                if (!config.isS3Config() && useTempDirectory && !connectorProperties.containsKey("lance.root")) {
                    Path tempDir = Files.createTempDirectory("lance-trino-test");
                    tempDir.toFile().deleteOnExit();
                    Map<String, String> configProps = config.buildConnectorProperties(tempDir.toUri().toString());
                    connectorProperties.putAll(configProps);
                    log.info("Using temporary directory for Lance with config %s: %s", config, tempDir);
                }
            }
            else if (useTempDirectory && !connectorProperties.containsKey("lance.root")) {
                // Default behavior: use temp directory without namespace config
                // Use single-level mode for backwards compatibility with DirectoryNamespace
                Path tempDir = Files.createTempDirectory("lance-trino-test");
                tempDir.toFile().deleteOnExit();
                connectorProperties.put("lance.root", tempDir.toUri().toString());
                connectorProperties.putIfAbsent("lance.single_level_ns", "true");
                log.info("Using temporary directory for Lance (single-level mode): %s", tempDir);
            }

            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new LancePlugin());

                // Determine if we're in single-level mode
                boolean singleLevelMode = connectorProperties.containsKey("lance.single_level_ns") &&
                        Boolean.parseBoolean(connectorProperties.get("lance.single_level_ns"));

                // Check if we have a parent namespace configuration
                boolean hasParent = namespaceTestConfig.isPresent() && namespaceTestConfig.get().hasParent();

                if (hasParent) {
                    // For configurations with parent namespaces, we need to create the parent
                    // namespaces first. We do this by temporarily creating the catalog without
                    // the parent setting, creating the parent namespaces, then recreating with
                    // the parent setting.
                    LanceNamespaceTestConfig config = namespaceTestConfig.get();
                    List<String> parentLevels = config.getParentLevels();

                    log.info("Creating parent namespaces: %s", parentLevels);

                    // Create catalog without parent setting to create parent namespaces
                    Map<String, String> propsWithoutParent = new HashMap<>(connectorProperties);
                    propsWithoutParent.remove("lance.parent");
                    queryRunner.createCatalog(LANCE_CATALOG, "lance", propsWithoutParent);

                    // Create parent namespaces progressively
                    // For parent=p1$p2, we need to create: p1, then p1/p2
                    // First level (p1) is created as a schema directly
                    // Second level (p2) requires reconfiguring with p1 as parent
                    if (!parentLevels.isEmpty()) {
                        // Create first level namespace (p1)
                        String firstLevel = parentLevels.get(0);
                        log.info("Creating first-level parent namespace: %s", firstLevel);
                        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + LANCE_CATALOG + ".\"" + firstLevel + "\"");

                        // For second level and beyond, we need to reconfigure
                        if (parentLevels.size() > 1) {
                            // Drop the current catalog
                            queryRunner.execute("DROP CATALOG " + LANCE_CATALOG);

                            // Build cumulative parent prefixes and create each level
                            List<String> cumulativeParent = new ArrayList<>();
                            cumulativeParent.add(firstLevel);

                            for (int i = 1; i < parentLevels.size(); i++) {
                                String currentLevel = parentLevels.get(i);
                                String parentPrefix = String.join("$", cumulativeParent);

                                log.info("Creating parent namespace level %d: %s (under parent: %s)", i + 1, currentLevel, parentPrefix);

                                // Create catalog with current parent prefix
                                Map<String, String> propsWithPartialParent = new HashMap<>(connectorProperties);
                                propsWithPartialParent.put("lance.parent", parentPrefix);
                                propsWithPartialParent.remove("lance.single_level_ns");
                                queryRunner.createCatalog(LANCE_CATALOG, "lance", propsWithPartialParent);

                                // Create the namespace at this level
                                queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + LANCE_CATALOG + ".\"" + currentLevel + "\"");

                                // Add current level to cumulative parent for next iteration
                                cumulativeParent.add(currentLevel);

                                // Drop catalog for next iteration or final setup
                                queryRunner.execute("DROP CATALOG " + LANCE_CATALOG);
                            }
                        }
                        else {
                            // Only one level parent, drop catalog for final setup
                            queryRunner.execute("DROP CATALOG " + LANCE_CATALOG);
                        }
                    }

                    // Finally, create the catalog with the full parent setting
                    queryRunner.createCatalog(LANCE_CATALOG, "lance", connectorProperties);
                }
                else {
                    // No parent configuration, create catalog normally
                    queryRunner.createCatalog(LANCE_CATALOG, "lance", connectorProperties);
                }

                // Create the default schema if not in single-level mode
                // In multi-level mode (default), we need to create the schema before creating tables
                if (!singleLevelMode) {
                    log.info("Creating default schema for multi-level namespace mode");
                    queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + LANCE_CATALOG + ".\"default\"");
                }

                // Copy TPCH tables if specified
                if (!initialTables.isEmpty()) {
                    log.info("Copying TPCH tables: %s", initialTables);
                    copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, initialTables);
                }

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }

        public Optional<LanceNamespaceTestConfig> getNamespaceTestConfig()
        {
            return namespaceTestConfig;
        }
    }

    @SuppressWarnings("resource")
    public static void main(String[] args)
            throws Exception
    {
        QueryRunner queryRunner = builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .build();
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
