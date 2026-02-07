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
import org.lance.namespace.RestAdapter;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.io.Closeable;
import java.net.URI;
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
     * For S3 configs, uses local LocalStack endpoint (http://localhost:4566).
     *
     * @param config the namespace test configuration
     * @return a builder configured for the specified namespace mode
     */
    public static Builder builderForConfig(LanceNamespaceTestConfig config)
    {
        Builder builder = new Builder()
                .setNamespaceTestConfig(config);

        if (config.isS3Config()) {
            // For S3 configs, use local LocalStack endpoint
            Map<String, String> s3Props = config.buildLocalS3ConnectorProperties();
            for (Map.Entry<String, String> entry : s3Props.entrySet()) {
                builder.addConnectorProperty(entry.getKey(), entry.getValue());
            }
        }
        else {
            builder.setUseTempDirectory(true);
        }

        return builder;
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

    /**
     * Create a builder for REST namespace tests.
     * This starts a local REST server backed by DirectoryNamespace.
     *
     * @param config the namespace test configuration (should be REST_*)
     * @return a builder configured for REST namespace mode with embedded REST server
     */
    public static Builder builderForRest(LanceNamespaceTestConfig config)
            throws Exception
    {
        // Create temp directory for REST backend
        Path tempDir = Files.createTempDirectory("lance-rest-test-" + config.name());
        tempDir.toFile().deleteOnExit();
        log.info("REST backend using directory: %s", tempDir);

        // Configure and start RestAdapter
        Map<String, String> backendConfig = new HashMap<>();
        backendConfig.put("root", tempDir.toString());

        RestAdapter restAdapter = new RestAdapter("dir", backendConfig, "127.0.0.1", 0);
        restAdapter.start();
        int restPort = restAdapter.getPort();
        String restUri = "http://127.0.0.1:" + restPort;
        log.info("REST server started on port: %d", restPort);

        // Create builder with REST connector properties
        Builder builder = new Builder()
                .setNamespaceTestConfig(config)
                .setExternalResource(restAdapter);

        // Set REST connector properties
        Map<String, String> restProps = config.buildRestConnectorProperties(restUri);
        for (Map.Entry<String, String> entry : restProps.entrySet()) {
            builder.addConnectorProperty(entry.getKey(), entry.getValue());
        }

        return builder;
    }

    /**
     * Ensure S3 bucket exists by creating it if it doesn't already exist.
     * Uses the S3 endpoint and credentials from the connector properties.
     *
     * @param endpoint the S3 endpoint URL
     * @param bucketName the bucket name to create
     */
    private static void ensureS3BucketExists(String endpoint, String bucketName)
    {
        AwsBasicCredentials credentials = AwsBasicCredentials.create("ACCESSKEY", "SECRETKEY");

        try (S3Client s3Client = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .build()) {
            try {
                log.info("Creating S3 bucket: %s", bucketName);
                s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
                log.info("S3 bucket created successfully: %s", bucketName);
            }
            catch (BucketAlreadyOwnedByYouException e) {
                log.info("S3 bucket already exists: %s", bucketName);
            }
        }
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
            else if (useTempDirectory && !connectorProperties.containsKey("lance.root") && !connectorProperties.containsKey("lance.uri")) {
                // Default behavior: use temp directory without namespace config
                // Use single-level mode for backwards compatibility with DirectoryNamespace
                // Only apply when not using REST (which has lance.uri instead of lance.root)
                Path tempDir = Files.createTempDirectory("lance-trino-test");
                tempDir.toFile().deleteOnExit();
                connectorProperties.put("lance.root", tempDir.toUri().toString());
                connectorProperties.putIfAbsent("lance.single_level_ns", "true");
                log.info("Using temporary directory for Lance (single-level mode): %s", tempDir);
            }

            // Create S3 bucket if using S3 configuration
            if (namespaceTestConfig.isPresent() && namespaceTestConfig.get().isS3Config()) {
                String endpoint = connectorProperties.get("lance.storage.aws_endpoint");
                String root = connectorProperties.get("lance.root");
                if (endpoint != null && root != null && root.startsWith("s3://")) {
                    String bucketName = root.substring(5).split("/")[0];
                    log.info("Ensuring S3 bucket exists: %s at endpoint %s", bucketName, endpoint);
                    ensureS3BucketExists(endpoint, bucketName);
                }
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
