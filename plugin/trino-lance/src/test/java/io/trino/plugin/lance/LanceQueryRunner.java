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
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
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
     */
    public static Builder builder()
    {
        return new Builder()
                .addConnectorProperty("lance.root", Resources.getResource(LanceReader.class, "/example_db").toString());
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

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private Set<TpchTable<?>> initialTables = ImmutableSet.of();
        private boolean useTempDirectory;

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

        public Builder setUseTempDirectory(boolean useTempDirectory)
        {
            this.useTempDirectory = useTempDirectory;
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
            // If using temp directory and no lance.root is set, create one
            if (useTempDirectory && !connectorProperties.containsKey("lance.root")) {
                Path tempDir = Files.createTempDirectory("lance-trino-test");
                tempDir.toFile().deleteOnExit();
                connectorProperties.put("lance.root", tempDir.toUri().toString());
                log.info("Using temporary directory for Lance: %s", tempDir);
            }

            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new LancePlugin());
                queryRunner.createCatalog(LANCE_CATALOG, "lance", connectorProperties);

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
