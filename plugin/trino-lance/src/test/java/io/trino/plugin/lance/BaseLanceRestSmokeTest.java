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

import io.airlift.log.Logger;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.nio.file.Files;
import java.nio.file.Path;

import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;

/**
 * Abstract base class for REST namespace smoke tests.
 * Uses a REST adapter backed by a directory namespace for testing.
 *
 * Note: This test requires a REST server implementation. Currently,
 * it falls back to using directory namespace with REST configuration
 * placeholders until a REST server test container is available.
 */
public abstract class BaseLanceRestSmokeTest
        extends BaseLanceConnectorSmokeTest
{
    private static final Logger log = Logger.get(BaseLanceRestSmokeTest.class);

    protected static Path tempDir;

    @BeforeAll
    public static void setupRestBackend()
            throws Exception
    {
        // Create temporary directory for the directory backend
        tempDir = Files.createTempDirectory("lance-rest-smoke-test");
        tempDir.toFile().deleteOnExit();
        log.info("REST smoke test backend using directory: %s", tempDir);

        // TODO: Start REST server here when available
    }

    @AfterAll
    public static void teardownRestBackend()
    {
        // TODO: Stop REST server here when available
        if (tempDir != null) {
            try {
                Files.walk(tempDir)
                        .sorted((a, b) -> b.toString().length() - a.toString().length())
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            }
                            catch (Exception e) {
                                // ignore cleanup errors
                            }
                        });
            }
            catch (Exception e) {
                // ignore cleanup errors
            }
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        LanceNamespaceTestConfig config = getNamespaceTestConfig();

        // For REST tests, we use a directory backend until REST server is available
        return LanceQueryRunner.builderForWriteTests()
                .addConnectorProperties(config.buildConnectorProperties(tempDir.toUri().toString()))
                .setInitialTables(NATION, REGION)
                .build();
    }
}
