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
import org.lance.namespace.RestAdapter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;

/**
 * Abstract base class for REST namespace smoke tests.
 * Uses a REST adapter backed by a directory namespace for testing.
 *
 * <p>This test starts a local REST server using {@link RestAdapter} with a
 * {@code DirectoryNamespace} backend. The REST server is started on an OS-assigned
 * port (port 0) and the actual port is retrieved after startup.
 *
 * <p>Each concrete test class gets its own REST server instance to ensure test isolation.
 */
public abstract class BaseLanceRestSmokeTest
        extends BaseLanceConnectorSmokeTest
{
    private static final Logger log = Logger.get(BaseLanceRestSmokeTest.class);

    // Per-class storage for REST adapters to ensure test isolation
    private static final ConcurrentHashMap<Class<?>, RestAdapterContext> REST_CONTEXTS = new ConcurrentHashMap<>();

    private static class RestAdapterContext
    {
        Path tempDir;
        RestAdapter restAdapter;
        int restPort;
    }

    protected Path tempDir;
    protected RestAdapter restAdapter;
    protected int restPort;

    /**
     * Initialize the REST server for the calling test class.
     * Each test class gets its own REST server to ensure isolation.
     * Called from createQueryRunner() to ensure proper initialization order.
     */
    protected void ensureRestServerStarted()
            throws Exception
    {
        Class<?> testClass = getClass();
        RestAdapterContext context = REST_CONTEXTS.get(testClass);

        if (context != null) {
            // Reuse existing context for this test class
            this.tempDir = context.tempDir;
            this.restAdapter = context.restAdapter;
            this.restPort = context.restPort;
            return;
        }

        // Create a new context for this test class
        context = new RestAdapterContext();

        // Create temporary directory for the directory backend
        context.tempDir = Files.createTempDirectory("lance-rest-smoke-test-" + testClass.getSimpleName());
        context.tempDir.toFile().deleteOnExit();
        log.info("[%s] REST smoke test backend using directory: %s", testClass.getSimpleName(), context.tempDir);

        // Configure the directory backend for RestAdapter
        Map<String, String> backendConfig = new HashMap<>();
        backendConfig.put("root", context.tempDir.toString());

        // Create and start REST adapter on port 0 (OS-assigned port)
        context.restAdapter = new RestAdapter("dir", backendConfig, "127.0.0.1", 0);
        context.restAdapter.start();
        context.restPort = context.restAdapter.getPort();
        log.info("[%s] REST server started on port: %d", testClass.getSimpleName(), context.restPort);

        // Store the context
        REST_CONTEXTS.put(testClass, context);

        // Update instance fields
        this.tempDir = context.tempDir;
        this.restAdapter = context.restAdapter;
        this.restPort = context.restPort;
    }

    // Note: We don't use @AfterAll for cleanup because when running multiple test classes
    // in parallel, the static cleanup would interfere with other tests.
    // Instead, we rely on:
    // 1. tempDir.toFile().deleteOnExit() for directory cleanup on JVM exit
    // 2. RestAdapter resources are cleaned up when the JVM exits
    // This is acceptable for test code since each test run gets a fresh JVM.

    protected String getRestUri()
    {
        return "http://127.0.0.1:" + restPort;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Ensure REST server is started before creating QueryRunner
        ensureRestServerStarted();

        LanceNamespaceTestConfig config = getNamespaceTestConfig();

        // Use REST connector properties with the local REST server URI
        return LanceQueryRunner.builderForWriteTests()
                .addConnectorProperties(config.buildRestConnectorProperties(getRestUri()))
                .setInitialTables(NATION, REGION)
                .build();
    }
}
