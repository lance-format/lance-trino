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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for Lance connector.
 * <p>
 * Configuration uses the same property names as other Lance integrations (Spark, etc.)
 * for consistency. Properties are passed directly to the LanceNamespace implementation.
 * <p>
 * Directory namespace example:
 * <pre>
 * connector.name=lance
 * lance.impl=dir
 * lance.root=/path/to/warehouse
 * </pre>
 * <p>
 * REST namespace example:
 * <pre>
 * connector.name=lance
 * lance.impl=rest
 * lance.uri=https://api.lancedb.com
 * </pre>
 */
public class LanceConfig
{
    /**
     * Namespace implementation type.
     * Built-in: "dir" for DirectoryNamespace, "rest" for RestNamespace.
     * External implementations can be specified by short name (if registered)
     * or full class name.
     */
    private String impl = "dir";

    /**
     * Root directory for DirectoryNamespace.
     */
    private String root;

    /**
     * URI for RestNamespace (REST API endpoint).
     */
    private String uri;

    private Duration connectionTimeout = new Duration(1, TimeUnit.MINUTES);

    private int fetchRetryCount = 5;

    @NotNull
    public String getImpl()
    {
        return impl;
    }

    @Config("lance.impl")
    @ConfigDescription("Namespace implementation: 'dir', 'rest', 'glue', 'hive2', 'hive3', or full class name")
    public LanceConfig setImpl(String impl)
    {
        this.impl = impl;
        return this;
    }

    public String getRoot()
    {
        return root;
    }

    @Config("lance.root")
    @ConfigDescription("Root directory for DirectoryNamespace (e.g., /path/to/warehouse, s3://bucket/path)")
    public LanceConfig setRoot(String root)
    {
        this.root = root;
        return this;
    }

    public String getUri()
    {
        return uri;
    }

    @Config("lance.uri")
    @ConfigDescription("REST API endpoint for RestNamespace")
    public LanceConfig setUri(String uri)
    {
        this.uri = uri;
        return this;
    }

    /**
     * Build namespace properties map to pass to LanceNamespace.connect().
     * This includes root, uri, and any other properties needed by the namespace implementation.
     */
    public Map<String, String> getNamespaceProperties()
    {
        Map<String, String> properties = new HashMap<>();
        if (root != null) {
            properties.put("root", root);
        }
        if (uri != null) {
            properties.put("uri", uri);
        }
        return properties;
    }

    @MinDuration("15s")
    @NotNull
    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("lance.connection-timeout")
    public LanceConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public Integer getFetchRetryCount()
    {
        return this.fetchRetryCount;
    }

    @Config("lance.connection-retry-count")
    public LanceConfig setFetchRetryCount(int fetchRetryCount)
    {
        this.fetchRetryCount = fetchRetryCount;
        return this;
    }
}
