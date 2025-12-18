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

import java.util.concurrent.TimeUnit;

/**
 * Configuration for Lance connector.
 * <p>
 * This class contains only the connector-specific configuration properties.
 * All other properties (e.g., lance.root, lance.uri, etc.) are passed through
 * to the LanceNamespace implementation via the catalog properties map.
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
 * <p>
 * All properties prefixed with "lance." are passed to the namespace implementation.
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

    // ===== Write Configuration =====

    private int maxRowsPerFile = 1_000_000;
    private int maxRowsPerGroup = 100_000;
    private int writeBatchSize = 10_000;

    public int getMaxRowsPerFile()
    {
        return maxRowsPerFile;
    }

    @Config("lance.max-rows-per-file")
    @ConfigDescription("Maximum number of rows per Lance file")
    public LanceConfig setMaxRowsPerFile(int maxRowsPerFile)
    {
        this.maxRowsPerFile = maxRowsPerFile;
        return this;
    }

    public int getMaxRowsPerGroup()
    {
        return maxRowsPerGroup;
    }

    @Config("lance.max-rows-per-group")
    @ConfigDescription("Maximum number of rows per row group within a Lance file")
    public LanceConfig setMaxRowsPerGroup(int maxRowsPerGroup)
    {
        this.maxRowsPerGroup = maxRowsPerGroup;
        return this;
    }

    public int getWriteBatchSize()
    {
        return writeBatchSize;
    }

    @Config("lance.write-batch-size")
    @ConfigDescription("Number of rows to batch before writing to Arrow")
    public LanceConfig setWriteBatchSize(int writeBatchSize)
    {
        this.writeBatchSize = writeBatchSize;
        return this;
    }
}
