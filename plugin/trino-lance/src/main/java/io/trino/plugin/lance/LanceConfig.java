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
import jakarta.validation.constraints.NotNull;

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

    // ===== Namespace Configuration =====

    /**
     * Single-level namespace mode.
     * When true, access 1st level (root) with virtual "default" schema.
     * CREATE SCHEMA is not allowed in this mode.
     */
    private boolean singleLevelNs;

    /**
     * Parent namespace prefix for multi-level namespaces (3+ levels).
     * Format: "prefix$path" using $ as delimiter.
     * Example: "hive$catalog" to access namespaces under hive/catalog.
     */
    private String parent;

    public boolean isSingleLevelNs()
    {
        return singleLevelNs;
    }

    @Config("lance.single_level_ns")
    @ConfigDescription("Access 1st level namespace with virtual 'default' schema (no CREATE SCHEMA)")
    public LanceConfig setSingleLevelNs(boolean singleLevelNs)
    {
        this.singleLevelNs = singleLevelNs;
        return this;
    }

    public String getParent()
    {
        return parent;
    }

    @Config("lance.parent")
    @ConfigDescription("Parent namespace prefix for 3+ level namespaces (use $ as delimiter)")
    public LanceConfig setParent(String parent)
    {
        this.parent = parent;
        return this;
    }

    // ===== Read Configuration =====

    private int readBatchSize = 8192;

    public int getReadBatchSize()
    {
        return readBatchSize;
    }

    @Config("lance.read_batch_size")
    @ConfigDescription("Number of rows per batch during vectorized reads (default 8192 for optimal OLAP performance)")
    public LanceConfig setReadBatchSize(int readBatchSize)
    {
        this.readBatchSize = readBatchSize;
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

    @Config("lance.max_rows_per_file")
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

    @Config("lance.max_rows_per_group")
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

    @Config("lance.write_batch_size")
    @ConfigDescription("Number of rows to batch before writing to Arrow")
    public LanceConfig setWriteBatchSize(int writeBatchSize)
    {
        this.writeBatchSize = writeBatchSize;
        return this;
    }

    // ===== Index-Aware Split Planning Configuration =====

    private long btreeIndexedRowsPerSplit = 100_000_000L;  // 100M rows
    private long bitmapIndexedRowsPerSplit = 10_000_000L;  // 10M rows

    public long getBtreeIndexedRowsPerSplit()
    {
        return btreeIndexedRowsPerSplit;
    }

    @Config("lance.btree_indexed_rows_per_split")
    @ConfigDescription("Row count threshold for grouping btree-indexed fragments per split (default 100M)")
    public LanceConfig setBtreeIndexedRowsPerSplit(long btreeIndexedRowsPerSplit)
    {
        this.btreeIndexedRowsPerSplit = btreeIndexedRowsPerSplit;
        return this;
    }

    public long getBitmapIndexedRowsPerSplit()
    {
        return bitmapIndexedRowsPerSplit;
    }

    @Config("lance.bitmap_indexed_rows_per_split")
    @ConfigDescription("Row count threshold for grouping bitmap-indexed fragments per split (default 10M)")
    public LanceConfig setBitmapIndexedRowsPerSplit(long bitmapIndexedRowsPerSplit)
    {
        this.bitmapIndexedRowsPerSplit = bitmapIndexedRowsPerSplit;
        return this;
    }

    // ===== Cache Configuration =====

    private int cacheMaxSessions = 100;
    private int cacheMaxDatasets = 100;
    private int cacheSessionTtlMinutes = 60;
    private int cacheDatasetTtlMinutes = 30;
    private Long sessionIndexCacheSizeBytes;  // null = use Lance default
    private Long sessionMetadataCacheSizeBytes;  // null = use Lance default

    public int getCacheMaxSessions()
    {
        return cacheMaxSessions;
    }

    @Config("lance.cache.max_sessions")
    @ConfigDescription("Maximum number of cached sessions per user (default 100)")
    public LanceConfig setCacheMaxSessions(int cacheMaxSessions)
    {
        this.cacheMaxSessions = cacheMaxSessions;
        return this;
    }

    public int getCacheMaxDatasets()
    {
        return cacheMaxDatasets;
    }

    @Config("lance.cache.max_datasets")
    @ConfigDescription("Maximum number of cached datasets (default 100)")
    public LanceConfig setCacheMaxDatasets(int cacheMaxDatasets)
    {
        this.cacheMaxDatasets = cacheMaxDatasets;
        return this;
    }

    public int getCacheSessionTtlMinutes()
    {
        return cacheSessionTtlMinutes;
    }

    @Config("lance.cache.session_ttl_minutes")
    @ConfigDescription("Session cache TTL in minutes (default 60)")
    public LanceConfig setCacheSessionTtlMinutes(int cacheSessionTtlMinutes)
    {
        this.cacheSessionTtlMinutes = cacheSessionTtlMinutes;
        return this;
    }

    public int getCacheDatasetTtlMinutes()
    {
        return cacheDatasetTtlMinutes;
    }

    @Config("lance.cache.dataset_ttl_minutes")
    @ConfigDescription("Dataset cache TTL in minutes (default 30)")
    public LanceConfig setCacheDatasetTtlMinutes(int cacheDatasetTtlMinutes)
    {
        this.cacheDatasetTtlMinutes = cacheDatasetTtlMinutes;
        return this;
    }

    public Long getSessionIndexCacheSizeBytes()
    {
        return sessionIndexCacheSizeBytes;
    }

    @Config("lance.session.index_cache_size_bytes")
    @ConfigDescription("Lance session index cache size in bytes (default: Lance default)")
    public LanceConfig setSessionIndexCacheSizeBytes(Long sessionIndexCacheSizeBytes)
    {
        this.sessionIndexCacheSizeBytes = sessionIndexCacheSizeBytes;
        return this;
    }

    public Long getSessionMetadataCacheSizeBytes()
    {
        return sessionMetadataCacheSizeBytes;
    }

    @Config("lance.session.metadata_cache_size_bytes")
    @ConfigDescription("Lance session metadata cache size in bytes (default: Lance default)")
    public LanceConfig setSessionMetadataCacheSizeBytes(Long sessionMetadataCacheSizeBytes)
    {
        this.sessionMetadataCacheSizeBytes = sessionMetadataCacheSizeBytes;
        return this;
    }
}
