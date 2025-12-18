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
package io.trino.plugin.lance.internal;

import com.google.common.cache.Cache;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.lance.LanceColumnHandle;
import io.trino.plugin.lance.LanceConfig;
import io.trino.plugin.lance.LanceNamespaceProperties;
import io.trino.plugin.lance.LanceTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class LanceReader
        implements Closeable
{
    private static final Logger log = Logger.get(LanceReader.class);

    // TODO: support multiple schemas
    public static final String SCHEMA = "default";
    private static final String TABLE_PATH_SUFFIX = ".lance";
    private static final BufferAllocator allocator = new RootAllocator(
            RootAllocator.configBuilder().from(RootAllocator.defaultConfig()).maxAllocation(4 * 1024 * 1024).build());

    // Cache for dataset metadata (fragments) - shared across all LanceReader instances per worker JVM
    // Maximum 100 entries, expires 1 hour after write (similar to lance-spark)
    private static final Cache<CacheKey, Map<Integer, Fragment>> FRAGMENT_CACHE =
            EvictableCacheBuilder.newBuilder()
                    .maximumSize(100)
                    .expireAfterWrite(1, TimeUnit.HOURS)
                    .shareNothingWhenDisabled()
                    .build();

    // Cache for schema metadata - shared across all LanceReader instances per worker JVM
    private static final Cache<CacheKey, Schema> SCHEMA_CACHE =
            EvictableCacheBuilder.newBuilder()
                    .maximumSize(100)
                    .expireAfterWrite(1, TimeUnit.HOURS)
                    .shareNothingWhenDisabled()
                    .build();

    private final String root;
    private final LanceNamespace namespace;

    @Inject
    public LanceReader(LanceConfig lanceConfig, @LanceNamespaceProperties Map<String, String> namespaceProperties)
    {
        String impl = lanceConfig.getImpl();

        // Build namespace properties from the raw properties map
        // Filter to only include lance.* properties (minus the prefix) for namespace initialization
        Map<String, String> properties = new HashMap<>();
        for (Map.Entry<String, String> entry : namespaceProperties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("lance.")) {
                // Strip the "lance." prefix for namespace properties
                properties.put(key.substring(6), entry.getValue());
            }
        }

        // For DirectoryNamespace, ensure default settings are applied
        if ("dir".equals(impl)) {
            properties.putIfAbsent("manifest_enabled", "true");
            properties.putIfAbsent("dir_listing_enabled", "true");
        }

        // Use LanceNamespace.connect() to dynamically load and initialize the namespace
        this.namespace = LanceNamespace.connect(impl, properties, allocator);

        // Extract root for table path construction (used by DirectoryNamespace)
        this.root = properties.get("root");
    }

    public List<SchemaTableName> listTables(ConnectorSession session, String schema)
    {
        if (SCHEMA.equals(schema)) {
            ListTablesRequest request = new ListTablesRequest();
            ListTablesResponse response = namespace.listTables(request);
            Set<String> tables = response.getTables();
            if (tables == null || tables.isEmpty()) {
                return Collections.emptyList();
            }
            return tables.stream()
                    .map(tableName -> new SchemaTableName(schema, tableName))
                    .collect(Collectors.toList());
        }
        else {
            return Collections.emptyList();
        }
    }

    public List<ColumnMetadata> getColumnsMetadata(String tableName)
    {
        Map<String, ColumnHandle> columnHandlers = this.getColumnHandle(tableName);
        return columnHandlers.values().stream().map(c -> ((LanceColumnHandle) c).getColumnMetadata())
                .collect(toImmutableList());
    }

    public Map<String, ColumnHandle> getColumnHandle(String tableName)
    {
        String tablePath = getTablePath(tableName);
        Schema arrowSchema = getSchema(tablePath);
        // Use LinkedHashMap to preserve column order
        return arrowSchema.getFields().stream().collect(Collectors.toMap(
                Field::getName,
                f -> new LanceColumnHandle(f.getName(), LanceColumnHandle.toTrinoType(f.getFieldType().getType()),
                        f.getFieldType()),
                (v1, v2) -> v1,
                LinkedHashMap::new));
    }

    public String getTablePath(ConnectorSession session, SchemaTableName schemaTableName)
    {
        List<SchemaTableName> schemaTableNameList = listTables(session, schemaTableName.getSchemaName());
        if (schemaTableNameList.contains(schemaTableName)) {
            return getTablePath(schemaTableName.getTableName());
        }
        else {
            return null;
        }
    }

    public List<Fragment> getFragments(LanceTableHandle tableHandle)
    {
        return getFragments(getTablePath(tableHandle.getTableName()));
    }

    /**
     * Get a specific fragment by ID from the cache.
     * This is useful for workers that need to access a specific fragment for data reading.
     *
     * @param tablePath the path to the lance table
     * @param fragmentId the fragment ID to retrieve
     * @return the Fragment object, or null if not found
     */
    public static Fragment getFragment(String tablePath, int fragmentId)
    {
        try {
            CacheKey key = new CacheKey(tablePath);
            Map<Integer, Fragment> fragments = FRAGMENT_CACHE.get(key, () -> loadFragments(tablePath));
            return fragments.get(fragmentId);
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Failed to get fragment from cache for table: " + tablePath, e);
        }
    }

    private static List<Fragment> getFragments(String tablePath)
    {
        try {
            CacheKey key = new CacheKey(tablePath);
            Map<Integer, Fragment> fragmentMap = FRAGMENT_CACHE.get(key, () -> loadFragments(tablePath));
            return List.copyOf(fragmentMap.values());
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Failed to get fragments from cache for table: " + tablePath, e);
        }
    }

    private static Map<Integer, Fragment> loadFragments(String tablePath)
    {
        log.debug("Loading fragments for table: %s", tablePath);
        Dataset dataset = Dataset.open(tablePath, allocator);
        return dataset.getFragments().stream()
                .collect(Collectors.toMap(Fragment::getId, f -> f));
    }

    private static Schema getSchema(String tablePath)
    {
        try {
            CacheKey key = new CacheKey(tablePath);
            return SCHEMA_CACHE.get(key, () -> loadSchema(tablePath));
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Failed to get schema from cache for table: " + tablePath, e);
        }
    }

    private static Schema loadSchema(String tablePath)
    {
        log.debug("Loading schema for table: %s", tablePath);
        try (Dataset dataset = Dataset.open(tablePath, allocator)) {
            return dataset.getSchema();
        }
    }

    private String getTablePath(String tableName)
    {
        // Construct table path from root location and table name
        String path = root;
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        return path + tableName + TABLE_PATH_SUFFIX;
    }

    /**
     * Get the table path for creating a new table.
     * This is the same as getTablePath but is public for use by write operations.
     */
    public String getTablePathForNewTable(String tableName)
    {
        return getTablePath(tableName);
    }

    /**
     * Get the Arrow schema for a table.
     * Returns the raw Arrow Schema for use in write operations.
     */
    public Schema getArrowSchema(String tablePath)
    {
        return getSchema(tablePath);
    }

    /**
     * Invalidate cache entries for a table path.
     * Should be called after write operations to ensure fresh data is read.
     */
    public void invalidateCache(String tablePath)
    {
        log.debug("Invalidating cache for table: %s", tablePath);
        CacheKey key = new CacheKey(tablePath);
        FRAGMENT_CACHE.invalidate(key);
        SCHEMA_CACHE.invalidate(key);
    }

    @Override
    public void close()
    {
        // LanceNamespace doesn't have a close method in the interface,
        // but implementations may be Closeable
        if (namespace instanceof Closeable) {
            try {
                ((Closeable) namespace).close();
            }
            catch (Exception e) {
                // ignore for now
            }
        }
    }

    /**
     * Cache key for dataset metadata caching.
     * Uses table path as the unique identifier.
     */
    private static class CacheKey
    {
        private final String tablePath;

        CacheKey(String tablePath)
        {
            this.tablePath = tablePath;
        }

        public String getTablePath()
        {
            return tablePath;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(tablePath, cacheKey.tablePath);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tablePath);
        }
    }
}
