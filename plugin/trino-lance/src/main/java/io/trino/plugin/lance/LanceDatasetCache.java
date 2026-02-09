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

import com.google.common.cache.Cache;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ManifestSummary;
import org.lance.ReadOptions;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Cache for Lance dataset metadata (fragments and schemas).
 * Shared across all connector instances per worker JVM.
 */
public final class LanceDatasetCache
{
    private static final Logger log = Logger.get(LanceDatasetCache.class);

    // Cache for dataset fragments - shared across all instances per worker JVM
    private static final Cache<CacheKey, Map<Integer, Fragment>> FRAGMENT_CACHE =
            EvictableCacheBuilder.newBuilder()
                    .maximumSize(100)
                    .expireAfterWrite(1, TimeUnit.HOURS)
                    .shareNothingWhenDisabled()
                    .build();

    // Cache for schema metadata - shared across all instances per worker JVM
    private static final Cache<CacheKey, Schema> SCHEMA_CACHE =
            EvictableCacheBuilder.newBuilder()
                    .maximumSize(100)
                    .expireAfterWrite(1, TimeUnit.HOURS)
                    .shareNothingWhenDisabled()
                    .build();

    private LanceDatasetCache()
    {
        // Utility class - prevent instantiation
    }

    /**
     * Get fragments for a table from cache or load them.
     */
    public static List<Fragment> getFragments(String tablePath, Map<String, String> storageOptions)
    {
        if (storageOptions == null || storageOptions.isEmpty()) {
            // Use cached version for local paths
            try {
                CacheKey key = new CacheKey(tablePath);
                Map<Integer, Fragment> fragmentMap = FRAGMENT_CACHE.get(key, () -> loadFragments(tablePath, null));
                return List.copyOf(fragmentMap.values());
            }
            catch (ExecutionException e) {
                throw new RuntimeException("Failed to get fragments from cache for table: " + tablePath, e);
            }
        }

        // For S3 with credentials, load directly (don't cache since credentials may expire)
        log.debug("Loading fragments with storage options for table: %s", tablePath);
        ReadOptions readOptions = new ReadOptions.Builder()
                .setStorageOptions(storageOptions)
                .build();
        Dataset dataset = Dataset.open(tablePath, readOptions);
        return List.copyOf(dataset.getFragments());
    }

    /**
     * Get a specific fragment by ID.
     */
    public static Fragment getFragment(String tablePath, int fragmentId, Map<String, String> storageOptions)
    {
        if (storageOptions == null || storageOptions.isEmpty()) {
            // Use cached version for local paths
            try {
                CacheKey key = new CacheKey(tablePath);
                Map<Integer, Fragment> fragments = FRAGMENT_CACHE.get(key, () -> loadFragments(tablePath, null));
                return fragments.get(fragmentId);
            }
            catch (ExecutionException e) {
                throw new RuntimeException("Failed to get fragment from cache for table: " + tablePath, e);
            }
        }

        // For S3 with credentials, load directly
        log.debug("Loading fragment %d with storage options for table: %s", fragmentId, tablePath);
        ReadOptions readOptions = new ReadOptions.Builder()
                .setStorageOptions(storageOptions)
                .build();
        Dataset dataset = Dataset.open(tablePath, readOptions);
        List<Fragment> fragments = dataset.getFragments();
        for (Fragment fragment : fragments) {
            if (fragment.getId() == fragmentId) {
                return fragment;
            }
        }
        return null;
    }

    /**
     * Get the Arrow schema for a table.
     */
    public static Schema getSchema(String tablePath, Map<String, String> storageOptions)
    {
        if (storageOptions == null || storageOptions.isEmpty()) {
            try {
                CacheKey key = new CacheKey(tablePath);
                return SCHEMA_CACHE.get(key, () -> loadSchema(tablePath, null));
            }
            catch (ExecutionException e) {
                throw new RuntimeException("Failed to get schema from cache for table: " + tablePath, e);
            }
        }

        // For S3 with credentials, load directly
        log.debug("Loading schema with storage options for table: %s", tablePath);
        ReadOptions readOptions = new ReadOptions.Builder()
                .setStorageOptions(storageOptions)
                .build();
        try (Dataset dataset = Dataset.open(tablePath, readOptions)) {
            return dataset.getSchema();
        }
    }

    /**
     * Get column handles for a table with field IDs.
     */
    public static Map<String, ColumnHandle> getColumnHandles(String tablePath, Map<String, String> storageOptions)
    {
        LanceSchema lanceSchema = getLanceSchema(tablePath, storageOptions);
        return lanceSchema.fields().stream().collect(Collectors.toMap(
                LanceField::getName,
                f -> new LanceColumnHandle(
                        f.getName(),
                        LanceColumnHandle.toTrinoType(f.getType()),
                        f.isNullable(),
                        f.getId()),
                (v1, v2) -> v1,
                LinkedHashMap::new));
    }

    /**
     * Get column handles as a list for a table with field IDs.
     */
    public static List<LanceColumnHandle> getColumnHandleList(String tablePath, Map<String, String> storageOptions)
    {
        LanceSchema lanceSchema = getLanceSchema(tablePath, storageOptions);
        return lanceSchema.fields().stream()
                .map(f -> new LanceColumnHandle(
                        f.getName(),
                        LanceColumnHandle.toTrinoType(f.getType()),
                        f.isNullable(),
                        f.getId()))
                .collect(toImmutableList());
    }

    /**
     * Get the Lance schema with field IDs.
     */
    public static LanceSchema getLanceSchema(String tablePath, Map<String, String> storageOptions)
    {
        log.debug("Loading Lance schema for table: %s", tablePath);
        ReadOptions.Builder optionsBuilder = new ReadOptions.Builder();
        if (storageOptions != null && !storageOptions.isEmpty()) {
            optionsBuilder.setStorageOptions(storageOptions);
        }
        try (Dataset dataset = Dataset.open(tablePath, optionsBuilder.build())) {
            return dataset.getLanceSchema();
        }
    }

    /**
     * Get column metadata for a table.
     */
    public static List<ColumnMetadata> getColumnMetadata(String tablePath, Map<String, String> storageOptions)
    {
        Map<String, ColumnHandle> columnHandles = getColumnHandles(tablePath, storageOptions);
        return columnHandles.values().stream()
                .map(c -> ((LanceColumnHandle) c).getColumnMetadata())
                .collect(toImmutableList());
    }

    /**
     * Get the manifest summary for a table. This provides table-level statistics
     * directly from the manifest without scanning data.
     */
    public static ManifestSummary getManifestSummary(String tablePath, Map<String, String> storageOptions)
    {
        log.debug("Getting manifest summary for table: %s", tablePath);
        ReadOptions.Builder optionsBuilder = new ReadOptions.Builder();
        if (storageOptions != null && !storageOptions.isEmpty()) {
            optionsBuilder.setStorageOptions(storageOptions);
        }
        try (Dataset dataset = Dataset.open(tablePath, optionsBuilder.build())) {
            return dataset.getVersion().getManifestSummary();
        }
    }

    /**
     * Invalidate cache entries for a table path.
     */
    public static void invalidate(String tablePath)
    {
        log.debug("Invalidating cache for table: %s", tablePath);
        CacheKey key = new CacheKey(tablePath);
        FRAGMENT_CACHE.invalidate(key);
        SCHEMA_CACHE.invalidate(key);
    }

    /**
     * Open a dataset and create a scanner for specific fragments.
     * This scanner respects deletion vectors from the manifest.
     *
     * @return Object array: [0] = Dataset (must be closed after scanning), [1] = LanceScanner
     */
    public static Object[] openDatasetScanner(String tablePath, List<Integer> fragmentIds,
            ScanOptions scanOptions, Map<String, String> storageOptions)
    {
        log.debug("Opening dataset scanner for fragments %s at table: %s", fragmentIds, tablePath);
        ReadOptions.Builder optionsBuilder = new ReadOptions.Builder();
        if (storageOptions != null && !storageOptions.isEmpty()) {
            optionsBuilder.setStorageOptions(storageOptions);
        }

        Dataset dataset = Dataset.open(tablePath, optionsBuilder.build());

        // Build scan options with fragment IDs
        ScanOptions.Builder scanBuilder = new ScanOptions.Builder(scanOptions)
                .fragmentIds(fragmentIds);

        LanceScanner scanner = dataset.newScan(scanBuilder.build());
        return new Object[] {dataset, scanner};
    }

    private static Map<Integer, Fragment> loadFragments(String tablePath, Map<String, String> storageOptions)
    {
        log.debug("Loading fragments for table: %s", tablePath);
        if (storageOptions != null && !storageOptions.isEmpty()) {
            ReadOptions readOptions = new ReadOptions.Builder()
                    .setStorageOptions(storageOptions)
                    .build();
            Dataset dataset = Dataset.open(tablePath, readOptions);
            return dataset.getFragments().stream()
                    .collect(Collectors.toMap(Fragment::getId, f -> f));
        }
        Dataset dataset = Dataset.open(tablePath, LanceNamespaceHolder.getAllocator());
        return dataset.getFragments().stream()
                .collect(Collectors.toMap(Fragment::getId, f -> f));
    }

    private static Schema loadSchema(String tablePath, Map<String, String> storageOptions)
    {
        log.debug("Loading schema for table: %s", tablePath);
        if (storageOptions != null && !storageOptions.isEmpty()) {
            ReadOptions readOptions = new ReadOptions.Builder()
                    .setStorageOptions(storageOptions)
                    .build();
            try (Dataset dataset = Dataset.open(tablePath, readOptions)) {
                return dataset.getSchema();
            }
        }
        try (Dataset dataset = Dataset.open(tablePath, LanceNamespaceHolder.getAllocator())) {
            return dataset.getSchema();
        }
    }

    /**
     * Cache key for dataset metadata caching.
     */
    private static class CacheKey
    {
        private final String tablePath;

        CacheKey(String tablePath)
        {
            this.tablePath = tablePath;
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
