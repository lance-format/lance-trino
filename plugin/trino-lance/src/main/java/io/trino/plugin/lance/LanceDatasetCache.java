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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ManifestSummary;
import org.lance.ReadOptions;
import org.lance.Session;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;

/**
 * Cache for Lance datasets, sessions, fragments, and schemas.
 *
 * <p>Key design principles:
 * <ul>
 *   <li>Session sharing: Different datasets opened by the same user share the same session,
 *       improving cache hit rates for index and metadata caches.</li>
 *   <li>User isolation: Different users get different sessions, ensuring cache isolation.</li>
 *   <li>Version-aware: Datasets are cached by (user, path, version) to support snapshot isolation.
 *       All workers access the same version during a query.</li>
 * </ul>
 */
@SuppressModernizer // Uses CacheBuilder for expireAfterAccess and removalListener which EvictableCacheBuilder doesn't support
public final class LanceDatasetCache
{
    private static final Logger log = Logger.get(LanceDatasetCache.class);

    // Default user identity for anonymous/system operations
    private static final String ANONYMOUS_USER = "__anonymous__";

    // Cache for sessions by user identity - sessions expire after 1 hour of inactivity
    private static final Cache<String, Session> SESSION_CACHE =
            CacheBuilder.newBuilder()
                    .maximumSize(100)
                    .expireAfterAccess(1, TimeUnit.HOURS)
                    .removalListener((RemovalListener<String, Session>) notification -> {
                        Session session = notification.getValue();
                        if (session != null && !session.isClosed()) {
                            log.debug("Closing expired session for user: %s", notification.getKey());
                            session.close();
                        }
                    })
                    .build();

    // Cache for opened datasets - keyed by (userIdentity, tablePath, version)
    private static final Cache<DatasetCacheKey, Dataset> DATASET_CACHE =
            CacheBuilder.newBuilder()
                    .maximumSize(100)
                    .expireAfterAccess(30, TimeUnit.MINUTES)
                    .removalListener((RemovalListener<DatasetCacheKey, Dataset>) notification -> {
                        Dataset dataset = notification.getValue();
                        if (dataset != null) {
                            try {
                                dataset.close();
                            }
                            catch (Exception e) {
                                log.warn(e, "Failed to close cached dataset");
                            }
                        }
                    })
                    .build();

    // Cache for schema metadata - keyed by (userIdentity, tablePath, version)
    private static final Cache<DatasetCacheKey, Schema> SCHEMA_CACHE =
            CacheBuilder.newBuilder()
                    .maximumSize(200)
                    .expireAfterWrite(1, TimeUnit.HOURS)
                    .build();

    // Cache for fragments - keyed by (userIdentity, tablePath, version)
    private static final Cache<DatasetCacheKey, Map<Integer, Fragment>> FRAGMENT_CACHE =
            CacheBuilder.newBuilder()
                    .maximumSize(200)
                    .expireAfterWrite(1, TimeUnit.HOURS)
                    .build();

    private LanceDatasetCache()
    {
        // Utility class - prevent instantiation
    }

    // ================== Session Management ==================

    /**
     * Get or create a session for the given user identity.
     * Sessions are shared across datasets for cache benefits.
     *
     * @param userIdentity the user identity, or null for anonymous
     * @return the session for this user
     */
    private static Session getOrCreateSession(String userIdentity)
    {
        String key = normalizeUserIdentity(userIdentity);

        try {
            return SESSION_CACHE.get(key, () -> {
                log.debug("Creating new session for user: %s", key);
                return Session.builder()
                        .indexCacheSizeBytes(Session.DEFAULT_INDEX_CACHE_SIZE_BYTES)
                        .metadataCacheSizeBytes(Session.DEFAULT_METADATA_CACHE_SIZE_BYTES)
                        .build();
            });
        }
        catch (ExecutionException e) {
            log.error(e, "Failed to create session for user: %s", key);
            throw new RuntimeException("Failed to create Lance session", e);
        }
    }

    /**
     * Get the number of active sessions (for monitoring).
     */
    public static long getActiveSessionCount()
    {
        return SESSION_CACHE.size();
    }

    private static String normalizeUserIdentity(String userIdentity)
    {
        return (userIdentity == null || userIdentity.isEmpty()) ? ANONYMOUS_USER : userIdentity;
    }

    // ================== Dataset Access ==================

    /**
     * Get or open a dataset for reading with session sharing and version support.
     *
     * @param userIdentity the user identity for session isolation
     * @param tablePath the path to the dataset
     * @param version the version to open (null for latest)
     * @param storageOptions storage options (S3 credentials, etc.)
     * @return the opened dataset
     */
    public static Dataset getDataset(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        DatasetCacheKey key = new DatasetCacheKey(userIdentity, tablePath, version);

        try {
            return DATASET_CACHE.get(key, () -> openDataset(userIdentity, tablePath, version, storageOptions));
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Failed to open dataset: " + tablePath, e);
        }
    }

    /**
     * Open a dataset without caching. Use this when you need a fresh dataset handle
     * that won't be cached (e.g., for write operations).
     */
    public static Dataset openDatasetDirect(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        return openDataset(userIdentity, tablePath, version, storageOptions);
    }

    private static Dataset openDataset(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        log.debug("Opening dataset: path=%s, version=%s, user=%s", tablePath, version, userIdentity);

        Session session = getOrCreateSession(userIdentity);

        ReadOptions.Builder optionsBuilder = new ReadOptions.Builder()
                .setSession(session);

        if (version != null) {
            optionsBuilder.setVersion(version);
        }

        if (storageOptions != null && !storageOptions.isEmpty()) {
            optionsBuilder.setStorageOptions(storageOptions);
        }

        return Dataset.open(tablePath, optionsBuilder.build());
    }

    /**
     * Get the current (latest) version of a dataset.
     * This opens the dataset at the latest version and returns its version number.
     */
    public static long getLatestVersion(String userIdentity, String tablePath, Map<String, String> storageOptions)
    {
        try (Dataset dataset = openDatasetDirect(userIdentity, tablePath, null, storageOptions)) {
            return dataset.version();
        }
    }

    // ================== Fragment Access ==================

    /**
     * Get fragments for a table from cache or load them.
     */
    public static List<Fragment> getFragments(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        DatasetCacheKey key = new DatasetCacheKey(userIdentity, tablePath, version);

        try {
            Map<Integer, Fragment> fragmentMap = FRAGMENT_CACHE.get(key, () -> {
                Dataset dataset = getDataset(userIdentity, tablePath, version, storageOptions);
                return dataset.getFragments().stream()
                        .collect(Collectors.toMap(Fragment::getId, f -> f));
            });
            return List.copyOf(fragmentMap.values());
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Failed to get fragments for table: " + tablePath, e);
        }
    }

    /**
     * Get a specific fragment by ID.
     */
    public static Fragment getFragment(String userIdentity, String tablePath, Long version,
            int fragmentId, Map<String, String> storageOptions)
    {
        DatasetCacheKey key = new DatasetCacheKey(userIdentity, tablePath, version);

        try {
            Map<Integer, Fragment> fragments = FRAGMENT_CACHE.get(key, () -> {
                Dataset dataset = getDataset(userIdentity, tablePath, version, storageOptions);
                return dataset.getFragments().stream()
                        .collect(Collectors.toMap(Fragment::getId, f -> f));
            });
            return fragments.get(fragmentId);
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Failed to get fragment from cache for table: " + tablePath, e);
        }
    }

    // ================== Schema Access ==================

    /**
     * Get the Arrow schema for a table.
     */
    public static Schema getSchema(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        DatasetCacheKey key = new DatasetCacheKey(userIdentity, tablePath, version);

        try {
            return SCHEMA_CACHE.get(key, () -> {
                Dataset dataset = getDataset(userIdentity, tablePath, version, storageOptions);
                return dataset.getSchema();
            });
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Failed to get schema for table: " + tablePath, e);
        }
    }

    /**
     * Get column handles for a table with field IDs.
     * Includes virtual columns (__blob_pos, __blob_size) for blob columns.
     */
    public static Map<String, ColumnHandle> getColumnHandles(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        LanceSchema lanceSchema = getLanceSchema(userIdentity, tablePath, version, storageOptions);
        Schema arrowSchema = getSchema(userIdentity, tablePath, version, storageOptions);
        Set<String> blobColumns = getBlobColumnsFromSchema(arrowSchema);

        Map<String, ColumnHandle> result = new LinkedHashMap<>();

        for (LanceField f : lanceSchema.fields()) {
            boolean isBlob = blobColumns.contains(f.getName());
            result.put(f.getName(), new LanceColumnHandle(
                    f.getName(),
                    LanceColumnHandle.toTrinoType(f),
                    f.isNullable(),
                    f.getId(),
                    isBlob));

            // Add virtual columns for blob columns
            if (isBlob) {
                String posColumnName = BlobUtils.getBlobPositionColumnName(f.getName());
                String sizeColumnName = BlobUtils.getBlobSizeColumnName(f.getName());

                result.put(posColumnName, new LanceColumnHandle(
                        posColumnName,
                        BIGINT,
                        true,
                        -1,
                        false,
                        BlobUtils.BlobVirtualColumnType.POSITION,
                        f.getName()));

                result.put(sizeColumnName, new LanceColumnHandle(
                        sizeColumnName,
                        BIGINT,
                        true,
                        -1,
                        false,
                        BlobUtils.BlobVirtualColumnType.SIZE,
                        f.getName()));
            }
        }

        return result;
    }

    /**
     * Get column handles as a list for a table with field IDs.
     * Includes virtual columns (__blob_pos, __blob_size) for blob columns.
     */
    public static List<LanceColumnHandle> getColumnHandleList(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        LanceSchema lanceSchema = getLanceSchema(userIdentity, tablePath, version, storageOptions);
        Schema arrowSchema = getSchema(userIdentity, tablePath, version, storageOptions);
        Set<String> blobColumns = getBlobColumnsFromSchema(arrowSchema);

        List<LanceColumnHandle> result = new ArrayList<>();

        for (LanceField f : lanceSchema.fields()) {
            boolean isBlob = blobColumns.contains(f.getName());
            result.add(new LanceColumnHandle(
                    f.getName(),
                    LanceColumnHandle.toTrinoType(f),
                    f.isNullable(),
                    f.getId(),
                    isBlob));

            // Add virtual columns for blob columns
            if (isBlob) {
                result.add(new LanceColumnHandle(
                        BlobUtils.getBlobPositionColumnName(f.getName()),
                        BIGINT,
                        true,
                        -1,
                        false,
                        BlobUtils.BlobVirtualColumnType.POSITION,
                        f.getName()));

                result.add(new LanceColumnHandle(
                        BlobUtils.getBlobSizeColumnName(f.getName()),
                        BIGINT,
                        true,
                        -1,
                        false,
                        BlobUtils.BlobVirtualColumnType.SIZE,
                        f.getName()));
            }
        }

        return result;
    }

    /**
     * Get names of blob columns from an Arrow schema by checking field metadata.
     */
    private static Set<String> getBlobColumnsFromSchema(Schema schema)
    {
        return schema.getFields().stream()
                .filter(BlobUtils::isBlobArrowField)
                .map(Field::getName)
                .collect(Collectors.toSet());
    }

    /**
     * Get the Lance schema with field IDs.
     */
    public static LanceSchema getLanceSchema(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        log.debug("Loading Lance schema for table: %s", tablePath);
        Dataset dataset = getDataset(userIdentity, tablePath, version, storageOptions);
        return dataset.getLanceSchema();
    }

    /**
     * Get column metadata for a table.
     * Virtual columns (__blob_pos, __blob_size) are included but marked as hidden.
     */
    public static List<ColumnMetadata> getColumnMetadata(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        Map<String, ColumnHandle> columnHandles = getColumnHandles(userIdentity, tablePath, version, storageOptions);
        return columnHandles.values().stream()
                .map(c -> ((LanceColumnHandle) c).getColumnMetadata())
                .collect(toImmutableList());
    }

    /**
     * Get the manifest summary for a table. This provides table-level statistics
     * directly from the manifest without scanning data.
     */
    public static ManifestSummary getManifestSummary(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        log.debug("Getting manifest summary for table: %s", tablePath);
        Dataset dataset = getDataset(userIdentity, tablePath, version, storageOptions);
        return dataset.getVersion().getManifestSummary();
    }

    // ================== Cache Invalidation ==================

    /**
     * Invalidate cache entries for a table path.
     * This invalidates all versions for the given user and path.
     */
    public static void invalidate(String userIdentity, String tablePath)
    {
        log.debug("Invalidating cache for user=%s, table=%s", userIdentity, tablePath);

        String normalizedUser = normalizeUserIdentity(userIdentity);

        // Invalidate all matching entries (any version)
        DATASET_CACHE.asMap().keySet().removeIf(key ->
                Objects.equals(key.userIdentity, normalizedUser) &&
                        Objects.equals(key.tablePath, tablePath));

        FRAGMENT_CACHE.asMap().keySet().removeIf(key ->
                Objects.equals(key.userIdentity, normalizedUser) &&
                        Objects.equals(key.tablePath, tablePath));

        SCHEMA_CACHE.asMap().keySet().removeIf(key ->
                Objects.equals(key.userIdentity, normalizedUser) &&
                        Objects.equals(key.tablePath, tablePath));
    }

    // ================== Scanner Operations ==================

    /**
     * Open a dataset and create a scanner for specific fragments.
     * This scanner respects deletion vectors from the manifest.
     *
     * @return the scanner for the specified fragments
     */
    public static LanceScanner openDatasetScanner(String userIdentity, String tablePath, Long version,
            List<Integer> fragmentIds, ScanOptions scanOptions, Map<String, String> storageOptions)
    {
        log.debug("Opening dataset scanner for fragments %s at table: %s", fragmentIds, tablePath);

        Dataset dataset = getDataset(userIdentity, tablePath, version, storageOptions);

        // Build scan options with fragment IDs
        ScanOptions.Builder scanBuilder = new ScanOptions.Builder(scanOptions)
                .fragmentIds(fragmentIds);

        return dataset.newScan(scanBuilder.build());
    }

    // ================== Cache Key ==================

    /**
     * Cache key for dataset caching.
     * Includes user identity for isolation, table path, and version for snapshot isolation.
     */
    private static class DatasetCacheKey
    {
        private final String userIdentity;
        private final String tablePath;
        private final Long version;  // null means latest

        DatasetCacheKey(String userIdentity, String tablePath, Long version)
        {
            this.userIdentity = normalizeUserIdentity(userIdentity);
            this.tablePath = tablePath;
            this.version = version;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DatasetCacheKey that = (DatasetCacheKey) o;
            return Objects.equals(userIdentity, that.userIdentity) &&
                    Objects.equals(tablePath, that.tablePath) &&
                    Objects.equals(version, that.version);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(userIdentity, tablePath, version);
        }

        @Override
        public String toString()
        {
            return "DatasetCacheKey{user=" + userIdentity + ", path=" + tablePath + ", version=" + version + "}";
        }
    }

    // ================== Legacy API compatibility methods ==================
    // These methods maintain backward compatibility with the old API

    /**
     * @deprecated Use {@link #getFragments(String, String, Long, Map)} instead
     */
    @Deprecated
    public static List<Fragment> getFragments(String tablePath, Map<String, String> storageOptions)
    {
        return getFragments(null, tablePath, null, storageOptions);
    }

    /**
     * @deprecated Use {@link #getFragment(String, String, Long, int, Map)} instead
     */
    @Deprecated
    public static Fragment getFragment(String tablePath, int fragmentId, Map<String, String> storageOptions)
    {
        return getFragment(null, tablePath, null, fragmentId, storageOptions);
    }

    /**
     * @deprecated Use {@link #getSchema(String, String, Long, Map)} instead
     */
    @Deprecated
    public static Schema getSchema(String tablePath, Map<String, String> storageOptions)
    {
        return getSchema(null, tablePath, null, storageOptions);
    }

    /**
     * @deprecated Use {@link #getColumnHandles(String, String, Long, Map)} instead
     */
    @Deprecated
    public static Map<String, ColumnHandle> getColumnHandles(String tablePath, Map<String, String> storageOptions)
    {
        return getColumnHandles(null, tablePath, null, storageOptions);
    }

    /**
     * @deprecated Use {@link #getColumnHandleList(String, String, Long, Map)} instead
     */
    @Deprecated
    public static List<LanceColumnHandle> getColumnHandleList(String tablePath, Map<String, String> storageOptions)
    {
        return getColumnHandleList(null, tablePath, null, storageOptions);
    }

    /**
     * @deprecated Use {@link #getLanceSchema(String, String, Long, Map)} instead
     */
    @Deprecated
    public static LanceSchema getLanceSchema(String tablePath, Map<String, String> storageOptions)
    {
        return getLanceSchema(null, tablePath, null, storageOptions);
    }

    /**
     * @deprecated Use {@link #getColumnMetadata(String, String, Long, Map)} instead
     */
    @Deprecated
    public static List<ColumnMetadata> getColumnMetadata(String tablePath, Map<String, String> storageOptions)
    {
        return getColumnMetadata(null, tablePath, null, storageOptions);
    }

    /**
     * @deprecated Use {@link #getManifestSummary(String, String, Long, Map)} instead
     */
    @Deprecated
    public static ManifestSummary getManifestSummary(String tablePath, Map<String, String> storageOptions)
    {
        return getManifestSummary(null, tablePath, null, storageOptions);
    }

    /**
     * @deprecated Use {@link #invalidate(String, String)} instead
     */
    @Deprecated
    public static void invalidate(String tablePath)
    {
        invalidate(null, tablePath);
    }

    /**
     * @deprecated Use {@link #openDatasetScanner(String, String, Long, List, ScanOptions, Map)} instead
     */
    @Deprecated
    public static Object[] openDatasetScanner(String tablePath, List<Integer> fragmentIds,
            ScanOptions scanOptions, Map<String, String> storageOptions)
    {
        Dataset dataset = getDataset(null, tablePath, null, storageOptions);
        LanceScanner scanner = openDatasetScanner(null, tablePath, null, fragmentIds, scanOptions, storageOptions);
        return new Object[] {dataset, scanner};
    }
}
