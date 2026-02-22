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
import com.google.inject.Inject;
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
import org.lance.Version;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;

import java.io.Closeable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;

/**
 * Cache for Lance sessions and datasets.
 *
 * <p>Key design principles:
 * <ul>
 *   <li>Session sharing: Different datasets opened by the same user share the same session,
 *       improving cache hit rates for index and metadata caches.</li>
 *   <li>User isolation: Different users get different sessions, ensuring cache isolation.</li>
 *   <li>Version-aware: Datasets are cached by (user, path, version) to support snapshot isolation.
 *       All workers access the same version during a query.</li>
 * </ul>
 *
 * <p>Schema and fragment data are accessed directly from the cached Dataset object,
 * as the manifest is already loaded when the Dataset is opened.
 */
@SuppressModernizer // Uses CacheBuilder for expireAfterAccess and removalListener
public class LanceDatasetCache
        implements Closeable
{
    private static final Logger log = Logger.get(LanceDatasetCache.class);

    private static final String ANONYMOUS_USER = "__anonymous__";

    private final Cache<String, Session> sessionCache;
    private final Cache<DatasetCacheKey, Dataset> datasetCache;
    private final Long sessionIndexCacheSizeBytes;
    private final Long sessionMetadataCacheSizeBytes;

    @Inject
    public LanceDatasetCache(LanceConfig config)
    {
        this.sessionIndexCacheSizeBytes = config.getCacheSessionIndexCacheSizeBytes();
        this.sessionMetadataCacheSizeBytes = config.getCacheSessionMetadataCacheSizeBytes();
        this.sessionCache = CacheBuilder.newBuilder()
                .maximumSize(config.getCacheSessionMaxEntries())
                .expireAfterAccess(config.getCacheSessionTtlMinutes(), TimeUnit.MINUTES)
                .removalListener((RemovalListener<String, Session>) notification -> {
                    Session session = notification.getValue();
                    if (session != null && !session.isClosed()) {
                        log.debug("Closing expired session for user: %s", notification.getKey());
                        session.close();
                    }
                })
                .build();

        this.datasetCache = CacheBuilder.newBuilder()
                .maximumSize(config.getCacheDatasetMaxEntries())
                .expireAfterAccess(config.getCacheDatasetTtlMinutes(), TimeUnit.MINUTES)
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

        log.info("LanceDatasetCache initialized: maxSessions=%d, maxDatasets=%d, sessionTtl=%dm, datasetTtl=%dm",
                config.getCacheSessionMaxEntries(), config.getCacheDatasetMaxEntries(),
                config.getCacheSessionTtlMinutes(), config.getCacheDatasetTtlMinutes());
    }

    // ================== Session Management ==================

    private Session getOrCreateSession(String userIdentity)
    {
        String key = normalizeUserIdentity(userIdentity);

        try {
            return sessionCache.get(key, () -> {
                log.debug("Creating new session for user: %s", key);
                Session.Builder builder = Session.builder();
                if (sessionIndexCacheSizeBytes != null) {
                    builder.indexCacheSizeBytes(sessionIndexCacheSizeBytes);
                }
                if (sessionMetadataCacheSizeBytes != null) {
                    builder.metadataCacheSizeBytes(sessionMetadataCacheSizeBytes);
                }
                return builder.build();
            });
        }
        catch (ExecutionException e) {
            log.error(e, "Failed to create session for user: %s", key);
            throw new RuntimeException("Failed to create Lance session", e);
        }
    }

    public long getActiveSessionCount()
    {
        return sessionCache.size();
    }

    public long getCachedDatasetCount()
    {
        return datasetCache.size();
    }

    private static String normalizeUserIdentity(String userIdentity)
    {
        return (userIdentity == null || userIdentity.isEmpty()) ? ANONYMOUS_USER : userIdentity;
    }

    // ================== Dataset Access ==================

    public Dataset getDataset(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        DatasetCacheKey key = new DatasetCacheKey(userIdentity, tablePath, version);

        try {
            return datasetCache.get(key, () -> openDataset(userIdentity, tablePath, version, storageOptions));
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Failed to open dataset: " + tablePath, e);
        }
    }

    public Dataset openDatasetDirect(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        return openDataset(userIdentity, tablePath, version, storageOptions);
    }

    private Dataset openDataset(String userIdentity, String tablePath, Long version,
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

    public long getLatestVersion(String userIdentity, String tablePath, Map<String, String> storageOptions)
    {
        try (Dataset dataset = openDatasetDirect(userIdentity, tablePath, null, storageOptions)) {
            return dataset.version();
        }
    }

    /**
     * Check if a specific version exists in the dataset.
     *
     * @param userIdentity the user identity
     * @param tablePath the path to the dataset
     * @param version the version to check
     * @param storageOptions the storage options
     * @return true if the version exists, false otherwise
     */
    public boolean versionExists(String userIdentity, String tablePath,
            long version, Map<String, String> storageOptions)
    {
        try (Dataset dataset = openDatasetDirect(userIdentity, tablePath, null, storageOptions)) {
            List<Version> versions = dataset.listVersions();
            return versions.stream().anyMatch(v -> v.getId() == version);
        }
    }

    /**
     * Get the version of the dataset at or before the given timestamp.
     *
     * @param userIdentity the user identity
     * @param tablePath the path to the dataset
     * @param timestampMillis the timestamp in milliseconds since epoch
     * @param storageOptions the storage options
     * @return the version at or before the timestamp, or empty if no such version exists
     */
    public Optional<Long> getVersionAtTimestamp(String userIdentity, String tablePath,
            long timestampMillis, Map<String, String> storageOptions)
    {
        try (Dataset dataset = openDatasetDirect(userIdentity, tablePath, null, storageOptions)) {
            List<Version> versions = dataset.listVersions();

            // Find the latest version where timestamp <= requested timestamp
            Version bestMatch = null;
            for (Version version : versions) {
                long versionTimestamp = version.getDataTime().toInstant().toEpochMilli();
                if (versionTimestamp <= timestampMillis) {
                    if (bestMatch == null || version.getId() > bestMatch.getId()) {
                        bestMatch = version;
                    }
                }
            }

            if (bestMatch != null) {
                log.debug("Found version %d at timestamp %s for requested time %s",
                        bestMatch.getId(),
                        bestMatch.getDataTime(),
                        Instant.ofEpochMilli(timestampMillis));
                return Optional.of(bestMatch.getId());
            }

            log.debug("No version found at or before timestamp %s", Instant.ofEpochMilli(timestampMillis));
            return Optional.empty();
        }
    }

    // ================== Fragment Access ==================

    public List<Fragment> getFragments(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        Dataset dataset = getDataset(userIdentity, tablePath, version, storageOptions);
        return dataset.getFragments();
    }

    public Fragment getFragment(String userIdentity, String tablePath, Long version,
            int fragmentId, Map<String, String> storageOptions)
    {
        Dataset dataset = getDataset(userIdentity, tablePath, version, storageOptions);
        return dataset.getFragment(fragmentId);
    }

    // ================== Schema Access ==================

    public Schema getSchema(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        Dataset dataset = getDataset(userIdentity, tablePath, version, storageOptions);
        return dataset.getSchema();
    }

    public LanceSchema getLanceSchema(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        Dataset dataset = getDataset(userIdentity, tablePath, version, storageOptions);
        return dataset.getLanceSchema();
    }

    public Map<String, ColumnHandle> getColumnHandles(String userIdentity, String tablePath, Long version,
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

    public List<LanceColumnHandle> getColumnHandleList(String userIdentity, String tablePath, Long version,
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

    private static Set<String> getBlobColumnsFromSchema(Schema schema)
    {
        return schema.getFields().stream()
                .filter(BlobUtils::isBlobArrowField)
                .map(Field::getName)
                .collect(Collectors.toSet());
    }

    public List<ColumnMetadata> getColumnMetadata(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        Map<String, ColumnHandle> columnHandles = getColumnHandles(userIdentity, tablePath, version, storageOptions);
        return columnHandles.values().stream()
                .map(c -> ((LanceColumnHandle) c).getColumnMetadata())
                .collect(toImmutableList());
    }

    public ManifestSummary getManifestSummary(String userIdentity, String tablePath, Long version,
            Map<String, String> storageOptions)
    {
        Dataset dataset = getDataset(userIdentity, tablePath, version, storageOptions);
        return dataset.getVersion().getManifestSummary();
    }

    // ================== Cache Invalidation ==================

    public void invalidate(String userIdentity, String tablePath)
    {
        log.debug("Invalidating cache for user=%s, table=%s", userIdentity, tablePath);

        String normalizedUser = normalizeUserIdentity(userIdentity);

        datasetCache.asMap().keySet().removeIf(key ->
                Objects.equals(key.userIdentity, normalizedUser) &&
                        Objects.equals(key.tablePath, tablePath));
    }

    // ================== Scanner Operations ==================

    public LanceScanner openDatasetScanner(String userIdentity, String tablePath, Long version,
            List<Integer> fragmentIds, ScanOptions scanOptions, Map<String, String> storageOptions)
    {
        Dataset dataset = getDataset(userIdentity, tablePath, version, storageOptions);

        ScanOptions.Builder scanBuilder = new ScanOptions.Builder(scanOptions)
                .fragmentIds(fragmentIds);

        return dataset.newScan(scanBuilder.build());
    }

    // ================== Lifecycle ==================

    @Override
    public void close()
    {
        log.info("Closing LanceDatasetCache: %d sessions, %d datasets", sessionCache.size(), datasetCache.size());

        datasetCache.asMap().forEach((key, dataset) -> {
            if (dataset != null) {
                try {
                    dataset.close();
                }
                catch (Exception e) {
                    log.warn(e, "Failed to close dataset during shutdown");
                }
            }
        });
        datasetCache.invalidateAll();

        sessionCache.asMap().forEach((key, session) -> {
            if (session != null && !session.isClosed()) {
                session.close();
            }
        });
        sessionCache.invalidateAll();
    }

    // ================== Cache Key ==================

    private static class DatasetCacheKey
    {
        private final String userIdentity;
        private final String tablePath;
        private final Long version;

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
    }
}
