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
import jakarta.annotation.PreDestroy;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
import org.lance.namespace.LanceNamespace;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;

import java.io.Closeable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;

/**
 * Central runtime component for Lance connector.
 * Holds the Arrow allocator, namespace, and dataset/session caches.
 *
 * <p>Key design principles:
 * <ul>
 *   <li>Single lifecycle: All resources (allocator, namespace, caches) share the same lifecycle.</li>
 *   <li>Session sharing: Different datasets opened by the same user share the same session,
 *       improving cache hit rates for index and metadata caches.</li>
 *   <li>User isolation: Different users get different sessions, ensuring cache isolation.</li>
 *   <li>Version-aware: Datasets are cached by (user, path, version) to support snapshot isolation.</li>
 * </ul>
 */
@SuppressModernizer
public class LanceRuntime
        implements Closeable
{
    private static final Logger log = Logger.get(LanceRuntime.class);

    // Virtual "default" schema for single-level namespace mode
    public static final String DEFAULT_SCHEMA = "default";
    public static final String TABLE_PATH_SUFFIX = ".lance";

    private static final String ARROW_ALLOCATION_MANAGER_TYPE = "arrow.allocation.manager.type";
    private static final String ARROW_ALLOCATION_MANAGER_TYPE_ENV = "ARROW_ALLOCATION_MANAGER_TYPE";
    private static final String ARROW_ALLOCATION_MANAGER_UNSAFE = "Unsafe";
    private static final String ANONYMOUS_USER = "__anonymous__";

    // Core resources
    private final BufferAllocator allocator;
    private final LanceNamespace namespace;

    // Namespace configuration
    private final String root;
    private final boolean singleLevelNs;
    private final Optional<List<String>> parentPrefix;
    private final Map<String, String> namespaceStorageOptions;

    // Caches
    private final Cache<String, CachedSession> sessionCache;
    private final Cache<DatasetCacheKey, CachedDataset> datasetCache;
    private final Long sessionIndexCacheSizeBytes;
    private final Long sessionMetadataCacheSizeBytes;

    @Inject
    public LanceRuntime(LanceConfig config, @LanceNamespaceProperties Map<String, String> namespaceProperties)
    {
        configureArrowAllocationManager();

        // Initialize allocator first - it's needed for namespace initialization
        this.allocator = new RootAllocator(
                RootAllocator.configBuilder()
                        .from(RootAllocator.defaultConfig())
                        .maxAllocation(Long.MAX_VALUE)
                        .build());

        // Parse namespace properties
        String impl = config.getImpl();
        Map<String, String> properties = new HashMap<>();
        Map<String, String> storageOpts = new HashMap<>();
        for (Map.Entry<String, String> entry : namespaceProperties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("lance.")) {
                String strippedKey = key.substring(6);
                properties.put(strippedKey, entry.getValue());
                if (strippedKey.startsWith("storage.")) {
                    storageOpts.put(strippedKey.substring(8), entry.getValue());
                }
            }
        }
        this.namespaceStorageOptions = storageOpts;

        // For DirectoryNamespace, ensure default settings are applied
        if ("dir".equals(impl)) {
            properties.putIfAbsent("manifest_enabled", "true");
            properties.putIfAbsent("dir_listing_enabled", "true");
        }

        // Initialize namespace
        this.namespace = LanceNamespace.connect(impl, properties, allocator);
        this.root = properties.get("root");

        // Initialize namespace level handling
        this.singleLevelNs = config.isSingleLevelNs();
        String parent = config.getParent();
        if (parent != null && !parent.isEmpty()) {
            this.parentPrefix = Optional.of(Arrays.asList(parent.split("\\$")));
        }
        else {
            this.parentPrefix = Optional.empty();
        }

        // Initialize caches
        this.sessionIndexCacheSizeBytes = config.getCacheSessionIndexCacheSizeBytes();
        this.sessionMetadataCacheSizeBytes = config.getCacheSessionMetadataCacheSizeBytes();

        this.sessionCache = CacheBuilder.newBuilder()
                .maximumSize(config.getCacheSessionMaxEntries())
                .expireAfterAccess(config.getCacheSessionTtlMinutes(), TimeUnit.MINUTES)
                .removalListener((RemovalListener<String, CachedSession>) notification -> {
                    CachedSession session = notification.getValue();
                    if (session != null) {
                        log.debug("Retiring expired session for user: %s", notification.getKey());
                        session.retire();
                    }
                })
                .build();

        this.datasetCache = CacheBuilder.newBuilder()
                .maximumSize(config.getCacheDatasetMaxEntries())
                .expireAfterAccess(config.getCacheDatasetTtlMinutes(), TimeUnit.MINUTES)
                .removalListener((RemovalListener<DatasetCacheKey, CachedDataset>) notification -> {
                    CachedDataset dataset = notification.getValue();
                    if (dataset != null) {
                        dataset.retire();
                    }
                })
                .build();

        log.info("LanceRuntime initialized: impl=%s, root=%s, singleLevelNs=%s, maxSessions=%d, maxDatasets=%d",
                impl,
                root,
                singleLevelNs,
                config.getCacheSessionMaxEntries(),
                config.getCacheDatasetMaxEntries());
    }

    static void configureArrowAllocationManager()
    {
        // Arrow 19's Netty allocator is incompatible with Netty 4.2 on Java 25. Prefer
        // Unsafe unless the process was explicitly configured with another Arrow allocator.
        if (System.getProperty(ARROW_ALLOCATION_MANAGER_TYPE) == null &&
                System.getenv(ARROW_ALLOCATION_MANAGER_TYPE_ENV) == null) {
            System.setProperty(ARROW_ALLOCATION_MANAGER_TYPE, ARROW_ALLOCATION_MANAGER_UNSAFE);
        }
    }

    // ================== Core Accessors ==================

    public BufferAllocator getAllocator()
    {
        return allocator;
    }

    public LanceNamespace getNamespace()
    {
        return namespace;
    }

    public String getRoot()
    {
        return root;
    }

    public boolean isSingleLevelNs()
    {
        return singleLevelNs;
    }

    public Optional<List<String>> getParentPrefix()
    {
        return parentPrefix;
    }

    public Map<String, String> getNamespaceStorageOptions()
    {
        return new HashMap<>(namespaceStorageOptions);
    }

    // ================== Namespace Utilities ==================

    /**
     * Transform Trino schema name to Lance namespace identifier.
     * In single-level mode, "default" maps to empty (root).
     * Otherwise, adds parent prefix if configured.
     */
    public List<String> trinoSchemaToLanceNamespace(String schema)
    {
        if (singleLevelNs) {
            return Collections.emptyList();
        }
        List<String> namespaceId = List.of(schema);
        return addParentPrefix(namespaceId);
    }

    /**
     * Add parent prefix for 3+ level namespaces.
     */
    public List<String> addParentPrefix(List<String> namespaceId)
    {
        if (parentPrefix.isEmpty()) {
            return namespaceId;
        }
        List<String> result = new ArrayList<>(parentPrefix.get());
        result.addAll(namespaceId);
        return result;
    }

    /**
     * Convert a Trino SchemaTableName to a Lance table identifier.
     */
    public List<String> getTableId(String schemaName, String tableName)
    {
        List<String> tableId = new ArrayList<>();
        if (parentPrefix.isPresent()) {
            tableId.addAll(parentPrefix.get());
        }
        if (!singleLevelNs) {
            tableId.add(schemaName);
        }
        tableId.add(tableName);
        return tableId;
    }

    // ================== Session Management ==================

    private SessionLease getOrCreateSessionLease(String userIdentity)
    {
        String key = normalizeUserIdentity(userIdentity);
        while (true) {
            try {
                CachedSession cachedSession = sessionCache.get(key, () -> new CachedSession(createSession(key)));
                SessionLease lease = cachedSession.tryAcquire();
                if (lease != null) {
                    return lease;
                }
                sessionCache.invalidate(key);
            }
            catch (ExecutionException e) {
                log.error(e, "Failed to create session for user: %s", key);
                throw new RuntimeException("Failed to create Lance session", e);
            }
        }
    }

    private Session createSession(String key)
    {
        log.debug("Creating new session for user: %s", key);
        Session.Builder builder = Session.builder();
        if (sessionIndexCacheSizeBytes != null) {
            builder.indexCacheSizeBytes(sessionIndexCacheSizeBytes);
        }
        if (sessionMetadataCacheSizeBytes != null) {
            builder.metadataCacheSizeBytes(sessionMetadataCacheSizeBytes);
        }
        return builder.build();
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

    DatasetLease getDatasetLease(
            String userIdentity,
            String tablePath,
            Long version,
            Map<String, String> storageOptions)
    {
        if (!isDatasetCacheable(storageOptions)) {
            return openDatasetDirectLease(userIdentity, tablePath, version, storageOptions);
        }

        DatasetCacheKey key = new DatasetCacheKey(userIdentity, tablePath, version, storageOptions);
        while (true) {
            try {
                CachedDataset cachedDataset = datasetCache.get(
                        key,
                        () -> openCachedDataset(userIdentity, tablePath, version, storageOptions));
                DatasetLease lease = cachedDataset.tryAcquire();
                if (lease != null) {
                    return lease;
                }
                datasetCache.invalidate(key);
            }
            catch (ExecutionException e) {
                throw new RuntimeException("Failed to open dataset: " + tablePath, e);
            }
        }
    }

    private CachedDataset openCachedDataset(
            String userIdentity,
            String tablePath,
            Long version,
            Map<String, String> storageOptions)
    {
        SessionLease sessionLease = getOrCreateSessionLease(userIdentity);
        try {
            return new CachedDataset(openDataset(sessionLease.getSession(), userIdentity, tablePath, version, storageOptions), sessionLease);
        }
        catch (RuntimeException | Error e) {
            sessionLease.close();
            throw e;
        }
    }

    DatasetLease openDatasetDirectLease(
            String userIdentity,
            String tablePath,
            Long version,
            Map<String, String> storageOptions)
    {
        SessionLease sessionLease = getOrCreateSessionLease(userIdentity);
        try {
            Dataset dataset = openDataset(sessionLease.getSession(), userIdentity, tablePath, version, storageOptions);
            return DatasetLease.direct(dataset, sessionLease);
        }
        catch (RuntimeException | Error e) {
            sessionLease.close();
            throw e;
        }
    }

    private Dataset openDataset(
            Session session,
            String userIdentity,
            String tablePath,
            Long version,
            Map<String, String> storageOptions)
    {
        log.debug("Opening dataset: path=%s, version=%s, user=%s", tablePath, version, userIdentity);

        ReadOptions.Builder optionsBuilder = new ReadOptions.Builder()
                .setSession(session);

        if (version != null) {
            optionsBuilder.setVersion(version);
        }

        if (storageOptions != null && !storageOptions.isEmpty()) {
            optionsBuilder.setStorageOptions(storageOptions);
        }

        // Use our shared allocator instead of letting Dataset create its own.
        // This prevents the allocator from being closed when datasets are closed,
        // which would break concurrent operations that are still using scanners.
        return Dataset.open(allocator, tablePath, optionsBuilder.build());
    }

    public long getLatestVersion(String userIdentity, String tablePath, Map<String, String> storageOptions)
    {
        try (DatasetLease datasetLease = openDatasetDirectLease(userIdentity, tablePath, null, storageOptions)) {
            return datasetLease.getDataset().version();
        }
    }

    public boolean versionExists(
            String userIdentity,
            String tablePath,
            long version,
            Map<String, String> storageOptions)
    {
        try (DatasetLease datasetLease = openDatasetDirectLease(userIdentity, tablePath, null, storageOptions)) {
            List<Version> versions = datasetLease.getDataset().listVersions();
            return versions.stream().anyMatch(v -> v.getId() == version);
        }
    }

    public Optional<Long> getVersionAtTimestamp(
            String userIdentity,
            String tablePath,
            long timestampMillis,
            Map<String, String> storageOptions)
    {
        try (DatasetLease datasetLease = openDatasetDirectLease(userIdentity, tablePath, null, storageOptions)) {
            List<Version> versions = datasetLease.getDataset().listVersions();

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
                log.debug(
                        "Found version %d at timestamp %s for requested time %s",
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

    public List<Fragment> getFragments(
            String userIdentity,
            String tablePath,
            Long version,
            Map<String, String> storageOptions)
    {
        try (DatasetLease datasetLease = getDatasetLease(userIdentity, tablePath, version, storageOptions)) {
            return datasetLease.getDataset().getFragments();
        }
    }

    public Fragment getFragment(
            String userIdentity,
            String tablePath,
            Long version,
            int fragmentId,
            Map<String, String> storageOptions)
    {
        try (DatasetLease datasetLease = getDatasetLease(userIdentity, tablePath, version, storageOptions)) {
            return datasetLease.getDataset().getFragment(fragmentId);
        }
    }

    // ================== Schema Access ==================

    public Schema getSchema(
            String userIdentity,
            String tablePath,
            Long version,
            Map<String, String> storageOptions)
    {
        try (DatasetLease datasetLease = getDatasetLease(userIdentity, tablePath, version, storageOptions)) {
            return datasetLease.getDataset().getSchema();
        }
    }

    public LanceSchema getLanceSchema(
            String userIdentity,
            String tablePath,
            Long version,
            Map<String, String> storageOptions)
    {
        try (DatasetLease datasetLease = getDatasetLease(userIdentity, tablePath, version, storageOptions)) {
            return datasetLease.getDataset().getLanceSchema();
        }
    }

    public Map<String, ColumnHandle> getColumnHandles(
            String userIdentity,
            String tablePath,
            Long version,
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

    public List<LanceColumnHandle> getColumnHandleList(
            String userIdentity,
            String tablePath,
            Long version,
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

    public List<ColumnMetadata> getColumnMetadata(
            String userIdentity,
            String tablePath,
            Long version,
            Map<String, String> storageOptions)
    {
        Map<String, ColumnHandle> columnHandles = getColumnHandles(userIdentity, tablePath, version, storageOptions);
        return columnHandles.values().stream()
                .map(c -> ((LanceColumnHandle) c).getColumnMetadata())
                .collect(toImmutableList());
    }

    public ManifestSummary getManifestSummary(
            String userIdentity,
            String tablePath,
            Long version,
            Map<String, String> storageOptions)
    {
        try (DatasetLease datasetLease = getDatasetLease(userIdentity, tablePath, version, storageOptions)) {
            return datasetLease.getDataset().getVersion().getManifestSummary();
        }
    }

    // ================== Cache Invalidation ==================

    public void invalidate(String userIdentity, String tablePath)
    {
        log.debug("Invalidating cache for user=%s, table=%s", userIdentity, tablePath);

        String normalizedUser = normalizeUserIdentity(userIdentity);

        List<DatasetCacheKey> matchingKeys = datasetCache.asMap().keySet().stream()
                .filter(key -> Objects.equals(key.userIdentity, normalizedUser) &&
                        Objects.equals(key.tablePath, tablePath))
                .toList();
        datasetCache.invalidateAll(matchingKeys);
    }

    // ================== Scanner Operations ==================

    LanceScanner openDatasetScanner(
            DatasetLease datasetLease,
            List<Integer> fragmentIds,
            ScanOptions scanOptions)
    {
        ScanOptions.Builder scanBuilder = new ScanOptions.Builder(scanOptions)
                .fragmentIds(fragmentIds);

        return datasetLease.getDataset().newScan(scanBuilder.build());
    }

    // ================== Lifecycle ==================

    @Override
    @PreDestroy
    public void close()
    {
        log.info("Closing LanceRuntime: %d sessions, %d datasets", sessionCache.size(), datasetCache.size());

        // Retire currently cached datasets before closing sessions they depend on.
        datasetCache.invalidateAll();
        datasetCache.cleanUp();

        // Close sessions
        sessionCache.asMap().forEach((_, session) -> session.retire());
        sessionCache.invalidateAll();
        sessionCache.cleanUp();

        // Close namespace
        if (namespace instanceof Closeable closeable) {
            try {
                closeable.close();
            }
            catch (Exception e) {
                log.warn(e, "Failed to close namespace");
            }
        }

        try {
            allocator.close();
        }
        catch (Exception e) {
            log.warn(e, "Failed to close Arrow allocator");
        }
    }

    private static boolean isDatasetCacheable(Map<String, String> storageOptions)
    {
        return storageOptions == null || !storageOptions.containsKey("expires_at_millis");
    }

    private static void closeDataset(Dataset dataset, SessionLease sessionLease)
    {
        try {
            dataset.close();
        }
        catch (Exception e) {
            log.warn(e, "Failed to close dataset");
        }
        finally {
            sessionLease.close();
        }
    }

    private static final class CachedSession
    {
        private final Session session;
        private final AtomicInteger references = new AtomicInteger();
        private final AtomicBoolean retired = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        private CachedSession(Session session)
        {
            this.session = session;
        }

        private SessionLease tryAcquire()
        {
            while (true) {
                if (retired.get() || closed.get()) {
                    return null;
                }

                int currentReferences = references.get();
                if (references.compareAndSet(currentReferences, currentReferences + 1)) {
                    if (retired.get() || closed.get()) {
                        release();
                        return null;
                    }
                    return new SessionLease(session, this::release);
                }
            }
        }

        private void release()
        {
            int remainingReferences = references.decrementAndGet();
            if (remainingReferences < 0) {
                throw new IllegalStateException("Session reference count is negative");
            }
            closeIfUnused();
        }

        private void retire()
        {
            retired.set(true);
            closeIfUnused();
        }

        private void closeIfUnused()
        {
            if (retired.get() && references.get() == 0 && closed.compareAndSet(false, true) && !session.isClosed()) {
                closeSession(session);
            }
        }
    }

    private static final class CachedDataset
    {
        private final Dataset dataset;
        private final SessionLease sessionLease;
        private final AtomicInteger references = new AtomicInteger();
        private final AtomicBoolean retired = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        private CachedDataset(Dataset dataset, SessionLease sessionLease)
        {
            this.dataset = dataset;
            this.sessionLease = sessionLease;
        }

        private DatasetLease tryAcquire()
        {
            while (true) {
                if (retired.get() || closed.get()) {
                    return null;
                }

                int currentReferences = references.get();
                if (references.compareAndSet(currentReferences, currentReferences + 1)) {
                    if (retired.get() || closed.get()) {
                        release();
                        return null;
                    }
                    return new DatasetLease(dataset, this::release);
                }
            }
        }

        private void release()
        {
            int remainingReferences = references.decrementAndGet();
            if (remainingReferences < 0) {
                throw new IllegalStateException("Dataset reference count is negative");
            }
            closeIfUnused();
        }

        private void retire()
        {
            retired.set(true);
            closeIfUnused();
        }

        private void closeIfUnused()
        {
            if (retired.get() && references.get() == 0 && closed.compareAndSet(false, true)) {
                closeDataset(dataset, sessionLease);
            }
        }
    }

    static final class DatasetLease
            implements Closeable
    {
        private final Dataset dataset;
        private final Runnable release;
        private final AtomicBoolean closed = new AtomicBoolean();

        private DatasetLease(Dataset dataset, Runnable release)
        {
            this.dataset = dataset;
            this.release = release;
        }

        private static DatasetLease direct(Dataset dataset, SessionLease sessionLease)
        {
            return new DatasetLease(dataset, () -> closeDataset(dataset, sessionLease));
        }

        Dataset getDataset()
        {
            if (closed.get()) {
                throw new IllegalStateException("Dataset lease is closed");
            }
            return dataset;
        }

        @Override
        public void close()
        {
            if (closed.compareAndSet(false, true)) {
                release.run();
            }
        }
    }

    private static final class SessionLease
            implements Closeable
    {
        private final Session session;
        private final Runnable release;
        private final AtomicBoolean closed = new AtomicBoolean();

        private SessionLease(Session session, Runnable release)
        {
            this.session = session;
            this.release = release;
        }

        private Session getSession()
        {
            if (closed.get()) {
                throw new IllegalStateException("Session lease is closed");
            }
            return session;
        }

        @Override
        public void close()
        {
            if (closed.compareAndSet(false, true)) {
                release.run();
            }
        }
    }

    private static Map<String, String> copyStorageOptions(Map<String, String> storageOptions)
    {
        if (storageOptions == null || storageOptions.isEmpty()) {
            return Map.of();
        }
        return Map.copyOf(storageOptions);
    }

    // ================== Cache Key ==================

    private static class DatasetCacheKey
    {
        private final String userIdentity;
        private final String tablePath;
        private final Long version;
        private final Map<String, String> storageOptions;

        DatasetCacheKey(String userIdentity, String tablePath, Long version, Map<String, String> storageOptions)
        {
            this.userIdentity = normalizeUserIdentity(userIdentity);
            this.tablePath = tablePath;
            this.version = version;
            this.storageOptions = copyStorageOptions(storageOptions);
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
                    Objects.equals(version, that.version) &&
                    Objects.equals(storageOptions, that.storageOptions);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(userIdentity, tablePath, version, storageOptions);
        }
    }

    private static void closeSession(Session session)
    {
        if (!session.isClosed()) {
            try {
                session.close();
            }
            catch (Exception e) {
                log.warn(e, "Failed to close session");
            }
        }
    }
}
