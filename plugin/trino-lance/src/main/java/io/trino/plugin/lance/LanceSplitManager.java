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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.index.Index;
import org.lance.index.IndexType;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class LanceSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(LanceSplitManager.class);

    // Index types that can accelerate equality filter predicates
    private static final Set<IndexType> FILTER_ACCELERATING_INDEX_TYPES = Set.of(
            IndexType.BTREE,
            IndexType.BITMAP);

    private final LanceRuntime runtime;
    private final long btreeRowsPerSplit;
    private final long bitmapRowsPerSplit;

    @Inject
    public LanceSplitManager(LanceRuntime runtime, LanceConfig config)
    {
        this.runtime = requireNonNull(runtime, "runtime is null");
        this.btreeRowsPerSplit = config.getBtreeIndexedRowsPerSplit();
        this.bitmapRowsPerSplit = config.getBitmapIndexedRowsPerSplit();
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
            ConnectorTableHandle tableHandle, DynamicFilter dynamicFilter, Constraint constraint)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;

        // For COUNT(*) without filter, return a single empty split
        // The count will be retrieved from ManifestSummary without scanning data
        if (lanceTableHandle.isCountStar() && !lanceTableHandle.hasFilter()) {
            return new FixedSplitSource(List.of(new LanceSplit(Collections.emptyList())));
        }

        // Use storage options from handle, refreshing if expired
        Map<String, String> storageOptions = getEffectiveStorageOptions(lanceTableHandle);
        String userIdentity = session.getUser();

        // Get all fragments (need full Fragment objects for row counts)
        // Use the version captured in the table handle for snapshot isolation
        List<Fragment> allFragments = runtime.getFragments(
                userIdentity, lanceTableHandle.getTablePath(), lanceTableHandle.getDatasetVersion(), storageOptions);
        List<Integer> allFragmentIds = allFragments.stream().map(Fragment::getId).toList();

        // Check if we can use index-aware split planning
        // Only equality predicates can benefit from btree/bitmap index acceleration
        if (lanceTableHandle.hasFilter() && !lanceTableHandle.getEqualityFilterColumns().isEmpty()) {
            Optional<IndexAwareSplits> indexSplits = createIndexAwareSplits(
                    userIdentity,
                    lanceTableHandle.getTablePath(),
                    lanceTableHandle.getDatasetVersion(),
                    storageOptions,
                    lanceTableHandle.getEqualityFilterColumns(),
                    allFragments);

            if (indexSplits.isPresent()) {
                List<ConnectorSplit> splits = indexSplits.get().toSplits();
                log.info("Using index-aware splits for table %s: %d indexed fragments grouped into %d splits (threshold %d rows), %d unindexed fragments",
                        lanceTableHandle.getTableName(),
                        indexSplits.get().indexedFragmentCount(),
                        indexSplits.get().indexedSplitCount(),
                        indexSplits.get().rowsPerSplit(),
                        indexSplits.get().unindexedFragmentCount());
                return new FixedSplitSource(splits);
            }
        }

        // Default: create one split per fragment
        return new FixedSplitSource(allFragmentIds.stream()
                .map(id -> new LanceSplit(Collections.singletonList(id)))
                .toList());
    }

    /**
     * Create index-aware splits if a matching index covers the equality filter columns.
     * Groups indexed fragments by row count threshold based on index type.
     */
    private Optional<IndexAwareSplits> createIndexAwareSplits(
            String userIdentity,
            String tablePath,
            Long version,
            Map<String, String> storageOptions,
            List<String> equalityFilterColumns,
            List<Fragment> allFragments)
    {
        // Use cached dataset for index checking (read-only operation)
        try {
            Dataset dataset = runtime.getDataset(userIdentity, tablePath, version, storageOptions);
            // Build field ID to name mapping
            LanceSchema schema = dataset.getLanceSchema();
            Map<Integer, String> fieldIdToName = new HashMap<>();
            for (LanceField field : schema.fields()) {
                fieldIdToName.put(field.getId(), field.getName());
            }

            // Find a matching index that covers any equality filter column
            List<Index> indexes = dataset.getIndexes();
            for (Index index : indexes) {
                if (!FILTER_ACCELERATING_INDEX_TYPES.contains(index.indexType())) {
                    continue;
                }

                // Check if this index covers any equality filter column
                for (Integer fieldId : index.fields()) {
                    String fieldName = fieldIdToName.get(fieldId);
                    if (fieldName != null && equalityFilterColumns.contains(fieldName)) {
                        // Found a matching index - get indexed fragments
                        Optional<List<Integer>> indexedFragmentsOpt = index.fragments();
                        if (indexedFragmentsOpt.isPresent() && !indexedFragmentsOpt.get().isEmpty()) {
                            Set<Integer> indexedSet = new HashSet<>(indexedFragmentsOpt.get());

                            // Separate indexed and unindexed fragments, keeping Fragment objects for row counts
                            List<Fragment> indexedFragments = new ArrayList<>();
                            List<Fragment> unindexedFragments = new ArrayList<>();

                            for (Fragment frag : allFragments) {
                                if (indexedSet.contains(frag.getId())) {
                                    indexedFragments.add(frag);
                                }
                                else {
                                    unindexedFragments.add(frag);
                                }
                            }

                            // Determine row threshold based on index type
                            long rowsPerSplit = (index.indexType() == IndexType.BTREE)
                                    ? btreeRowsPerSplit
                                    : bitmapRowsPerSplit;

                            log.debug("Index '%s' (type=%s) on column '%s' covers %d/%d fragments, using %d rows per split",
                                    index.name(), index.indexType(), fieldName,
                                    indexedFragments.size(), allFragments.size(), rowsPerSplit);

                            return Optional.of(new IndexAwareSplits(
                                    index.name(),
                                    index.indexType(),
                                    indexedFragments,
                                    unindexedFragments,
                                    rowsPerSplit));
                        }
                    }
                }
            }
            // Don't close the cached dataset - it will be managed by the cache
        }
        catch (Exception e) {
            log.warn("Failed to check indexes for table %s: %s", tablePath, e.getMessage());
        }

        return Optional.empty();
    }

    /**
     * Represents index-aware split grouping based on row count thresholds.
     */
    private static class IndexAwareSplits
    {
        private final String indexName;
        private final IndexType indexType;
        private final List<Fragment> indexedFragments;
        private final List<Fragment> unindexedFragments;
        private final long rowsPerSplit;

        IndexAwareSplits(
                String indexName,
                IndexType indexType,
                List<Fragment> indexedFragments,
                List<Fragment> unindexedFragments,
                long rowsPerSplit)
        {
            this.indexName = indexName;
            this.indexType = indexType;
            this.indexedFragments = indexedFragments;
            this.unindexedFragments = unindexedFragments;
            this.rowsPerSplit = rowsPerSplit;
        }

        int indexedFragmentCount()
        {
            return indexedFragments.size();
        }

        int unindexedFragmentCount()
        {
            return unindexedFragments.size();
        }

        long rowsPerSplit()
        {
            return rowsPerSplit;
        }

        int indexedSplitCount()
        {
            // Count how many splits we'll create for indexed fragments
            int count = 0;
            long currentRows = 0;
            for (Fragment frag : indexedFragments) {
                if (currentRows > 0 && currentRows + frag.countRows() > rowsPerSplit) {
                    count++;
                    currentRows = 0;
                }
                currentRows += frag.countRows();
            }
            if (currentRows > 0) {
                count++;
            }
            return count;
        }

        List<ConnectorSplit> toSplits()
        {
            List<ConnectorSplit> splits = new ArrayList<>();

            // Group indexed fragments by row count threshold
            // Lance will automatically use the index when scanning with substrait filter
            if (!indexedFragments.isEmpty()) {
                List<Integer> currentBatch = new ArrayList<>();
                long currentRows = 0;

                for (Fragment frag : indexedFragments) {
                    int fragRows = frag.countRows();

                    // If adding this fragment would exceed threshold, flush current batch
                    if (currentRows > 0 && currentRows + fragRows > rowsPerSplit) {
                        splits.add(new LanceSplit(new ArrayList<>(currentBatch)));
                        currentBatch.clear();
                        currentRows = 0;
                    }

                    currentBatch.add(frag.getId());
                    currentRows += fragRows;
                }

                // Flush remaining fragments
                if (!currentBatch.isEmpty()) {
                    splits.add(new LanceSplit(currentBatch));
                }
            }

            // Create individual splits for unindexed fragments
            for (Fragment frag : unindexedFragments) {
                splits.add(new LanceSplit(Collections.singletonList(frag.getId())));
            }

            return splits;
        }
    }

    /**
     * Get effective storage options from the table handle, refreshing if expired.
     */
    private Map<String, String> getEffectiveStorageOptions(LanceTableHandle handle)
    {
        // If handle has storage options and they're not expired, use them directly
        if (!handle.getStorageOptions().isEmpty() && !handle.isStorageOptionsExpired()) {
            return handle.getStorageOptions();
        }
        // Otherwise, refresh from the namespace
        return refreshStorageOptions(handle.getTableId());
    }

    private Map<String, String> refreshStorageOptions(List<String> tableId)
    {
        try {
            DescribeTableRequest request = new DescribeTableRequest().id(tableId);
            DescribeTableResponse response = runtime.getNamespace().describeTable(request);
            Map<String, String> storageOptions = response.getStorageOptions();
            if (storageOptions != null && !storageOptions.isEmpty()) {
                return storageOptions;
            }
        }
        catch (Exception e) {
            // Fall through to namespace-level options
        }

        Map<String, String> nsOptions = runtime.getNamespaceStorageOptions();
        if (!nsOptions.isEmpty()) {
            return nsOptions;
        }

        return new HashMap<>();
    }
}
