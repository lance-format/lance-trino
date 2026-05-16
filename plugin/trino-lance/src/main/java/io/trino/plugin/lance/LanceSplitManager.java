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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import org.lance.Fragment;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LanceSplitManager
        implements ConnectorSplitManager
{
    private final LanceRuntime runtime;

    @Inject
    public LanceSplitManager(LanceRuntime runtime)
    {
        this.runtime = requireNonNull(runtime, "runtime is null");
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

        // Get all fragments
        // Use the version captured in the table handle for snapshot isolation
        List<Fragment> allFragments = runtime.getFragments(
                userIdentity, lanceTableHandle.getTablePath(), lanceTableHandle.getDatasetVersion(), storageOptions);

        // When a LIMIT is set without a filter, coalesce just enough fragments into a
        // single split instead of creating one split per fragment. Without this, each
        // fragment gets its own split and each applies the full LIMIT, reading
        // (numFragments * LIMIT) rows instead of LIMIT. For tables with large rows
        // (e.g. 165MB each), this OOMs workers.
        //
        // We accumulate per-fragment logical row counts until the LIMIT is reached.
        // Fragment.metadata().getNumRows() is deletion-aware (physicalRows -
        // numDeletions), so fragments emptied by deletion vectors contribute 0 and we
        // keep walking until enough live rows are covered, while a single large
        // fragment can satisfy a small LIMIT on its own. The counts come from the
        // manifest already loaded when the dataset was opened, so this adds no IO.
        if (lanceTableHandle.getLimit().isPresent() && !lanceTableHandle.hasFilter()) {
            long limit = lanceTableHandle.getLimit().getAsLong();
            List<Integer> ids = allFragments.stream().map(Fragment::getId).toList();
            List<Long> rowCounts = allFragments.stream()
                    .map(fragment -> fragment.metadata().getNumRows()).toList();
            List<Integer> fragmentIds = coalesceFragmentsForLimit(ids, rowCounts, limit);
            return new FixedSplitSource(List.of(new LanceSplit(fragmentIds)));
        }

        // Create one split per fragment for full scans and filtered queries
        // Lance will automatically use indexes during scanning when substrait filter is applied
        return new FixedSplitSource(allFragments.stream()
                .map(frag -> new LanceSplit(Collections.singletonList(frag.getId())))
                .toList());
    }

    /**
     * Selects the prefix of fragments whose cumulative logical (post-deletion) row
     * count covers {@code limit}, returning their fragment IDs.
     *
     * <p>{@code rowCounts} must already account for deletion vectors (e.g.
     * {@code Fragment.metadata().getNumRows()}). Fragments emptied by deletions
     * contribute 0 and are effectively skipped, so the scan still sees enough live
     * rows; conversely a single large fragment satisfies a small limit on its own.
     * The result always contains at least one fragment (when any exist) so the scan
     * is never handed an empty split.
     *
     * @param fragmentIds fragment IDs, in scan order
     * @param rowCounts   parallel list of per-fragment logical row counts
     * @param limit       the pushed-down LIMIT
     */
    static List<Integer> coalesceFragmentsForLimit(List<Integer> fragmentIds, List<Long> rowCounts, long limit)
    {
        List<Integer> selected = new ArrayList<>();
        long accumulated = 0;
        for (int i = 0; i < fragmentIds.size() && accumulated < limit; i++) {
            selected.add(fragmentIds.get(i));
            accumulated += rowCounts.get(i);
        }
        if (selected.isEmpty() && !fragmentIds.isEmpty()) {
            selected.add(fragmentIds.get(0));
        }
        return selected;
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
