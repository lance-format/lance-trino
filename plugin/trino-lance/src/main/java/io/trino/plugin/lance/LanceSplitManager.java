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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LanceSplitManager
        implements ConnectorSplitManager
{
    private final LanceNamespaceHolder namespaceHolder;

    @Inject
    public LanceSplitManager(LanceNamespaceHolder namespaceHolder)
    {
        this.namespaceHolder = requireNonNull(namespaceHolder, "namespaceHolder is null");
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

        // Get fragments from cache and create splits for parallel processing
        // For COUNT(*) with filter, each split will count rows matching the filter
        List<Integer> fragmentIds = LanceDatasetCache.getFragments(lanceTableHandle.getTablePath(), storageOptions)
                .stream().map(Fragment::getId).toList();
        return new FixedSplitSource(fragmentIds.stream().map(id -> new LanceSplit(Collections.singletonList(id))).toList());
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
            DescribeTableResponse response = namespaceHolder.getNamespace().describeTable(request);
            Map<String, String> storageOptions = response.getStorageOptions();
            if (storageOptions != null && !storageOptions.isEmpty()) {
                return storageOptions;
            }
        }
        catch (Exception e) {
            // Fall through to namespace-level options
        }

        Map<String, String> nsOptions = namespaceHolder.getNamespaceStorageOptions();
        if (!nsOptions.isEmpty()) {
            return nsOptions;
        }

        return new HashMap<>();
    }
}
