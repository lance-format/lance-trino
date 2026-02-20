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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class LancePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final LanceNamespaceHolder namespaceHolder;
    private final LanceConfig lanceConfig;

    @Inject
    public LancePageSourceProvider(LanceNamespaceHolder namespaceHolder, LanceConfig lanceConfig)
    {
        this.namespaceHolder = requireNonNull(namespaceHolder, "namespaceHolder is null");
        this.lanceConfig = requireNonNull(lanceConfig, "lanceConfig is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session,
            ConnectorSplit split, ConnectorTableHandle tableHandle, List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");
        LanceSplit lanceSplit = (LanceSplit) split;
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        List<LanceColumnHandle> lanceColumns = columns.stream()
                .map(LanceColumnHandle.class::cast)
                .toList();

        // Use storage options from handle, refreshing if expired
        Map<String, String> storageOptions = getEffectiveStorageOptions(lanceTableHandle);

        // For COUNT(*) queries, use the count page source
        if (lanceTableHandle.isCountStar()) {
            return new LanceCountPageSource(lanceTableHandle, storageOptions);
        }

        // Get additional projection columns for filter pushdown (column names only, not for output conversion)
        List<String> filterProjectionColumns = getFilterProjectionColumns(lanceTableHandle, lanceColumns);

        // Create page source - split may contain one or more fragments
        return new LanceFragmentPageSource(
                lanceTableHandle,
                lanceColumns,
                filterProjectionColumns,
                lanceSplit.getFragments(),
                storageOptions,
                lanceConfig.getReadBatchSize());
    }

    /**
     * Get additional columns needed for filter evaluation that aren't in the output.
     * These columns will be projected by Lance for filter pushdown but won't be converted to output.
     *
     * @return List of column names needed for filtering but not in output columns
     */
    private List<String> getFilterProjectionColumns(
            LanceTableHandle tableHandle,
            List<LanceColumnHandle> outputColumns)
    {
        List<String> filterColumns = tableHandle.getFilterColumns();
        if (filterColumns.isEmpty()) {
            return List.of();
        }

        // Get output column names
        Set<String> outputColumnNames = new HashSet<>();
        for (LanceColumnHandle col : outputColumns) {
            outputColumnNames.add(col.name());
        }

        // Return filter columns that aren't already in output
        List<String> result = new ArrayList<>();
        Set<String> addedColumns = new HashSet<>();
        for (String filterCol : filterColumns) {
            if (!outputColumnNames.contains(filterCol) && !addedColumns.contains(filterCol)) {
                result.add(filterCol);
                addedColumns.add(filterCol);
            }
        }

        return result;
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
