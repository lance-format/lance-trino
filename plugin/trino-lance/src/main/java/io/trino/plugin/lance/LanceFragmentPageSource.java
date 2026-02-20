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

import io.airlift.log.Logger;
import org.apache.arrow.memory.BufferAllocator;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.lance.RowAddress.LANCE_ROW_ADDRESS;
import static io.trino.spi.type.BigintType.BIGINT;

public class LanceFragmentPageSource
        extends LanceBasePageSource
{
    private static final Logger log = Logger.get(LanceFragmentPageSource.class);
    private static final String LANCE_INTERNAL_ROW_ADDRESS = "_rowaddr";

    public LanceFragmentPageSource(LanceTableHandle tableHandle, List<LanceColumnHandle> columns, List<Integer> fragments, Map<String, String> storageOptions, int readBatchSize)
    {
        this(tableHandle, columns, List.of(), fragments, storageOptions, readBatchSize);
    }

    public LanceFragmentPageSource(LanceTableHandle tableHandle, List<LanceColumnHandle> columns, List<String> filterProjectionColumns, List<Integer> fragments, Map<String, String> storageOptions, int readBatchSize)
    {
        super(tableHandle, prepareColumns(columns), filterProjectionColumns, createScannerFactory(fragments, hasRowAddressColumn(columns), readBatchSize), storageOptions);
    }

    /**
     * Prepare columns for scanning by mapping $row_address to _rowaddr.
     * Lance uses _rowaddr as the internal name for row addresses when withRowAddress(true) is set.
     */
    private static List<LanceColumnHandle> prepareColumns(List<LanceColumnHandle> columns)
    {
        List<LanceColumnHandle> result = new ArrayList<>(columns.size());
        for (LanceColumnHandle col : columns) {
            if (LANCE_ROW_ADDRESS.equals(col.name())) {
                // Map $row_address to _rowaddr (Lance's internal row address column name)
                result.add(new LanceColumnHandle(LANCE_INTERNAL_ROW_ADDRESS, BIGINT, false, -1));
            }
            else {
                result.add(col);
            }
        }
        return result;
    }

    private static boolean hasRowAddressColumn(List<LanceColumnHandle> columns)
    {
        return columns.stream().anyMatch(col -> LANCE_ROW_ADDRESS.equals(col.name()));
    }

    private static ScannerFactory createScannerFactory(List<Integer> fragments, boolean includeRowAddress, int readBatchSize)
    {
        return new FragmentScannerFactory(fragments, includeRowAddress, readBatchSize);
    }

    /**
     * Scanner factory that uses Dataset.scan() with fragment filtering.
     * This ensures deletion vectors from the manifest are properly applied.
     * When includeRowAddress is true, uses Lance's withRowAddress feature to get actual row addresses.
     * Supports scanning multiple fragments together for efficient index utilization.
     */
    public static class FragmentScannerFactory
            implements ScannerFactory
    {
        private final List<Integer> fragmentIds;
        private final boolean includeRowAddress;
        private final int readBatchSize;
        private Dataset lanceDataset;
        private LanceScanner lanceScanner;

        public FragmentScannerFactory(List<Integer> fragmentIds, boolean includeRowAddress, int readBatchSize)
        {
            this.fragmentIds = fragmentIds;
            this.includeRowAddress = includeRowAddress;
            this.readBatchSize = readBatchSize;
        }

        @Override
        public LanceScanner open(String tablePath, BufferAllocator allocator, List<String> columns,
                Map<String, String> storageOptions, Optional<ByteBuffer> substraitFilter, OptionalLong limit)
        {
            ScanOptions.Builder optionsBuilder = new ScanOptions.Builder();
            if (!columns.isEmpty()) {
                optionsBuilder.columns(columns);
            }
            optionsBuilder.batchSize(readBatchSize);
            substraitFilter.ifPresent(optionsBuilder::substraitFilter);
            limit.ifPresent(optionsBuilder::limit);

            // Use Lance's built-in row address for accurate row identification in MERGE operations
            if (includeRowAddress) {
                optionsBuilder.withRowAddress(true);
            }

            log.debug("Opening dataset scanner for %d fragments with batchSize: %d, substraitFilter: %s, limit: %s, withRowAddress: %s",
                    fragmentIds.size(),
                    readBatchSize,
                    substraitFilter.isPresent() ? "present" : "none",
                    limit.isPresent() ? limit.getAsLong() : "none",
                    includeRowAddress);

            // Use dataset-level scan with fragment filtering to respect deletion vectors
            // When scanning multiple fragments with a substrait filter, Lance will automatically
            // use scalar indexes (btree, bitmap) if they cover the filter columns
            Object[] result = LanceDatasetCache.openDatasetScanner(
                    tablePath, fragmentIds, optionsBuilder.build(), storageOptions);
            this.lanceDataset = (Dataset) result[0];
            this.lanceScanner = (LanceScanner) result[1];
            return lanceScanner;
        }

        @Override
        public void close()
        {
            try {
                if (lanceScanner != null) {
                    lanceScanner.close();
                }
            }
            catch (Exception e) {
                log.warn("error while closing lance scanner, Exception: %s", e.getMessage());
            }
            try {
                if (lanceDataset != null) {
                    lanceDataset.close();
                }
            }
            catch (Exception e) {
                log.warn("error while closing lance dataset, Exception: %s", e.getMessage());
            }
        }
    }
}
