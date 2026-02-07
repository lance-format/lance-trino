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
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import org.lance.Fragment;
import org.lance.ManifestSummary;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.spi.type.BigintType.BIGINT;

/**
 * Page source for COUNT(*) queries.
 * - Without filter: returns count from ManifestSummary (no data scan)
 * - With filter: counts rows matching the filter for the given fragment
 */
public class LanceCountPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(LanceCountPageSource.class);

    private final LanceTableHandle tableHandle;
    private final Map<String, String> storageOptions;
    private final List<Integer> fragments;
    private final AtomicBoolean finished = new AtomicBoolean(false);

    public LanceCountPageSource(
            LanceTableHandle tableHandle,
            Map<String, String> storageOptions,
            List<Integer> fragments)
    {
        this.tableHandle = tableHandle;
        this.storageOptions = storageOptions;
        this.fragments = fragments;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished.get();
    }

    @Override
    public Page getNextPage()
    {
        if (finished.get()) {
            return null;
        }
        finished.set(true);

        long count = computeCount();

        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 1);
        BIGINT.writeLong(blockBuilder, count);

        return new Page(blockBuilder.build());
    }

    private long computeCount()
    {
        if (!tableHandle.hasFilter() && fragments.isEmpty()) {
            // No filter and no fragments: use manifest summary for exact count
            return getManifestCount();
        }

        // With filter or specific fragment: use fragment scanner with countRows()
        if (fragments.isEmpty()) {
            throw new IllegalStateException("Expected fragments for filtered COUNT(*)");
        }

        // Each split has exactly one fragment
        int fragmentId = fragments.getFirst();
        Fragment fragment = LanceDatasetCache.getFragment(tableHandle.getTablePath(), fragmentId, storageOptions);
        if (fragment == null) {
            throw new RuntimeException("Fragment not found: " + fragmentId);
        }

        ScanOptions.Builder optionsBuilder = new ScanOptions.Builder();
        // Don't select any columns for COUNT(*) - just count rows
        // withRowId is required for countRows() to work with empty columns
        optionsBuilder.columns(java.util.Collections.emptyList());
        optionsBuilder.withRowId(true);
        tableHandle.getSubstraitFilterBuffer().ifPresent(optionsBuilder::substraitFilter);

        LanceScanner scanner = fragment.newScan(optionsBuilder.build());
        try {
            long count = scanner.countRows();
            log.debug("COUNT(*) for fragment %d: %d rows (hasFilter=%s)",
                    fragmentId, count, tableHandle.hasFilter());
            return count;
        }
        finally {
            try {
                scanner.close();
            }
            catch (Exception e) {
                log.warn(e, "Failed to close scanner");
            }
        }
    }

    private long getManifestCount()
    {
        ManifestSummary summary = LanceDatasetCache.getManifestSummary(
                tableHandle.getTablePath(), storageOptions);
        log.debug("COUNT(*) without filter: returning manifest count %d", summary.getTotalRows());
        return summary.getTotalRows();
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        // Nothing to close - resources are closed in computeCount
    }
}
