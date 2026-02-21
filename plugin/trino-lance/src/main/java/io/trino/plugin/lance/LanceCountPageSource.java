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
import org.lance.ManifestSummary;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.spi.type.BigintType.BIGINT;

/**
 * Page source for COUNT(*) queries without filter.
 * Returns count from ManifestSummary (no data scan needed).
 */
public class LanceCountPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(LanceCountPageSource.class);

    private final String tablePath;
    private final Long datasetVersion;
    private final Map<String, String> storageOptions;
    private final String userIdentity;
    private final LanceDatasetCache datasetCache;
    private final AtomicBoolean finished = new AtomicBoolean(false);

    public LanceCountPageSource(
            LanceTableHandle tableHandle,
            Map<String, String> storageOptions,
            String userIdentity,
            LanceDatasetCache datasetCache)
    {
        this.tablePath = tableHandle.getTablePath();
        this.datasetVersion = tableHandle.getDatasetVersion();
        this.storageOptions = storageOptions;
        this.userIdentity = userIdentity;
        this.datasetCache = datasetCache;
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
        ManifestSummary summary = datasetCache.getManifestSummary(userIdentity, tablePath, datasetVersion, storageOptions);
        log.debug("COUNT(*) returning manifest count %d", summary.getTotalRows());
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
        // Nothing to close
    }
}
