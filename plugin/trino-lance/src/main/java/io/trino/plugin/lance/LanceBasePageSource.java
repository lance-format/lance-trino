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

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.VisibleForTesting;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

public abstract class LanceBasePageSource
        implements ConnectorPageSource
{
    private static final BufferAllocator allocator = new RootAllocator(
            RootAllocator.configBuilder().from(RootAllocator.defaultConfig()).maxAllocation(Integer.MAX_VALUE).build());

    protected final LanceTableHandle tableHandle;

    protected final AtomicLong readBytes = new AtomicLong();
    protected final AtomicBoolean isFinished = new AtomicBoolean();

    protected final LanceArrowToPageScanner lanceArrowToPageScanner;
    protected final BufferAllocator bufferAllocator;
    protected final PageBuilder pageBuilder;

    public LanceBasePageSource(LanceTableHandle tableHandle, List<LanceColumnHandle> columns, ScannerFactory scannerFactory, Map<String, String> storageOptions)
    {
        this.tableHandle = tableHandle;
        this.bufferAllocator = allocator.newChildAllocator(tableHandle.getTableName(), 1024, Long.MAX_VALUE);

        this.lanceArrowToPageScanner =
                new LanceArrowToPageScanner(
                        bufferAllocator,
                        tableHandle.getTablePath(),
                        columns,
                        scannerFactory,
                        storageOptions,
                        tableHandle.getSubstraitFilterBuffer(),
                        tableHandle.getLimit());
        this.pageBuilder =
                new PageBuilder(columns.stream().map(LanceColumnHandle::trinoType).collect(toImmutableList()));
        this.isFinished.set(false);
    }

    @VisibleForTesting
    public static List<LanceColumnHandle> toColumnHandles(LanceTableHandle tableHandle, Map<String, String> storageOptions)
    {
        return LanceDatasetCache.getColumnHandles(tableHandle.getTablePath(), storageOptions).values().stream()
                .map(c -> (LanceColumnHandle) c).collect(Collectors.toList());
    }

    @Override
    public long getCompletedBytes()
    {
        return readBytes.get();
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0L;
    }

    @Override
    public boolean isFinished()
    {
        return isFinished.get();
    }

    @Override
    public Page getNextPage()
    {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");
        if (!lanceArrowToPageScanner.read()) {
            isFinished.set(true);
            return null;
        }
        lanceArrowToPageScanner.convert(pageBuilder);
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0L;
    }

    @Override
    public void close()
    {
        lanceArrowToPageScanner.close();
        bufferAllocator.close();
    }
}
