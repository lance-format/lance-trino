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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.arrow.memory.BufferAllocator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;

public abstract class LanceBasePageSource
        implements ConnectorPageSource
{
    protected final LanceTableHandle tableHandle;

    protected final AtomicLong readBytes = new AtomicLong();
    protected final AtomicBoolean isFinished = new AtomicBoolean();

    protected final LanceArrowToPageScanner lanceArrowToPageScanner;
    protected final BufferAllocator bufferAllocator;
    protected final PageBuilder pageBuilder;

    public LanceBasePageSource(LanceTableHandle tableHandle, List<LanceColumnHandle> columns, ScannerFactory scannerFactory, Map<String, String> storageOptions, String userIdentity, BufferAllocator parentAllocator)
    {
        this(tableHandle, columns, List.of(), scannerFactory, storageOptions, userIdentity, parentAllocator);
    }

    public LanceBasePageSource(LanceTableHandle tableHandle, List<LanceColumnHandle> columns, List<String> filterProjectionColumns, ScannerFactory scannerFactory, Map<String, String> storageOptions, String userIdentity, BufferAllocator allocator)
    {
        this.tableHandle = tableHandle;
        this.bufferAllocator = allocator;

        try {
            this.lanceArrowToPageScanner =
                    new LanceArrowToPageScanner(
                            bufferAllocator,
                            tableHandle.getTablePath(),
                            columns,
                            filterProjectionColumns,
                            scannerFactory,
                            storageOptions,
                            tableHandle.getSubstraitFilterBuffer(),
                            tableHandle.getLimit(),
                            userIdentity,
                            tableHandle.getDatasetVersion());
        }
        catch (RuntimeException e) {
            // Handle concurrent modification errors (e.g., fragment not found due to concurrent update)
            if (isConcurrentModificationError(e)) {
                throw new TrinoException(TRANSACTION_CONFLICT, "Concurrent modification detected", e);
            }
            throw e;
        }
        this.pageBuilder =
                new PageBuilder(columns.stream().map(LanceColumnHandle::trinoType).collect(toImmutableList()));
        this.isFinished.set(false);
    }

    private static boolean isConcurrentModificationError(RuntimeException e)
    {
        Throwable current = e;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && (
                    message.toLowerCase().contains("not found") ||
                    message.toLowerCase().contains("concurrent") ||
                    message.toLowerCase().contains("conflict"))) {
                return true;
            }
            if (current instanceof NullPointerException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
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
        // Track bytes read from Arrow buffers
        readBytes.addAndGet(lanceArrowToPageScanner.getLastBatchBytes());
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
        // Don't close the allocator - it's the shared LanceRuntime allocator
    }
}
