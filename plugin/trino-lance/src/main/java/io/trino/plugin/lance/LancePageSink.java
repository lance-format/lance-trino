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

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.LanceNamespaceStorageOptionsProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * ConnectorPageSink implementation for Lance.
 * Receives Trino Pages, converts them to Arrow format, and writes them as Lance fragments.
 * Supports namespace-aware storage options for credential vending.
 */
public class LancePageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(LancePageSink.class);

    private final String datasetUri;
    private final Schema arrowSchema;
    private final List<Type> columnTypes;
    private final JsonCodec<LanceCommitTaskData> jsonCodec;
    private final BufferAllocator allocator;
    private final LanceNamespace namespace;
    private final List<String> tableId;
    private final Map<String, String> configuredStorageOptions;

    private final List<Page> bufferedPages = new ArrayList<>();
    private long writtenBytes;
    private long rowCount;
    private boolean finished;

    public LancePageSink(
            String datasetUri,
            Schema arrowSchema,
            List<LanceColumnHandle> columns,
            JsonCodec<LanceCommitTaskData> jsonCodec,
            LanceNamespace namespace,
            List<String> tableId,
            Map<String, String> configuredStorageOptions,
            BufferAllocator parentAllocator)
    {
        this.datasetUri = requireNonNull(datasetUri, "datasetUri is null");
        this.arrowSchema = requireNonNull(arrowSchema, "arrowSchema is null");
        this.columnTypes = columns.stream()
                .map(LanceColumnHandle::trinoType)
                .collect(toImmutableList());
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.namespace = requireNonNull(namespace, "namespace is null");
        this.tableId = requireNonNull(tableId, "tableId is null");
        this.configuredStorageOptions = requireNonNull(configuredStorageOptions, "configuredStorageOptions is null");
        this.allocator = parentAllocator.newChildAllocator("page-sink", 0, Long.MAX_VALUE);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (finished) {
            throw new IllegalStateException("PageSink already finished");
        }

        bufferedPages.add(page);
        rowCount += page.getPositionCount();

        // Estimate written bytes based on page size
        writtenBytes += page.getSizeInBytes();

        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (finished) {
            throw new IllegalStateException("PageSink already finished");
        }
        finished = true;

        try {
            List<String> fragmentsJson;

            if (bufferedPages.isEmpty()) {
                // No data to write
                fragmentsJson = ImmutableList.of();
            }
            else {
                // Create fragments from buffered pages
                fragmentsJson = writeFragments();
            }

            LanceCommitTaskData commitData = new LanceCommitTaskData(
                    fragmentsJson,
                    writtenBytes,
                    rowCount);

            Slice slice = wrappedBuffer(jsonCodec.toJsonBytes(commitData));
            return completedFuture(ImmutableList.of(slice));
        }
        catch (Exception e) {
            log.error(e, "Failed to finish page sink for dataset: %s", datasetUri);
            throw new RuntimeException("Failed to write Lance fragments", e);
        }
        finally {
            cleanup();
        }
    }

    private List<String> writeFragments()
    {
        log.debug("Writing %d pages (%d rows) to dataset: %s", bufferedPages.size(), rowCount, datasetUri);

        // Create a combined VectorSchemaRoot from all pages
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            // Calculate total rows
            int totalRows = bufferedPages.stream()
                    .mapToInt(Page::getPositionCount)
                    .sum();

            // Allocate vectors for all rows
            root.allocateNew();

            // Copy all pages into the VectorSchemaRoot
            int currentOffset = 0;
            for (Page page : bufferedPages) {
                int pageRows = page.getPositionCount();
                copyPageToVectors(page, root, currentOffset);
                currentOffset += pageRows;
            }
            root.setRowCount(totalRows);

            // Use storage options from the handle (already merged with namespace options)
            Map<String, String> storageOptions = configuredStorageOptions;
            log.debug("Using storage options for table %s: %s", tableId, storageOptions);

            // Write fragments using Lance API
            // Use storageOptionsProvider only if credentials have expiration (expires_at_millis)
            // Otherwise use static storage_options directly
            var fragmentWriter = Fragment.write()
                    .datasetUri(datasetUri)
                    .allocator(allocator)
                    .data(root);

            if (storageOptions != null && !storageOptions.isEmpty()) {
                if (storageOptions.containsKey("expires_at_millis")) {
                    // Credentials have expiration - use provider for auto-refresh
                    LanceNamespaceStorageOptionsProvider storageOptionsProvider =
                            new LanceNamespaceStorageOptionsProvider(namespace, tableId);
                    fragmentWriter = fragmentWriter
                            .storageOptions(storageOptions)
                            .storageOptionsProvider(storageOptionsProvider);
                }
                else {
                    // Static credentials - use storage options directly without provider
                    fragmentWriter = fragmentWriter.storageOptions(storageOptions);
                }
            }

            List<FragmentMetadata> fragments = fragmentWriter.execute();

            return LanceMetadata.serializeFragments(fragments);
        }
    }

    /**
     * Copy a Trino Page into a VectorSchemaRoot at the specified offset.
     */
    private void copyPageToVectors(Page page, VectorSchemaRoot root, int offset)
    {
        int pageRows = page.getPositionCount();

        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            LancePageToArrowConverter.writeBlockToVectorAtOffset(
                    page.getBlock(channel),
                    root.getVector(channel),
                    columnTypes.get(channel),
                    pageRows,
                    offset);
        }
    }

    @Override
    public void abort()
    {
        cleanup();
    }

    private void cleanup()
    {
        bufferedPages.clear();
        try {
            allocator.close();
        }
        catch (Exception e) {
            log.warn(e, "Failed to close allocator");
        }
    }
}
