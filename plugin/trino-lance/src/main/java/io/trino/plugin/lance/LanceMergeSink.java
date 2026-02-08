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
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.connector.ConnectorMergeSink.DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_DELETE_OPERATION_NUMBER;
import static io.trino.spi.connector.ConnectorMergeSink.UPDATE_INSERT_OPERATION_NUMBER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Merge sink for Lance connector.
 * Handles DELETE, UPDATE, and MERGE operations using merge-on-read pattern.
 *
 * Updates and deletes are tracked as deletion vectors per fragment.
 * New rows (from INSERT or UPDATE) are written as new fragments.
 */
public class LanceMergeSink
        implements ConnectorMergeSink
{
    private static final Logger log = Logger.get(LanceMergeSink.class);

    private final LanceMergeTableHandle mergeHandle;
    private final LancePageSink insertPageSink;
    private final JsonCodec<LanceMergeCommitData> commitDataCodec;

    private final Map<Integer, List<Integer>> deletedRows = new HashMap<>();

    private long rowsDeleted;
    private long rowsInserted;

    public LanceMergeSink(
            LanceMergeTableHandle mergeHandle,
            Schema arrowSchema,
            JsonCodec<LanceCommitTaskData> insertCodec,
            JsonCodec<LanceMergeCommitData> commitDataCodec,
            LanceNamespaceHolder namespaceHolder)
    {
        this.mergeHandle = requireNonNull(mergeHandle, "mergeHandle is null");
        this.commitDataCodec = requireNonNull(commitDataCodec, "commitDataCodec is null");

        this.insertPageSink = new LancePageSink(
                mergeHandle.getTablePath(),
                arrowSchema,
                mergeHandle.inputColumns(),
                insertCodec,
                namespaceHolder.getNamespace(),
                mergeHandle.getTableId(),
                mergeHandle.getStorageOptions());
    }

    @Override
    public void storeMergedRows(Page page)
    {
        int channelCount = page.getChannelCount();
        if (channelCount < 3) {
            throw new IllegalArgumentException("Merge page must have at least 3 channels (operation + caseNumber + rowId)");
        }

        // Page structure per ConnectorMergeSink javadoc:
        // - Blocks 0..n-4 are data columns
        // - Block n-3 is the tinyint operation
        // - Block n-2 is the integer merge case number
        // - Block n-1 is the rowId column
        int operationChannel = channelCount - 3;
        int rowIdChannel = channelCount - 1;
        int dataColumnCount = channelCount - 3;

        Block operationBlock = page.getBlock(operationChannel);
        Block rowIdBlock = page.getBlock(rowIdChannel);

        List<Integer> deletePositions = new ArrayList<>();
        List<Integer> insertPositions = new ArrayList<>();

        for (int position = 0; position < page.getPositionCount(); position++) {
            if (operationBlock.isNull(position)) {
                continue;
            }
            byte operation = TINYINT.getByte(operationBlock, position);

            switch (operation) {
                case DELETE_OPERATION_NUMBER, UPDATE_DELETE_OPERATION_NUMBER -> {
                    if (!rowIdBlock.isNull(position)) {
                        long rowAddress = BIGINT.getLong(rowIdBlock, position);
                        int fragmentId = RowAddress.fragmentId(rowAddress);
                        int rowIndex = RowAddress.rowIndex(rowAddress);
                        log.debug("DELETE: rowAddress=%d, fragmentId=%d, rowIndex=%d", rowAddress, fragmentId, rowIndex);
                        deletedRows.computeIfAbsent(fragmentId, k -> new ArrayList<>())
                                .add(rowIndex);
                        deletePositions.add(position);
                        rowsDeleted++;
                    }
                }
                case INSERT_OPERATION_NUMBER, UPDATE_INSERT_OPERATION_NUMBER -> {
                    insertPositions.add(position);
                    rowsInserted++;
                }
                default -> {
                    log.warn("Unknown merge operation: %d", operation);
                }
            }
        }

        if (!insertPositions.isEmpty()) {
            Page insertPage = extractDataPage(page, insertPositions, dataColumnCount);
            insertPageSink.appendPage(insertPage);
        }

        log.debug("storeMergedRows: processed %d rows, deleted=%d, inserted=%d",
                page.getPositionCount(), deletePositions.size(), insertPositions.size());
    }

    /**
     * Extract data columns from a page, selecting only the specified positions.
     */
    private Page extractDataPage(Page page, List<Integer> positions, int dataColumnCount)
    {
        int[] positionsArray = positions.stream().mapToInt(Integer::intValue).toArray();
        Block[] dataBlocks = new Block[dataColumnCount];

        for (int channel = 0; channel < dataColumnCount; channel++) {
            dataBlocks[channel] = page.getBlock(channel).getPositions(positionsArray, 0, positionsArray.length);
        }

        return new Page(positionsArray.length, dataBlocks);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        log.debug("finish: completing merge with %d deletions across %d fragments, %d inserts",
                rowsDeleted, deletedRows.size(), rowsInserted);

        Collection<Slice> insertResults = insertPageSink.finish().join();

        List<FragmentDeletion> deletions = deletedRows.entrySet().stream()
                .map(e -> new FragmentDeletion(e.getKey(), e.getValue()))
                .toList();

        List<String> newFragmentsJson = collectFragmentsJson(insertResults);

        LanceMergeCommitData commitData = new LanceMergeCommitData(
                deletions,
                newFragmentsJson,
                0L,
                rowsInserted);

        Slice slice = wrappedBuffer(commitDataCodec.toJsonBytes(commitData));
        return completedFuture(ImmutableList.of(slice));
    }

    private List<String> collectFragmentsJson(Collection<Slice> insertResults)
    {
        List<String> allFragmentsJson = new ArrayList<>();
        JsonCodec<LanceCommitTaskData> insertCodec = JsonCodec.jsonCodec(LanceCommitTaskData.class);

        for (Slice slice : insertResults) {
            LanceCommitTaskData insertData = insertCodec.fromJson(slice.getBytes());
            allFragmentsJson.addAll(insertData.getFragmentsJson());
        }

        return allFragmentsJson;
    }

    @Override
    public void abort()
    {
        insertPageSink.abort();
        deletedRows.clear();
    }
}
