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
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.lance.Dataset;
import org.lance.ReadOptions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Page source for aggregate queries that executes SQL via DataFusion.
 */
public class LanceAggregatePageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(LanceAggregatePageSource.class);
    private static final BufferAllocator rootAllocator = new RootAllocator(
            RootAllocator.configBuilder().from(RootAllocator.defaultConfig()).maxAllocation(Integer.MAX_VALUE).build());

    private final LanceTableHandle tableHandle;
    private final List<LanceColumnHandle> columns;
    private final Map<String, String> storageOptions;
    private final BufferAllocator bufferAllocator;
    private final PageBuilder pageBuilder;
    private final AtomicBoolean finished = new AtomicBoolean(false);

    private Dataset dataset;
    private ArrowReader arrowReader;
    private VectorSchemaRoot vectorSchemaRoot;

    public LanceAggregatePageSource(
            LanceTableHandle tableHandle,
            List<LanceColumnHandle> columns,
            Map<String, String> storageOptions)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.storageOptions = requireNonNull(storageOptions, "storageOptions is null");
        this.bufferAllocator = rootAllocator.newChildAllocator(
                "aggregate-" + tableHandle.getTableName(), 1024, Long.MAX_VALUE);
        this.pageBuilder = new PageBuilder(columns.stream().map(LanceColumnHandle::trinoType).collect(toImmutableList()));

        initializeReader();
    }

    private void initializeReader()
    {
        try {
            String sql = tableHandle.getAggregateSql()
                    .orElseThrow(() -> new IllegalStateException("No aggregate SQL in table handle"));

            log.debug("Executing aggregate SQL: %s", sql);

            ReadOptions.Builder optionsBuilder = new ReadOptions.Builder();
            if (storageOptions != null && !storageOptions.isEmpty()) {
                optionsBuilder.setStorageOptions(storageOptions);
            }

            dataset = Dataset.open(tableHandle.getTablePath(), optionsBuilder.build());
            arrowReader = dataset.sql(sql).intoBatchRecords();
            vectorSchemaRoot = arrowReader.getVectorSchemaRoot();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to execute aggregate query", e);
        }
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

        try {
            if (!arrowReader.loadNextBatch()) {
                finished.set(true);
                return null;
            }

            // Convert Arrow batch to Trino page
            pageBuilder.reset();
            int rowCount = vectorSchemaRoot.getRowCount();
            pageBuilder.declarePositions(rowCount);

            for (int col = 0; col < columns.size(); col++) {
                LanceColumnHandle column = columns.get(col);
                FieldVector fieldVector = vectorSchemaRoot.getVector(column.name());
                convertType(pageBuilder.getBlockBuilder(col), column.trinoType(), fieldVector);
            }

            vectorSchemaRoot.clear();
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read aggregate result", e);
        }
    }

    private void convertType(BlockBuilder output, Type type, FieldVector vector)
    {
        Class<?> javaType = type.getJavaType();
        int length = vector.getValueCount();

        try {
            if (javaType == boolean.class) {
                writeVectorValues(output, vector, index -> type.writeBoolean(output, ((BitVector) vector).get(index) == 1), length);
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, ((BigIntVector) vector).get(index)), length);
                }
                else if (type.equals(INTEGER)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, ((IntVector) vector).get(index)), length);
                }
                else if (type.equals(REAL)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, Float.floatToIntBits(((Float4Vector) vector).get(index))), length);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled long type: %s", type));
                }
            }
            else if (javaType == double.class) {
                if (type.equals(DOUBLE)) {
                    writeVectorValues(output, vector, index -> type.writeDouble(output, ((Float8Vector) vector).get(index)), length);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled double type: %s", type));
                }
            }
            else if (javaType == Slice.class) {
                if (type instanceof VarcharType) {
                    writeVectorValues(output, vector, index -> {
                        byte[] bytes = ((VarCharVector) vector).get(index);
                        type.writeSlice(output, wrappedBuffer(bytes));
                    }, length);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled Slice type: %s", type));
                }
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled java type: %s for trino type: %s", javaType, type));
            }
        }
        catch (ClassCastException ex) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Type conversion error for %s", type), ex);
        }
    }

    private void writeVectorValues(BlockBuilder output, FieldVector vector, Consumer<Integer> consumer, int length)
    {
        for (int i = 0; i < length; i++) {
            if (vector.isNull(i)) {
                output.appendNull();
            }
            else {
                consumer.accept(i);
            }
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        try {
            if (arrowReader != null) {
                arrowReader.close();
            }
            if (dataset != null) {
                dataset.close();
            }
        }
        catch (Exception e) {
            log.warn(e, "Failed to close aggregate page source");
        }
        finally {
            bufferAllocator.close();
        }
    }
}
