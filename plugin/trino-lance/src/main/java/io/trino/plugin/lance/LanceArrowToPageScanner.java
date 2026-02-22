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

import io.airlift.slice.Slice;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;
import org.lance.ipc.LanceScanner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.OFFSET_WIDTH;

public class LanceArrowToPageScanner
        implements AutoCloseable
{
    private final BufferAllocator allocator;
    private final List<Type> columnTypes;
    private final List<String> columnNames;
    private final List<LanceColumnHandle> columns;
    private final ScannerFactory scannerFactory;

    private final LanceScanner lanceScanner;
    private final ArrowReader arrowReader;
    private final VectorSchemaRoot vectorSchemaRoot;

    public LanceArrowToPageScanner(
            BufferAllocator allocator,
            String path,
            List<LanceColumnHandle> columns,
            ScannerFactory scannerFactory,
            Map<String, String> storageOptions,
            Optional<ByteBuffer> substraitFilter,
            OptionalLong limit,
            String userIdentity,
            Long datasetVersion)
    {
        this(allocator, path, columns, List.of(), scannerFactory, storageOptions, substraitFilter, limit, userIdentity, datasetVersion);
    }

    public LanceArrowToPageScanner(
            BufferAllocator allocator,
            String path,
            List<LanceColumnHandle> columns,
            List<String> filterProjectionColumns,
            ScannerFactory scannerFactory,
            Map<String, String> storageOptions,
            Optional<ByteBuffer> substraitFilter,
            OptionalLong limit,
            String userIdentity,
            Long datasetVersion)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        requireNonNull(columns, "columns is null");
        this.columns = columns;
        // Output columns - these will be converted to Trino format
        this.columnTypes = columns.stream().map(LanceColumnHandle::trinoType)
                .collect(toImmutableList());
        this.columnNames = columns.stream().map(LanceColumnHandle::name).collect(toImmutableList());
        this.scannerFactory = scannerFactory;

        // Filter out virtual columns from projection - they don't exist in Lance data
        // Also add base blob columns for virtual columns if not already present
        List<String> projectionColumns = new java.util.ArrayList<>();
        java.util.Set<String> addedColumns = new java.util.HashSet<>();

        for (LanceColumnHandle col : columns) {
            if (col.isBlobVirtualColumn()) {
                // Virtual column - add the base blob column instead
                String baseName = col.baseBlobColumnName();
                if (baseName != null && !addedColumns.contains(baseName)) {
                    projectionColumns.add(baseName);
                    addedColumns.add(baseName);
                }
            }
            else {
                if (!addedColumns.contains(col.name())) {
                    projectionColumns.add(col.name());
                    addedColumns.add(col.name());
                }
            }
        }

        // Add filter projection columns
        for (String filterCol : filterProjectionColumns) {
            if (!addedColumns.contains(filterCol)) {
                projectionColumns.add(filterCol);
                addedColumns.add(filterCol);
            }
        }

        lanceScanner = scannerFactory.open(path, allocator, projectionColumns, storageOptions, substraitFilter, limit, userIdentity, datasetVersion);
        this.arrowReader = lanceScanner.scanBatches();
        try {
            this.vectorSchemaRoot = arrowReader.getVectorSchemaRoot();
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to get vector schema root", e);
        }
    }

    private long lastBatchBytes;

    public boolean read()
    {
        try {
            boolean hasNext = arrowReader.loadNextBatch();
            if (hasNext) {
                // Calculate bytes read from Arrow buffers
                lastBatchBytes = 0;
                for (FieldVector vector : vectorSchemaRoot.getFieldVectors()) {
                    lastBatchBytes += getVectorBytes(vector);
                }
            }
            else {
                lastBatchBytes = 0;
            }
            return hasNext;
        }
        catch (IOException e) {
            throw new RuntimeException("Error loading next batch!", e);
        }
    }

    /**
     * Get the bytes used by this batch (after read() returns true).
     */
    public long getLastBatchBytes()
    {
        return lastBatchBytes;
    }

    private long getVectorBytes(FieldVector vector)
    {
        long bytes = 0;
        for (var buffer : vector.getBuffers(false)) {
            bytes += buffer.capacity();
        }
        // Handle nested vectors (list, struct)
        if (vector instanceof ListVector listVector) {
            bytes += getVectorBytes(listVector.getDataVector());
        }
        else if (vector instanceof FixedSizeListVector fslVector) {
            bytes += getVectorBytes(fslVector.getDataVector());
        }
        else if (vector instanceof StructVector structVector) {
            for (FieldVector child : structVector.getChildrenFromFields()) {
                bytes += getVectorBytes(child);
            }
        }
        return bytes;
    }

    public void convert(PageBuilder pageBuilder)
    {
        int rowCount = vectorSchemaRoot.getRowCount();
        pageBuilder.declarePositions(rowCount);

        // For COUNT(*) and similar queries, columnTypes may be empty
        // In that case, we just need to report the row count, no actual conversion needed
        // Look up vectors by name to ensure correct column ordering matches the requested projection
        for (int column = 0; column < columnTypes.size(); column++) {
            LanceColumnHandle colHandle = columns.get(column);
            String colName = columnNames.get(column);

            if (colHandle.isBlobVirtualColumn()) {
                // Virtual column - extract from blob struct
                String baseBlobName = colHandle.baseBlobColumnName();
                FieldVector blobVector = vectorSchemaRoot.getVector(baseBlobName);
                convertBlobVirtualColumn(pageBuilder.getBlockBuilder(column), blobVector, colHandle.blobVirtualColumnType(), rowCount);
            }
            else {
                FieldVector fieldVector = vectorSchemaRoot.getVector(colName);
                convertType(pageBuilder.getBlockBuilder(column), columnTypes.get(column), fieldVector, 0,
                        fieldVector.getValueCount());
            }
        }
        vectorSchemaRoot.clear();
    }

    private void convertBlobVirtualColumn(BlockBuilder output, FieldVector blobVector, BlobUtils.BlobVirtualColumnType virtualType, int rowCount)
    {
        if (!(blobVector instanceof StructVector structVector)) {
            // Not a struct - output nulls
            for (int i = 0; i < rowCount; i++) {
                output.appendNull();
            }
            return;
        }

        // Get position and size vectors from the blob struct
        FieldVector positionVector = structVector.getChild("position");
        FieldVector sizeVector = structVector.getChild("size");

        for (int i = 0; i < rowCount; i++) {
            if (structVector.isNull(i)) {
                output.appendNull();
            }
            else {
                long value;
                if (virtualType == BlobUtils.BlobVirtualColumnType.POSITION) {
                    if (positionVector instanceof UInt8Vector uint8Vec) {
                        value = uint8Vec.get(i);
                    }
                    else if (positionVector instanceof BigIntVector bigIntVec) {
                        value = bigIntVec.get(i);
                    }
                    else {
                        output.appendNull();
                        continue;
                    }
                }
                else {
                    // SIZE
                    if (sizeVector instanceof UInt8Vector uint8Vec) {
                        value = uint8Vec.get(i);
                    }
                    else if (sizeVector instanceof BigIntVector bigIntVec) {
                        value = bigIntVec.get(i);
                    }
                    else {
                        output.appendNull();
                        continue;
                    }
                }
                BIGINT.writeLong(output, value);
            }
        }
    }

    private void convertType(BlockBuilder output, Type type, FieldVector vector, int offset, int length)
    {
        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                writeVectorValues(output, vector,
                        index -> type.writeBoolean(output, ((BitVector) vector).get(index) == 1), offset, length);
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    // Handle both signed (BigIntVector) and unsigned (UInt8Vector) 64-bit integers
                    if (vector instanceof UInt8Vector uint8Vector) {
                        writeVectorValues(output, vector,
                                index -> type.writeLong(output, uint8Vector.get(index)), offset, length);
                    }
                    else {
                        writeVectorValues(output, vector,
                                index -> type.writeLong(output, ((BigIntVector) vector).get(index)), offset, length);
                    }
                }
                else if (type.equals(INTEGER)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output, ((IntVector) vector).get(index)),
                            offset, length);
                }
                else if (type.equals(DATE)) {
                    writeVectorValues(output, vector,
                            index -> type.writeLong(output, ((DateDayVector) vector).get(index)), offset, length);
                }
                else if (type.equals(TIME_MICROS)) {
                    writeVectorValues(output, vector, index -> type.writeLong(output,
                            ((TimeMicroVector) vector).get(index) * PICOSECONDS_PER_MICROSECOND), offset, length);
                }
                else if (type.equals(REAL)) {
                    // REAL stores float bits as int which is widened to long
                    writeVectorValues(output, vector, index -> type.writeLong(output,
                            Float.floatToIntBits(((Float4Vector) vector).get(index))), offset, length);
                }
                else if (type instanceof TimestampWithTimeZoneType) {
                    // Timestamp with timezone - stored as microseconds in Arrow
                    // Convert to milliseconds and pack with UTC timezone for TIMESTAMP_TZ_MILLIS
                    if (vector instanceof TimeStampMicroTZVector tsVector) {
                        writeVectorValues(output, vector, index -> {
                            long micros = tsVector.get(index);
                            long millis = micros / 1000;
                            type.writeLong(output, packDateTimeWithZone(millis, UTC_KEY));
                        }, offset, length);
                    }
                    else if (vector instanceof TimeStampMicroVector tsVector) {
                        // Handle case where Arrow has no TZ but we map to TZ type
                        writeVectorValues(output, vector, index -> {
                            long micros = tsVector.get(index);
                            long millis = micros / 1000;
                            type.writeLong(output, packDateTimeWithZone(millis, UTC_KEY));
                        }, offset, length);
                    }
                    else {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR,
                                format("Expected TimeStampMicroTZVector but got: %s", vector.getClass().getSimpleName()));
                    }
                }
                else if (type instanceof TimestampType) {
                    // Timestamp without timezone - stored as microseconds in Arrow
                    if (vector instanceof TimeStampMicroVector tsVector) {
                        writeVectorValues(output, vector, index -> type.writeLong(output, tsVector.get(index)),
                                offset, length);
                    }
                    else if (vector instanceof TimeStampMicroTZVector tsVector) {
                        // Handle case where Arrow has TZ but Trino doesn't need it
                        writeVectorValues(output, vector, index -> type.writeLong(output, tsVector.get(index)),
                                offset, length);
                    }
                    else {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR,
                                format("Expected TimeStampMicroVector but got: %s", vector.getClass().getSimpleName()));
                    }
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR,
                            format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (javaType == double.class) {
                writeVectorValues(output, vector, index -> type.writeDouble(output, ((Float8Vector) vector).get(index)),
                        offset, length);
            }
            else if (javaType == Slice.class) {
                writeVectorValues(output, vector, index -> writeSlice(output, type, vector, index), offset, length);
            }
            else if (type instanceof ArrayType arrayType) {
                // Handle both ListVector and FixedSizeListVector
                if (vector instanceof FixedSizeListVector) {
                    writeVectorValues(output, vector, index -> writeFixedSizeArrayBlock(output, arrayType, vector, index), offset,
                            length);
                }
                else {
                    writeVectorValues(output, vector, index -> writeArrayBlock(output, arrayType, vector, index), offset,
                            length);
                }
            }
            else if (type instanceof RowType rowType) {
                writeVectorValues(output, vector, index -> writeRowBlock(output, rowType, vector, index), offset,
                        length);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException ex) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    format("Unhandled type for %s: %s", javaType.getSimpleName(), type), ex);
        }
    }

    private void writeVectorValues(BlockBuilder output, FieldVector vector, Consumer<Integer> consumer, int offset,
            int length)
    {
        for (int i = offset; i < offset + length; i++) {
            if (vector.isNull(i)) {
                output.appendNull();
            }
            else {
                consumer.accept(i);
            }
        }
    }

    private void writeSlice(BlockBuilder output, Type type, FieldVector vector, int index)
    {
        if (type instanceof VarcharType) {
            byte[] slice = ((VarCharVector) vector).get(index);
            type.writeSlice(output, wrappedBuffer(slice));
        }
        else if (type instanceof VarbinaryType) {
            if (vector instanceof VarBinaryVector varBinaryVector) {
                byte[] slice = varBinaryVector.get(index);
                type.writeSlice(output, wrappedBuffer(slice));
            }
            else if (vector instanceof LargeVarBinaryVector largeVarBinaryVector) {
                byte[] slice = largeVarBinaryVector.get(index);
                type.writeSlice(output, wrappedBuffer(slice));
            }
            else if (vector instanceof StructVector structVector) {
                // Blob columns come back as struct with position and size
                // The actual blob data is not materialized, return empty byte array
                Field field = structVector.getField();
                if (BlobUtils.isBlobArrowField(field)) {
                    type.writeSlice(output, wrappedBuffer(new byte[0]));
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected StructVector for VARBINARY: " + field);
                }
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected vector type for VARBINARY: " + vector.getClass().getSimpleName());
            }
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private void writeArrayBlock(BlockBuilder output, ArrayType arrayType, FieldVector vector, int index)
    {
        Type elementType = arrayType.getElementType();
        ((ArrayBlockBuilder) output).buildEntry(elementBuilder -> {
            ArrowBuf offsetBuffer = vector.getOffsetBuffer();

            int start = offsetBuffer.getInt((long) index * OFFSET_WIDTH);
            int end = offsetBuffer.getInt((long) (index + 1) * OFFSET_WIDTH);

            FieldVector innerVector = ((ListVector) vector).getDataVector();

            TransferPair transferPair = innerVector.getTransferPair(allocator);
            transferPair.splitAndTransfer(start, end - start);
            try (FieldVector sliced = (FieldVector) transferPair.getTo()) {
                convertType(elementBuilder, elementType, sliced, 0, sliced.getValueCount());
            }
        });
    }

    private void writeFixedSizeArrayBlock(BlockBuilder output, ArrayType arrayType, FieldVector vector, int index)
    {
        Type elementType = arrayType.getElementType();
        FixedSizeListVector fslVector = (FixedSizeListVector) vector;
        int listSize = fslVector.getListSize();

        ((ArrayBlockBuilder) output).buildEntry(elementBuilder -> {
            FieldVector innerVector = fslVector.getDataVector();
            int start = index * listSize;

            TransferPair transferPair = innerVector.getTransferPair(allocator);
            transferPair.splitAndTransfer(start, listSize);
            try (FieldVector sliced = (FieldVector) transferPair.getTo()) {
                convertType(elementBuilder, elementType, sliced, 0, sliced.getValueCount());
            }
        });
    }

    private void writeRowBlock(BlockBuilder output, RowType rowType, FieldVector vector, int index)
    {
        List<RowType.Field> fields = rowType.getFields();
        ((RowBlockBuilder) output).buildEntry(fieldBuilders -> {
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field field = fields.get(i);
                FieldVector innerVector = ((StructVector) vector).getChild(field.getName().orElse("field" + i));
                convertType(fieldBuilders.get(i), field.getType(), innerVector, index, 1);
            }
        });
    }

    @Override
    public void close()
    {
        vectorSchemaRoot.close();
        try {
            arrowReader.close();
        }
        catch (IOException ioe) {
            // ignore for now.
        }
        scannerFactory.close();
    }
}
