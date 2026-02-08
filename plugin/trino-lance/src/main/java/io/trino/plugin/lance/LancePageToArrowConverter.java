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
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

/**
 * Utility class for converting Trino types and data to Arrow format.
 * This is the reverse of LanceArrowToPageScanner.
 */
public final class LancePageToArrowConverter
{
    private LancePageToArrowConverter() {}

    /**
     * Convert Trino Type to Arrow ArrowType.
     */
    public static ArrowType toArrowType(Type trinoType)
    {
        if (trinoType.equals(BOOLEAN)) {
            return ArrowType.Bool.INSTANCE;
        }
        else if (trinoType.equals(INTEGER)) {
            return new ArrowType.Int(32, true);
        }
        else if (trinoType.equals(BIGINT)) {
            return new ArrowType.Int(64, true);
        }
        else if (trinoType.equals(REAL)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        }
        else if (trinoType.equals(DOUBLE)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }
        else if (trinoType instanceof VarcharType) {
            return ArrowType.Utf8.INSTANCE;
        }
        else if (trinoType instanceof VarbinaryType) {
            return ArrowType.Binary.INSTANCE;
        }
        else if (trinoType instanceof DateType) {
            return new ArrowType.Date(DateUnit.DAY);
        }
        else if (trinoType instanceof ArrayType arrayType) {
            // Arrow List type - the children field defines element type
            return ArrowType.List.INSTANCE;
        }
        else if (trinoType instanceof RowType) {
            return ArrowType.Struct.INSTANCE;
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported Trino type for Arrow conversion: %s", trinoType));
    }

    /**
     * Convert list of Trino ColumnMetadata to Arrow Schema.
     */
    public static Schema toArrowSchema(List<ColumnMetadata> columns)
    {
        List<Field> fields = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            fields.add(toArrowField(column.getName(), column.getType(), column.isNullable()));
        }
        return new Schema(fields);
    }

    /**
     * Convert a Trino column to an Arrow Field.
     */
    public static Field toArrowField(String name, Type trinoType, boolean nullable)
    {
        ArrowType arrowType = toArrowType(trinoType);
        FieldType fieldType = new FieldType(nullable, arrowType, null);

        if (trinoType instanceof ArrayType arrayType) {
            // For List type, we need to add the child field
            Type elementType = arrayType.getElementType();
            Field elementField = toArrowField("item", elementType, true);
            return new Field(name, fieldType, List.of(elementField));
        }
        else if (trinoType instanceof RowType rowType) {
            // For Struct type, add all child fields
            List<Field> childFields = new ArrayList<>();
            for (RowType.Field field : rowType.getFields()) {
                String fieldName = field.getName().orElse("field" + childFields.size());
                childFields.add(toArrowField(fieldName, field.getType(), true));
            }
            return new Field(name, fieldType, childFields);
        }
        else {
            return new Field(name, fieldType, null);
        }
    }

    /**
     * Create a VectorSchemaRoot from an Arrow Schema using the given allocator.
     */
    public static VectorSchemaRoot createVectorSchemaRoot(BufferAllocator allocator, Schema schema)
    {
        return VectorSchemaRoot.create(schema, allocator);
    }

    /**
     * Write a Trino Page to Arrow VectorSchemaRoot.
     * The schema of the VectorSchemaRoot must match the columns in the Page.
     *
     * @param page The Trino Page to convert
     * @param root The VectorSchemaRoot to write to (must be pre-allocated)
     * @param columnTypes The Trino types for each column
     */
    public static void writePageToVectors(Page page, VectorSchemaRoot root, List<Type> columnTypes)
    {
        int rowCount = page.getPositionCount();

        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            FieldVector vector = root.getVector(channel);
            Type type = columnTypes.get(channel);
            writeBlockToVector(block, vector, type, rowCount);
        }

        root.setRowCount(rowCount);
    }

    /**
     * Write a Trino Block to an Arrow FieldVector.
     * Note: Caller must have already allocated the vector. This method writes at offset 0.
     */
    public static void writeBlockToVector(Block block, FieldVector vector, Type type, int rowCount)
    {
        writeBlockToVectorAtOffset(block, vector, type, rowCount, 0);
    }

    /**
     * Write a Trino Block to an Arrow FieldVector at a specific offset.
     * Note: Caller must have already allocated the vector.
     */
    public static void writeBlockToVectorAtOffset(Block block, FieldVector vector, Type type, int rowCount, int offset)
    {
        if (type.equals(BOOLEAN)) {
            writeBooleanBlock(block, (BitVector) vector, rowCount, offset);
        }
        else if (type.equals(INTEGER)) {
            writeIntegerBlock(block, (IntVector) vector, rowCount, offset);
        }
        else if (type.equals(BIGINT)) {
            writeBigintBlock(block, (BigIntVector) vector, rowCount, offset);
        }
        else if (type.equals(REAL)) {
            writeRealBlock(block, (Float4Vector) vector, rowCount, offset);
        }
        else if (type.equals(DOUBLE)) {
            writeDoubleBlock(block, (Float8Vector) vector, rowCount, offset);
        }
        else if (type instanceof VarcharType) {
            writeVarcharBlock(block, (VarCharVector) vector, rowCount, offset);
        }
        else if (type instanceof VarbinaryType) {
            writeVarbinaryBlock(block, (VarBinaryVector) vector, rowCount, offset);
        }
        else if (type instanceof DateType) {
            writeDateBlock(block, (DateDayVector) vector, rowCount, offset);
        }
        else if (type instanceof ArrayType arrayType) {
            writeArrayBlock(block, (ListVector) vector, arrayType, rowCount, offset);
        }
        else if (type instanceof RowType rowType) {
            writeRowBlock(block, (StructVector) vector, rowType, rowCount, offset);
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported type for writing to Arrow: %s", type));
        }
    }

    private static void writeBooleanBlock(Block block, BitVector vector, int rowCount, int offset)
    {
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i)) {
                vector.setNull(offset + i);
            }
            else {
                vector.setSafe(offset + i, BOOLEAN.getBoolean(block, i) ? 1 : 0);
            }
        }
        vector.setValueCount(offset + rowCount);
    }

    private static void writeIntegerBlock(Block block, IntVector vector, int rowCount, int offset)
    {
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i)) {
                vector.setNull(offset + i);
            }
            else {
                vector.setSafe(offset + i, (int) INTEGER.getLong(block, i));
            }
        }
        vector.setValueCount(offset + rowCount);
    }

    private static void writeBigintBlock(Block block, BigIntVector vector, int rowCount, int offset)
    {
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i)) {
                vector.setNull(offset + i);
            }
            else {
                vector.setSafe(offset + i, BIGINT.getLong(block, i));
            }
        }
        vector.setValueCount(offset + rowCount);
    }

    private static void writeRealBlock(Block block, Float4Vector vector, int rowCount, int offset)
    {
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i)) {
                vector.setNull(offset + i);
            }
            else {
                // Trino stores REAL as int bits, need to convert to float
                int intBits = (int) REAL.getLong(block, i);
                vector.setSafe(offset + i, Float.intBitsToFloat(intBits));
            }
        }
        vector.setValueCount(offset + rowCount);
    }

    private static void writeDoubleBlock(Block block, Float8Vector vector, int rowCount, int offset)
    {
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i)) {
                vector.setNull(offset + i);
            }
            else {
                vector.setSafe(offset + i, DOUBLE.getDouble(block, i));
            }
        }
        vector.setValueCount(offset + rowCount);
    }

    private static void writeVarcharBlock(Block block, VarCharVector vector, int rowCount, int offset)
    {
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i)) {
                vector.setNull(offset + i);
            }
            else {
                Slice slice = VARCHAR.getSlice(block, i);
                vector.setSafe(offset + i, slice.getBytes());
            }
        }
        vector.setValueCount(offset + rowCount);
    }

    private static void writeVarbinaryBlock(Block block, VarBinaryVector vector, int rowCount, int offset)
    {
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i)) {
                vector.setNull(offset + i);
            }
            else {
                Slice slice = VARBINARY.getSlice(block, i);
                vector.setSafe(offset + i, slice.getBytes());
            }
        }
        vector.setValueCount(offset + rowCount);
    }

    private static void writeDateBlock(Block block, DateDayVector vector, int rowCount, int offset)
    {
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i)) {
                vector.setNull(offset + i);
            }
            else {
                // Trino DATE is days since epoch stored as int
                vector.setSafe(offset + i, (int) DATE.getLong(block, i));
            }
        }
        vector.setValueCount(offset + rowCount);
    }

    private static void writeArrayBlock(Block block, ListVector vector, ArrayType arrayType, int rowCount, int rowOffset)
    {
        Type elementType = arrayType.getElementType();
        FieldVector dataVector = vector.getDataVector();

        int elementOffset = 0;
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i)) {
                vector.setNull(rowOffset + i);
            }
            else {
                Block arrayBlock = arrayType.getObject(block, i);
                int arrayLength = arrayBlock.getPositionCount();

                vector.startNewValue(rowOffset + i);
                // Write array elements to data vector starting at current offset
                writeArrayElements(arrayBlock, dataVector, elementType, elementOffset, arrayLength);
                vector.endValue(rowOffset + i, arrayLength);
                elementOffset += arrayLength;
            }
        }
        vector.setValueCount(rowOffset + rowCount);
    }

    private static void writeArrayElements(Block arrayBlock, FieldVector dataVector, Type elementType, int offset, int length)
    {
        // This is a simplified implementation - for complex nested types more work is needed
        // Note: We don't call allocateNew() here as the parent vector manages allocation

        if (elementType.equals(INTEGER)) {
            IntVector intVector = (IntVector) dataVector;
            for (int i = 0; i < length; i++) {
                if (arrayBlock.isNull(i)) {
                    intVector.setNull(offset + i);
                }
                else {
                    intVector.setSafe(offset + i, (int) INTEGER.getLong(arrayBlock, i));
                }
            }
            intVector.setValueCount(offset + length);
        }
        else if (elementType.equals(BIGINT)) {
            BigIntVector bigIntVector = (BigIntVector) dataVector;
            for (int i = 0; i < length; i++) {
                if (arrayBlock.isNull(i)) {
                    bigIntVector.setNull(offset + i);
                }
                else {
                    bigIntVector.setSafe(offset + i, BIGINT.getLong(arrayBlock, i));
                }
            }
            bigIntVector.setValueCount(offset + length);
        }
        else if (elementType.equals(DOUBLE)) {
            Float8Vector doubleVector = (Float8Vector) dataVector;
            for (int i = 0; i < length; i++) {
                if (arrayBlock.isNull(i)) {
                    doubleVector.setNull(offset + i);
                }
                else {
                    doubleVector.setSafe(offset + i, DOUBLE.getDouble(arrayBlock, i));
                }
            }
            doubleVector.setValueCount(offset + length);
        }
        else if (elementType instanceof VarcharType) {
            VarCharVector varcharVector = (VarCharVector) dataVector;
            for (int i = 0; i < length; i++) {
                if (arrayBlock.isNull(i)) {
                    varcharVector.setNull(offset + i);
                }
                else {
                    Slice slice = VARCHAR.getSlice(arrayBlock, i);
                    varcharVector.setSafe(offset + i, slice.getBytes());
                }
            }
            varcharVector.setValueCount(offset + length);
        }
        // Add more element types as needed
    }

    private static void writeRowBlock(Block block, StructVector vector, RowType rowType, int rowCount, int offset)
    {
        // For now, ROW type write is not fully implemented
        // This requires more complex handling of SqlRow objects
        throw new TrinoException(NOT_SUPPORTED, "ROW type write is not yet implemented");
    }
}
