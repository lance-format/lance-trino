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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.lance.schema.LanceField;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public record LanceColumnHandle(String name, Type trinoType, boolean isNullable, int fieldId)
        implements ColumnHandle
{
    public LanceColumnHandle(String name, Type trinoType, FieldType fieldType)
    {
        this(name, trinoType, fieldType.isNullable(), -1);
    }

    public LanceColumnHandle(String name, Type trinoType, boolean isNullable)
    {
        this(name, trinoType, isNullable, -1);
    }

    public LanceColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(trinoType, "trinoType is null");
    }

    /**
     * Convert a LanceField to a Trino Type.
     * This method handles complex types like FixedSizeList by examining children.
     */
    public static Type toTrinoType(LanceField field)
    {
        ArrowType type = field.getType();

        // Handle FixedSizeList specially - need to get element type from children or logical type
        if (type instanceof ArrowType.FixedSizeList) {
            Type elementType = REAL; // Default for embeddings
            if (!field.getChildren().isEmpty()) {
                elementType = toTrinoType(field.getChildren().get(0));
            }
            else {
                // Try to parse from logical type (e.g., "fixed_size_list:float:768")
                String logicalType = field.getLogicalType();
                if (logicalType != null && logicalType.startsWith("fixed_size_list:")) {
                    String[] parts = logicalType.split(":");
                    if (parts.length >= 2) {
                        elementType = logicalTypeToTrinoType(parts[1]);
                    }
                }
            }
            return new ArrayType(elementType);
        }

        // Handle List type - need element type from children
        if (type instanceof ArrowType.List) {
            Type elementType = REAL; // Default
            if (!field.getChildren().isEmpty()) {
                elementType = toTrinoType(field.getChildren().get(0));
            }
            return new ArrayType(elementType);
        }

        // For simple types, delegate to the ArrowType-based method
        return toTrinoType(type);
    }

    private static Type logicalTypeToTrinoType(String logicalType)
    {
        return switch (logicalType) {
            case "bool" -> BOOLEAN;
            case "int32" -> INTEGER;
            case "int64" -> BIGINT;
            case "float", "halffloat" -> REAL;
            case "double" -> DOUBLE;
            case "string" -> VARCHAR;
            default -> REAL; // Default for unknown types in embeddings
        };
    }

    public static Type toTrinoType(ArrowType type)
    {
        if (type instanceof ArrowType.Bool) {
            return BOOLEAN;
        }
        else if (type instanceof ArrowType.Int intType) {
            if (intType.getBitWidth() == 32) {
                return INTEGER;
            }
            else if (intType.getBitWidth() == 64) {
                return BIGINT;
            }
        }
        else if (type instanceof ArrowType.FloatingPoint fpType) {
            if (fpType.getPrecision() == FloatingPointPrecision.SINGLE) {
                return REAL;
            }
            return DOUBLE;
        }
        else if (type instanceof ArrowType.Utf8) {
            return VARCHAR;
        }
        else if (type instanceof ArrowType.LargeUtf8) {
            return VARCHAR;
        }
        else if (type instanceof ArrowType.Date) {
            return DATE;
        }
        else if (type instanceof ArrowType.Timestamp tsType) {
            // Convert Arrow timestamp to Trino timestamp
            // If timezone is present, use TIMESTAMP WITH TIME ZONE (millis precision for simple representation)
            // otherwise plain TIMESTAMP (micros)
            if (tsType.getTimezone() != null && !tsType.getTimezone().isEmpty()) {
                return TIMESTAMP_TZ_MILLIS;
            }
            return TIMESTAMP_MICROS;
        }
        else if (type instanceof ArrowType.List) {
            // List type - handled by parent field's children
            return new ArrayType(REAL); // Default element type, actual type comes from children
        }
        else if (type instanceof ArrowType.FixedSizeList fsl) {
            // FixedSizeList is commonly used for vector embeddings
            // We map it to Trino ARRAY type - the element type comes from children
            return new ArrayType(REAL); // Default to REAL for embeddings
        }
        throw new UnsupportedOperationException("Unsupported arrow type: " + type);
    }

    /**
     * Convert Trino Type to Arrow ArrowType.
     * This is the reverse of toTrinoType().
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
        else if (trinoType instanceof TimestampWithTimeZoneType) {
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
        }
        else if (trinoType instanceof TimestampType) {
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        }
        else if (trinoType instanceof ArrayType) {
            return ArrowType.List.INSTANCE;
        }
        else if (trinoType instanceof RowType) {
            return ArrowType.Struct.INSTANCE;
        }
        throw new UnsupportedOperationException("Unsupported Trino type for Arrow conversion: " + trinoType);
    }

    @JsonIgnore
    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder().setName(name).setType(trinoType).setNullable(isNullable).build();
    }
}
