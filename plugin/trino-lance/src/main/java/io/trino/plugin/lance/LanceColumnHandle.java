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
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
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
