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

import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.TinyintType.TINYINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLancePageToArrowConverter
{
    private static final ArrayType TINYINT_ARRAY = new ArrayType(TINYINT);
    private static final Schema TINYINT_ARRAY_SCHEMA = new Schema(
            List.of(new Field("values", FieldType.nullable(new ArrowType.List()),
                    List.of(Field.nullable("item", new ArrowType.Int(8, true))))),
            null);

    @Test
    public void testWriteArrayAtNonzeroRowOffset()
    {
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(TINYINT_ARRAY_SCHEMA, allocator)) {
            root.allocateNew();
            ListVector values = (ListVector) root.getVector("values");

            LancePageToArrowConverter.writeBlockToVectorAtOffset(
                    tinyintArrayRow(1, 2), values, TINYINT_ARRAY, 1, 0);
            LancePageToArrowConverter.writeBlockToVectorAtOffset(
                    tinyintArrayRow(3, 4), values, TINYINT_ARRAY, 1, 1);
            root.setRowCount(2);

            TinyIntVector items = (TinyIntVector) values.getDataVector();
            assertList(values, items, 0, (byte) 1, (byte) 2);
            assertList(values, items, 1, (byte) 3, (byte) 4);
        }
    }

    private static Block tinyintArrayRow(long... elements)
    {
        ArrayBlockBuilder builder = (ArrayBlockBuilder) TINYINT_ARRAY.createBlockBuilder(null, 1);
        builder.buildEntry(entry -> {
            for (long element : elements) {
                TINYINT.writeLong(entry, element);
            }
        });
        return builder.build();
    }

    private static void assertList(ListVector values, TinyIntVector items, int row, byte... expected)
    {
        assertThat(values.isNull(row)).isFalse();
        int start = values.getElementStartIndex(row);
        assertThat(values.getElementEndIndex(row) - start).isEqualTo(expected.length);
        for (int i = 0; i < expected.length; i++) {
            assertThat(items.get(start + i)).isEqualTo(expected[i]);
        }
    }
}
