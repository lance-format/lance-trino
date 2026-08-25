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

import io.trino.spi.type.ArrayType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.WriteParams;
import org.lance.ipc.LanceScanner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.trino.plugin.lance.LanceRuntime.TABLE_PATH_SUFFIX;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLanceUnsignedInt32
        extends AbstractTestQueryFramework
{
    private static final Schema UNSIGNED_INT32_SCHEMA = new Schema(
            List.of(
                    Field.nullable("id", new ArrowType.Int(32, true)),
                    Field.nullable("unsigned_int32", new ArrowType.Int(32, false)),
                    new Field("unsigned_int32_array", FieldType.nullable(new ArrowType.List()),
                            List.of(Field.nullable("item", new ArrowType.Int(32, false))))),
            null);

    private Path lanceRoot;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        lanceRoot = Files.createTempDirectory("lance-unsigned-int32");
        lanceRoot.toFile().deleteOnExit();
        return LanceQueryRunner.builder()
                .addConnectorProperty("lance.root", lanceRoot.toUri().toString())
                .build();
    }

    @Test
    public void testInsertIntoUnsignedInt32Dataset()
            throws Exception
    {
        String tableName = "unsigned_int32";
        Path datasetPath = lanceRoot.resolve(tableName + TABLE_PATH_SUFFIX);
        try (BufferAllocator allocator = new RootAllocator()) {
            Dataset.create(
                    allocator,
                    datasetPath.toString(),
                    UNSIGNED_INT32_SCHEMA,
                    new WriteParams.Builder().build()).close();
        }

        assertThat(computeActual("SELECT unsigned_int32, unsigned_int32_array FROM " + tableName).getTypes())
                .containsExactly(BIGINT, new ArrayType(BIGINT));

        assertUpdate("INSERT INTO " + tableName + " VALUES " +
                "(1, BIGINT '3000000000', ARRAY[BIGINT '3000000000', BIGINT '4294967295']), " +
                "(2, CAST(NULL AS BIGINT), CAST(NULL AS ARRAY(BIGINT))), " +
                "(3, BIGINT '1', ARRAY[BIGINT '1', CAST(NULL AS BIGINT)])", 3);

        assertLanceStoredUnsignedInt32(datasetPath);

        assertQueryFails(
                "INSERT INTO " + tableName + " VALUES (2, BIGINT '-1', ARRAY[BIGINT '1'])",
                ".*out of range for unsigned 32-bit integer.*");
        assertQueryFails(
                "INSERT INTO " + tableName + " VALUES (3, BIGINT '4294967296', ARRAY[BIGINT '1'])",
                ".*out of range for unsigned 32-bit integer.*");
        assertQueryFails(
                "INSERT INTO " + tableName + " VALUES (4, BIGINT '1', ARRAY[BIGINT '4294967296'])",
                ".*out of range for unsigned 32-bit integer.*");
    }

    private static void assertLanceStoredUnsignedInt32(Path datasetPath)
            throws Exception
    {
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open(allocator, datasetPath.toString(), new ReadOptions.Builder().build());
                LanceScanner scanner = dataset.newScan();
                ArrowReader reader = scanner.scanBatches()) {
            Schema schema = dataset.getSchema();
            assertThat(schema.findField("unsigned_int32").getType()).isEqualTo(new ArrowType.Int(32, false));
            assertThat(schema.findField("unsigned_int32_array").getType()).isEqualTo(ArrowType.List.INSTANCE);
            assertThat(schema.findField("unsigned_int32_array").getChildren().get(0).getType()).isEqualTo(new ArrowType.Int(32, false));

            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(3);
            assertThat(root.getVector("unsigned_int32")).isInstanceOf(UInt4Vector.class);
            assertThat(((ListVector) root.getVector("unsigned_int32_array")).getDataVector()).isInstanceOf(UInt4Vector.class);

            IntVector id = (IntVector) root.getVector("id");
            UInt4Vector unsignedInt32 = (UInt4Vector) root.getVector("unsigned_int32");
            ListVector unsignedInt32Array = (ListVector) root.getVector("unsigned_int32_array");
            UInt4Vector items = (UInt4Vector) unsignedInt32Array.getDataVector();

            int valuesRow = rowWithId(id, 1);
            assertThat(Integer.toUnsignedLong(unsignedInt32.get(valuesRow))).isEqualTo(3_000_000_000L);
            assertThat(unsignedInt32Array.isNull(valuesRow)).isFalse();
            int valuesStart = unsignedInt32Array.getElementStartIndex(valuesRow);
            assertThat(unsignedInt32Array.getElementEndIndex(valuesRow) - valuesStart).isEqualTo(2);
            assertThat(Integer.toUnsignedLong(items.get(valuesStart))).isEqualTo(3_000_000_000L);
            assertThat(Integer.toUnsignedLong(items.get(valuesStart + 1))).isEqualTo(4_294_967_295L);

            int nullScalarsRow = rowWithId(id, 2);
            assertThat(unsignedInt32.isNull(nullScalarsRow)).isTrue();
            assertThat(unsignedInt32Array.isNull(nullScalarsRow)).isTrue();

            int nullElementRow = rowWithId(id, 3);
            assertThat(Integer.toUnsignedLong(unsignedInt32.get(nullElementRow))).isEqualTo(1L);
            int nullElementStart = unsignedInt32Array.getElementStartIndex(nullElementRow);
            assertThat(unsignedInt32Array.getElementEndIndex(nullElementRow) - nullElementStart).isEqualTo(2);
            assertThat(Integer.toUnsignedLong(items.get(nullElementStart))).isEqualTo(1L);
            assertThat(items.isNull(nullElementStart + 1)).isTrue();

            assertThat(reader.loadNextBatch()).isFalse();
        }
    }

    private static int rowWithId(IntVector id, int expected)
    {
        for (int i = 0; i < id.getValueCount(); i++) {
            if (!id.isNull(i) && id.get(i) == expected) {
                return i;
            }
        }
        throw new AssertionError("missing id " + expected);
    }
}
