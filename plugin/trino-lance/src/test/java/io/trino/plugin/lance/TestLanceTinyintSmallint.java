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
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
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
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLanceTinyintSmallint
        extends AbstractTestQueryFramework
{
    private static final Schema UNSIGNED_INT8_INT16_SCHEMA = new Schema(
            List.of(
                    Field.nullable("id", new ArrowType.Int(32, true)),
                    Field.nullable("unsigned_int8", new ArrowType.Int(8, false)),
                    Field.nullable("unsigned_int16", new ArrowType.Int(16, false)),
                    new Field("unsigned_int8_array", FieldType.nullable(new ArrowType.List()),
                            List.of(Field.nullable("item", new ArrowType.Int(8, false))))),
            null);

    private Path lanceRoot;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        lanceRoot = Files.createTempDirectory("lance-tinyint-smallint");
        lanceRoot.toFile().deleteOnExit();
        return LanceQueryRunner.builder()
                .addConnectorProperty("lance.root", lanceRoot.toUri().toString())
                .build();
    }

    @Test
    public void testInsertSelectTinyintSmallintAndTinyintArray()
    {
        String tableName = "tinyint_smallint_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " (id INTEGER, tinyint_val TINYINT, smallint_val SMALLINT, tinyint_flags ARRAY(TINYINT))");
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, TINYINT '-128', SMALLINT '-32768', ARRAY[TINYINT '1', TINYINT '0', TINYINT '-1']), " +
                    "(2, TINYINT '127', SMALLINT '32767', ARRAY[TINYINT '127'])", 2);
            var rows = computeActual(
                    "SELECT id, tinyint_val, smallint_val, tinyint_flags FROM " + tableName + " ORDER BY id")
                    .getMaterializedRows();
            assertThat(rows).hasSize(2);
            assertThat(rows.get(0).getFields()).containsExactly(
                    1, (byte) -128, (short) -32768, List.of((byte) 1, (byte) 0, (byte) -1));
            assertThat(rows.get(1).getFields()).containsExactly(
                    2, (byte) 127, (short) 32767, List.of((byte) 127));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testInsertIntoUnsignedInt8AndInt16Dataset()
            throws Exception
    {
        String tableName = "unsigned_ints";
        createUnsignedInt8Int16Dataset(lanceRoot.resolve(tableName + TABLE_PATH_SUFFIX));

        assertThat(computeActual("SELECT unsigned_int8, unsigned_int16, unsigned_int8_array FROM " + tableName).getTypes())
                .containsExactly(SMALLINT, INTEGER, new ArrayType(SMALLINT));

        assertUpdate("INSERT INTO " + tableName + " VALUES " +
                "(1, SMALLINT '200', 65535, ARRAY[SMALLINT '200', SMALLINT '255']), " +
                "(2, CAST(NULL AS SMALLINT), CAST(NULL AS INTEGER), CAST(NULL AS ARRAY(SMALLINT))), " +
                "(3, SMALLINT '1', 0, ARRAY[SMALLINT '1', CAST(NULL AS SMALLINT)])", 3);

        assertLanceStoredUnsignedInt8AndInt16(lanceRoot.resolve(tableName + TABLE_PATH_SUFFIX));

        assertQueryFails(
                "INSERT INTO " + tableName + " VALUES (2, SMALLINT '-1', 0, ARRAY[SMALLINT '1'])",
                ".*out of range for unsigned 8-bit integer.*");
        assertQueryFails(
                "INSERT INTO " + tableName + " VALUES (3, SMALLINT '256', 0, ARRAY[SMALLINT '1'])",
                ".*out of range for unsigned 8-bit integer.*");
        assertQueryFails(
                "INSERT INTO " + tableName + " VALUES (4, SMALLINT '1', -1, ARRAY[SMALLINT '1'])",
                ".*out of range for unsigned 16-bit integer.*");
        assertQueryFails(
                "INSERT INTO " + tableName + " VALUES (5, SMALLINT '1', 65536, ARRAY[SMALLINT '1'])",
                ".*out of range for unsigned 16-bit integer.*");
        assertQueryFails(
                "INSERT INTO " + tableName + " VALUES (6, SMALLINT '1', 0, ARRAY[SMALLINT '256'])",
                ".*out of range for unsigned 8-bit integer.*");
    }

    @Test
    public void testTinyintAndSmallintCtasPredicates()
    {
        assertCtasPredicates("tinyint", "37", "127");
        assertCtasPredicates("smallint", "32123", "32767");
    }

    private void assertCtasPredicates(String trinoTypeName, String sampleValueLiteral, String highValueLiteral)
    {
        String tableName = "data_mapping_" + trinoTypeName + "_" + System.currentTimeMillis();
        assertUpdate("CREATE TABLE " + tableName + " AS " +
                "SELECT CAST(row_id AS varchar(50)) row_id, CAST(value AS " + trinoTypeName + ") value, CAST(value AS " + trinoTypeName + ") another_column " +
                "FROM (VALUES " +
                "  ('null value', NULL), " +
                "  ('sample value', " + sampleValueLiteral + "), " +
                "  ('high value', " + highValueLiteral + ")) " +
                " t(row_id, value)", 3);
        try {
            assertQuery("SELECT row_id FROM " + tableName + " WHERE rand() = 42 OR value IS NULL", "VALUES 'null value'");
            assertQuery("SELECT row_id FROM " + tableName + " WHERE rand() = 42 OR value IS NOT NULL", "VALUES 'sample value', 'high value'");
            assertQuery("SELECT row_id FROM " + tableName + " WHERE rand() = 42 OR value = " + sampleValueLiteral, "VALUES 'sample value'");
            assertQuery("SELECT row_id FROM " + tableName + " WHERE rand() = 42 OR value = " + highValueLiteral, "VALUES 'high value'");

            assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL", "VALUES 'null value'");
            assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NOT NULL", "VALUES 'sample value', 'high value'");
            assertQuery("SELECT row_id FROM " + tableName + " WHERE value = " + sampleValueLiteral, "VALUES 'sample value'");
            assertQuery("SELECT row_id FROM " + tableName + " WHERE value != " + sampleValueLiteral, "VALUES 'high value'");
            assertQuery("SELECT row_id FROM " + tableName + " WHERE value <= " + sampleValueLiteral, "VALUES 'sample value'");
            assertQuery("SELECT row_id FROM " + tableName + " WHERE value > " + sampleValueLiteral, "VALUES 'high value'");
            assertQuery("SELECT row_id FROM " + tableName + " WHERE value <= " + highValueLiteral, "VALUES 'sample value', 'high value'");

            assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value = " + sampleValueLiteral, "VALUES 'null value', 'sample value'");
            assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value != " + sampleValueLiteral, "VALUES 'null value', 'high value'");
            assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value <= " + sampleValueLiteral, "VALUES 'null value', 'sample value'");
            assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value > " + sampleValueLiteral, "VALUES 'null value', 'high value'");
            assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value <= " + highValueLiteral, "VALUES 'null value', 'sample value', 'high value'");

            assertQuery("SELECT row_id FROM " + tableName + " WHERE value = " + sampleValueLiteral + " OR another_column = " + sampleValueLiteral, "VALUES 'sample value'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private static void createUnsignedInt8Int16Dataset(Path datasetPath)
    {
        try (BufferAllocator allocator = new RootAllocator()) {
            Dataset.create(allocator, datasetPath.toString(), UNSIGNED_INT8_INT16_SCHEMA, new WriteParams.Builder().build())
                    .close();
        }
    }

    private static void assertLanceStoredUnsignedInt8AndInt16(Path datasetPath)
            throws Exception
    {
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open(allocator, datasetPath.toString(), new ReadOptions.Builder().build());
                LanceScanner scanner = dataset.newScan();
                ArrowReader reader = scanner.scanBatches()) {
            Schema schema = dataset.getSchema();
            assertThat(schema.findField("unsigned_int8").getType()).isEqualTo(new ArrowType.Int(8, false));
            assertThat(schema.findField("unsigned_int16").getType()).isEqualTo(new ArrowType.Int(16, false));
            assertThat(schema.findField("unsigned_int8_array").getType()).isEqualTo(ArrowType.List.INSTANCE);
            assertThat(schema.findField("unsigned_int8_array").getChildren().get(0).getType()).isEqualTo(new ArrowType.Int(8, false));

            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(3);
            assertThat(root.getVector("unsigned_int8")).isInstanceOf(UInt1Vector.class);
            assertThat(root.getVector("unsigned_int16")).isInstanceOf(UInt2Vector.class);
            assertThat(((ListVector) root.getVector("unsigned_int8_array")).getDataVector()).isInstanceOf(UInt1Vector.class);

            IntVector id = (IntVector) root.getVector("id");
            UInt1Vector unsignedInt8 = (UInt1Vector) root.getVector("unsigned_int8");
            UInt2Vector unsignedInt16 = (UInt2Vector) root.getVector("unsigned_int16");
            ListVector unsignedInt8Array = (ListVector) root.getVector("unsigned_int8_array");
            UInt1Vector items = (UInt1Vector) unsignedInt8Array.getDataVector();

            int valuesRow = rowWithId(id, 1);
            assertThat(Byte.toUnsignedInt(unsignedInt8.get(valuesRow))).isEqualTo(200);
            assertThat(Short.toUnsignedInt((short) unsignedInt16.get(valuesRow))).isEqualTo(65535);
            assertListUnsignedInt8(unsignedInt8Array, items, valuesRow, 200, 255);

            int nullScalarsRow = rowWithId(id, 2);
            assertThat(unsignedInt8.isNull(nullScalarsRow)).isTrue();
            assertThat(unsignedInt16.isNull(nullScalarsRow)).isTrue();
            assertThat(unsignedInt8Array.isNull(nullScalarsRow)).isTrue();

            int nullElementRow = rowWithId(id, 3);
            assertThat(Byte.toUnsignedInt(unsignedInt8.get(nullElementRow))).isEqualTo(1);
            assertThat(Short.toUnsignedInt((short) unsignedInt16.get(nullElementRow))).isEqualTo(0);
            int nullElementStart = unsignedInt8Array.getElementStartIndex(nullElementRow);
            assertThat(unsignedInt8Array.getElementEndIndex(nullElementRow) - nullElementStart).isEqualTo(2);
            assertThat(Byte.toUnsignedInt(items.get(nullElementStart))).isEqualTo(1);
            assertThat(items.isNull(nullElementStart + 1)).isTrue();

            assertThat(reader.loadNextBatch()).isFalse();
        }
    }

    private static void assertListUnsignedInt8(ListVector unsignedInt8Array, UInt1Vector items, int row, int first, int second)
    {
        assertThat(unsignedInt8Array.isNull(row)).isFalse();
        int start = unsignedInt8Array.getElementStartIndex(row);
        assertThat(unsignedInt8Array.getElementEndIndex(row) - start).isEqualTo(2);
        assertThat(Byte.toUnsignedInt(items.get(start))).isEqualTo(first);
        assertThat(Byte.toUnsignedInt(items.get(start + 1))).isEqualTo(second);
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
