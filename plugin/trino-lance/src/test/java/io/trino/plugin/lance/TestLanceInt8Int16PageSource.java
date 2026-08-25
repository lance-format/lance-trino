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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.ArrayType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.WriteParams;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLanceInt8Int16PageSource
{
    private static final Schema SCHEMA = new Schema(
            List.of(
                    Field.nullable("id", new ArrowType.Int(32, true)),
                    Field.nullable("signed_int8", new ArrowType.Int(8, true)),
                    Field.nullable("unsigned_int8", new ArrowType.Int(8, false)),
                    Field.nullable("signed_int16", new ArrowType.Int(16, true)),
                    Field.nullable("unsigned_int16", new ArrowType.Int(16, false)),
                    new Field("signed_int8_array", FieldType.nullable(new ArrowType.List()),
                            List.of(Field.nullable("item", new ArrowType.Int(8, true))))),
            null);

    @Test
    public void testReadSignedAndUnsignedInt8AndInt16(@TempDir Path tempDir)
            throws Exception
    {
        String datasetPath = tempDir.resolve("int8_int16_test.lance").toString();

        try (BufferAllocator allocator = new RootAllocator()) {
            Dataset.create(allocator, datasetPath, SCHEMA, new WriteParams.Builder().build()).close();

            try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
                root.allocateNew();
                IntVector id = (IntVector) root.getVector("id");
                TinyIntVector signedInt8 = (TinyIntVector) root.getVector("signed_int8");
                UInt1Vector unsignedInt8 = (UInt1Vector) root.getVector("unsigned_int8");
                SmallIntVector signedInt16 = (SmallIntVector) root.getVector("signed_int16");
                UInt2Vector unsignedInt16 = (UInt2Vector) root.getVector("unsigned_int16");
                ListVector signedInt8Array = (ListVector) root.getVector("signed_int8_array");
                TinyIntVector items = (TinyIntVector) signedInt8Array.getDataVector();

                id.setSafe(0, 1);
                signedInt8.setSafe(0, (byte) -128);
                unsignedInt8.setSafe(0, 255);
                signedInt16.setSafe(0, (short) -32768);
                unsignedInt16.setSafe(0, 65535);
                signedInt8Array.startNewValue(0);
                items.setSafe(0, (byte) 1);
                items.setSafe(1, (byte) 0);
                items.setSafe(2, (byte) -1);
                signedInt8Array.endValue(0, 3);

                id.setSafe(1, 2);
                signedInt8.setSafe(1, (byte) 127);
                unsignedInt8.setSafe(1, 0);
                signedInt16.setSafe(1, (short) 32767);
                unsignedInt16.setSafe(1, 0);
                signedInt8Array.startNewValue(1);
                items.setSafe(3, (byte) 127);
                signedInt8Array.endValue(1, 1);

                root.setRowCount(2);

                List<FragmentMetadata> fragments = Fragment.create(
                        datasetPath, allocator, root, new WriteParams.Builder().build());
                try (Dataset appended = Dataset.commit(
                        allocator, datasetPath, new FragmentOperation.Append(fragments), Optional.of(1L))) {
                    assertThat(appended.countRows()).isEqualTo(2);
                }
            }

            LanceRuntime runtime = new LanceRuntime(
                    new LanceConfig().setSingleLevelNs(true),
                    ImmutableMap.of("lance.root", tempDir.toString()));

            Map<String, ColumnHandle> columnHandles = runtime.getColumnHandles(null, datasetPath, null, Map.of());
            assertThat(columnHandles).hasSize(6);
            assertThat(((LanceColumnHandle) columnHandles.get("signed_int8")).trinoType()).isEqualTo(TINYINT);
            assertThat(((LanceColumnHandle) columnHandles.get("unsigned_int8")).trinoType()).isEqualTo(SMALLINT);
            assertThat(((LanceColumnHandle) columnHandles.get("signed_int16")).trinoType()).isEqualTo(SMALLINT);
            assertThat(((LanceColumnHandle) columnHandles.get("unsigned_int16")).trinoType()).isEqualTo(INTEGER);
            assertThat(((LanceColumnHandle) columnHandles.get("signed_int8_array")).trinoType()).isEqualTo(new ArrayType(TINYINT));

            LanceTableHandle tableHandle = new LanceTableHandle(
                    "default", "int8_int16_test", datasetPath, List.of("int8_int16_test"), Map.of());
            var splitSource = new LanceSplitManager(runtime).getSplits(null, SESSION, tableHandle, null, null);
            LanceSplit split = (LanceSplit) splitSource.getNextBatch(10).get().getSplits().get(0);

            List<LanceColumnHandle> columns = runtime.getColumnHandleList(null, datasetPath, null, Map.of());
            try (LanceFragmentPageSource pageSource = new LanceFragmentPageSource(
                    tableHandle, columns, split.getFragments(), Map.of(), 8192, null, runtime)) {
                Page page = pageSource.getNextPage();
                assertThat(page).isNotNull();
                assertThat(page.getPositionCount()).isEqualTo(2);

                int signedInt8Channel = columnIndex(columns, "signed_int8");
                int unsignedInt8Channel = columnIndex(columns, "unsigned_int8");
                int signedInt16Channel = columnIndex(columns, "signed_int16");
                int unsignedInt16Channel = columnIndex(columns, "unsigned_int16");
                int signedInt8ArrayChannel = columnIndex(columns, "signed_int8_array");

                assertThat(TINYINT.getLong(page.getBlock(signedInt8Channel), 0)).isEqualTo(-128L);
                assertThat(TINYINT.getLong(page.getBlock(signedInt8Channel), 1)).isEqualTo(127L);
                assertThat(SMALLINT.getLong(page.getBlock(unsignedInt8Channel), 0)).isEqualTo(255L);
                assertThat(SMALLINT.getLong(page.getBlock(unsignedInt8Channel), 1)).isEqualTo(0L);
                assertThat(SMALLINT.getLong(page.getBlock(signedInt16Channel), 0)).isEqualTo(-32768L);
                assertThat(SMALLINT.getLong(page.getBlock(signedInt16Channel), 1)).isEqualTo(32767L);
                assertThat(INTEGER.getLong(page.getBlock(unsignedInt16Channel), 0)).isEqualTo(65535L);
                assertThat(INTEGER.getLong(page.getBlock(unsignedInt16Channel), 1)).isEqualTo(0L);

                Block signedInt8ArrayRow0 = (Block) new ArrayType(TINYINT).getObject(page.getBlock(signedInt8ArrayChannel), 0);
                assertThat(signedInt8ArrayRow0.getPositionCount()).isEqualTo(3);
                assertThat(TINYINT.getLong(signedInt8ArrayRow0, 0)).isEqualTo(1L);
                assertThat(TINYINT.getLong(signedInt8ArrayRow0, 1)).isEqualTo(0L);
                assertThat(TINYINT.getLong(signedInt8ArrayRow0, 2)).isEqualTo(-1L);

                Block signedInt8ArrayRow1 = (Block) new ArrayType(TINYINT).getObject(page.getBlock(signedInt8ArrayChannel), 1);
                assertThat(signedInt8ArrayRow1.getPositionCount()).isEqualTo(1);
                assertThat(TINYINT.getLong(signedInt8ArrayRow1, 0)).isEqualTo(127L);
            }
        }
    }

    private static int columnIndex(List<LanceColumnHandle> columns, String name)
    {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(name)) {
                return i;
            }
        }
        throw new AssertionError("missing column: " + name);
    }
}
