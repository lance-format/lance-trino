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
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.WriteParams;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for LargeUtf8 (large varchar) support in the Lance connector.
 */
public class TestLargeUtf8Support
{
    private static final Schema LARGE_UTF8_SCHEMA = new Schema(
            Arrays.asList(
                    Field.nullable("id", new ArrowType.Int(32, true)),
                    Field.nullable("large_text", ArrowType.LargeUtf8.INSTANCE)),
            null);

    @Test
    public void testToTrinoTypeWithLargeUtf8()
    {
        // Test that LargeUtf8 is correctly converted to VARCHAR
        assertThat(LanceColumnHandle.toTrinoType(ArrowType.LargeUtf8.INSTANCE))
                .isEqualTo(VarcharType.VARCHAR);

        // Also verify regular Utf8 still works
        assertThat(LanceColumnHandle.toTrinoType(ArrowType.Utf8.INSTANCE))
                .isEqualTo(VarcharType.VARCHAR);
    }

    @Test
    public void testReadLargeUtf8Dataset(@TempDir Path tempDir)
    {
        String datasetPath = tempDir.resolve("large_utf8_test.lance").toString();

        // Step 1: Create a Lance dataset with LargeUtf8 column using the Java SDK
        try (BufferAllocator allocator = new RootAllocator()) {
            // Create empty dataset with schema
            Dataset dataset = Dataset.create(allocator, datasetPath, LARGE_UTF8_SCHEMA,
                    new WriteParams.Builder().build());
            dataset.close();

            // Write some data
            try (VectorSchemaRoot root = VectorSchemaRoot.create(LARGE_UTF8_SCHEMA, allocator)) {
                root.allocateNew();
                IntVector idVector = (IntVector) root.getVector("id");
                LargeVarCharVector textVector = (LargeVarCharVector) root.getVector("large_text");

                // Add test data
                for (int i = 0; i < 5; i++) {
                    idVector.setSafe(i, i);
                    String text = "Large text value " + i;
                    textVector.setSafe(i, text.getBytes(StandardCharsets.UTF_8));
                }
                root.setRowCount(5);

                List<FragmentMetadata> fragments = Fragment.create(
                        datasetPath,
                        allocator,
                        root,
                        new WriteParams.Builder().build());

                FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments);
                try (Dataset appendedDataset = Dataset.commit(allocator, datasetPath, appendOp, Optional.of(1L))) {
                    assertThat(appendedDataset.countRows()).isEqualTo(5);
                }
            }

            // Step 2: Read the dataset using LanceReader and verify schema mapping
            LanceConfig config = new LanceConfig();
            Map<String, String> catalogProperties = ImmutableMap.of("lance.root", tempDir.toString());
            LanceReader reader = new LanceReader(config, catalogProperties);

            // Get column handles
            Map<String, ColumnHandle> columnHandles = reader.getColumnHandle("large_utf8_test");
            assertThat(columnHandles).hasSize(2);

            // Verify the large_text column is mapped to VARCHAR
            LanceColumnHandle largeTextHandle = (LanceColumnHandle) columnHandles.get("large_text");
            assertThat(largeTextHandle).isNotNull();
            assertThat(largeTextHandle.trinoType()).isEqualTo(VarcharType.VARCHAR);

            // Verify id column
            LanceColumnHandle idHandle = (LanceColumnHandle) columnHandles.get("id");
            assertThat(idHandle).isNotNull();
            assertThat(idHandle.trinoType()).isEqualTo(INTEGER);

            // Step 3: Verify table metadata
            List<ColumnMetadata> columnsMetadata = reader.getColumnsMetadata("large_utf8_test");
            assertThat(columnsMetadata).hasSize(2);

            // Find the large_text column metadata
            ColumnMetadata largeTextMetadata = columnsMetadata.stream()
                    .filter(cm -> cm.getName().equals("large_text"))
                    .findFirst()
                    .orElseThrow();
            assertThat(largeTextMetadata.getType()).isEqualTo(VarcharType.VARCHAR);
        }
    }

    @Test
    public void testLanceMetadataWithLargeUtf8(@TempDir Path tempDir)
    {
        String datasetPath = tempDir.resolve("metadata_test.lance").toString();

        try (BufferAllocator allocator = new RootAllocator()) {
            // Create and populate dataset
            Dataset dataset = Dataset.create(allocator, datasetPath, LARGE_UTF8_SCHEMA,
                    new WriteParams.Builder().build());
            dataset.close();

            try (VectorSchemaRoot root = VectorSchemaRoot.create(LARGE_UTF8_SCHEMA, allocator)) {
                root.allocateNew();
                IntVector idVector = (IntVector) root.getVector("id");
                LargeVarCharVector textVector = (LargeVarCharVector) root.getVector("large_text");

                idVector.setSafe(0, 1);
                textVector.setSafe(0, "test".getBytes(StandardCharsets.UTF_8));
                root.setRowCount(1);

                List<FragmentMetadata> fragments = Fragment.create(datasetPath, allocator, root,
                        new WriteParams.Builder().build());
                FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments);
                Dataset.commit(allocator, datasetPath, appendOp, Optional.of(1L)).close();
            }

            // Create LanceMetadata and verify it can read the table
            LanceConfig config = new LanceConfig();
            Map<String, String> catalogProperties = ImmutableMap.of("lance.root", tempDir.toString());
            LanceReader reader = new LanceReader(config, catalogProperties);
            LanceMetadata metadata = new LanceMetadata(reader, config);

            // Get table handle - this should NOT return null anymore
            LanceTableHandle tableHandle = (LanceTableHandle) metadata.getTableHandle(
                    SESSION,
                    new io.trino.spi.connector.SchemaTableName("default", "metadata_test"),
                    Optional.empty(),
                    Optional.empty());
            assertThat(tableHandle).isNotNull();

            // Get table metadata - this should NOT return null anymore
            var tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
            assertThat(tableMetadata)
                    .describedAs("getTableMetadata should not return null for LargeUtf8 columns")
                    .isNotNull();
            assertThat(tableMetadata.getColumns()).hasSize(2);
        }
    }
}
