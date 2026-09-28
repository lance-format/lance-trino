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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.WriteParams;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLanceBlobV2Columns
        extends AbstractTestQueryFramework
{
    private static final String TABLE = "blob_v2_table";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path root = Files.createTempDirectory("lance-blob-v2-test");
        closeAfterClass(() -> deleteRecursively(root, ALLOW_INSECURE));
        writeBlobV2Dataset(root.resolve(TABLE + ".lance").toString());
        return LanceQueryRunner.builder()
                .addConnectorProperty("lance.root", root.toUri().toString())
                .build();
    }

    private static void writeBlobV2Dataset(String datasetPath)
    {
        Field blob = new Field(
                "img",
                new FieldType(true, ArrowType.Struct.INSTANCE, null,
                        Map.of(BlobUtils.ARROW_EXTENSION_NAME_KEY, BlobUtils.LANCE_BLOB_V2_EXTENSION_NAME)),
                List.of(
                        new Field("data", FieldType.nullable(ArrowType.LargeBinary.INSTANCE), null),
                        new Field("uri", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
        Schema schema = new Schema(List.of(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null), blob));
        WriteParams params = new WriteParams.Builder().withDataStorageVersion("2.2").build();

        try (BufferAllocator allocator = new RootAllocator()) {
            Dataset.create(allocator, datasetPath, schema, params).close();
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                root.allocateNew();
                IntVector id = (IntVector) root.getVector("id");
                StructVector img = (StructVector) root.getVector("img");
                LargeVarBinaryVector data = (LargeVarBinaryVector) img.getChild("data");
                VarCharVector uri = (VarCharVector) img.getChild("uri");

                String[] values = {"HELLO", "WORLD!", null};
                for (int i = 0; i < values.length; i++) {
                    id.setSafe(i, i);
                    if (values[i] == null) {
                        img.setNull(i);
                    }
                    else {
                        img.setIndexDefined(i);
                        data.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
                        uri.setNull(i);
                    }
                }
                root.setRowCount(values.length);

                List<FragmentMetadata> fragments = Fragment.create(datasetPath, allocator, root, params);
                Dataset.commit(allocator, datasetPath, new FragmentOperation.Append(fragments), Optional.of(1L)).close();
            }
        }
    }

    @Test
    public void testBlobV2ColumnMapsToDescriptorRow()
    {
        MaterializedResult result = computeActual("DESCRIBE " + TABLE);
        assertThat(result.getMaterializedRows())
                .anySatisfy(row -> {
                    assertThat(row.getField(0)).isEqualTo("img");
                    assertThat(row.getField(1)).isEqualTo(BlobUtils.BLOB_V2_DESCRIPTOR_TYPE.getDisplayName());
                });
        assertThat(result.getMaterializedRows())
                .noneSatisfy(row -> assertThat((String) row.getField(0)).startsWith("img__blob_"));
    }

    @Test
    public void testSelectBlobV2Descriptor()
    {
        assertQuery(
                "SELECT id, img.kind, img.size, img.blob_id, img.blob_uri, img.position IS NOT NULL FROM " + TABLE,
                "VALUES (0, 0, 5, 0, '', true), (1, 0, 6, 0, '', true), (2, NULL, NULL, NULL, NULL, false)");
        assertQuery("SELECT id FROM " + TABLE + " WHERE img IS NULL", "VALUES 2");
        assertThat(computeActual("SELECT * FROM " + TABLE).getRowCount()).isEqualTo(3);
    }

    @Test
    public void testDistinctBlobV2DescriptorsAreNotEqual()
    {
        assertQuery("SELECT count(DISTINCT img) FROM " + TABLE, "VALUES 2");
    }

    @Test
    public void testBlobV2HasNoVirtualColumns()
    {
        assertQueryFails("SELECT img__blob_size FROM " + TABLE, ".*Column 'img__blob_size' cannot be resolved.*");
        assertQueryFails("SELECT img__blob_pos FROM " + TABLE, ".*Column 'img__blob_pos' cannot be resolved.*");
    }

    @Test
    public void testFilterPhysicalColumnWithBlobV2Column()
    {
        assertQuery("SELECT id FROM " + TABLE + " WHERE id = 1", "VALUES 1");
        assertQuery("SELECT img.size FROM " + TABLE + " WHERE id = 1", "VALUES 6");
        assertQuery("SELECT id FROM " + TABLE + " WHERE img.size = 6", "VALUES 1");
        assertQuery("SELECT id FROM " + TABLE + " WHERE img.kind = 0 AND img.size > 5", "VALUES 1");
        assertExplain(
                "EXPLAIN SELECT id FROM " + TABLE + " WHERE id = 1",
                "constraint.{0,10}(id|ID)");
        assertQuery("SELECT id FROM " + TABLE + " WHERE id < 2 AND img.size = 6", "VALUES 1");
    }

    @Test
    public void testWriteToBlobV2ColumnNotSupported()
    {
        assertQueryFails(
                "INSERT INTO " + TABLE + " VALUES (3, NULL)",
                ".*Writing to Lance blob v2 column 'img' is not supported.*");
        assertQueryFails(
                "UPDATE " + TABLE + " SET id = 10 WHERE id = 0",
                ".*Writing to Lance blob v2 column 'img' is not supported.*");
        assertQuery("SELECT id FROM " + TABLE, "VALUES 0, 1, 2");
    }
}
