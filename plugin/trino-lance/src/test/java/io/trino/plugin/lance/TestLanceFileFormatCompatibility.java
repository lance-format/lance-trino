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
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.lance.Dataset;
import org.lance.schema.LanceField;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLanceFileFormatCompatibility
        extends AbstractTestQueryFramework
{
    private Path root;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        root = Files.createTempDirectory("lance-format-compat");
        root.toFile().deleteOnExit();
        return LanceQueryRunner.builder()
                .addConnectorProperty("lance.root", root.toUri().toString())
                .addConnectorProperty("lance.single_level_ns", "true")
                .build();
    }

    @Test
    public void testDefaultCreateStoresBlobV2Metadata()
    {
        String tableName = "blob_v2_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName + " " +
                    "WITH (blob_columns = 'content') AS " +
                    "SELECT BIGINT '1' AS id, 'alpha' AS name, X'48454C4C4F' AS content",
                    1);

            assertThat(computeScalar("SELECT typeof(content) FROM " + tableName)).isEqualTo("varbinary");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName)).contains("blob_columns = 'content'");
            assertQuery(
                    "SELECT id, name, content__blob_size FROM " + tableName,
                    "SELECT CAST(1 AS BIGINT), 'alpha', CAST(5 AS BIGINT)");

            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'beta', X'574F524C44')", 1);
            assertQuery(
                    "SELECT id, name, content__blob_size FROM " + tableName,
                    "SELECT CAST(1 AS BIGINT), 'alpha', CAST(5 AS BIGINT) " +
                            "UNION ALL SELECT CAST(2 AS BIGINT), 'beta', CAST(5 AS BIGINT)");
            assertStoredBlob(tableName, "2.2", BlobUtils.ARROW_EXTENSION_NAME, BlobUtils.BLOB_V2_EXTENSION, true);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"legacy", "0.1", "2.0", "2.1", "2.2", "stable", "next"})
    public void testTruncate(String fileFormatVersion)
    {
        try (TestTable table = newTrinoTable("test_truncate", "WITH (file_format_version = '" + fileFormatVersion + "') AS SELECT 1 AS id")) {
            assertUpdate("TRUNCATE TABLE " + table.getName());
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
        }
    }

    @Test
    public void testMergeInsertAndCtasRoundTrip()
    {
        roundTrip("2.2");
        roundTrip("2.1");
    }

    private void roundTrip(String fileFormatVersion)
    {
        String tableName = "round_trip_" + fileFormatVersion.replace('.', '_') + "_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName + " " +
                    "WITH (file_format_version = '" + fileFormatVersion + "') AS " +
                    "SELECT BIGINT '1' AS id, 'alpha' AS name",
                    1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'beta')", 1);
            assertUpdate("MERGE INTO " + tableName + " t " +
                    "USING (SELECT * FROM (VALUES (1, 'renamed'), (3, 'gamma')) AS u(id, name)) s " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET name = s.name " +
                    "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
                    2);
            assertQuery(
                    "SELECT id, name FROM " + tableName,
                    "SELECT CAST(1 AS BIGINT), 'renamed' " +
                            "UNION ALL SELECT CAST(2 AS BIGINT), 'beta' " +
                            "UNION ALL SELECT CAST(3 AS BIGINT), 'gamma'");
            assertThat(storedFormat(tableName)).isEqualTo(fileFormatVersion);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private void assertStoredBlob(String tableName, String formatVersion, String metadataKey, String metadataValue, boolean blobV2)
    {
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = openDataset(allocator, tableName)) {
            assertThat(dataset.getLanceFileFormatVersion()).isEqualTo(formatVersion);
            LanceField content = dataset.getLanceSchema().fields().stream()
                    .filter(field -> field.getName().equals("content"))
                    .findFirst()
                    .orElseThrow();
            assertThat(content.getMetadata()).containsEntry(metadataKey, metadataValue);
            if (blobV2) {
                assertThat(content.getChildren()).extracting(LanceField::getName).containsExactly("data", "uri");
                assertThat(content.getChildren().get(0).getType()).isEqualTo(ArrowType.LargeBinary.INSTANCE);
                assertThat(content.getChildren().get(1).getType()).isEqualTo(new ArrowType.Utf8());
            }
        }
    }

    private String storedFormat(String tableName)
    {
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = openDataset(allocator, tableName)) {
            return dataset.getLanceFileFormatVersion();
        }
    }

    private Dataset openDataset(BufferAllocator allocator, String tableName)
    {
        return Dataset.open().allocator(allocator).uri(root.resolve(tableName + ".lance").toString()).build();
    }
}
