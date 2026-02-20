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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLanceBlobEncoding
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return LanceQueryRunner.builder()
                .setUseTempDirectory(true)
                .build();
    }

    @Test
    public void testCreateTableWithBlobEncoding()
    {
        String tableName = "test_blob_create_" + System.currentTimeMillis();
        try {
            // Create table with blob encoding using table property
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, content VARBINARY) " +
                    "WITH (blob_columns = 'content')");

            // Verify table was created
            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(0L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateTableAsSelectWithBlobEncoding()
    {
        String tableName = "test_blob_ctas_" + System.currentTimeMillis();
        try {
            // Create table with blob encoding via CTAS
            assertUpdate("CREATE TABLE " + tableName + " " +
                            "WITH (blob_columns = 'content') AS " +
                            "SELECT CAST(1 AS BIGINT) as id, X'48454C4C4F' as content",
                    1);

            // Verify data was inserted
            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(1L);
            assertThat(computeScalar("SELECT id FROM " + tableName)).isEqualTo(1L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testInsertIntoBlobColumn()
    {
        String tableName = "test_blob_insert_" + System.currentTimeMillis();
        try {
            // Create table with blob encoding
            assertUpdate("CREATE TABLE " + tableName + " " +
                            "WITH (blob_columns = 'content') AS " +
                            "SELECT CAST(1 AS BIGINT) as id, X'48454C4C4F' as content",
                    1);

            // Insert more data
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, X'574F524C44')", 1);

            // Verify all data
            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(2L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testBlobColumnTypeValidation()
    {
        String tableName = "test_blob_type_validation_" + System.currentTimeMillis();
        // Blob encoding should fail for non-VARBINARY columns
        assertQueryFails(
                "CREATE TABLE " + tableName + " (id BIGINT, content VARCHAR) " +
                        "WITH (blob_columns = 'content')",
                ".*must have VARBINARY type.*");
    }

    @Test
    public void testMultipleBlobColumns()
    {
        String tableName = "test_multi_blob_" + System.currentTimeMillis();
        try {
            // Create table with multiple blob columns
            assertUpdate("CREATE TABLE " + tableName + " " +
                            "WITH (blob_columns = 'content1, content2') AS " +
                            "SELECT CAST(1 AS BIGINT) as id, X'41' as content1, X'42' as content2",
                    1);

            // Verify table was created
            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(1L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testBlobWithMixedColumns()
    {
        String tableName = "test_blob_mixed_" + System.currentTimeMillis();
        try {
            // Create table with mix of blob and non-blob columns
            assertUpdate("CREATE TABLE " + tableName + " " +
                            "WITH (blob_columns = 'blob_content') AS " +
                            "SELECT CAST(1 AS BIGINT) as id, " +
                            "       'regular text' as text_content, " +
                            "       X'424C4F42' as blob_content, " +
                            "       X'424C4F42' as regular_binary",
                    1);

            // Verify data
            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(1L);
            assertThat(computeScalar("SELECT text_content FROM " + tableName)).isEqualTo("regular text");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testBlobVirtualColumnsSelectable()
    {
        String tableName = "test_blob_virtual_select_" + System.currentTimeMillis();
        try {
            // Create table with blob encoding
            assertUpdate("CREATE TABLE " + tableName + " " +
                            "WITH (blob_columns = 'content') AS " +
                            "SELECT CAST(1 AS BIGINT) as id, X'48454C4C4F' as content",
                    1);

            // Virtual columns should be selectable by name
            // content__blob_size should be 5 (length of 'HELLO')
            assertThat(computeScalar("SELECT content__blob_size FROM " + tableName)).isEqualTo(5L);

            // content__blob_pos should be non-null
            assertThat(computeScalar("SELECT content__blob_pos FROM " + tableName)).isNotNull();

            // Should be able to select all together
            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName + " WHERE content__blob_size = 5")).isEqualTo(1L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testMultipleBlobVirtualColumns()
    {
        String tableName = "test_multi_blob_virtual_" + System.currentTimeMillis();
        try {
            // Create table with multiple blob columns
            assertUpdate("CREATE TABLE " + tableName + " " +
                            "WITH (blob_columns = 'data1, data2') AS " +
                            "SELECT CAST(1 AS BIGINT) as id, X'41' as data1, X'424344' as data2",
                    1);

            // Verify both blob columns have virtual columns
            assertThat(computeScalar("SELECT data1__blob_size FROM " + tableName)).isEqualTo(1L);
            assertThat(computeScalar("SELECT data2__blob_size FROM " + tableName)).isEqualTo(3L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
