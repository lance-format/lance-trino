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
public class TestLanceVectorColumns
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
    public void testCreateTableWithVectorColumn()
    {
        String tableName = "test_vector_create_" + System.currentTimeMillis();
        try {
            // Create table with vector column using table property
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, embedding ARRAY(REAL)) " +
                    "WITH (vector_columns = 'embedding:768')");

            // Verify table was created
            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(0L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateTableAsSelectWithVectorColumn()
    {
        String tableName = "test_vector_ctas_" + System.currentTimeMillis();
        try {
            // Create table with vector column via CTAS
            assertUpdate("CREATE TABLE " + tableName + " " +
                            "WITH (vector_columns = 'embedding:3') AS " +
                            "SELECT CAST(1 AS BIGINT) as id, ARRAY[1.0E0, 2.0E0, 3.0E0] as embedding",
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
    public void testInsertIntoVectorColumn()
    {
        String tableName = "test_vector_insert_" + System.currentTimeMillis();
        try {
            // Create table with vector column
            assertUpdate("CREATE TABLE " + tableName + " " +
                            "WITH (vector_columns = 'embedding:3') AS " +
                            "SELECT CAST(1 AS BIGINT) as id, ARRAY[1.0E0, 2.0E0, 3.0E0] as embedding",
                    1);

            // Insert more data
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, ARRAY[4.0E0, 5.0E0, 6.0E0])", 1);

            // Verify all data
            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(2L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testVectorColumnTypeValidation()
    {
        String tableName = "test_vector_type_validation_" + System.currentTimeMillis();
        // Vector encoding should fail for non-ARRAY columns
        assertQueryFails(
                "CREATE TABLE " + tableName + " (id BIGINT, embedding BIGINT) " +
                        "WITH (vector_columns = 'embedding:768')",
                ".*must have ARRAY type.*");
    }

    @Test
    public void testVectorColumnElementTypeValidation()
    {
        String tableName = "test_vector_element_type_" + System.currentTimeMillis();
        // Vector encoding should fail for ARRAY with non-float element types
        assertQueryFails(
                "CREATE TABLE " + tableName + " (id BIGINT, embedding ARRAY(BIGINT)) " +
                        "WITH (vector_columns = 'embedding:768')",
                ".*must have ARRAY\\(REAL\\) or ARRAY\\(DOUBLE\\).*");
    }

    @Test
    public void testMultipleVectorColumns()
    {
        String tableName = "test_multi_vector_" + System.currentTimeMillis();
        try {
            // Create table with multiple vector columns
            assertUpdate("CREATE TABLE " + tableName + " " +
                            "WITH (vector_columns = 'embedding1:2, embedding2:3') AS " +
                            "SELECT CAST(1 AS BIGINT) as id, " +
                            "       ARRAY[1.0E0, 2.0E0] as embedding1, " +
                            "       ARRAY[3.0E0, 4.0E0, 5.0E0] as embedding2",
                    1);

            // Verify table was created
            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(1L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testVectorWithDoubleType()
    {
        String tableName = "test_vector_double_" + System.currentTimeMillis();
        try {
            // Create table with ARRAY(DOUBLE) vector column
            assertUpdate("CREATE TABLE " + tableName + " " +
                            "WITH (vector_columns = 'embedding:3') AS " +
                            "SELECT CAST(1 AS BIGINT) as id, " +
                            "       CAST(ARRAY[1.0E0, 2.0E0, 3.0E0] AS ARRAY(DOUBLE)) as embedding",
                    1);

            // Verify table was created
            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(1L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testVectorWithMixedColumns()
    {
        String tableName = "test_vector_mixed_" + System.currentTimeMillis();
        try {
            // Create table with mix of vector and non-vector columns
            assertUpdate("CREATE TABLE " + tableName + " " +
                            "WITH (vector_columns = 'vector_col:3') AS " +
                            "SELECT CAST(1 AS BIGINT) as id, " +
                            "       'regular text' as text_content, " +
                            "       ARRAY[1.0E0, 2.0E0, 3.0E0] as vector_col, " +
                            "       ARRAY[4.0E0, 5.0E0] as regular_array",
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
    public void testBlobAndVectorCombined()
    {
        String tableName = "test_blob_vector_combined_" + System.currentTimeMillis();
        try {
            // Create table with both blob and vector columns
            assertUpdate("CREATE TABLE " + tableName + " " +
                            "WITH (blob_columns = 'content', vector_columns = 'embedding:3') AS " +
                            "SELECT CAST(1 AS BIGINT) as id, " +
                            "       X'48454C4C4F' as content, " +
                            "       ARRAY[1.0E0, 2.0E0, 3.0E0] as embedding",
                    1);

            // Verify table was created
            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(1L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
