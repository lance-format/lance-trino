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

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_MATERIALIZED_VIEW_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_VIEW_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_MATERIALIZED_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_MERGE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_LEVEL_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TRUNCATE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_UPDATE;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke test for the Lance connector extending BaseConnectorSmokeTest.
 * This test verifies basic connector functionality including read and write operations.
 */
public class TestLanceConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return LanceQueryRunner.builderForWriteTests()
                .setInitialTables(NATION, REGION)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            // Supported behaviors
            case SUPPORTS_CREATE_TABLE,
                    SUPPORTS_CREATE_TABLE_WITH_DATA,
                    SUPPORTS_INSERT -> true;

            // Not supported behaviors
            case SUPPORTS_CREATE_SCHEMA,
                    SUPPORTS_RENAME_SCHEMA,
                    SUPPORTS_RENAME_TABLE,
                    SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                    SUPPORTS_DELETE,
                    SUPPORTS_ROW_LEVEL_DELETE,
                    SUPPORTS_UPDATE,
                    SUPPORTS_TRUNCATE,
                    SUPPORTS_MERGE,
                    SUPPORTS_CREATE_VIEW,
                    SUPPORTS_COMMENT_ON_VIEW_COLUMN,
                    SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_COMMENT_ON_MATERIALIZED_VIEW_COLUMN -> false;

            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("CREATE TABLE lance\\." + schemaName + "\\.region \\(\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        "\\)");
    }

    @Test
    public void testCreateTableAndInsert()
    {
        String tableName = "test_create_insert_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id bigint, name varchar)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'test')", 1);
            assertQuery("SELECT * FROM " + tableName, "SELECT CAST(1 AS BIGINT), CAST('test' AS VARCHAR)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateTableAsSelect()
    {
        String tableName = "test_ctas_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName + " AS SELECT regionkey, name FROM region", 5);
            assertQuery("SELECT COUNT(*) FROM " + tableName, "SELECT 5");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateOrReplaceTable()
    {
        String tableName = "test_create_or_replace_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName + " AS SELECT BIGINT '1' a, VARCHAR 'one' b", 1);
            assertQuery("SELECT a, b FROM " + tableName, "SELECT CAST(1 AS BIGINT), CAST('one' AS VARCHAR)");

            // Replace with different data
            assertUpdate("CREATE OR REPLACE TABLE " + tableName + " AS SELECT BIGINT '2' a, VARCHAR 'two' b", 1);
            assertQuery("SELECT a, b FROM " + tableName, "SELECT CAST(2 AS BIGINT), CAST('two' AS VARCHAR)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testInsertMultipleRows()
    {
        String tableName = "test_insert_multi_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id bigint, value double)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1.5), (2, 2.5), (3, 3.5)", 3);
            assertQuery("SELECT COUNT(*) FROM " + tableName, "SELECT 3");
            assertQuery("SELECT SUM(value) FROM " + tableName, "SELECT 7.5e0");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
