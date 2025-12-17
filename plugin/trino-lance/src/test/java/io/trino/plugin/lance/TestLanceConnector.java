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
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import static org.assertj.core.api.Assertions.assertThat;

// TODO: Extend io.trino.testing.BaseConnectorTest once we have write capability
// to generate TPCH test data. BaseConnectorTest requires TPCH tables (nation, region,
// customer, orders, lineitem, part, supplier, partsupp) to be populated.
// See TestLanceConnectorTest in ~/oss/trino for reference implementation.
//
// Note: This test is disabled on macOS due to a Jetty ServerConnector issue
// (ClosedChannelException) that occurs on macOS. Trino's CI only tests on Linux,
// so this is consistent with upstream behavior.
@DisabledOnOs(value = OS.MAC, disabledReason = "Jetty ServerConnector fails on macOS with ClosedChannelException")
public class TestLanceConnector
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return LanceQueryRunner.builder().build();
    }

    @Test
    public void testShowSchemas()
    {
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet())
                .containsExactly("default", "information_schema");
    }

    @Test
    public void testShowTables()
    {
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .containsExactly("test_table1", "test_table2", "test_table3", "test_table4");
    }

    @Test
    public void testShowColumns()
    {
        // Column order in test data: x, y, b, c
        assertThat(query("SHOW COLUMNS FROM test_table1"))
                .skippingTypesCheck()
                .matches("""
                        VALUES
                        ('x', 'bigint', '', ''),
                        ('y', 'bigint', '', ''),
                        ('b', 'bigint', '', ''),
                        ('c', 'bigint', '', '')
                        """);
    }

    @Test
    public void testSelect()
    {
        // SELECT * returns columns in alphabetical order: b, c, x, y
        // Row 0: b=0, c=0, x=0, y=0
        // Row 1: b=1, c=2, x=3, y=-1
        // Row 2: b=2, c=4, x=6, y=-2
        // Row 3: b=3, c=6, x=9, y=-3
        assertThat(query("SELECT * FROM test_table1"))
                .matches("""
                        VALUES
                        (BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0'),
                        (1, 2, 3, -1),
                        (2, 4, 6, -2),
                        (3, 6, 9, -3)
                        """);
    }

    @Test
    public void testSelectWithPredicate()
    {
        // Filter is applied by Trino engine, not pushed down to connector
        // WHERE b > 1 means b=2,3 => rows with b=2,3 (columns in alphabetical order: b, c, x, y)
        // Data: b=0,1,2,3 / c=0,2,4,6 / x=0,3,6,9 / y=0,-1,-2,-3
        assertThat(query("SELECT * FROM test_table1 WHERE b > 1"))
                .matches("""
                        VALUES
                        (BIGINT '2', BIGINT '4', BIGINT '6', BIGINT '-2'),
                        (3, 6, 9, -3)
                        """);
    }

    @Test
    public void testSelectWithOrderBy()
    {
        // ORDER BY b DESC: b=3,2,1,0 (columns in order: b, c, x, y)
        assertThat(query("SELECT * FROM test_table1 ORDER BY b DESC"))
                .matches("""
                        VALUES
                        (BIGINT '3', BIGINT '6', BIGINT '9', BIGINT '-3'),
                        (2, 4, 6, -2),
                        (1, 2, 3, -1),
                        (0, 0, 0, 0)
                        """);
    }

    @Test
    public void testSelectWithLimit()
    {
        // ORDER BY b LIMIT 2: b=0,1 (columns in order: b, c, x, y)
        assertThat(query("SELECT * FROM test_table1 ORDER BY b LIMIT 2"))
                .matches("""
                        VALUES
                        (BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0'),
                        (1, 2, 3, -1)
                        """);
    }

    @Test
    public void testSelectWithAggregation()
    {
        assertThat(query("SELECT COUNT(*) FROM test_table1"))
                .matches("VALUES (BIGINT '4')");

        // SUM of column b: 0 + 1 + 2 + 3 = 6
        assertThat(query("SELECT SUM(b) FROM test_table1"))
                .matches("VALUES (BIGINT '6')");

        // AVG of column c: (0 + 2 + 4 + 6) / 4 = 3.0
        assertThat(query("SELECT AVG(c) FROM test_table1"))
                .matches("VALUES (3.0e0)");
    }

    @Test
    public void testSelectWithGroupBy()
    {
        // Group by column c which has values: 0, 2, 4, 6
        assertThat(query("SELECT c, COUNT(*) FROM test_table1 GROUP BY c ORDER BY c"))
                .matches("""
                        VALUES
                        (BIGINT '0', BIGINT '1'),
                        (2, 1),
                        (4, 1),
                        (6, 1)
                        """);
    }

    @Test
    public void testSelectWithProjection()
    {
        // Test selecting columns in different order than schema (schema is x, y, b, c)
        // b values: 0, 1, 2, 3; x values: 0, 3, 6, 9
        assertThat(query("SELECT b, x FROM test_table1 ORDER BY b"))
                .matches("""
                        VALUES
                        (BIGINT '0', BIGINT '0'),
                        (1, 3),
                        (2, 6),
                        (3, 9)
                        """);
    }

    @Test
    public void testSelectCount()
    {
        // Verify we can query different tables
        assertThat(query("SELECT COUNT(*) FROM test_table1"))
                .matches("VALUES (BIGINT '4')");
    }

    @Test
    public void testDescribeTable()
    {
        assertThat(query("DESCRIBE test_table1"))
                .skippingTypesCheck()
                .matches("""
                        VALUES
                        ('b', 'bigint', '', ''),
                        ('c', 'bigint', '', ''),
                        ('x', 'bigint', '', ''),
                        ('y', 'bigint', '', '')
                        """);
    }

    @Test
    public void testShowCreateTable()
    {
        String result = (String) computeScalar("SHOW CREATE TABLE test_table1");
        assertThat(result)
                .contains("CREATE TABLE lance.default.test_table1")
                .contains("b bigint")
                .contains("c bigint")
                .contains("x bigint")
                .contains("y bigint");
    }

    @Test
    public void testInformationSchema()
    {
        // Test information_schema.tables
        assertThat(query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'default' ORDER BY table_name"))
                .skippingTypesCheck()
                .matches("""
                        VALUES
                        ('test_table1'),
                        ('test_table2'),
                        ('test_table3'),
                        ('test_table4')
                        """);

        // Test information_schema.columns
        assertThat(query("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_table1' ORDER BY ordinal_position"))
                .skippingTypesCheck()
                .matches("""
                        VALUES
                        ('b'),
                        ('c'),
                        ('x'),
                        ('y')
                        """);
    }

    @Test
    public void testNonExistentTable()
    {
        assertThat(query("SELECT * FROM non_existent_table"))
                .failure()
                .hasMessageContaining("Table 'lance.default.non_existent_table' does not exist");
    }

    @Test
    public void testNonExistentSchema()
    {
        assertThat(query("SELECT * FROM non_existent_schema.some_table"))
                .failure()
                .hasMessageContaining("Schema 'non_existent_schema' does not exist");
    }
}
