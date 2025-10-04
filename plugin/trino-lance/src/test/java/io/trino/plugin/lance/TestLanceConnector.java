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

import static org.assertj.core.api.Assertions.assertThat;

// TODO Extend io.trino.testing.BaseConnectorTest
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
        assertThat(query("SHOW COLUMNS FROM test_table1"))
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
    public void testSelect()
    {
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
    public void testProjection()
    {
        assertThat(query("SELECT b FROM test_table1"))
                .matches("VALUES BIGINT '0', 1, 2, 3");
    }

    @Test
    public void testFilter()
    {
        assertThat(query("SELECT * FROM test_table1 WHERE b = 0"))
                .matches("VALUES (BIGINT '0', BIGINT '0', BIGINT '0', BIGINT '0')");
    }

    @Test
    public void testLimit()
    {
        assertThat(query("SELECT * FROM test_table1 LIMIT 100"))
                .matches("SELECT * FROM test_table1");
    }
}
