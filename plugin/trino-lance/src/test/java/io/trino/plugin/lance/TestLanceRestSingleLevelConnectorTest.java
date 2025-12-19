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

import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Connector test for REST namespace with single_level_ns=true backend.
 * This mode accesses 1st level (root) with a virtual "default" schema.
 * CREATE SCHEMA is not allowed in this mode.
 */
public class TestLanceRestSingleLevelConnectorTest
        extends BaseLanceConnectorTest
{
    @Override
    protected LanceNamespaceTestConfig getNamespaceTestConfig()
    {
        return LanceNamespaceTestConfig.REST_SINGLE_LEVEL;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return LanceQueryRunner.builderForRest(getNamespaceTestConfig())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Test
    @Override
    public void testCreateSchemaWithLongName()
    {
        // REST namespace has URL length limits that prevent very long schema names
        abort("REST namespace has URL length limits for schema names");
    }
}
