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

import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;

/**
 * Smoke test for directory namespace with single_level_ns=true.
 * This mode accesses 1st level (root) with a virtual "default" schema.
 */
public class TestLanceDirectorySingleLevelConnectorSmokeTest
        extends BaseLanceConnectorSmokeTest
{
    @Override
    protected LanceNamespaceTestConfig getNamespaceTestConfig()
    {
        return LanceNamespaceTestConfig.DIRECTORY_SINGLE_LEVEL;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return LanceQueryRunner.builderForConfig(getNamespaceTestConfig())
                .setInitialTables(NATION, REGION)
                .build();
    }
}
