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
import org.junit.jupiter.api.Disabled;

/**
 * Connector test for S3 directory namespace with single_level_ns=true.
 * This mode accesses 1st level (root) with a virtual "default" schema.
 * CREATE SCHEMA is not allowed in this mode.
 * Requires LocalStack to be running locally via docker-compose.
 *
 * <p>Disabled due to S3 eventual consistency issues with table listing.
 * See: https://github.com/lancedb/lance-trino/issues/XXX
 */
@Disabled("S3 eventual consistency issues with table listing - to be fixed in follow-up")
public class TestLanceS3SingleLevelConnectorTest
        extends BaseLanceConnectorTest
{
    @Override
    protected LanceNamespaceTestConfig getNamespaceTestConfig()
    {
        return LanceNamespaceTestConfig.S3_SINGLE_LEVEL;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return LanceQueryRunner.builderForConfig(getNamespaceTestConfig())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }
}
