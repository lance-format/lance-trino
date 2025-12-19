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

/**
 * Connector test for S3 directory namespace with parent prefix.
 * Uses LocalStack for S3 emulation.
 */
public class TestLanceS3WithParentConnectorTest
        extends BaseLanceS3ConnectorTest
{
    @Override
    protected LanceNamespaceTestConfig getNamespaceTestConfig()
    {
        return LanceNamespaceTestConfig.S3_WITH_PARENT;
    }
}
