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

import org.junit.jupiter.api.Disabled;

/**
 * Smoke test for REST namespace in default mode backend.
 *
 * Note: This test is currently disabled until a REST server test container
 * is available.
 */
@Disabled("REST server test container not yet implemented")
public class TestLanceRestDefaultSmokeTest
        extends BaseLanceRestSmokeTest
{
    @Override
    protected LanceNamespaceTestConfig getNamespaceTestConfig()
    {
        return LanceNamespaceTestConfig.REST_DEFAULT;
    }
}
