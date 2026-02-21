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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestLanceCountPageSource
{
    private static final SchemaTableName TEST_TABLE_1 = new SchemaTableName("default", "test_table1");

    private LanceMetadata metadata;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        URL lanceURL = Resources.getResource(TestLanceCountPageSource.class, "/example_db");
        assertThat(lanceURL)
                .describedAs("example db is null")
                .isNotNull();
        LanceConfig lanceConfig = new LanceConfig()
                .setSingleLevelNs(true);
        Map<String, String> catalogProperties = ImmutableMap.of("lance.root", lanceURL.toString());
        LanceNamespaceHolder namespaceHolder = new LanceNamespaceHolder(lanceConfig, catalogProperties);
        JsonCodec<LanceCommitTaskData> commitTaskDataCodec = JsonCodec.jsonCodec(LanceCommitTaskData.class);
        JsonCodec<LanceMergeCommitData> mergeCommitDataCodec = JsonCodec.jsonCodec(LanceMergeCommitData.class);
        this.metadata = new LanceMetadata(namespaceHolder, lanceConfig, commitTaskDataCodec, mergeCommitDataCodec);
    }

    @Test
    public void testCountStarWithoutFilter()
    {
        // Test COUNT(*) without filter - uses ManifestSummary for row count
        ConnectorTableHandle tableHandle = metadata.getTableHandle(TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;

        // Create a COUNT(*) table handle
        LanceTableHandle countHandle = lanceTableHandle.withCountStar();

        try (LanceCountPageSource pageSource = new LanceCountPageSource(
                countHandle,
                Collections.emptyMap(),
                null)) {
            assertThat(pageSource.isFinished()).isFalse();

            Page page = pageSource.getNextPage();
            assertThat(page).isNotNull();
            assertThat(page.getChannelCount()).isEqualTo(1);
            assertThat(page.getPositionCount()).isEqualTo(1);

            // test_table1 has 4 rows total (2 fragments x 2 rows each)
            long count = BIGINT.getLong(page.getBlock(0), 0);
            assertThat(count).isEqualTo(4L);

            // Second call should return null
            assertThat(pageSource.getNextPage()).isNull();
            assertThat(pageSource.isFinished()).isTrue();
        }
    }
}
