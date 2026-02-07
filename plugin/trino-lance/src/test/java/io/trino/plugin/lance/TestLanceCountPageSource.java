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
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestLanceCountPageSource
{
    private static final SchemaTableName TEST_TABLE_1 = new SchemaTableName("default", "test_table1");

    private LanceMetadata metadata;
    private LanceSplitManager splitManager;

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
        this.metadata = new LanceMetadata(namespaceHolder, lanceConfig, commitTaskDataCodec);
        this.splitManager = new LanceSplitManager(namespaceHolder);
    }

    @Test
    public void testCountStarWithoutFilter()
    {
        // Test COUNT(*) without filter - should use ManifestSummary
        ConnectorTableHandle tableHandle = metadata.getTableHandle(null, TEST_TABLE_1, Optional.empty(), Optional.empty());
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;

        // Create a COUNT(*) table handle (no filter, no fragments)
        LanceTableHandle countHandle = lanceTableHandle.withCountStar();

        try (LanceCountPageSource pageSource = new LanceCountPageSource(
                countHandle,
                Collections.emptyMap(),
                Collections.emptyList())) {
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

    @Test
    public void testCountStarPerFragment()
            throws ExecutionException, InterruptedException
    {
        // Test COUNT(*) per fragment - simulates distributed count
        ConnectorTableHandle tableHandle = metadata.getTableHandle(null, TEST_TABLE_1, Optional.empty(), Optional.empty());
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;

        // Get splits (each split has one fragment)
        ConnectorSplitSource splits = splitManager.getSplits(null, null, tableHandle, null, null);
        ConnectorSplitSource.ConnectorSplitBatch batch = splits.getNextBatch(2).get();
        assertThat(batch.getSplits().size()).isEqualTo(2);

        // Create COUNT(*) handle
        LanceTableHandle countHandle = lanceTableHandle.withCountStar();

        // Count rows in first fragment
        LanceSplit split0 = (LanceSplit) batch.getSplits().get(0);
        try (LanceCountPageSource pageSource = new LanceCountPageSource(
                countHandle,
                Collections.emptyMap(),
                split0.getFragments())) {
            Page page = pageSource.getNextPage();
            assertThat(page).isNotNull();
            long count = BIGINT.getLong(page.getBlock(0), 0);
            // Each fragment has 2 rows
            assertThat(count).isEqualTo(2L);
        }

        // Count rows in second fragment
        LanceSplit split1 = (LanceSplit) batch.getSplits().get(1);
        try (LanceCountPageSource pageSource = new LanceCountPageSource(
                countHandle,
                Collections.emptyMap(),
                split1.getFragments())) {
            Page page = pageSource.getNextPage();
            assertThat(page).isNotNull();
            long count = BIGINT.getLong(page.getBlock(0), 0);
            assertThat(count).isEqualTo(2L);
        }
    }

    @Test
    public void testCountStarDistributedSum()
            throws ExecutionException, InterruptedException
    {
        // Test that distributed COUNT(*) across fragments sums to correct total
        ConnectorTableHandle tableHandle = metadata.getTableHandle(null, TEST_TABLE_1, Optional.empty(), Optional.empty());
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;

        // Create COUNT(*) handle
        LanceTableHandle countHandle = lanceTableHandle.withCountStar();

        // Get splits
        ConnectorSplitSource splits = splitManager.getSplits(null, null, tableHandle, null, null);
        ConnectorSplitSource.ConnectorSplitBatch batch = splits.getNextBatch(10).get();
        assertThat(batch.getSplits().size()).isGreaterThanOrEqualTo(2);

        // Count rows across all fragments (simulates distributed execution)
        long totalCount = 0;
        for (var split : batch.getSplits()) {
            LanceSplit lanceSplit = (LanceSplit) split;
            try (LanceCountPageSource pageSource = new LanceCountPageSource(
                    countHandle,
                    Collections.emptyMap(),
                    lanceSplit.getFragments())) {
                Page page = pageSource.getNextPage();
                assertThat(page).isNotNull();
                totalCount += BIGINT.getLong(page.getBlock(0), 0);
            }
        }

        // Total should equal manifest count (4 rows)
        assertThat(totalCount).isEqualTo(4L);
    }
}
