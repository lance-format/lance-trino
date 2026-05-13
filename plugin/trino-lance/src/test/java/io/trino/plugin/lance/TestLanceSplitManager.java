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
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestLanceSplitManager
{
    private static final SchemaTableName TEST_TABLE_1 = new SchemaTableName("default", "test_table1");

    private LanceMetadata metadata;
    private LanceSplitManager splitManager;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        URL lanceURL = Resources.getResource(TestLanceSplitManager.class, "/example_db");
        assertThat(lanceURL)
                .describedAs("example db is null")
                .isNotNull();
        LanceConfig lanceConfig = new LanceConfig()
                .setSingleLevelNs(true);
        Map<String, String> catalogProperties = ImmutableMap.of("lance.root", lanceURL.toString());
        LanceRuntime runtime = new LanceRuntime(lanceConfig, catalogProperties);
        JsonCodec<LanceCommitTaskData> commitTaskDataCodec = JsonCodec.jsonCodec(LanceCommitTaskData.class);
        JsonCodec<LanceMergeCommitData> mergeCommitDataCodec = JsonCodec.jsonCodec(LanceMergeCommitData.class);
        this.metadata = new LanceMetadata(runtime, lanceConfig, commitTaskDataCodec, mergeCommitDataCodec);
        this.splitManager = new LanceSplitManager(runtime);
    }

    @Test
    public void testFullScanCreatesSplitPerFragment()
            throws ExecutionException, InterruptedException
    {
        ConnectorTableHandle tableHandle = metadata.getTableHandle(
                TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());

        ConnectorSplitSource splitSource = splitManager.getSplits(
                null, TestingConnectorSession.SESSION, tableHandle, null, null);
        List<LanceSplit> splits = getAllSplits(splitSource);

        // test_table1 has 2 fragments, each should get its own split
        assertThat(splits).hasSize(2);
        for (LanceSplit split : splits) {
            assertThat(split.getFragments()).hasSize(1);
        }
    }

    @Test
    public void testLimitWithoutFilterCoalescesSplits()
            throws ExecutionException, InterruptedException
    {
        LanceTableHandle tableHandle = (LanceTableHandle) metadata.getTableHandle(
                TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());

        // Apply LIMIT 1 without a filter
        LanceTableHandle withLimit = tableHandle.withLimit(1);
        ConnectorSplitSource splitSource = splitManager.getSplits(
                null, TestingConnectorSession.SESSION, withLimit, null, null);
        List<LanceSplit> splits = getAllSplits(splitSource);

        // Should produce a single split with 1 fragment (min of limit=1, totalFragments=2))
        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).getFragments()).hasSize(1);
    }

    @Test
    public void testLimitLargerThanFragmentsCoalescesAll()
            throws ExecutionException, InterruptedException
    {
        LanceTableHandle tableHandle = (LanceTableHandle) metadata.getTableHandle(
                TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());

        // Apply LIMIT larger than the number of fragments
        LanceTableHandle withLimit = tableHandle.withLimit(100);
        ConnectorSplitSource splitSource = splitManager.getSplits(
                null, TestingConnectorSession.SESSION, withLimit, null, null);
        List<LanceSplit> splits = getAllSplits(splitSource);

        // Should produce a single split containing all fragments
        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).getFragments()).hasSize(2);
    }

    @Test
    public void testLimitWithFilterDoesNotCoalesce()
            throws ExecutionException, InterruptedException
    {
        LanceTableHandle tableHandle = (LanceTableHandle) metadata.getTableHandle(
                TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());

        // Apply both a LIMIT and a filter — coalescing should NOT apply
        LanceTableHandle withLimitAndFilter = tableHandle
                .withLimit(1)
                .withSubstraitFilter(new byte[] {0x01}, List.of("x"));
        ConnectorSplitSource splitSource = splitManager.getSplits(
                null, TestingConnectorSession.SESSION, withLimitAndFilter, null, null);
        List<LanceSplit> splits = getAllSplits(splitSource);

        // With a filter present, each fragment gets its own split
        assertThat(splits).hasSize(2);
        for (LanceSplit split : splits) {
            assertThat(split.getFragments()).hasSize(1);
        }
    }

    private static List<LanceSplit> getAllSplits(ConnectorSplitSource splitSource)
            throws ExecutionException, InterruptedException
    {
        ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(100).get();
        return batch.getSplits().stream()
                .map(LanceSplit.class::cast)
                .toList();
    }
}
