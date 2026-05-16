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

        // Fragment 0 holds 2 rows, which alone covers LIMIT 1, so only 1 fragment
        // is coalesced into the single split.
        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).getFragments()).hasSize(1);
    }

    @Test
    public void testLimitStopsAtFirstFragmentWhenRowsSatisfyLimit()
            throws ExecutionException, InterruptedException
    {
        LanceTableHandle tableHandle = (LanceTableHandle) metadata.getTableHandle(
                TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());

        // LIMIT 2 == fragment 0's full row count. The positional logic would have
        // grabbed 2 fragments (min(limit, numFragments)); the row-count logic stops
        // after fragment 0 since it already satisfies the limit.
        LanceTableHandle withLimit = tableHandle.withLimit(2);
        ConnectorSplitSource splitSource = splitManager.getSplits(
                null, TestingConnectorSession.SESSION, withLimit, null, null);
        List<LanceSplit> splits = getAllSplits(splitSource);

        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).getFragments()).hasSize(1);
    }

    @Test
    public void testLimitSpansFragmentsWhenFirstInsufficient()
            throws ExecutionException, InterruptedException
    {
        LanceTableHandle tableHandle = (LanceTableHandle) metadata.getTableHandle(
                TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());

        // LIMIT 3 exceeds fragment 0's 2 rows, so fragment 1 is pulled in as well.
        LanceTableHandle withLimit = tableHandle.withLimit(3);
        ConnectorSplitSource splitSource = splitManager.getSplits(
                null, TestingConnectorSession.SESSION, withLimit, null, null);
        List<LanceSplit> splits = getAllSplits(splitSource);

        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).getFragments()).hasSize(2);
    }

    @Test
    public void testLimitLargerThanFragmentsCoalescesAll()
            throws ExecutionException, InterruptedException
    {
        LanceTableHandle tableHandle = (LanceTableHandle) metadata.getTableHandle(
                TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());

        // Apply a LIMIT larger than the table's total row count (4 rows)
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

    @Test
    public void testCoalesceSkipsDeletionEmptiedFragments()
    {
        // Fragments 10 and 11 are fully deleted (0 logical rows); the only live rows
        // are in fragment 12. Positional selection of LIMIT fragments would have
        // returned [10, 11] and yielded 0 rows — the bug this fixes. Row-count
        // accumulation keeps walking past the empty fragments to reach fragment 12.
        assertThat(LanceSplitManager.coalesceFragmentsForLimit(
                List.of(10, 11, 12), List.of(0L, 0L, 2L), 2))
                .containsExactly(10, 11, 12);

        // A single large fragment satisfies a small limit on its own.
        assertThat(LanceSplitManager.coalesceFragmentsForLimit(
                List.of(0, 1), List.of(5L, 5L), 3))
                .containsExactly(0);

        // Exact-fit: fragment 0's count equals the limit, so fragment 1 is not needed.
        assertThat(LanceSplitManager.coalesceFragmentsForLimit(
                List.of(0, 1), List.of(2L, 2L), 2))
                .containsExactly(0);

        // Limit spills into the next fragment when the first is insufficient.
        assertThat(LanceSplitManager.coalesceFragmentsForLimit(
                List.of(0, 1), List.of(2L, 2L), 3))
                .containsExactly(0, 1);

        // Every fragment fully deleted: return them all (table is logically empty,
        // scan correctly yields fewer rows than the limit).
        assertThat(LanceSplitManager.coalesceFragmentsForLimit(
                List.of(0, 1), List.of(0L, 0L), 5))
                .containsExactly(0, 1);

        // Never emit an empty split: LIMIT 0 still selects the first fragment.
        assertThat(LanceSplitManager.coalesceFragmentsForLimit(
                List.of(7, 8), List.of(2L, 2L), 0))
                .containsExactly(7);

        // No fragments at all -> empty selection (no split to emit).
        assertThat(LanceSplitManager.coalesceFragmentsForLimit(
                List.of(), List.of(), 5))
                .isEmpty();
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
