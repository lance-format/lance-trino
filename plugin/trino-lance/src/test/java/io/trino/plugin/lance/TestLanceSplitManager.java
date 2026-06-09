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
import org.lance.Fragment;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestLanceSplitManager
{
    private static final SchemaTableName TEST_TABLE_1 = new SchemaTableName("default", "test_table1");

    private LanceMetadata metadata;
    private LanceSplitManager splitManager;
    private TrackingLanceRuntime runtime;

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
        runtime = new TrackingLanceRuntime(lanceConfig, catalogProperties);
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

        // Fragment 0's 2 rows alone cover LIMIT 1, so only 1 fragment is selected.
        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).getFragments()).hasSize(1);
    }

    @Test
    public void testLimitStopsAtFirstFragmentWhenRowsSatisfyLimit()
            throws ExecutionException, InterruptedException
    {
        LanceTableHandle tableHandle = (LanceTableHandle) metadata.getTableHandle(
                TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());

        // LIMIT 2 == fragment 0's row count: stops at 1 fragment (old positional
        // logic would have grabbed 2).
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
    public void testLimitWithFilterUsesAllFragmentsSplit()
            throws ExecutionException, InterruptedException
    {
        LanceTableHandle tableHandle = (LanceTableHandle) metadata.getTableHandle(
                TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());

        LanceTableHandle withLimitAndFilter = tableHandle
                .withLimit(1)
                .withSubstraitFilter(new byte[] {0x01}, List.of("x"));
        ConnectorSplitSource splitSource = splitManager.getSplits(
                null, TestingConnectorSession.SESSION, withLimitAndFilter, null, null);
        List<LanceSplit> splits = getAllSplits(splitSource);

        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).isAllFragments()).isTrue();
        assertThat(splits.get(0).getFragments()).isEmpty();
    }

    @Test
    public void testFilteredLimitDoesNotEnumerateFragments()
            throws ExecutionException, InterruptedException
    {
        LanceTableHandle tableHandle = (LanceTableHandle) metadata.getTableHandle(
                TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());

        LanceTableHandle withLimitAndFilter = tableHandle
                .withLimit(10)
                .withSubstraitFilter(new byte[] {0x01}, List.of("x"));
        runtime.failOnGetFragments();

        ConnectorSplitSource splitSource = splitManager.getSplits(
                null, TestingConnectorSession.SESSION, withLimitAndFilter, null, null);
        List<LanceSplit> splits = getAllSplits(splitSource);

        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).isAllFragments()).isTrue();
    }

    @Test
    public void testAllFragmentsSplit()
    {
        LanceSplit split = LanceSplit.allFragments();

        assertThat(split.isAllFragments()).isTrue();
        assertThat(split.getFragments()).isEmpty();
        assertThat(split.getSplitInfo()).containsEntry("fragments", "ALL");

        JsonCodec<LanceSplit> codec = JsonCodec.jsonCodec(LanceSplit.class);
        String json = codec.toJson(split);
        assertThat(json)
                .contains("\"fragments\"")
                .contains("null")
                .contains("\"allFragments\"")
                .contains("true")
                .doesNotContain("[]");

        LanceSplit copy = codec.fromJson(json);
        assertThat(copy.isAllFragments()).isTrue();
        assertThat(copy.getFragments()).isEmpty();
    }

    @Test
    public void testLanceSplitJsonCompatibility()
    {
        JsonCodec<LanceSplit> codec = JsonCodec.jsonCodec(LanceSplit.class);

        LanceSplit legacySplit = codec.fromJson("{\"fragments\":[1,2]}");
        assertThat(legacySplit.isAllFragments()).isFalse();
        assertThat(legacySplit.getFragments()).containsExactly(1, 2);
        assertThat(codec.toJson(legacySplit))
                .contains("\"fragments\"")
                .contains("1")
                .contains("2")
                .doesNotContain("allFragments");

        LanceSplit allFragmentsSplit = codec.fromJson("{\"fragments\":null,\"allFragments\":true}");
        assertThat(allFragmentsSplit.isAllFragments()).isTrue();
        assertThat(allFragmentsSplit.getFragments()).isEmpty();

        assertThatThrownBy(() -> new LanceSplit((List<Integer>) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fragments is null");
        assertThatThrownBy(() -> codec.fromJson("{}"))
                .hasRootCauseMessage("fragments is null");
        assertThatThrownBy(() -> codec.fromJson("{\"fragments\":null}"))
                .hasRootCauseMessage("fragments is null");
        assertThatThrownBy(() -> codec.fromJson("{\"fragments\":[1],\"allFragments\":true}"))
                .hasRootCauseMessage("allFragments split cannot include explicit fragments");
    }

    @Test
    public void testCoalesceSkipsDeletionEmptiedFragments()
    {
        // Fragments 10,11 fully deleted (0 rows): positional selection would stop
        // at [10,11] and yield 0 rows; row-count accumulation walks on to live 12.
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

        // Every fragment fully deleted: return all (table logically empty).
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
        List<LanceSplit> splits = new ArrayList<>();
        boolean lastBatch;
        do {
            ConnectorSplitSource.ConnectorSplitBatch batch = splitSource.getNextBatch(100).get();
            batch.getSplits().stream()
                    .map(LanceSplit.class::cast)
                    .forEach(splits::add);
            lastBatch = batch.isNoMoreSplits();
        }
        while (!lastBatch);
        return splits;
    }

    private static class TrackingLanceRuntime
            extends LanceRuntime
    {
        private boolean failOnGetFragments;

        public TrackingLanceRuntime(LanceConfig config, Map<String, String> namespaceProperties)
        {
            super(config, namespaceProperties);
        }

        public void failOnGetFragments()
        {
            failOnGetFragments = true;
        }

        @Override
        public List<Fragment> getFragments(String userIdentity, String tablePath, Long version, Map<String, String> storageOptions)
        {
            if (failOnGetFragments) {
                throw new AssertionError("filtered LIMIT split generation should not enumerate fragments");
            }
            return super.getFragments(userIdentity, tablePath, version, storageOptions);
        }
    }
}
