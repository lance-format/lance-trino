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
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.testing.TestingConnectorSession;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.lance.Fragment;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestLanceFragmentPageSource
{
    private static final SchemaTableName TEST_TABLE_1 = new SchemaTableName("default", "test_table1");

    private LanceMetadata metadata;
    private LanceSplitManager splitManager;
    private LanceRuntime runtime;
    private LanceConfig lanceConfig;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        URL lanceURL = Resources.getResource(TestLanceFragmentPageSource.class, "/example_db");
        assertThat(lanceURL)
                .describedAs("example db is null")
                .isNotNull();
        lanceConfig = new LanceConfig()
                .setSingleLevelNs(true);  // example_db is flat (tables at root)
        Map<String, String> catalogProperties = ImmutableMap.of("lance.root", lanceURL.toString());
        runtime = new LanceRuntime(lanceConfig, catalogProperties);
        JsonCodec<LanceCommitTaskData> commitTaskDataCodec = JsonCodec.jsonCodec(LanceCommitTaskData.class);
        JsonCodec<LanceMergeCommitData> mergeCommitDataCodec = JsonCodec.jsonCodec(LanceMergeCommitData.class);
        this.metadata = new LanceMetadata(runtime, lanceConfig, commitTaskDataCodec, mergeCommitDataCodec);
        this.splitManager = new LanceSplitManager(runtime);
    }

    @Test
    public void testDatasetScanWithoutFragmentIdsRespectsLimit()
            throws Exception
    {
        LanceTableHandle tableHandle = (LanceTableHandle) metadata.getTableHandle(
                TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());
        ScanOptions scanOptions = new ScanOptions.Builder()
                .limit(3)
                .build();

        try (LanceScanner scanner = runtime.openDatasetScanner(
                TestingConnectorSession.SESSION.getUser(),
                tableHandle.getTablePath(),
                tableHandle.getDatasetVersion(),
                Optional.empty(),
                scanOptions,
                Collections.emptyMap())) {
            assertThat(readAllRows(scanner)).isEqualTo(3);
        }
    }

    @Test
    public void testDatasetScanWithoutFragmentIdsClearsScanOptionsFragmentIds()
            throws Exception
    {
        LanceTableHandle tableHandle = (LanceTableHandle) metadata.getTableHandle(
                TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());
        List<Fragment> fragments = runtime.getFragments(
                TestingConnectorSession.SESSION.getUser(),
                tableHandle.getTablePath(),
                tableHandle.getDatasetVersion(),
                Collections.emptyMap());
        ScanOptions scanOptions = new ScanOptions.Builder()
                .fragmentIds(List.of(fragments.get(0).getId()))
                .build();

        try (LanceScanner scanner = runtime.openDatasetScanner(
                TestingConnectorSession.SESSION.getUser(),
                tableHandle.getTablePath(),
                tableHandle.getDatasetVersion(),
                Optional.empty(),
                scanOptions,
                Collections.emptyMap())) {
            assertThat(readAllRows(scanner)).isEqualTo(4);
        }
    }

    @Test
    public void testAllFragmentsSplitAppliesFilterAndLimitThroughPageSource()
            throws Exception
    {
        LanceTableHandle tableHandle = (LanceTableHandle) metadata.getTableHandle(
                TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());
        List<LanceColumnHandle> allColumns = runtime.getColumnHandleList(
                TestingConnectorSession.SESSION.getUser(),
                tableHandle.getTablePath(),
                tableHandle.getDatasetVersion(),
                Collections.emptyMap());
        LanceColumnHandle colX = allColumns.stream()
                .filter(column -> column.name().equals("x"))
                .findFirst()
                .orElseThrow();
        LanceTableHandle filteredLimitHandle = tableHandle
                .withLimit(1)
                .withSubstraitFilter(greaterThanOrEqualFilter(allColumns, colX, 2), List.of(colX.name()));
        LancePageSourceProvider pageSourceProvider = new LancePageSourceProvider(runtime, lanceConfig);

        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                null,
                TestingConnectorSession.SESSION,
                LanceSplit.allFragments(),
                filteredLimitHandle,
                List.of(colX),
                null)) {
            Page page = pageSource.getNextPage();
            assertThat(page).isNotNull();
            assertThat(page.getChannelCount()).isEqualTo(1);
            assertThat(page.getPositionCount()).isEqualTo(1);
            assertThat(BIGINT.getLong(page.getBlock(0), 0)).isGreaterThanOrEqualTo(2L);

            assertThat(pageSource.getNextPage()).isNull();
            assertThat(pageSource.isFinished()).isTrue();
        }
    }

    @Test
    public void testFragmentScan()
            throws ExecutionException, InterruptedException
    {
        ConnectorTableHandle tableHandle = metadata.getTableHandle(TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());
        ConnectorSplitSource splits = splitManager.getSplits(null, TestingConnectorSession.SESSION, tableHandle, null, null);
        ConnectorSplitSource.ConnectorSplitBatch batch = splits.getNextBatch(2).get();
        assertThat(batch.getSplits().size()).isEqualTo(2);
        LanceSplit lanceSplit = (LanceSplit) batch.getSplits().get(0);
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        List<LanceColumnHandle> columns = runtime.getColumnHandleList(null, lanceTableHandle.getTablePath(), null, Collections.emptyMap());
        // testing split 0 is enough
        try (LanceFragmentPageSource pageSource = new LanceFragmentPageSource(lanceTableHandle, columns, lanceSplit.getFragments(), Collections.emptyMap(), 8192, null, runtime)) {
            Page page = pageSource.getNextPage();
            // assert row/column count
            assertThat(page.getChannelCount()).isEqualTo(4);
            assertThat(page.getPositionCount()).isEqualTo(2);
            // assert block content
            Block block = page.getBlock(0);
            assertThat(BIGINT.getLong(block, 0)).isEqualTo(0L);
            block = page.getBlock(1);
            assertThat(BIGINT.getLong(block, 1)).isEqualTo(2L);
            // assert no second page. it should come from the other split
            page = pageSource.getNextPage();
            assertThat(page).isNull();
            // assert that page is now finish
            assertThat(pageSource.isFinished()).isTrue();
        }
    }

    @Test
    public void testColumnProjection()
            throws ExecutionException, InterruptedException
    {
        // Test that columns are returned in the requested projection order
        // Dataset schema order is: [x, y, b, c]
        // Dataset values in fragment 0: x=[0,1], y=[0,2], b=[0,3], c=[0,-1]
        ConnectorTableHandle tableHandle = metadata.getTableHandle(TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());
        ConnectorSplitSource splits = splitManager.getSplits(null, TestingConnectorSession.SESSION, tableHandle, null, null);
        ConnectorSplitSource.ConnectorSplitBatch batch = splits.getNextBatch(2).get();
        LanceSplit lanceSplit = (LanceSplit) batch.getSplits().get(0);

        // Get column handles
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        List<LanceColumnHandle> allColumns = runtime.getColumnHandleList(null, lanceTableHandle.getTablePath(), null, Collections.emptyMap());
        LanceColumnHandle colB = allColumns.stream().filter(c -> c.name().equals("b")).findFirst().orElseThrow();
        LanceColumnHandle colX = allColumns.stream().filter(c -> c.name().equals("x")).findFirst().orElseThrow();

        // Request columns in order: [b, x] - different from schema order [x, y, b, c]
        List<LanceColumnHandle> projectedColumns = List.of(colB, colX);

        try (LanceFragmentPageSource pageSource = new LanceFragmentPageSource(
                lanceTableHandle,
                projectedColumns,
                lanceSplit.getFragments(),
                Collections.emptyMap(),
                8192,
                null,
                runtime)) {
            Page page = pageSource.getNextPage();

            assertThat(page.getChannelCount()).isEqualTo(2);
            assertThat(page.getPositionCount()).isEqualTo(2);

            // Block 0 should contain b's data: [0, 3]
            Block bBlock = page.getBlock(0);
            assertThat(BIGINT.getLong(bBlock, 0)).isEqualTo(0L);
            assertThat(BIGINT.getLong(bBlock, 1)).isEqualTo(3L);

            // Block 1 should contain x's data: [0, 1]
            Block xBlock = page.getBlock(1);
            assertThat(BIGINT.getLong(xBlock, 0)).isEqualTo(0L);
            assertThat(BIGINT.getLong(xBlock, 1)).isEqualTo(1L);
        }
    }

    @Test
    public void testPartialColumnProjection()
            throws ExecutionException, InterruptedException
    {
        // Test selecting only a subset of columns in a specific order
        // Dataset values in fragment 0: x=[0,1], y=[0,2], b=[0,3], c=[0,-1]
        ConnectorTableHandle tableHandle = metadata.getTableHandle(TestingConnectorSession.SESSION, TEST_TABLE_1, Optional.empty(), Optional.empty());
        ConnectorSplitSource splits = splitManager.getSplits(null, TestingConnectorSession.SESSION, tableHandle, null, null);
        ConnectorSplitSource.ConnectorSplitBatch batch = splits.getNextBatch(2).get();
        LanceSplit lanceSplit = (LanceSplit) batch.getSplits().get(0);

        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        List<LanceColumnHandle> allColumns = runtime.getColumnHandleList(null, lanceTableHandle.getTablePath(), null, Collections.emptyMap());
        LanceColumnHandle colX = allColumns.stream().filter(c -> c.name().equals("x")).findFirst().orElseThrow();
        LanceColumnHandle colC = allColumns.stream().filter(c -> c.name().equals("c")).findFirst().orElseThrow();

        // Request only c and x (in that order - reversed from schema)
        List<LanceColumnHandle> projectedColumns = List.of(colC, colX);

        try (LanceFragmentPageSource pageSource = new LanceFragmentPageSource(
                lanceTableHandle,
                projectedColumns,
                lanceSplit.getFragments(),
                Collections.emptyMap(),
                8192,
                null,
                runtime)) {
            Page page = pageSource.getNextPage();

            // assert only 2 columns returned
            assertThat(page.getChannelCount()).isEqualTo(2);
            assertThat(page.getPositionCount()).isEqualTo(2);

            // Block 0 should be c: [0, -1]
            Block cBlock = page.getBlock(0);
            assertThat(BIGINT.getLong(cBlock, 0)).isEqualTo(0L);
            assertThat(BIGINT.getLong(cBlock, 1)).isEqualTo(-1L);

            // Block 1 should be x: [0, 1]
            Block xBlock = page.getBlock(1);
            assertThat(BIGINT.getLong(xBlock, 0)).isEqualTo(0L);
            assertThat(BIGINT.getLong(xBlock, 1)).isEqualTo(1L);
        }
    }

    private static long readAllRows(LanceScanner scanner)
            throws IOException
    {
        long rows = 0;
        try (ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                rows += reader.getVectorSchemaRoot().getRowCount();
            }
        }
        return rows;
    }

    private static byte[] greaterThanOrEqualFilter(List<LanceColumnHandle> allColumns, LanceColumnHandle column, long value)
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(column, Domain.create(
                        ValueSet.ofRanges(Range.greaterThanOrEqual(column.trinoType(), value)), false)));
        ByteBuffer buffer = SubstraitExpressionBuilder.tupleDomainToSubstrait(
                        domain, allColumns, LanceMetadata.buildPositionalOrdinals(allColumns))
                .orElseThrow();
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
