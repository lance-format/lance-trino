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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.DeclareTableRequest;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

import static io.trino.plugin.lance.LanceRuntime.TABLE_PATH_SUFFIX;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestLanceMetadata
{
    // Use URL.toString() to match the format used by LanceNamespaceHolder (file:/... vs file:///...)
    private static final String TEST_DB_PATH = Resources.getResource(TestLanceMetadata.class, "/example_db").toString() + "/";
    private static final LanceTableHandle TEST_TABLE_1_HANDLE = new LanceTableHandle("default", "test_table1",
            TEST_DB_PATH + "test_table1.lance", List.of("test_table1"), Map.of());
    private static final LanceTableHandle TEST_TABLE_2_HANDLE = new LanceTableHandle("default", "test_table2",
            TEST_DB_PATH + "test_table2.lance", List.of("test_table2"), Map.of());

    // Actual column order in test data: x, y, b, c (field IDs 0, 1, 2, 3)
    private static final ArrowType INT64_TYPE = new ArrowType.Int(64, true);
    private LanceRuntime runtime;
    private LanceMetadata metadata;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        URL lanceURL = Resources.getResource(TestLanceMetadata.class, "/example_db");
        assertThat(lanceURL)
                .describedAs("example db is null")
                .isNotNull();
        LanceConfig lanceConfig = new LanceConfig()
                .setSingleLevelNs(true);  // example_db is flat (tables at root)
        Map<String, String> catalogProperties = ImmutableMap.of("lance.root", lanceURL.toString());
        runtime = new LanceRuntime(lanceConfig, catalogProperties);
        JsonCodec<LanceCommitTaskData> commitTaskDataCodec = JsonCodec.jsonCodec(LanceCommitTaskData.class);
        JsonCodec<LanceMergeCommitData> mergeCommitDataCodec = JsonCodec.jsonCodec(LanceMergeCommitData.class);
        metadata = new LanceMetadata(runtime, lanceConfig, commitTaskDataCodec, mergeCommitDataCodec);
    }

    @Test
    public void testListSchemaNames()
    {
        assertThat(metadata.listSchemaNames(SESSION)).containsExactlyElementsOf(ImmutableSet.of("default"));
    }

    @Test
    public void testGetTableHandle()
    {
        // Compare relevant fields rather than exact equality since datasetVersion is now dynamically captured
        LanceTableHandle table1Handle = metadata.getTableHandle(SESSION, new SchemaTableName("default", "test_table1"), Optional.empty(), Optional.empty());
        assertThat(table1Handle.getTableName()).isEqualTo(TEST_TABLE_1_HANDLE.getTableName());
        assertThat(table1Handle.getTablePath()).isEqualTo(TEST_TABLE_1_HANDLE.getTablePath());
        assertThat(table1Handle.getTableId()).isEqualTo(TEST_TABLE_1_HANDLE.getTableId());
        assertThat(table1Handle.getDatasetVersion()).isNotNull();  // Version should be captured

        LanceTableHandle table2Handle = metadata.getTableHandle(SESSION, new SchemaTableName("default", "test_table2"), Optional.empty(), Optional.empty());
        assertThat(table2Handle.getTableName()).isEqualTo(TEST_TABLE_2_HANDLE.getTableName());
        assertThat(table2Handle.getTablePath()).isEqualTo(TEST_TABLE_2_HANDLE.getTablePath());
        assertThat(table2Handle.getTableId()).isEqualTo(TEST_TABLE_2_HANDLE.getTableId());
        assertThat(table2Handle.getDatasetVersion()).isNotNull();

        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("other_schema", "test_table3"), Optional.empty(), Optional.empty())).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown"), Optional.empty(), Optional.empty())).isNull();
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table - field IDs are assigned by Lance based on schema order (x=0, y=1, b=2, c=3)
        assertThat(metadata.getColumnHandles(SESSION, TEST_TABLE_1_HANDLE)).isEqualTo(ImmutableMap.of(
                "b", new LanceColumnHandle("b", LanceColumnHandle.toTrinoType(INT64_TYPE), true, 2),
                "c", new LanceColumnHandle("c", LanceColumnHandle.toTrinoType(INT64_TYPE), true, 3),
                "x", new LanceColumnHandle("x", LanceColumnHandle.toTrinoType(INT64_TYPE), true, 0),
                "y", new LanceColumnHandle("y", LanceColumnHandle.toTrinoType(INT64_TYPE), true, 1)));

        // unknown table
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new LanceTableHandle("unknown", "unknown", "unknown", List.of("unknown"), Map.of())))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table 'unknown.unknown' not found");
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new LanceTableHandle("example", "unknown", "unknown", List.of("unknown"), Map.of())))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table 'example.unknown' not found");
    }

    @Test
    public void getTableMetadata()
    {
        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, TEST_TABLE_1_HANDLE);
        assertThat(tableMetadata.getTable()).isEqualTo(new SchemaTableName("default", "test_table1"));
        // Column order in test data: x, y, b, c
        assertThat(tableMetadata.getColumns()).isEqualTo(ImmutableList.of(
                new LanceColumnHandle("x", LanceColumnHandle.toTrinoType(INT64_TYPE), FieldType.nullable(INT64_TYPE)).getColumnMetadata(),
                new LanceColumnHandle("y", LanceColumnHandle.toTrinoType(INT64_TYPE), FieldType.nullable(INT64_TYPE)).getColumnMetadata(),
                new LanceColumnHandle("b", LanceColumnHandle.toTrinoType(INT64_TYPE), FieldType.nullable(INT64_TYPE)).getColumnMetadata(),
                new LanceColumnHandle("c", LanceColumnHandle.toTrinoType(INT64_TYPE), FieldType.nullable(INT64_TYPE)).getColumnMetadata()));

        // unknown tables should produce null
        assertThat(metadata.getTableMetadata(SESSION, new LanceTableHandle("unknown", "unknown", "unknown", List.of("unknown"), Map.of()))).isNull();
        assertThat(metadata.getTableMetadata(SESSION, new LanceTableHandle("default", "unknown", "unknown", List.of("unknown"), Map.of()))).isNull();
    }

    @Test
    public void testBuildPositionalOrdinalsWithNonSequentialFieldIds()
    {
        LanceTableHandle table = metadata.getTableHandle(SESSION, new SchemaTableName("default", "test_table5"), Optional.empty(), Optional.empty());

        List<LanceColumnHandle> columns = runtime.getColumnHandleList(
                SESSION.getUser(), table.getTablePath(), table.getDatasetVersion(), table.getStorageOptions());
        assertThat(columns).extracting(LanceColumnHandle::name)
                .containsExactlyInAnyOrder("x", "b", "c", "e");

        Map<String, Integer> ordinals = LanceMetadata.buildPositionalOrdinals(columns);

        // Field IDs: x=0, b=2, c=3, e=4 → sorted positions: x=0, b=1, c=2, e=3
        assertThat(ordinals)
                .as("Positional ordinals must be 0-based indices in field-ID-sorted order, not raw field IDs")
                .containsEntry("x", 0)
                .containsEntry("b", 1)
                .containsEntry("c", 2)
                .containsEntry("e", 3);
    }

    @Test
    public void testCountStarPushdownEligibility()
    {
        Optional<AggregationApplicationResult<ConnectorTableHandle>> result = applyAggregation(TEST_TABLE_1_HANDLE, countStar());
        assertThat(result).isPresent();
        assertThat(((LanceTableHandle) result.orElseThrow().getHandle()).isCountStar()).isTrue();

        assertThat(applyAggregation(TEST_TABLE_1_HANDLE, countStarWithFilter())).isEmpty();
        assertThat(applyAggregation(TEST_TABLE_1_HANDLE, countStarWithOrderBy())).isEmpty();
        assertThat(applyAggregation(TEST_TABLE_1_HANDLE, countDistinct())).isEmpty();
        assertThat(applyAggregation(TEST_TABLE_1_HANDLE, countColumn())).isEmpty();
        assertThat(applyAggregation(TEST_TABLE_1_HANDLE, sumColumn())).isEmpty();
    }

    @Test
    public void testGroupedCountStarPushdown()
    {
        assertThat(applyAggregation(
                TEST_TABLE_1_HANDLE,
                List.of(List.of(new LanceColumnHandle("x", BIGINT, true, 0))),
                countStar()))
                .isEmpty();
    }

    @Test
    public void testCountStarPushdownWithExistingAggregation()
    {
        assertThat(applyAggregation(TEST_TABLE_1_HANDLE.withCountStar(), countStar())).isEmpty();
    }

    @Test
    public void testCountStarPushdownWithTableFilter()
    {
        assertThat(applyAggregation(
                TEST_TABLE_1_HANDLE.withSubstraitFilter(new byte[] {1}, List.of("x")),
                countStar()))
                .isEmpty();
    }

    @Test
    public void testMultipleAggregatesPushdown()
    {
        assertThat(applyAggregation(TEST_TABLE_1_HANDLE, countStar(), countStar())).isEmpty();
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.empty()))).isEqualTo(ImmutableSet.of(
                new SchemaTableName("default", "test_table1"),
                new SchemaTableName("default", "test_table2"),
                new SchemaTableName("default", "test_table3"),
                new SchemaTableName("default", "test_table4"),
                new SchemaTableName("default", "test_table5"),
                new SchemaTableName("default", "wide_types_table")));

        // specific schema
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("default")))).isEqualTo(ImmutableSet.of(
                new SchemaTableName("default", "test_table1"),
                new SchemaTableName("default", "test_table2"),
                new SchemaTableName("default", "test_table3"),
                new SchemaTableName("default", "test_table4"),
                new SchemaTableName("default", "test_table5"),
                new SchemaTableName("default", "wide_types_table")));
    }

    @Test
    public void testListTablesEmptySchemaNameListsTablesFromEverySchema()
            throws Exception
    {
        Path root = Files.createTempDirectory("lance-list-tables-all-schemas");
        root.toFile().deleteOnExit();
        LanceConfig config = new LanceConfig().setSingleLevelNs(false);
        LanceRuntime runtime = new LanceRuntime(config, Map.of("lance.root", root.toUri().toString()));
        try {
            createAnalyticsAndSalesTables(runtime.getNamespace());
            LanceMetadata metadata = new LanceMetadata(
                    runtime,
                    config,
                    JsonCodec.jsonCodec(LanceCommitTaskData.class),
                    JsonCodec.jsonCodec(LanceMergeCommitData.class));
            assertThat(metadata.listTables(SESSION, Optional.empty()))
                    .containsExactlyInAnyOrder(
                            new SchemaTableName("analytics", "events"),
                            new SchemaTableName("sales", "orders"));
        }
        finally {
            runtime.close();
        }
    }

    @Test
    public void testStreamRelationColumnsListsTablesFromEverySchemaWhenSchemaNameIsEmpty()
            throws Exception
    {
        Path root = Files.createTempDirectory("lance-relation-columns");
        root.toFile().deleteOnExit();
        LanceConfig config = new LanceConfig().setSingleLevelNs(false);
        LanceRuntime runtime = new LanceRuntime(config, Map.of("lance.root", root.toUri().toString()));
        try {
            createAnalyticsAndSalesTables(runtime.getNamespace());
            LanceMetadata metadata = new LanceMetadata(
                    runtime,
                    config,
                    JsonCodec.jsonCodec(LanceCommitTaskData.class),
                    JsonCodec.jsonCodec(LanceMergeCommitData.class));
            assertThat(relationColumns(metadata, Optional.empty(), names -> names))
                    .extracting(RelationColumnsMetadata::name)
                    .containsExactlyInAnyOrder(
                            new SchemaTableName("analytics", "events"),
                            new SchemaTableName("sales", "orders"));
            assertThat(relationColumns(metadata, Optional.of("analytics"), names -> names))
                    .extracting(RelationColumnsMetadata::name)
                    .containsExactly(new SchemaTableName("analytics", "events"));
        }
        finally {
            runtime.close();
        }
    }

    @Test
    public void testStreamRelationColumnsAppliesRelationFilterBeforeLoadingColumns()
            throws Exception
    {
        Path root = Files.createTempDirectory("lance-relation-filter");
        root.toFile().deleteOnExit();
        LanceConfig config = new LanceConfig().setSingleLevelNs(true);
        LanceRuntime runtime = new LanceRuntime(config, Map.of("lance.root", root.toUri().toString()));
        try {
            createDataset(root, "good_table");
            Files.createDirectories(root.resolve("broken_table" + TABLE_PATH_SUFFIX));
            Files.writeString(root.resolve("broken_table" + TABLE_PATH_SUFFIX).resolve("not-a-dataset"), "invalid");

            LanceMetadata metadata = new LanceMetadata(
                    runtime,
                    config,
                    JsonCodec.jsonCodec(LanceCommitTaskData.class),
                    JsonCodec.jsonCodec(LanceMergeCommitData.class));
            SchemaTableName goodTable = new SchemaTableName("default", "good_table");
            SchemaTableName brokenTable = new SchemaTableName("default", "broken_table");
            assertThat(metadata.listTables(SESSION, Optional.of("default"))).contains(goodTable, brokenTable);

            List<RelationColumnsMetadata> relationColumns = relationColumns(
                    metadata,
                    Optional.empty(),
                    names -> {
                        assertThat(names).contains(goodTable, brokenTable);
                        return Set.of(goodTable);
                    });
            assertThat(relationColumns).extracting(RelationColumnsMetadata::name).containsExactly(goodTable);
            assertThat(relationColumns.getFirst().tableColumns()).isPresent();
        }
        finally {
            runtime.close();
        }
    }

    @Test
    public void testResolveRemoteTableNameRestoresTimestampCase()
    {
        assertThat(LanceMetadata.resolveRemoteTableName(
                Set.of("table-main-20260729T170548Z"),
                "table-main-20260729t170548z"))
                .contains("table-main-20260729T170548Z");
    }

    @Test
    public void testResolveRemoteTableNameRestoresCamelCase()
    {
        assertThat(LanceMetadata.resolveRemoteTableName(Set.of("fooBar"), "foobar")).contains("fooBar");
    }

    @Test
    public void testResolveRemoteTableNameKeepsExactMatch()
    {
        assertThat(LanceMetadata.resolveRemoteTableName(Set.of("nation"), "nation")).contains("nation");
    }

    @Test
    public void testResolveRemoteTableNameUnknownTableIsEmpty()
    {
        assertThat(LanceMetadata.resolveRemoteTableName(Set.of("nation"), "region")).isEmpty();
    }

    @Test
    public void testResolveRemoteTableNameRejectsCaseCollisions()
    {
        assertThatThrownBy(() -> LanceMetadata.resolveRemoteTableName(Set.of("Foo", "foo"), "foo"))
                .hasMessageContaining("Multiple Lance tables match foo");
    }

    private static List<RelationColumnsMetadata> relationColumns(
            LanceMetadata metadata,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return ImmutableList.copyOf(metadata.streamRelationColumns(SESSION, schemaName, relationFilter));
    }

    private static void createAnalyticsAndSalesTables(LanceNamespace namespace)
    {
        createNamespace(namespace, List.of("analytics"));
        createNamespace(namespace, List.of("sales"));
        createDeclaredDataset(namespace, List.of("analytics", "events"));
        createDeclaredDataset(namespace, List.of("sales", "orders"));
    }

    private static void createNamespace(LanceNamespace namespace, List<String> namespaceId)
    {
        CreateNamespaceRequest request = new CreateNamespaceRequest();
        request.setId(namespaceId);
        namespace.createNamespace(request);
    }

    private static void createDeclaredDataset(LanceNamespace namespace, List<String> tableId)
    {
        String location = namespace.declareTable(new DeclareTableRequest().id(tableId)).getLocation();
        Schema schema = new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, true))), null);
        try (BufferAllocator allocator = new RootAllocator()) {
            Dataset.create(allocator, location, schema, new WriteParams.Builder().build()).close();
        }
    }

    private static void createDataset(Path root, String remoteTableName)
    {
        Schema schema = new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, true))), null);
        try (BufferAllocator allocator = new RootAllocator()) {
            Dataset.create(allocator, root.resolve(remoteTableName + TABLE_PATH_SUFFIX).toString(), schema, new WriteParams.Builder().build()).close();
        }
    }

    private Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorTableHandle table,
            AggregateFunction... aggregates)
    {
        return applyAggregation(table, List.of(List.of()), aggregates);
    }

    private Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorTableHandle table,
            List<List<ColumnHandle>> groupingSets,
            AggregateFunction... aggregates)
    {
        return metadata.applyAggregation(SESSION, table, List.of(aggregates), Map.of(), groupingSets);
    }

    private static AggregateFunction countStar()
    {
        return new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty());
    }

    private static AggregateFunction countStarWithFilter()
    {
        return new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.of(Constant.FALSE));
    }

    private static AggregateFunction countStarWithOrderBy()
    {
        return new AggregateFunction("count", BIGINT, List.of(), List.of(new SortItem("x", ASC_NULLS_LAST)), false, Optional.empty());
    }

    private static AggregateFunction countDistinct()
    {
        return new AggregateFunction("count", BIGINT, List.of(new Variable("x", BIGINT)), List.of(), true, Optional.empty());
    }

    private static AggregateFunction countColumn()
    {
        return new AggregateFunction("count", BIGINT, List.of(new Variable("x", BIGINT)), List.of(), false, Optional.empty());
    }

    private static AggregateFunction sumColumn()
    {
        return new AggregateFunction("sum", BIGINT, List.of(new Variable("x", BIGINT)), List.of(), false, Optional.empty());
    }
}
