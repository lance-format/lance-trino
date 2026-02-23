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
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    // Actual column order in test data: x, y, b, c
    private static final ArrowType INT64_TYPE = new ArrowType.Int(64, true);
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
        LanceRuntime runtime = new LanceRuntime(lanceConfig, catalogProperties);
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
    public void testListTables()
    {
        // all schemas
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.empty()))).isEqualTo(ImmutableSet.of(
                new SchemaTableName("default", "test_table1"),
                new SchemaTableName("default", "test_table2"),
                new SchemaTableName("default", "test_table3"),
                new SchemaTableName("default", "test_table4")));

        // specific schema
        assertThat(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("default")))).isEqualTo(ImmutableSet.of(
                new SchemaTableName("default", "test_table1"),
                new SchemaTableName("default", "test_table2"),
                new SchemaTableName("default", "test_table3"),
                new SchemaTableName("default", "test_table4")));
    }
}
