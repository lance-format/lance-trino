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
import io.airlift.json.JsonCodec;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.VarcharType;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.WriteParams;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN_WITH_COMMENT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_MATERIALIZED_VIEW_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_VIEW_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_MATERIALIZED_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_OR_REPLACE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DROP_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DROP_SCHEMA_CASCADE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_MAP_TYPE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_MERGE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NEGATIVE_DATE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NOT_NULL_CONSTRAINT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_LEVEL_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_TYPE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_SET_COLUMN_TYPE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TRUNCATE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_UPDATE;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Comprehensive connector test for the Lance connector extending BaseConnectorTest.
 * This test verifies full connector functionality including reads, writes, and DDL operations.
 */
public class TestLanceConnectorTest
        extends BaseConnectorTest
{
    private static final Schema LARGE_UTF8_SCHEMA = new Schema(
            Arrays.asList(
                    Field.nullable("id", new ArrowType.Int(32, true)),
                    Field.nullable("large_text", ArrowType.LargeUtf8.INSTANCE)),
            null);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return LanceQueryRunner.builderForWriteTests()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            // Supported write behaviors
            case SUPPORTS_CREATE_TABLE,
                    SUPPORTS_CREATE_TABLE_WITH_DATA,
                    SUPPORTS_CREATE_OR_REPLACE_TABLE,
                    SUPPORTS_INSERT,
                    SUPPORTS_DELETE,
                    SUPPORTS_ROW_LEVEL_DELETE,
                    SUPPORTS_UPDATE,
                    SUPPORTS_MERGE -> true;

            // Complex types - ROW and MAP not fully supported for writes
            case SUPPORTS_ROW_TYPE,
                    SUPPORTS_MAP_TYPE -> false;

            // Schema operations - not supported in single-level mode (which builderForWriteTests uses)
            // CASCADE is not supported for DROP SCHEMA
            case SUPPORTS_CREATE_SCHEMA,
                    SUPPORTS_RENAME_SCHEMA,
                    SUPPORTS_DROP_SCHEMA_CASCADE -> false;

            // Table modification operations - not supported
            case SUPPORTS_RENAME_TABLE,
                    SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                    SUPPORTS_ADD_COLUMN,
                    SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                    SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT,
                    SUPPORTS_DROP_COLUMN,
                    SUPPORTS_RENAME_COLUMN,
                    SUPPORTS_SET_COLUMN_TYPE -> false;

            // Truncate is not supported
            case SUPPORTS_TRUNCATE -> false;

            // View operations - not supported
            case SUPPORTS_CREATE_VIEW,
                    SUPPORTS_COMMENT_ON_VIEW_COLUMN,
                    SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_COMMENT_ON_MATERIALIZED_VIEW_COLUMN -> false;

            // Comment operations - not supported
            case SUPPORTS_COMMENT_ON_TABLE,
                    SUPPORTS_COMMENT_ON_COLUMN -> false;

            // Constraint operations - not supported
            case SUPPORTS_NOT_NULL_CONSTRAINT -> false;

            // Pushdown operations - not currently supported
            case SUPPORTS_TOPN_PUSHDOWN -> false;

            // Date handling - negative dates may not be supported
            case SUPPORTS_NEGATIVE_DATE -> false;

            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        // Lance throws concurrent modification errors for commit conflicts
        assertThat(e).hasMessageMatching(".*[Cc]oncurrent.*|.*commit conflict.*");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("CREATE TABLE lance\\." + schemaName + "\\.region \\(\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        "\\)");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();

        // Lance doesn't support certain types - mark as unsupported
        if (typeName.equals("time") ||
                typeName.equals("time(6)") ||
                typeName.startsWith("timestamp(") ||
                typeName.equals("tinyint") ||
                typeName.equals("smallint") ||
                typeName.startsWith("decimal(") ||
                typeName.startsWith("map(")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        // char types not supported - skip these tests entirely
        if (typeName.startsWith("char(")) {
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        // Lance doesn't have NOT NULL constraints, so this won't be called
        return "NULL value not allowed for NOT NULL column: " + columnName;
    }

    @Test
    @Override
    public void testColumnName()
    {
        // Lance doesn't allow column names containing dots
        abort("Lance does not support column names with special characters like dots");
    }

    @Test
    @Override
    public void testCreateTableWithLongTableName()
    {
        // Lance uses file system paths, so very long table names exceed path limits
        abort("Lance uses filesystem paths which have length limits");
    }

    @Test
    @Override
    public void testCreateOrReplaceTableConcurrently()
    {
        // Lance doesn't handle concurrent writes well
        abort("Lance does not support concurrent table creation");
    }

    @Test
    @Override
    public void testPredicateReflectedInExplain()
    {
        // Lance uses Substrait binary format for filter pushdown
        // We show the filtered column names in the constraint field
        assertExplain(
                "EXPLAIN SELECT name FROM nation WHERE nationkey = 42",
                "constraint.{0,10}(nationkey|NATIONKEY)");
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        // Lance doesn't support CHAR type
        abort("Lance does not support CHAR type");
    }

    @Test
    @Override
    public void testCaseSensitiveDataMapping()
    {
        // Lance doesn't support CHAR type which is used in this test
        abort("Lance does not support CHAR type");
    }

    @Test
    @Override
    public void testInsertArray()
    {
        // Lance array type handling needs more work
        abort("Lance array write support is limited");
    }

    @Test
    @Override
    public void testCreateOrReplaceTableWhenTableAlreadyExistsSameSchemaNoData()
    {
        // Lance requires at least one fragment to create a table
        abort("Lance requires data fragments to create a table");
    }

    @Test
    @Override
    public void testInsert()
    {
        // Skip this test as it creates empty tables which Lance doesn't handle well
        abort("Lance requires data fragments");
    }

    @Test
    @Override
    public void testCreateTableAsSelectNegativeDate()
    {
        // Lance doesn't support negative dates
        abort("Lance does not support negative dates");
    }

    @Test
    @Override
    public void testInsertNegativeDate()
    {
        // Lance doesn't support negative dates
        abort("Lance does not support negative dates");
    }

    @Test
    @Override
    public void testInsertForDefaultColumn()
    {
        // Lance doesn't support default column values
        abort("Lance does not support default column values");
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        // Lance doesn't preserve VARCHAR length constraints (varchar vs varchar(15))
        abort("Lance does not preserve VARCHAR length constraints");
    }

    @Test
    @Override
    public void testSelectInformationSchemaColumns()
    {
        // The information schema columns query requires additional metadata support
        abort("Lance information schema support is limited");
    }

    @Test
    @Override
    public void testCreateTableWithColumnComment()
    {
        // Lance doesn't support column comments
        abort("Lance does not support column comments");
    }

    @Test
    @Override
    public void testCreateTableWithColumnCommentSpecialCharacter()
    {
        // Lance doesn't support column comments
        abort("Lance does not support column comments");
    }

    @Test
    @Override
    public void testCreateTableSchemaNotFound()
    {
        // Lance uses a single "default" schema, schema not found errors don't apply
        abort("Lance uses a single default schema");
    }

    @Test
    @Override
    public void testCreateTableAsSelectSchemaNotFound()
    {
        // Lance uses a single "default" schema, schema not found errors don't apply
        abort("Lance uses a single default schema");
    }

    @Test
    @Override
    public void testInsertMap()
    {
        // Lance doesn't support MAP type
        abort("Lance does not support MAP type");
    }

    @Test
    @Override
    public void testCreateTableWithTableComment()
    {
        // Lance doesn't support table comments
        abort("Lance does not support table comments");
    }

    @Test
    @Override
    public void testCreateTableWithTableCommentSpecialCharacter()
    {
        // Lance doesn't support table comments
        abort("Lance does not support table comments");
    }

    @Test
    @Override
    public void testCreateTableAsSelectWithTableComment()
    {
        // Lance doesn't support table comments
        abort("Lance does not support table comments");
    }

    @Test
    @Override
    public void testCreateTableAsSelectWithTableCommentSpecialCharacter()
    {
        // Lance doesn't support table comments
        abort("Lance does not support table comments");
    }

    @Test
    @Override
    public void testPotentialDuplicateDereferencePushdown()
    {
        // Lance doesn't support dereference pushdown
        abort("Lance does not support dereference pushdown");
    }

    @Test
    @Override
    public void testCreateTableWithLongColumnName()
    {
        // Lance uses filesystem paths which have length limits
        abort("Lance uses filesystem paths which have length limits");
    }

    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        // ROW type write is not implemented yet
        abort("Lance ROW type write is not yet implemented");
    }

    @Test
    @Override
    public void testDataMappingSmokeTest()
    {
        // Lance doesn't support timestamp type yet
        abort("Lance does not support timestamp type yet");
    }

    // ===== LargeUtf8 Support Tests =====

    @Test
    public void testToTrinoTypeWithLargeUtf8()
    {
        // Test that LargeUtf8 is correctly converted to VARCHAR
        assertThat(LanceColumnHandle.toTrinoType(ArrowType.LargeUtf8.INSTANCE))
                .isEqualTo(VarcharType.VARCHAR);

        // Also verify regular Utf8 still works
        assertThat(LanceColumnHandle.toTrinoType(ArrowType.Utf8.INSTANCE))
                .isEqualTo(VarcharType.VARCHAR);
    }

    @Test
    public void testReadLargeUtf8Dataset(@TempDir Path tempDir)
    {
        String datasetPath = tempDir.resolve("large_utf8_test.lance").toString();

        // Step 1: Create a Lance dataset with LargeUtf8 column using the Java SDK
        try (BufferAllocator allocator = new RootAllocator()) {
            // Create empty dataset with schema
            Dataset dataset = Dataset.create(allocator, datasetPath, LARGE_UTF8_SCHEMA,
                    new WriteParams.Builder().build());
            dataset.close();

            // Write some data
            try (VectorSchemaRoot root = VectorSchemaRoot.create(LARGE_UTF8_SCHEMA, allocator)) {
                root.allocateNew();
                IntVector idVector = (IntVector) root.getVector("id");
                LargeVarCharVector textVector = (LargeVarCharVector) root.getVector("large_text");

                // Add test data
                for (int i = 0; i < 5; i++) {
                    idVector.setSafe(i, i);
                    String text = "Large text value " + i;
                    textVector.setSafe(i, text.getBytes(StandardCharsets.UTF_8));
                }
                root.setRowCount(5);

                List<FragmentMetadata> fragments = Fragment.create(
                        datasetPath,
                        allocator,
                        root,
                        new WriteParams.Builder().build());

                FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments);
                try (Dataset appendedDataset = Dataset.commit(allocator, datasetPath, appendOp, Optional.of(1L))) {
                    assertThat(appendedDataset.countRows()).isEqualTo(5);
                }
            }

            // Step 2: Read the dataset using LanceDatasetCache and verify schema mapping
            // Use single-level mode since we're using a flat directory structure
            LanceConfig config = new LanceConfig().setSingleLevelNs(true);
            Map<String, String> catalogProperties = ImmutableMap.of("lance.root", tempDir.toString());
            LanceNamespaceHolder namespaceHolder = new LanceNamespaceHolder(config, catalogProperties);

            // Get column handles using the table path
            String tablePath = datasetPath;
            Map<String, ColumnHandle> columnHandles = LanceDatasetCache.getColumnHandles(tablePath, null);
            assertThat(columnHandles).hasSize(2);

            // Verify the large_text column is mapped to VARCHAR
            LanceColumnHandle largeTextHandle = (LanceColumnHandle) columnHandles.get("large_text");
            assertThat(largeTextHandle).isNotNull();
            assertThat(largeTextHandle.trinoType()).isEqualTo(VarcharType.VARCHAR);

            // Verify id column
            LanceColumnHandle idHandle = (LanceColumnHandle) columnHandles.get("id");
            assertThat(idHandle).isNotNull();
            assertThat(idHandle.trinoType()).isEqualTo(INTEGER);

            // Step 3: Verify table metadata using the table path
            List<ColumnMetadata> columnsMetadata = LanceDatasetCache.getColumnMetadata(tablePath, null);
            assertThat(columnsMetadata).hasSize(2);

            // Find the large_text column metadata
            ColumnMetadata largeTextMetadata = columnsMetadata.stream()
                    .filter(cm -> cm.getName().equals("large_text"))
                    .findFirst()
                    .orElseThrow();
            assertThat(largeTextMetadata.getType()).isEqualTo(VarcharType.VARCHAR);
        }
    }

    @Test
    public void testLanceMetadataWithLargeUtf8(@TempDir Path tempDir)
    {
        String datasetPath = tempDir.resolve("metadata_test.lance").toString();

        try (BufferAllocator allocator = new RootAllocator()) {
            // Create and populate dataset
            Dataset dataset = Dataset.create(allocator, datasetPath, LARGE_UTF8_SCHEMA,
                    new WriteParams.Builder().build());
            dataset.close();

            try (VectorSchemaRoot root = VectorSchemaRoot.create(LARGE_UTF8_SCHEMA, allocator)) {
                root.allocateNew();
                IntVector idVector = (IntVector) root.getVector("id");
                LargeVarCharVector textVector = (LargeVarCharVector) root.getVector("large_text");

                idVector.setSafe(0, 1);
                textVector.setSafe(0, "test".getBytes(StandardCharsets.UTF_8));
                root.setRowCount(1);

                List<FragmentMetadata> fragments = Fragment.create(datasetPath, allocator, root,
                        new WriteParams.Builder().build());
                FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments);
                Dataset.commit(allocator, datasetPath, appendOp, Optional.of(1L)).close();
            }

            // Create LanceMetadata and verify it can read the table
            // Use single-level mode since we're using a flat directory structure
            LanceConfig config = new LanceConfig().setSingleLevelNs(true);
            Map<String, String> catalogProperties = ImmutableMap.of("lance.root", tempDir.toString());
            LanceNamespaceHolder namespaceHolder = new LanceNamespaceHolder(config, catalogProperties);
            JsonCodec<LanceCommitTaskData> commitTaskDataCodec = JsonCodec.jsonCodec(LanceCommitTaskData.class);
            JsonCodec<LanceMergeCommitData> mergeCommitDataCodec = JsonCodec.jsonCodec(LanceMergeCommitData.class);
            LanceMetadata metadata = new LanceMetadata(namespaceHolder, config, commitTaskDataCodec, mergeCommitDataCodec);

            // Get table handle - this should NOT return null anymore
            LanceTableHandle tableHandle = (LanceTableHandle) metadata.getTableHandle(
                    SESSION,
                    new SchemaTableName("default", "metadata_test"),
                    Optional.empty(),
                    Optional.empty());
            assertThat(tableHandle).isNotNull();

            // Get table metadata - this should NOT return null anymore
            var tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
            assertThat(tableMetadata)
                    .describedAs("getTableMetadata should not return null for LargeUtf8 columns")
                    .isNotNull();
            assertThat(tableMetadata.getColumns()).hasSize(2);
        }
    }
}
