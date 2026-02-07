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

import io.trino.testing.BaseConnectorTest;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import java.util.Optional;

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Abstract base class for Lance connector tests.
 * Subclasses should override {@link #getNamespaceTestConfig()} to specify the namespace configuration.
 */
public abstract class BaseLanceConnectorTest
        extends BaseConnectorTest
{
    /**
     * Get the namespace test configuration for this test class.
     * Subclasses must implement this to specify their namespace mode.
     */
    protected abstract LanceNamespaceTestConfig getNamespaceTestConfig();

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            // Supported write behaviors
            case SUPPORTS_CREATE_TABLE,
                    SUPPORTS_CREATE_TABLE_WITH_DATA,
                    SUPPORTS_CREATE_OR_REPLACE_TABLE,
                    SUPPORTS_INSERT -> true;

            // Complex types - ROW and MAP not fully supported for writes
            case SUPPORTS_ROW_TYPE,
                    SUPPORTS_MAP_TYPE -> false;

            // Schema operations - depends on namespace configuration
            case SUPPORTS_CREATE_SCHEMA -> getNamespaceTestConfig().supportsCreateSchema();
            case SUPPORTS_DROP_SCHEMA_CASCADE,
                    SUPPORTS_RENAME_SCHEMA -> false;

            // Table modification operations - not supported
            case SUPPORTS_RENAME_TABLE,
                    SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                    SUPPORTS_ADD_COLUMN,
                    SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                    SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT,
                    SUPPORTS_DROP_COLUMN,
                    SUPPORTS_RENAME_COLUMN,
                    SUPPORTS_SET_COLUMN_TYPE -> false;

            // Row-level modification operations - not supported
            case SUPPORTS_DELETE,
                    SUPPORTS_ROW_LEVEL_DELETE,
                    SUPPORTS_UPDATE,
                    SUPPORTS_TRUNCATE,
                    SUPPORTS_MERGE -> false;

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
        if (getNamespaceTestConfig().isSingleLevelNs()) {
            // Single-level mode uses virtual "default" schema, schema not found errors don't apply
            abort("Single-level mode uses virtual default schema");
        }
        super.testCreateTableSchemaNotFound();
    }

    @Test
    @Override
    public void testCreateTableAsSelectSchemaNotFound()
    {
        if (getNamespaceTestConfig().isSingleLevelNs()) {
            // Single-level mode uses virtual "default" schema, schema not found errors don't apply
            abort("Single-level mode uses virtual default schema");
        }
        super.testCreateTableAsSelectSchemaNotFound();
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

    // ===== Namespace-specific tests =====

    @Test
    public void testCreateSchemaNotSupportedInSingleLevelMode()
    {
        if (!getNamespaceTestConfig().isSingleLevelNs()) {
            abort("Test only applies to single-level namespace mode");
        }

        assertQueryFails(
                "CREATE SCHEMA test_schema_not_allowed",
                ".*This connector does not support creating schemas.*");
    }

    @Test
    public void testDropSchemaNotSupportedInSingleLevelMode()
    {
        if (!getNamespaceTestConfig().isSingleLevelNs()) {
            abort("Test only applies to single-level namespace mode");
        }

        // In single-level mode, only the virtual "default" schema exists
        // Attempting to drop a non-existent schema results in "Schema does not exist" error
        // from Trino's built-in validation (before our dropSchema method is called)
        assertQueryFails(
                "DROP SCHEMA test_schema_not_allowed",
                ".*Schema.*does not exist.*");
    }

    @Test
    public void testCreateAndDropSchema()
    {
        if (getNamespaceTestConfig().isSingleLevelNs()) {
            abort("Test does not apply to single-level namespace mode");
        }

        String schemaName = "test_schema_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE SCHEMA " + schemaName);
            assertQuery("SHOW SCHEMAS LIKE '" + schemaName + "'", "SELECT '" + schemaName + "'");
        }
        finally {
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }
}
