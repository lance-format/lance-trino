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

import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.RowType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestLanceStructColumns
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return LanceQueryRunner.builderForWriteTests()
                .build();
    }

    @Test
    public void testCreateTableWithStructColumn()
    {
        String tableName = "test_struct_create_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, metadata ROW(name VARCHAR, value BIGINT))");
            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(0L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testInsertAndReadStructColumn()
    {
        abort("Lance ROW type write is not yet implemented");
    }

    @Test
    public void testStructWithBinaryField()
    {
        abort("Lance ROW type write is not yet implemented");
    }

    @Test
    public void testNestedStruct()
    {
        abort("Lance ROW type write is not yet implemented");
    }

    @Test
    public void testMultipleStructColumns()
    {
        abort("Lance ROW type write is not yet implemented");
    }

    @Test
    public void testStructWithMultipleRows()
    {
        abort("Lance ROW type write is not yet implemented");
    }

    @Test
    public void testDescribeTableWithStructColumn()
    {
        String tableName = "test_struct_describe_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " (id BIGINT, metadata ROW(name VARCHAR, value BIGINT))");

            assertQuery("SELECT column_name, data_type FROM information_schema.columns " +
                            "WHERE table_name = '" + tableName + "' ORDER BY ordinal_position",
                    "VALUES ('id', 'bigint'), ('metadata', 'row(name varchar, value bigint)')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testFilterOnNestedFieldReturnsCorrectResults()
    {
        abort("Lance ROW type write is not yet implemented");
    }

    @Test
    public void testFilterOnDeeplyNestedFieldReturnsCorrectResults()
    {
        abort("Lance ROW type write is not yet implemented");
    }

    @Test
    public void testNestedFieldFilterPushdown()
    {
        RowType metadataType = metadataType();
        LanceColumnHandle id = new LanceColumnHandle("id", INTEGER, true, 0);
        LanceColumnHandle metadata = new LanceColumnHandle("metadata", metadataType, true, 1);
        LanceColumnHandle metadataName = LanceColumnHandle.nestedColumn(
                "metadata.name", VARCHAR, true, 1, "metadata", metadataType, List.of(0), List.of("name"));

        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(
                metadataName, Domain.singleValue(VARCHAR, Slices.utf8Slice("alice"))));
        SubstraitExpressionBuilder.TupleDomainExtractionResult result =
                SubstraitExpressionBuilder.extractTupleDomain(
                        domain,
                        LanceMetadata.buildPositionalOrdinals(List.of(id, metadata)));

        assertThat(result.expression()).isPresent();
        assertThat(result.pushedTupleDomain().getDomains().orElseThrow()).containsKey(metadataName);
        assertThat(result.remainingTupleDomain()).isEqualTo(TupleDomain.all());
    }

    @Test
    public void testNestedFieldLocalFallback()
    {
        RowType metadataType = metadataType();
        LanceColumnHandle metadataName = LanceColumnHandle.nestedColumn(
                "metadata.name", VARCHAR, true, 1, "metadata", metadataType, List.of(0), List.of("name"));

        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(
                metadataName, Domain.singleValue(VARCHAR, Slices.utf8Slice("alice"))));
        SubstraitExpressionBuilder.TupleDomainExtractionResult result =
                SubstraitExpressionBuilder.extractTupleDomain(domain, Map.of());

        assertThat(result.expression()).isEmpty();
        assertThat(result.pushedTupleDomain()).isEqualTo(TupleDomain.all());
        assertThat(result.remainingTupleDomain().getDomains().orElseThrow()).containsKey(metadataName);
    }

    @Test
    public void testNestedProjectionAndFilterLiteralDot()
    {
        RowType metadataType = RowType.from(List.of(
                new RowType.Field(Optional.of("child.name"), VARCHAR),
                new RowType.Field(Optional.of("value"), BIGINT)));
        LanceColumnHandle metadata = new LanceColumnHandle("meta.data", metadataType, true, 1);
        LanceColumnHandle childName = LanceColumnHandle.nestedColumn(
                "meta\\.data.child\\.name", VARCHAR, true, 1, "meta.data", metadataType, List.of(0), List.of("child.name"));

        assertThat(metadata.path()).isEqualTo("meta\\.data");
        assertThat(childName.path()).isEqualTo("meta\\.data.child\\.name");
        assertThat(metadata.scanProjectionName()).isEqualTo("meta.data");
        assertThat(childName.scanProjectionName()).isEqualTo("meta.data");
        assertThat(LanceFieldPath.rootName(childName.path())).isEqualTo("meta.data");

        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(
                childName, Domain.singleValue(VARCHAR, Slices.utf8Slice("alice"))));
        SubstraitExpressionBuilder.TupleDomainExtractionResult result =
                SubstraitExpressionBuilder.extractTupleDomain(
                        domain,
                        LanceMetadata.buildPositionalOrdinals(List.of(metadata)));
        assertThat(result.expression()).isPresent();
        assertThat(result.remainingTupleDomain()).isEqualTo(TupleDomain.all());
    }

    @Test
    public void testNestedProjectionAndFilterSameLeafName()
    {
        RowType rowType = RowType.from(List.of(new RowType.Field(Optional.of("name"), VARCHAR)));
        LanceColumnHandle left = new LanceColumnHandle("left_meta", rowType, true, 1);
        LanceColumnHandle right = new LanceColumnHandle("right_meta", rowType, true, 2);

        ConnectorExpression leftName = new FieldDereference(VARCHAR, new Variable("left_meta", rowType), 0);
        ConnectorExpression rightName = new FieldDereference(VARCHAR, new Variable("right_meta", rowType), 0);
        ConnectorExpression leftFilter = new Call(
                BOOLEAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(leftName, new Constant(Slices.utf8Slice("alice"), VARCHAR)));
        ConnectorExpression rightFilter = new Call(
                BOOLEAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(rightName, new Constant(Slices.utf8Slice("bob"), VARCHAR)));

        Map<String, ColumnHandle> assignments = Map.of("left_meta", left, "right_meta", right);
        Map<String, Integer> ordinals = LanceMetadata.buildPositionalOrdinals(List.of(left, right));

        SubstraitExpressionBuilder.ExpressionExtractionResult leftResult =
                SubstraitExpressionBuilder.extractPushableExpressions(leftFilter, assignments, ordinals);
        SubstraitExpressionBuilder.ExpressionExtractionResult rightResult =
                SubstraitExpressionBuilder.extractPushableExpressions(rightFilter, assignments, ordinals);

        assertThat(leftResult.columnNames()).containsExactly("left_meta.name");
        assertThat(rightResult.columnNames()).containsExactly("right_meta.name");
    }

    @Test
    public void testMixedTopLevelAndNestedFieldFilter()
    {
        RowType metadataType = metadataType();
        LanceColumnHandle id = new LanceColumnHandle("id", INTEGER, true, 0);
        LanceColumnHandle metadata = new LanceColumnHandle("metadata", metadataType, true, 1);
        LanceColumnHandle metadataValue = LanceColumnHandle.nestedColumn(
                "metadata.value", BIGINT, true, 1, "metadata", metadataType, List.of(1), List.of("value"));

        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(
                id, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(INTEGER, 2L)), false),
                metadataValue, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 20L)), false)));
        SubstraitExpressionBuilder.TupleDomainExtractionResult result =
                SubstraitExpressionBuilder.extractTupleDomain(
                        domain,
                        LanceMetadata.buildPositionalOrdinals(List.of(id, metadata)));

        assertThat(result.expression()).isPresent();
        assertThat(result.pushedTupleDomain().getDomains().orElseThrow().keySet()).containsExactlyInAnyOrder(id, metadataValue);
        assertThat(result.remainingTupleDomain()).isEqualTo(TupleDomain.all());
    }

    private static RowType metadataType()
    {
        return RowType.from(List.of(
                new RowType.Field(Optional.of("name"), VARCHAR),
                new RowType.Field(Optional.of("value"), BIGINT)));
    }
}
