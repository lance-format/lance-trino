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
import io.substrait.expression.Expression;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarbinaryType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSubstraitExpressionBuilder
{
    private static final LanceColumnHandle INT_COLUMN = new LanceColumnHandle("id", INTEGER, true, 0);
    private static final LanceColumnHandle BIGINT_COLUMN = new LanceColumnHandle("big_id", BIGINT, true, 1);
    private static final LanceColumnHandle VARCHAR_COLUMN = new LanceColumnHandle("name", VARCHAR, true, 2);
    private static final LanceColumnHandle BOOLEAN_COLUMN = new LanceColumnHandle("active", BOOLEAN, true, 3);

    private static final List<LanceColumnHandle> ALL_COLUMNS = List.of(
            INT_COLUMN, BIGINT_COLUMN, VARCHAR_COLUMN, BOOLEAN_COLUMN);

    private static final Map<String, Integer> COLUMN_ORDINALS = Map.of(
            "id", 0,
            "big_id", 1,
            "name", 2,
            "active", 3);

    @Test
    public void testAllDomain()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.all();
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, ALL_COLUMNS, COLUMN_ORDINALS);
        assertThat(result).isEmpty();
    }

    @Test
    public void testNoneDomain()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.none();
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, ALL_COLUMNS, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        // The result should be a false literal
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testSingleEquality()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.singleValue(INTEGER, 42L)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, ALL_COLUMNS, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testVarcharEquality()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(VARCHAR_COLUMN, Domain.singleValue(VARCHAR, Slices.utf8Slice("test"))));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, ALL_COLUMNS, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testBooleanValue()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(BOOLEAN_COLUMN, Domain.singleValue(BOOLEAN, true)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, ALL_COLUMNS, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testInClause()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.multipleValues(INTEGER, java.util.List.of(1L, 2L, 3L))));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, ALL_COLUMNS, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testRangeGreaterThan()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.create(
                        ValueSet.ofRanges(Range.greaterThan(INTEGER, 10L)), false)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, ALL_COLUMNS, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testRangeBetween()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.create(
                        ValueSet.ofRanges(Range.range(INTEGER, 10L, true, 100L, true)), false)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, ALL_COLUMNS, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testIsNull()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.onlyNull(INTEGER)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, ALL_COLUMNS, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testIsNotNull()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.notNull(INTEGER)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, ALL_COLUMNS, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testMultipleColumns()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(
                        INT_COLUMN, Domain.singleValue(INTEGER, 42L),
                        VARCHAR_COLUMN, Domain.singleValue(VARCHAR, Slices.utf8Slice("test"))));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, ALL_COLUMNS, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testExpressionConversionDirect()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.singleValue(INTEGER, 42L)));
        Optional<Expression> result = SubstraitExpressionBuilder.tupleDomainToExpression(domain, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        // Verify it's a scalar function invocation (equality)
        assertThat(result.get()).isInstanceOf(Expression.ScalarFunctionInvocation.class);
    }

    @Test
    public void testIsSupportedType()
    {
        assertThat(SubstraitExpressionBuilder.isSupportedType(BOOLEAN)).isTrue();
        assertThat(SubstraitExpressionBuilder.isSupportedType(INTEGER)).isTrue();
        assertThat(SubstraitExpressionBuilder.isSupportedType(BIGINT)).isTrue();
        assertThat(SubstraitExpressionBuilder.isSupportedType(VARCHAR)).isTrue();
        assertThat(SubstraitExpressionBuilder.isSupportedType(VarbinaryType.VARBINARY)).isTrue();
    }

    @Test
    public void testIsDomainPushable()
    {
        Domain simpleDomain = Domain.singleValue(INTEGER, 42L);
        assertThat(SubstraitExpressionBuilder.isDomainPushable(simpleDomain)).isTrue();

        assertThat(SubstraitExpressionBuilder.isDomainPushable(Domain.all(INTEGER))).isTrue();
        assertThat(SubstraitExpressionBuilder.isDomainPushable(Domain.none(INTEGER))).isTrue();
    }

    @Test
    public void testTimestampMicrosEquality()
    {
        // TIMESTAMP(6) - microsecond precision
        LanceColumnHandle tsColumn = new LanceColumnHandle("created_at", TIMESTAMP_MICROS, true, 4);
        List<LanceColumnHandle> columns = List.of(INT_COLUMN, tsColumn);
        Map<String, Integer> ordinals = Map.of("id", 0, "created_at", 1);

        // Trino stores timestamps as microseconds since epoch
        long epochMicros = 1704067200000000L; // 2024-01-01 00:00:00 UTC in microseconds
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(tsColumn, Domain.singleValue(TIMESTAMP_MICROS, epochMicros)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, columns, ordinals);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testTimestampMillisEquality()
    {
        // TIMESTAMP(3) - millisecond precision
        LanceColumnHandle tsColumn = new LanceColumnHandle("created_at", TIMESTAMP_MILLIS, true, 4);
        List<LanceColumnHandle> columns = List.of(INT_COLUMN, tsColumn);
        Map<String, Integer> ordinals = Map.of("id", 0, "created_at", 1);

        // Trino stores timestamps as microseconds since epoch (even for TIMESTAMP(3))
        long epochMicros = 1704067200000000L;
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(tsColumn, Domain.singleValue(TIMESTAMP_MILLIS, epochMicros)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, columns, ordinals);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testTimestampRange()
    {
        LanceColumnHandle tsColumn = new LanceColumnHandle("created_at", TIMESTAMP_MICROS, true, 4);
        List<LanceColumnHandle> columns = List.of(INT_COLUMN, tsColumn);
        Map<String, Integer> ordinals = Map.of("id", 0, "created_at", 1);

        long startMicros = 1704067200000000L; // 2024-01-01 00:00:00 UTC
        long endMicros = 1704153600000000L;   // 2024-01-02 00:00:00 UTC
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(tsColumn, Domain.create(
                        ValueSet.ofRanges(Range.range(TIMESTAMP_MICROS, startMicros, true, endMicros, false)), false)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, columns, ordinals);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testTimestampIsSupportedType()
    {
        assertThat(SubstraitExpressionBuilder.isSupportedType(TIMESTAMP_MICROS)).isTrue();
        assertThat(SubstraitExpressionBuilder.isSupportedType(TIMESTAMP_MILLIS)).isTrue();
        assertThat(SubstraitExpressionBuilder.isSupportedType(TimestampType.createTimestampType(0))).isTrue();
        assertThat(SubstraitExpressionBuilder.isSupportedType(TimestampType.createTimestampType(9))).isTrue();
        assertThat(SubstraitExpressionBuilder.isSupportedType(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(3))).isTrue();
    }

    // LIKE pattern pushdown tests

    @Test
    public void testIsPushableLikePattern()
    {
        // All non-empty patterns should be pushable (Lance filters at storage layer)
        assertThat(SubstraitExpressionBuilder.isPushableLikePattern("foo%")).isTrue();
        assertThat(SubstraitExpressionBuilder.isPushableLikePattern("%foo")).isTrue();
        assertThat(SubstraitExpressionBuilder.isPushableLikePattern("%foo%")).isTrue();
        assertThat(SubstraitExpressionBuilder.isPushableLikePattern("foo%bar")).isTrue();
        assertThat(SubstraitExpressionBuilder.isPushableLikePattern("_foo%")).isTrue();
        assertThat(SubstraitExpressionBuilder.isPushableLikePattern("exact")).isTrue();

        // Only empty/null patterns are not pushable
        assertThat(SubstraitExpressionBuilder.isPushableLikePattern("")).isFalse();
        assertThat(SubstraitExpressionBuilder.isPushableLikePattern(null)).isFalse();
    }

    @Test
    public void testExtractLikePredicatesSimple()
    {
        // Create a LIKE expression: name LIKE 'foo%'
        ConnectorExpression likeExpr = new Call(
                io.trino.spi.type.BooleanType.BOOLEAN,
                LIKE_FUNCTION_NAME,
                List.of(
                        new Variable("name", VARCHAR),
                        new Constant(Slices.utf8Slice("foo%"), VARCHAR)));

        Map<String, ColumnHandle> assignments = Map.of("name", VARCHAR_COLUMN);

        SubstraitExpressionBuilder.LikePredicateExtractionResult result =
                SubstraitExpressionBuilder.extractLikePredicates(likeExpr, assignments);

        assertThat(result.likePredicates()).hasSize(1);
        assertThat(result.likePredicates().getFirst().columnName()).isEqualTo("name");
        assertThat(result.likePredicates().getFirst().pattern()).isEqualTo("foo%");
        assertThat(result.remainingExpression()).isEqualTo(Constant.TRUE);
    }

    @Test
    public void testExtractLikePredicatesSuffixPattern()
    {
        // Create a LIKE expression with suffix pattern: name LIKE '%foo'
        // All patterns are pushable now (Lance filters at storage layer)
        ConnectorExpression likeExpr = new Call(
                io.trino.spi.type.BooleanType.BOOLEAN,
                LIKE_FUNCTION_NAME,
                List.of(
                        new Variable("name", VARCHAR),
                        new Constant(Slices.utf8Slice("%foo"), VARCHAR)));

        Map<String, ColumnHandle> assignments = Map.of("name", VARCHAR_COLUMN);

        SubstraitExpressionBuilder.LikePredicateExtractionResult result =
                SubstraitExpressionBuilder.extractLikePredicates(likeExpr, assignments);

        // All patterns are now pushable
        assertThat(result.likePredicates()).hasSize(1);
        assertThat(result.likePredicates().getFirst().pattern()).isEqualTo("%foo");
        assertThat(result.remainingExpression()).isEqualTo(Constant.TRUE);
    }

    @Test
    public void testExtractLikePredicatesWithAnd()
    {
        // Create: name LIKE 'foo%' AND id > 10
        ConnectorExpression likeExpr = new Call(
                io.trino.spi.type.BooleanType.BOOLEAN,
                LIKE_FUNCTION_NAME,
                List.of(
                        new Variable("name", VARCHAR),
                        new Constant(Slices.utf8Slice("foo%"), VARCHAR)));

        ConnectorExpression otherExpr = new Call(
                io.trino.spi.type.BooleanType.BOOLEAN,
                new FunctionName("$greater_than"),
                List.of(
                        new Variable("id", INTEGER),
                        new Constant(10L, INTEGER)));

        ConnectorExpression andExpr = new Call(
                io.trino.spi.type.BooleanType.BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(likeExpr, otherExpr));

        Map<String, ColumnHandle> assignments = Map.of("name", VARCHAR_COLUMN, "id", INT_COLUMN);

        SubstraitExpressionBuilder.LikePredicateExtractionResult result =
                SubstraitExpressionBuilder.extractLikePredicates(andExpr, assignments);

        // LIKE should be extracted, other predicate should remain
        assertThat(result.likePredicates()).hasSize(1);
        assertThat(result.likePredicates().getFirst().pattern()).isEqualTo("foo%");
        // The remaining expression should be the > predicate
        assertThat(result.remainingExpression()).isEqualTo(otherExpr);
    }

    @Test
    public void testLikeExpressionCreation()
    {
        Expression likeExpr = SubstraitExpressionBuilder.likeExpression(2, io.substrait.type.TypeCreator.of(false).STRING, "foo%");
        assertThat(likeExpr).isInstanceOf(Expression.ScalarFunctionInvocation.class);
    }

    @Test
    public void testCombineExpressionsWithLike()
    {
        // Create TupleDomain expression
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.singleValue(INTEGER, 42L)));
        Optional<Expression> tupleDomainExpr = SubstraitExpressionBuilder.tupleDomainToExpression(domain, COLUMN_ORDINALS);

        // Create LIKE predicate
        List<SubstraitExpressionBuilder.LikePredicate> likePredicates = List.of(
                new SubstraitExpressionBuilder.LikePredicate("name", VARCHAR_COLUMN, "foo%"));

        Optional<ByteBuffer> result = SubstraitExpressionBuilder.combineExpressionsToSubstrait(
                tupleDomainExpr, likePredicates, ALL_COLUMNS, COLUMN_ORDINALS);

        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testCombineExpressionsOnlyLike()
    {
        // Only LIKE predicate, no TupleDomain
        List<SubstraitExpressionBuilder.LikePredicate> likePredicates = List.of(
                new SubstraitExpressionBuilder.LikePredicate("name", VARCHAR_COLUMN, "prefix%"));

        Optional<ByteBuffer> result = SubstraitExpressionBuilder.combineExpressionsToSubstrait(
                Optional.empty(), likePredicates, ALL_COLUMNS, COLUMN_ORDINALS);

        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }
}
