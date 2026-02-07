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
import io.trino.plugin.lance.internal.SubstraitExpressionBuilder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSubstraitExpressionBuilder
{
    private static final LanceColumnHandle INT_COLUMN = new LanceColumnHandle("id", INTEGER, true);
    private static final LanceColumnHandle BIGINT_COLUMN = new LanceColumnHandle("big_id", BIGINT, true);
    private static final LanceColumnHandle VARCHAR_COLUMN = new LanceColumnHandle("name", VARCHAR, true);
    private static final LanceColumnHandle BOOLEAN_COLUMN = new LanceColumnHandle("active", BOOLEAN, true);

    private static final Map<String, Integer> COLUMN_ORDINALS = Map.of(
            "id", 0,
            "big_id", 1,
            "name", 2,
            "active", 3);

    @Test
    public void testAllDomain()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.all();
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, COLUMN_ORDINALS);
        assertThat(result).isEmpty();
    }

    @Test
    public void testNoneDomain()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.none();
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        // The result should be a false literal
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testSingleEquality()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.singleValue(INTEGER, 42L)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testVarcharEquality()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(VARCHAR_COLUMN, Domain.singleValue(VARCHAR, Slices.utf8Slice("test"))));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testBooleanValue()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(BOOLEAN_COLUMN, Domain.singleValue(BOOLEAN, true)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testInClause()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.multipleValues(INTEGER, java.util.List.of(1L, 2L, 3L))));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testRangeGreaterThan()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.create(
                        ValueSet.ofRanges(Range.greaterThan(INTEGER, 10L)), false)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testRangeBetween()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.create(
                        ValueSet.ofRanges(Range.range(INTEGER, 10L, true, 100L, true)), false)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testIsNull()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.onlyNull(INTEGER)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, COLUMN_ORDINALS);
        assertThat(result).isPresent();
        assertThat(result.get().remaining()).isGreaterThan(0);
    }

    @Test
    public void testIsNotNull()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.notNull(INTEGER)));
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, COLUMN_ORDINALS);
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
        Optional<ByteBuffer> result = SubstraitExpressionBuilder.tupleDomainToSubstrait(domain, COLUMN_ORDINALS);
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
    }

    @Test
    public void testIsDomainPushable()
    {
        Domain simpleDomain = Domain.singleValue(INTEGER, 42L);
        assertThat(SubstraitExpressionBuilder.isDomainPushable(simpleDomain)).isTrue();

        assertThat(SubstraitExpressionBuilder.isDomainPushable(Domain.all(INTEGER))).isTrue();
        assertThat(SubstraitExpressionBuilder.isDomainPushable(Domain.none(INTEGER))).isTrue();
    }
}
