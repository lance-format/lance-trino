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
import io.trino.plugin.lance.internal.FilterPushDown;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFilterPushDown
{
    private static final LanceColumnHandle INT_COLUMN = new LanceColumnHandle("id", INTEGER, true);
    private static final LanceColumnHandle BIGINT_COLUMN = new LanceColumnHandle("big_id", BIGINT, true);
    private static final LanceColumnHandle VARCHAR_COLUMN = new LanceColumnHandle("name", VARCHAR, true);
    private static final LanceColumnHandle BOOLEAN_COLUMN = new LanceColumnHandle("active", BOOLEAN, true);
    private static final LanceColumnHandle DOUBLE_COLUMN = new LanceColumnHandle("price", DOUBLE, true);
    private static final LanceColumnHandle DATE_COLUMN = new LanceColumnHandle("created_at", DATE, true);
    private static final LanceColumnHandle REAL_COLUMN = new LanceColumnHandle("score", REAL, true);
    private static final LanceColumnHandle SMALLINT_COLUMN = new LanceColumnHandle("small_val", SMALLINT, true);
    private static final LanceColumnHandle TINYINT_COLUMN = new LanceColumnHandle("tiny_val", TINYINT, true);

    @Test
    public void testAllDomain()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.all();
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isEmpty();
    }

    @Test
    public void testNoneDomain()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.none();
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("1 = 0");
    }

    @Test
    public void testSingleEquality()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.singleValue(INTEGER, 42L)));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(id == 42)");
    }

    @Test
    public void testVarcharEquality()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(VARCHAR_COLUMN, Domain.singleValue(VARCHAR, Slices.utf8Slice("test"))));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(name == 'test')");
    }

    @Test
    public void testVarcharWithQuotes()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(VARCHAR_COLUMN, Domain.singleValue(VARCHAR, Slices.utf8Slice("it's a test"))));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(name == 'it''s a test')");
    }

    @Test
    public void testBooleanValue()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(BOOLEAN_COLUMN, Domain.singleValue(BOOLEAN, true)));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(active == true)");
    }

    @Test
    public void testDateValue()
    {
        long daysSinceEpoch = LocalDate.of(2024, 1, 15).toEpochDay();
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(DATE_COLUMN, Domain.singleValue(DATE, daysSinceEpoch)));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(created_at == date '2024-01-15')");
    }

    @Test
    public void testRealValue()
    {
        // REAL values are stored as the integer bits of the float
        long floatBits = Float.floatToIntBits(3.14f);
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(REAL_COLUMN, Domain.singleValue(REAL, floatBits)));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(score == 3.14)");
    }

    @Test
    public void testDoubleValue()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(DOUBLE_COLUMN, Domain.singleValue(DOUBLE, 99.99)));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(price == 99.99)");
    }

    @Test
    public void testSmallintValue()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(SMALLINT_COLUMN, Domain.singleValue(SMALLINT, 100L)));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(small_val == 100)");
    }

    @Test
    public void testTinyintValue()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(TINYINT_COLUMN, Domain.singleValue(TINYINT, 10L)));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(tiny_val == 10)");
    }

    @Test
    public void testInClause()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.multipleValues(INTEGER, java.util.List.of(1L, 2L, 3L))));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(id IN (1, 2, 3))");
    }

    @Test
    public void testRangeGreaterThan()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.create(
                        ValueSet.ofRanges(Range.greaterThan(INTEGER, 10L)), false)));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(id > 10)");
    }

    @Test
    public void testRangeGreaterThanOrEqual()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.create(
                        ValueSet.ofRanges(Range.greaterThanOrEqual(INTEGER, 10L)), false)));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(id >= 10)");
    }

    @Test
    public void testRangeLessThan()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.create(
                        ValueSet.ofRanges(Range.lessThan(INTEGER, 100L)), false)));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(id < 100)");
    }

    @Test
    public void testRangeLessThanOrEqual()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.create(
                        ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 100L)), false)));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(id <= 100)");
    }

    @Test
    public void testRangeBetween()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.create(
                        ValueSet.ofRanges(Range.range(INTEGER, 10L, true, 100L, true)), false)));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(id >= 10 AND id <= 100)");
    }

    @Test
    public void testIsNull()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.onlyNull(INTEGER)));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(id IS NULL)");
    }

    @Test
    public void testIsNotNull()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(INT_COLUMN, Domain.notNull(INTEGER)));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        assertThat(filter.get()).isEqualTo("(id IS NOT NULL)");
    }

    @Test
    public void testMultipleColumns()
    {
        TupleDomain<LanceColumnHandle> domain = TupleDomain.withColumnDomains(
                Map.of(
                        INT_COLUMN, Domain.singleValue(INTEGER, 42L),
                        VARCHAR_COLUMN, Domain.singleValue(VARCHAR, Slices.utf8Slice("test"))));
        Optional<String> filter = FilterPushDown.tupleDomainToFilter(domain);
        assertThat(filter).isPresent();
        // Order may vary, so check both conditions are present
        assertThat(filter.get()).contains("id == 42");
        assertThat(filter.get()).contains("name == 'test'");
        assertThat(filter.get()).contains(" AND ");
    }

    @Test
    public void testIsSupportedType()
    {
        assertThat(FilterPushDown.isSupportedType(BOOLEAN)).isTrue();
        assertThat(FilterPushDown.isSupportedType(TINYINT)).isTrue();
        assertThat(FilterPushDown.isSupportedType(SMALLINT)).isTrue();
        assertThat(FilterPushDown.isSupportedType(INTEGER)).isTrue();
        assertThat(FilterPushDown.isSupportedType(BIGINT)).isTrue();
        assertThat(FilterPushDown.isSupportedType(REAL)).isTrue();
        assertThat(FilterPushDown.isSupportedType(DOUBLE)).isTrue();
        assertThat(FilterPushDown.isSupportedType(VARCHAR)).isTrue();
        assertThat(FilterPushDown.isSupportedType(DATE)).isTrue();
    }

    @Test
    public void testIsDomainPushable()
    {
        // Simple domain should be pushable
        Domain simpleDomain = Domain.singleValue(INTEGER, 42L);
        assertThat(FilterPushDown.isDomainPushable(simpleDomain)).isTrue();

        // All domain should be pushable
        assertThat(FilterPushDown.isDomainPushable(Domain.all(INTEGER))).isTrue();

        // None domain should be pushable
        assertThat(FilterPushDown.isDomainPushable(Domain.none(INTEGER))).isTrue();

        // Small IN clause should be pushable
        Domain smallIn = Domain.multipleValues(INTEGER, java.util.List.of(1L, 2L, 3L));
        assertThat(FilterPushDown.isDomainPushable(smallIn)).isTrue();
    }
}
