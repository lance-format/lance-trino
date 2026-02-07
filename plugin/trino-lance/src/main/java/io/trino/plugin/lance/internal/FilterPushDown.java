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
package io.trino.plugin.lance.internal;

import io.airlift.slice.Slice;
import io.trino.plugin.lance.LanceColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DateType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;

public final class FilterPushDown
{
    private static final int MAX_RANGES_FOR_PUSHDOWN = 100;

    private FilterPushDown() {}

    public static Optional<String> tupleDomainToFilter(TupleDomain<LanceColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return Optional.empty();
        }
        if (tupleDomain.isNone()) {
            return Optional.of("1 = 0");
        }

        Map<LanceColumnHandle, Domain> domains = tupleDomain.getDomains().orElse(Map.of());
        if (domains.isEmpty()) {
            return Optional.empty();
        }

        List<String> columnFilters = new ArrayList<>();
        for (Map.Entry<LanceColumnHandle, Domain> entry : domains.entrySet()) {
            LanceColumnHandle column = entry.getKey();
            Domain domain = entry.getValue();

            Optional<String> columnFilter = domainToFilter(column.name(), column.trinoType(), domain);
            columnFilter.ifPresent(columnFilters::add);
        }

        if (columnFilters.isEmpty()) {
            return Optional.empty();
        }

        String combined = columnFilters.stream()
                .map(f -> "(" + f + ")")
                .collect(Collectors.joining(" AND "));
        return Optional.of(combined);
    }

    private static Optional<String> domainToFilter(String columnName, Type type, Domain domain)
    {
        if (domain.isAll()) {
            return Optional.empty();
        }
        if (domain.isNone()) {
            return Optional.of("1 = 0");
        }

        List<String> predicates = new ArrayList<>();

        if (domain.isNullAllowed()) {
            predicates.add(columnName + " IS NULL");
        }

        ValueSet valueSet = domain.getValues();
        if (!valueSet.isNone()) {
            Optional<String> valueFilter = valueSetToFilter(columnName, type, valueSet, domain.isNullAllowed());
            valueFilter.ifPresent(predicates::add);
        }

        if (predicates.isEmpty()) {
            return Optional.empty();
        }

        if (predicates.size() == 1) {
            return Optional.of(predicates.getFirst());
        }

        return Optional.of(predicates.stream()
                .map(p -> "(" + p + ")")
                .collect(Collectors.joining(" OR ")));
    }

    private static Optional<String> valueSetToFilter(String columnName, Type type, ValueSet valueSet, boolean nullAllowed)
    {
        if (valueSet.isNone()) {
            return Optional.empty();
        }
        if (valueSet.isAll()) {
            if (!nullAllowed) {
                return Optional.of(columnName + " IS NOT NULL");
            }
            return Optional.empty();
        }

        if (valueSet.isSingleValue()) {
            Object value = valueSet.getSingleValue();
            return Optional.of(columnName + " == " + formatValue(type, value));
        }

        List<Range> ranges = valueSet.getRanges().getOrderedRanges();
        if (ranges.isEmpty()) {
            return Optional.empty();
        }

        // Skip pushdown for very complex filters (e.g., large NOT IN clauses)
        // Lance has a limit of 500 conditions, so we use a conservative threshold
        if (ranges.size() > MAX_RANGES_FOR_PUSHDOWN) {
            return Optional.empty();
        }

        if (ranges.size() == 1 && ranges.getFirst().isSingleValue()) {
            Object value = ranges.getFirst().getSingleValue();
            return Optional.of(columnName + " == " + formatValue(type, value));
        }

        boolean allSingleValues = ranges.stream().allMatch(Range::isSingleValue);
        if (allSingleValues && ranges.size() > 1) {
            String inValues = ranges.stream()
                    .map(r -> formatValue(type, r.getSingleValue()))
                    .collect(Collectors.joining(", "));
            return Optional.of(columnName + " IN (" + inValues + ")");
        }

        List<String> rangePredicates = new ArrayList<>();
        for (Range range : ranges) {
            Optional<String> rangePredicate = rangeToFilter(columnName, type, range);
            rangePredicate.ifPresent(rangePredicates::add);
        }

        if (rangePredicates.isEmpty()) {
            return Optional.empty();
        }
        if (rangePredicates.size() == 1) {
            return Optional.of(rangePredicates.getFirst());
        }

        return Optional.of(rangePredicates.stream()
                .map(p -> "(" + p + ")")
                .collect(Collectors.joining(" OR ")));
    }

    private static Optional<String> rangeToFilter(String columnName, Type type, Range range)
    {
        if (range.isAll()) {
            return Optional.empty();
        }

        if (range.isSingleValue()) {
            return Optional.of(columnName + " == " + formatValue(type, range.getSingleValue()));
        }

        List<String> bounds = new ArrayList<>();

        if (!range.isLowUnbounded()) {
            String op = range.isLowInclusive() ? " >= " : " > ";
            bounds.add(columnName + op + formatValue(type, range.getLowBoundedValue()));
        }

        if (!range.isHighUnbounded()) {
            String op = range.isHighInclusive() ? " <= " : " < ";
            bounds.add(columnName + op + formatValue(type, range.getHighBoundedValue()));
        }

        if (bounds.isEmpty()) {
            return Optional.empty();
        }
        if (bounds.size() == 1) {
            return Optional.of(bounds.getFirst());
        }

        return Optional.of(String.join(" AND ", bounds));
    }

    private static String formatValue(Type type, Object value)
    {
        if (value == null) {
            return "NULL";
        }

        if (type instanceof VarcharType) {
            String strValue = ((Slice) value).toStringUtf8();
            return "'" + strValue.replace("'", "''") + "'";
        }
        else if (type.equals(BOOLEAN)) {
            return value.toString();
        }
        else if (type.equals(INTEGER) || type.equals(BIGINT)) {
            return value.toString();
        }
        else if (type.equals(REAL)) {
            int floatBits = ((Long) value).intValue();
            float floatValue = Float.intBitsToFloat(floatBits);
            return String.valueOf(floatValue);
        }
        else if (type.equals(DOUBLE)) {
            return value.toString();
        }
        else if (type instanceof DateType) {
            long daysSinceEpoch = (Long) value;
            LocalDate date = LocalDate.ofEpochDay(daysSinceEpoch);
            return "date '" + date.toString() + "'";
        }

        return value.toString();
    }

    public static boolean isSupportedType(Type type)
    {
        return type.equals(BOOLEAN) ||
                type.equals(INTEGER) ||
                type.equals(BIGINT) ||
                type.equals(REAL) ||
                type.equals(DOUBLE) ||
                type instanceof VarcharType ||
                type instanceof DateType;
    }

    /**
     * Check if a domain can be pushed down to Lance.
     * A domain cannot be pushed if it has too many ranges (e.g., large IN/NOT IN clauses).
     */
    public static boolean isDomainPushable(Domain domain)
    {
        if (domain.isAll() || domain.isNone()) {
            return true;
        }

        ValueSet valueSet = domain.getValues();
        if (valueSet.isNone() || valueSet.isAll()) {
            return true;
        }

        if (valueSet.isSingleValue()) {
            return true;
        }

        List<Range> ranges = valueSet.getRanges().getOrderedRanges();
        return ranges.size() <= MAX_RANGES_FOR_PUSHDOWN;
    }
}
