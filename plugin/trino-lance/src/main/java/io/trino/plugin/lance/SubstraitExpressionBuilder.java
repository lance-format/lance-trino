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

import io.airlift.slice.Slice;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.ExpressionReference;
import io.substrait.proto.ExtendedExpression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type.Nullability;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarcharType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

/**
 * Converts Trino TupleDomain predicates to Substrait Expression format.
 * The resulting expression can be serialized to protobuf and passed to Lance's substraitFilter.
 */
public final class SubstraitExpressionBuilder
{
    private static final TypeCreator R = TypeCreator.of(false);

    private static final SimpleExtension.ExtensionCollection EXTENSIONS;

    static {
        try {
            EXTENSIONS = SimpleExtension.loadDefaults();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to load Substrait extensions", e);
        }
    }

    private SubstraitExpressionBuilder() {}

    /**
     * Converts a TupleDomain to a Substrait ExtendedExpression serialized as a ByteBuffer.
     *
     * @param tupleDomain the predicate to convert
     * @param allColumns all columns from the table (for building the full schema)
     * @param columnOrdinals map of column name to ordinal position in the schema (field ID)
     * @return Optional containing the serialized Substrait ExtendedExpression, or empty if no filter
     */
    public static Optional<ByteBuffer> tupleDomainToSubstrait(
            TupleDomain<LanceColumnHandle> tupleDomain,
            List<LanceColumnHandle> allColumns,
            Map<String, Integer> columnOrdinals)
    {
        Optional<Expression> expression = tupleDomainToExpression(tupleDomain, columnOrdinals);
        if (expression.isEmpty()) {
            return Optional.empty();
        }

        // Sort columns by field ID to match the schema order
        List<LanceColumnHandle> sortedColumns = allColumns.stream()
                .sorted(Comparator.comparingInt(LanceColumnHandle::fieldId))
                .toList();

        return Optional.of(serializeAsExtendedExpression(expression.get(), sortedColumns, columnOrdinals));
    }

    /**
     * Converts a TupleDomain to a Substrait Expression.
     */
    public static Optional<Expression> tupleDomainToExpression(
            TupleDomain<LanceColumnHandle> tupleDomain,
            Map<String, Integer> columnOrdinals)
    {
        if (tupleDomain.isAll()) {
            return Optional.empty();
        }
        if (tupleDomain.isNone()) {
            // Return a false expression: false AND true = false
            return Optional.of(ExpressionCreator.bool(false, false));
        }

        Map<LanceColumnHandle, Domain> domains = tupleDomain.getDomains().orElse(Map.of());
        if (domains.isEmpty()) {
            return Optional.empty();
        }

        List<Expression> columnExpressions = new ArrayList<>();
        for (Map.Entry<LanceColumnHandle, Domain> entry : domains.entrySet()) {
            LanceColumnHandle column = entry.getKey();
            Domain domain = entry.getValue();
            Integer ordinal = columnOrdinals.get(column.name());
            if (ordinal == null) {
                continue;
            }

            Optional<Expression> columnExpr = domainToExpression(column.name(), column.trinoType(), domain, ordinal);
            columnExpr.ifPresent(columnExpressions::add);
        }

        if (columnExpressions.isEmpty()) {
            return Optional.empty();
        }

        if (columnExpressions.size() == 1) {
            return Optional.of(columnExpressions.getFirst());
        }

        // Combine with AND
        return Optional.of(andExpressions(columnExpressions));
    }

    private static Optional<Expression> domainToExpression(
            String columnName,
            io.trino.spi.type.Type trinoType,
            Domain domain,
            int ordinal)
    {
        if (domain.isAll()) {
            return Optional.empty();
        }
        if (domain.isNone()) {
            return Optional.of(ExpressionCreator.bool(false, false));
        }

        Type substraitType = trinoTypeToSubstrait(trinoType);
        List<Expression> predicates = new ArrayList<>();

        // Handle null check
        if (domain.isNullAllowed()) {
            predicates.add(isNullExpression(ordinal, substraitType));
        }

        ValueSet valueSet = domain.getValues();
        if (!valueSet.isNone()) {
            Optional<Expression> valueExpr = valueSetToExpression(trinoType, valueSet, domain.isNullAllowed(), ordinal, substraitType);
            valueExpr.ifPresent(predicates::add);
        }

        if (predicates.isEmpty()) {
            return Optional.empty();
        }

        if (predicates.size() == 1) {
            return Optional.of(predicates.getFirst());
        }

        // Multiple predicates for same column use OR (e.g., IS NULL OR value conditions)
        return Optional.of(orExpressions(predicates));
    }

    private static Optional<Expression> valueSetToExpression(
            io.trino.spi.type.Type trinoType,
            ValueSet valueSet,
            boolean nullAllowed,
            int ordinal,
            Type substraitType)
    {
        if (valueSet.isNone()) {
            return Optional.empty();
        }
        if (valueSet.isAll()) {
            if (!nullAllowed) {
                return Optional.of(isNotNullExpression(ordinal, substraitType));
            }
            return Optional.empty();
        }

        if (valueSet.isSingleValue()) {
            Object value = valueSet.getSingleValue();
            return Optional.of(equalExpression(ordinal, substraitType, trinoType, value));
        }

        List<Range> ranges = valueSet.getRanges().getOrderedRanges();
        if (ranges.isEmpty()) {
            return Optional.empty();
        }

        // Handle single value range
        if (ranges.size() == 1 && ranges.getFirst().isSingleValue()) {
            Object value = ranges.getFirst().getSingleValue();
            return Optional.of(equalExpression(ordinal, substraitType, trinoType, value));
        }

        // Handle IN clause (all single values)
        boolean allSingleValues = ranges.stream().allMatch(Range::isSingleValue);
        if (allSingleValues && ranges.size() > 1) {
            return Optional.of(inExpression(ordinal, substraitType, trinoType, ranges));
        }

        // Handle range predicates
        List<Expression> rangeExpressions = new ArrayList<>();
        for (Range range : ranges) {
            Optional<Expression> rangeExpr = rangeToExpression(trinoType, range, ordinal, substraitType);
            rangeExpr.ifPresent(rangeExpressions::add);
        }

        if (rangeExpressions.isEmpty()) {
            return Optional.empty();
        }
        if (rangeExpressions.size() == 1) {
            return Optional.of(rangeExpressions.getFirst());
        }

        return Optional.of(orExpressions(rangeExpressions));
    }

    private static Optional<Expression> rangeToExpression(
            io.trino.spi.type.Type trinoType,
            Range range,
            int ordinal,
            Type substraitType)
    {
        if (range.isAll()) {
            return Optional.empty();
        }

        if (range.isSingleValue()) {
            return Optional.of(equalExpression(ordinal, substraitType, trinoType, range.getSingleValue()));
        }

        List<Expression> bounds = new ArrayList<>();

        if (!range.isLowUnbounded()) {
            Expression fieldRef = fieldReference(ordinal, substraitType);
            Expression literal = toLiteral(trinoType, range.getLowBoundedValue(), substraitType);
            if (range.isLowInclusive()) {
                bounds.add(greaterThanOrEqual(fieldRef, literal));
            }
            else {
                bounds.add(greaterThan(fieldRef, literal));
            }
        }

        if (!range.isHighUnbounded()) {
            Expression fieldRef = fieldReference(ordinal, substraitType);
            Expression literal = toLiteral(trinoType, range.getHighBoundedValue(), substraitType);
            if (range.isHighInclusive()) {
                bounds.add(lessThanOrEqual(fieldRef, literal));
            }
            else {
                bounds.add(lessThan(fieldRef, literal));
            }
        }

        if (bounds.isEmpty()) {
            return Optional.empty();
        }
        if (bounds.size() == 1) {
            return Optional.of(bounds.getFirst());
        }

        return Optional.of(andExpressions(bounds));
    }

    // Expression builders

    private static Expression fieldReference(int ordinal, Type type)
    {
        return FieldReference.newRootStructReference(ordinal, type);
    }

    private static Expression isNullExpression(int ordinal, Type type)
    {
        Expression fieldRef = fieldReference(ordinal, type);
        return scalarFunction("is_null:any", R.BOOLEAN, fieldRef);
    }

    private static Expression isNotNullExpression(int ordinal, Type type)
    {
        Expression fieldRef = fieldReference(ordinal, type);
        return scalarFunction("is_not_null:any", R.BOOLEAN, fieldRef);
    }

    private static Expression equalExpression(int ordinal, Type substraitType, io.trino.spi.type.Type trinoType, Object value)
    {
        Expression fieldRef = fieldReference(ordinal, substraitType);
        Expression literal = toLiteral(trinoType, value, substraitType);
        return scalarFunction("equal:any_any", R.BOOLEAN, fieldRef, literal);
    }

    private static Expression inExpression(int ordinal, Type substraitType, io.trino.spi.type.Type trinoType, List<Range> ranges)
    {
        Expression fieldRef = fieldReference(ordinal, substraitType);
        List<Expression> options = new ArrayList<>();
        for (Range range : ranges) {
            options.add(toLiteral(trinoType, range.getSingleValue(), substraitType));
        }
        return Expression.SingleOrList.builder()
                .condition(fieldRef)
                .options(options)
                .build();
    }

    private static Expression greaterThan(Expression left, Expression right)
    {
        return scalarFunction("gt:any_any", R.BOOLEAN, left, right);
    }

    private static Expression greaterThanOrEqual(Expression left, Expression right)
    {
        return scalarFunction("gte:any_any", R.BOOLEAN, left, right);
    }

    private static Expression lessThan(Expression left, Expression right)
    {
        return scalarFunction("lt:any_any", R.BOOLEAN, left, right);
    }

    private static Expression lessThanOrEqual(Expression left, Expression right)
    {
        return scalarFunction("lte:any_any", R.BOOLEAN, left, right);
    }

    private static Expression andExpressions(List<Expression> expressions)
    {
        if (expressions.size() == 1) {
            return expressions.getFirst();
        }
        // Build AND using boolean function with varargs
        SimpleExtension.ScalarFunctionVariant declaration =
                EXTENSIONS.getScalarFunction(SimpleExtension.FunctionAnchor.of(
                        DefaultExtensionCatalog.FUNCTIONS_BOOLEAN, "and:bool"));
        return Expression.ScalarFunctionInvocation.builder()
                .declaration(declaration)
                .outputType(R.BOOLEAN)
                .arguments(expressions)
                .build();
    }

    private static Expression orExpressions(List<Expression> expressions)
    {
        if (expressions.size() == 1) {
            return expressions.getFirst();
        }
        // Build OR using boolean function with varargs
        SimpleExtension.ScalarFunctionVariant declaration =
                EXTENSIONS.getScalarFunction(SimpleExtension.FunctionAnchor.of(
                        DefaultExtensionCatalog.FUNCTIONS_BOOLEAN, "or:bool"));
        return Expression.ScalarFunctionInvocation.builder()
                .declaration(declaration)
                .outputType(R.BOOLEAN)
                .arguments(expressions)
                .build();
    }

    private static Expression scalarFunction(String key, Type outputType, Expression... args)
    {
        SimpleExtension.ScalarFunctionVariant declaration =
                EXTENSIONS.getScalarFunction(SimpleExtension.FunctionAnchor.of(
                        DefaultExtensionCatalog.FUNCTIONS_COMPARISON, key));
        return Expression.ScalarFunctionInvocation.builder()
                .declaration(declaration)
                .outputType(outputType)
                .arguments(List.of(args))
                .build();
    }

    private static Expression toLiteral(io.trino.spi.type.Type trinoType, Object value, Type substraitType)
    {
        if (value == null) {
            return ExpressionCreator.typedNull(substraitType);
        }

        if (trinoType.equals(BOOLEAN)) {
            return ExpressionCreator.bool(false, (Boolean) value);
        }
        else if (trinoType.equals(TINYINT)) {
            return ExpressionCreator.i8(false, ((Long) value).byteValue());
        }
        else if (trinoType.equals(SMALLINT)) {
            return ExpressionCreator.i16(false, ((Long) value).shortValue());
        }
        else if (trinoType.equals(INTEGER)) {
            return ExpressionCreator.i32(false, ((Long) value).intValue());
        }
        else if (trinoType.equals(BIGINT)) {
            return ExpressionCreator.i64(false, (Long) value);
        }
        else if (trinoType.equals(REAL)) {
            int floatBits = ((Long) value).intValue();
            float floatValue = Float.intBitsToFloat(floatBits);
            return ExpressionCreator.fp32(false, floatValue);
        }
        else if (trinoType.equals(DOUBLE)) {
            return ExpressionCreator.fp64(false, (Double) value);
        }
        else if (trinoType instanceof VarcharType) {
            String strValue = ((Slice) value).toStringUtf8();
            return ExpressionCreator.string(false, strValue);
        }
        else if (trinoType instanceof DateType) {
            int daysSinceEpoch = ((Long) value).intValue();
            return ExpressionCreator.date(false, daysSinceEpoch);
        }
        else if (trinoType instanceof TimestampType) {
            // Trino stores timestamps as microseconds since epoch
            long epochMicros = (Long) value;
            // Substrait uses microsecond precision timestamps
            return ExpressionCreator.precisionTimestamp(false, epochMicros, 6);
        }
        else if (trinoType instanceof TimestampWithTimeZoneType) {
            // Trino stores timestamp with timezone as packed long (millis + zone key)
            // Extract milliseconds and convert to microseconds
            long packedValue = (Long) value;
            long epochMillis = io.trino.spi.type.DateTimeEncoding.unpackMillisUtc(packedValue);
            return ExpressionCreator.precisionTimestampTZ(false, epochMillis * 1000, 6);
        }

        throw new UnsupportedOperationException("Unsupported type for Substrait literal: " + trinoType);
    }

    private static Type trinoTypeToSubstrait(io.trino.spi.type.Type trinoType)
    {
        if (trinoType.equals(BOOLEAN)) {
            return R.BOOLEAN;
        }
        else if (trinoType.equals(TINYINT)) {
            return R.I8;
        }
        else if (trinoType.equals(SMALLINT)) {
            return R.I16;
        }
        else if (trinoType.equals(INTEGER)) {
            return R.I32;
        }
        else if (trinoType.equals(BIGINT)) {
            return R.I64;
        }
        else if (trinoType.equals(REAL)) {
            return R.FP32;
        }
        else if (trinoType.equals(DOUBLE)) {
            return R.FP64;
        }
        else if (trinoType instanceof VarcharType) {
            return R.STRING;
        }
        else if (trinoType instanceof DateType) {
            return R.DATE;
        }
        else if (trinoType instanceof TimestampType timestampType) {
            return R.precisionTimestamp(timestampType.getPrecision());
        }
        else if (trinoType instanceof TimestampWithTimeZoneType tzType) {
            return R.precisionTimestampTZ(tzType.getPrecision());
        }

        throw new UnsupportedOperationException("Unsupported type for Substrait: " + trinoType);
    }

    /**
     * Serializes an expression as a Substrait ExtendedExpression.
     * Lance requires ExtendedExpression format, not raw Expression.
     */
    private static ByteBuffer serializeAsExtendedExpression(
            Expression expression,
            List<LanceColumnHandle> columns,
            Map<String, Integer> columnOrdinals)
    {
        ExtensionCollector extensionCollector = new ExtensionCollector();
        RelProtoConverter relProtoConverter = new RelProtoConverter(extensionCollector);
        ExpressionProtoConverter converter = new ExpressionProtoConverter(extensionCollector, relProtoConverter);

        // Convert the expression to proto
        io.substrait.proto.Expression protoExpression = expression.accept(converter);

        // Build the schema (NamedStruct)
        NamedStruct.Builder schemaBuilder = NamedStruct.newBuilder();
        io.substrait.proto.Type.Struct.Builder structBuilder = io.substrait.proto.Type.Struct.newBuilder()
                .setNullability(Nullability.NULLABILITY_REQUIRED);

        for (LanceColumnHandle column : columns) {
            schemaBuilder.addNames(column.name());
            structBuilder.addTypes(trinoTypeToProtoType(column.trinoType()));
        }
        schemaBuilder.setStruct(structBuilder.build());

        // Build the ExpressionReference
        ExpressionReference exprRef = ExpressionReference.newBuilder()
                .setExpression(protoExpression)
                .addOutputNames("filter_mask")
                .build();

        // Build the ExtendedExpression with collected extensions
        ExtendedExpression.Builder extendedExprBuilder = ExtendedExpression.newBuilder()
                .setVersion(io.substrait.proto.Version.newBuilder()
                        .setMajorNumber(0)
                        .setMinorNumber(70)
                        .setPatchNumber(0)
                        .setProducer("trino-lance")
                        .build())
                .setBaseSchema(schemaBuilder.build())
                .addReferredExpr(exprRef);

        // Add extension URIs and declarations from collector
        extensionCollector.addExtensionsToExtendedExpression(extendedExprBuilder);

        byte[] bytes = extendedExprBuilder.build().toByteArray();
        return ByteBuffer.wrap(bytes);
    }

    /**
     * Converts a Trino type to Substrait proto Type.
     */
    private static io.substrait.proto.Type trinoTypeToProtoType(io.trino.spi.type.Type trinoType)
    {
        io.substrait.proto.Type.Builder builder = io.substrait.proto.Type.newBuilder();

        if (trinoType.equals(BOOLEAN)) {
            return builder.setBool(io.substrait.proto.Type.Boolean.newBuilder()
                    .setNullability(Nullability.NULLABILITY_NULLABLE)).build();
        }
        else if (trinoType.equals(TINYINT)) {
            return builder.setI8(io.substrait.proto.Type.I8.newBuilder()
                    .setNullability(Nullability.NULLABILITY_NULLABLE)).build();
        }
        else if (trinoType.equals(SMALLINT)) {
            return builder.setI16(io.substrait.proto.Type.I16.newBuilder()
                    .setNullability(Nullability.NULLABILITY_NULLABLE)).build();
        }
        else if (trinoType.equals(INTEGER)) {
            return builder.setI32(io.substrait.proto.Type.I32.newBuilder()
                    .setNullability(Nullability.NULLABILITY_NULLABLE)).build();
        }
        else if (trinoType.equals(BIGINT)) {
            return builder.setI64(io.substrait.proto.Type.I64.newBuilder()
                    .setNullability(Nullability.NULLABILITY_NULLABLE)).build();
        }
        else if (trinoType.equals(REAL)) {
            return builder.setFp32(io.substrait.proto.Type.FP32.newBuilder()
                    .setNullability(Nullability.NULLABILITY_NULLABLE)).build();
        }
        else if (trinoType.equals(DOUBLE)) {
            return builder.setFp64(io.substrait.proto.Type.FP64.newBuilder()
                    .setNullability(Nullability.NULLABILITY_NULLABLE)).build();
        }
        else if (trinoType instanceof VarcharType) {
            return builder.setString(io.substrait.proto.Type.String.newBuilder()
                    .setNullability(Nullability.NULLABILITY_NULLABLE)).build();
        }
        else if (trinoType instanceof DateType) {
            return builder.setDate(io.substrait.proto.Type.Date.newBuilder()
                    .setNullability(Nullability.NULLABILITY_NULLABLE)).build();
        }
        else if (trinoType instanceof TimestampType timestampType) {
            return builder.setPrecisionTimestamp(io.substrait.proto.Type.PrecisionTimestamp.newBuilder()
                    .setPrecision(timestampType.getPrecision())
                    .setNullability(Nullability.NULLABILITY_NULLABLE)).build();
        }
        else if (trinoType instanceof TimestampWithTimeZoneType tzType) {
            return builder.setPrecisionTimestampTz(io.substrait.proto.Type.PrecisionTimestampTZ.newBuilder()
                    .setPrecision(tzType.getPrecision())
                    .setNullability(Nullability.NULLABILITY_NULLABLE)).build();
        }
        else if (trinoType instanceof ArrayType arrayType) {
            io.trino.spi.type.Type elementType = arrayType.getElementType();
            return builder.setList(io.substrait.proto.Type.List.newBuilder()
                    .setType(trinoTypeToProtoType(elementType))
                    .setNullability(Nullability.NULLABILITY_NULLABLE)).build();
        }

        throw new UnsupportedOperationException("Unsupported type for Substrait proto: " + trinoType);
    }

    /**
     * Check if a type is supported for Substrait conversion.
     */
    public static boolean isSupportedType(io.trino.spi.type.Type type)
    {
        return type.equals(BOOLEAN) ||
                type.equals(TINYINT) ||
                type.equals(SMALLINT) ||
                type.equals(INTEGER) ||
                type.equals(BIGINT) ||
                type.equals(REAL) ||
                type.equals(DOUBLE) ||
                type instanceof VarcharType ||
                type instanceof DateType ||
                type instanceof TimestampType ||
                type instanceof TimestampWithTimeZoneType;
    }

    /**
     * Check if a domain can be pushed down via Substrait.
     * Lance is optimized for random reads, so we always push down all predicates including large IN clauses.
     */
    public static boolean isDomainPushable(Domain domain)
    {
        return true;
    }
}
