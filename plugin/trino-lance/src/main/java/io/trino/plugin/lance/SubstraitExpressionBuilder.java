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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
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
     * @param columnOrdinals map of column name to ordinal position in the schema
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

        return Optional.of(serializeAsExtendedExpression(expression.get(), sortedColumns));
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
        else if (trinoType instanceof TimestampType timestampType) {
            // Trino stores timestamps as microseconds since epoch for precision <= 6
            long epochMicros = (Long) value;
            // Use the same precision as the type to ensure type consistency in Substrait
            int precision = timestampType.getPrecision();
            return ExpressionCreator.precisionTimestamp(false, epochMicros, precision);
        }
        else if (trinoType instanceof TimestampWithTimeZoneType tzType) {
            // Trino stores timestamp with timezone as packed long (millis + zone key)
            // Extract milliseconds and convert to microseconds
            long packedValue = (Long) value;
            long epochMillis = io.trino.spi.type.DateTimeEncoding.unpackMillisUtc(packedValue);
            // Use the same precision as the type to ensure type consistency in Substrait
            int precision = tzType.getPrecision();
            return ExpressionCreator.precisionTimestampTZ(false, epochMillis * 1000, precision);
        }
        else if (trinoType instanceof VarbinaryType) {
            return ExpressionCreator.binary(false, ((Slice) value).getBytes());
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
        else if (trinoType instanceof VarbinaryType) {
            return R.BINARY;
        }

        throw new UnsupportedOperationException("Unsupported type for Substrait: " + trinoType);
    }

    /**
     * Serializes an expression as a Substrait ExtendedExpression.
     * Lance requires ExtendedExpression format, not raw Expression.
     */
    private static ByteBuffer serializeAsExtendedExpression(
            Expression expression,
            List<LanceColumnHandle> columns)
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
        else if (trinoType instanceof VarbinaryType) {
            return builder.setBinary(io.substrait.proto.Type.Binary.newBuilder()
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
                type instanceof TimestampWithTimeZoneType ||
                type instanceof VarbinaryType;
    }

    /**
     * Check if a domain can be pushed down via Substrait.
     * Lance is optimized for random reads, so we always push down all predicates including large IN clauses.
     */
    public static boolean isDomainPushable(Domain domain)
    {
        return true;
    }

    /**
     * Result of extracting pushable expressions from a ConnectorExpression.
     */
    public record ExpressionExtractionResult(
            List<Expression> substraitExpressions,
            List<String> columnNames,
            ConnectorExpression remainingExpression) {}

    /**
     * A LIKE predicate that can be pushed down to Lance.
     */
    public record LikePredicate(
            String columnName,
            LanceColumnHandle column,
            String pattern) {}

    /**
     * Result of extracting LIKE predicates from an expression (legacy interface).
     */
    public record LikePredicateExtractionResult(
            List<LikePredicate> likePredicates,
            ConnectorExpression remainingExpression) {}

    /**
     * Extracts pushable LIKE predicates from a ConnectorExpression (legacy method).
     *
     * @param expression the expression to extract LIKE predicates from
     * @param assignments map of variable names to column handles
     * @return extracted LIKE predicates and the remaining unpushable expression
     */
    public static LikePredicateExtractionResult extractLikePredicates(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments)
    {
        List<LikePredicate> likePredicates = new ArrayList<>();
        ConnectorExpression remaining = extractLikePredicatesRecursive(expression, assignments, likePredicates);
        return new LikePredicateExtractionResult(likePredicates, remaining);
    }

    /**
     * Extracts all pushable expressions from a ConnectorExpression.
     * Supports: LIKE, OR, NOT, IS NULL, comparisons ({@code =, <>, <, <=, >, >=}), IN.
     *
     * @param expression the expression to extract from
     * @param assignments map of variable names to column handles
     * @param columnOrdinals map of column name to ordinal position
     * @return extracted Substrait expressions, column names, and remaining unpushable expression
     */
    public static ExpressionExtractionResult extractPushableExpressions(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            Map<String, Integer> columnOrdinals)
    {
        List<Expression> substraitExprs = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();
        ConnectorExpression remaining = extractExpressionsRecursive(
                expression, assignments, columnOrdinals, substraitExprs, columnNames);
        return new ExpressionExtractionResult(substraitExprs, columnNames, remaining);
    }

    private static ConnectorExpression extractExpressionsRecursive(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            Map<String, Integer> columnOrdinals,
            List<Expression> substraitExprs,
            List<String> columnNames)
    {
        if (expression instanceof Constant) {
            return expression;
        }

        if (!(expression instanceof Call call)) {
            return expression;
        }

        // Handle AND - process both sides and combine remaining
        if (call.getFunctionName().equals(AND_FUNCTION_NAME)) {
            List<ConnectorExpression> remainingArgs = new ArrayList<>();
            for (ConnectorExpression arg : call.getArguments()) {
                ConnectorExpression remaining = extractExpressionsRecursive(
                        arg, assignments, columnOrdinals, substraitExprs, columnNames);
                if (!isConstantTrue(remaining)) {
                    remainingArgs.add(remaining);
                }
            }
            if (remainingArgs.isEmpty()) {
                return Constant.TRUE;
            }
            if (remainingArgs.size() == 1) {
                return remainingArgs.getFirst();
            }
            return new Call(call.getType(), AND_FUNCTION_NAME, remainingArgs);
        }

        // Try to convert entire expression to Substrait
        Optional<Expression> substraitExpr = tryConvertToSubstrait(call, assignments, columnOrdinals, columnNames);
        if (substraitExpr.isPresent()) {
            substraitExprs.add(substraitExpr.get());
            return Constant.TRUE;
        }

        // Not a pushable expression - return as is
        return expression;
    }

    /**
     * Try to convert a Call expression to Substrait.
     * Returns empty if the expression cannot be pushed down.
     */
    private static Optional<Expression> tryConvertToSubstrait(
            Call call,
            Map<String, ColumnHandle> assignments,
            Map<String, Integer> columnOrdinals,
            List<String> columnNames)
    {
        // Handle LIKE
        if (call.getFunctionName().equals(LIKE_FUNCTION_NAME)) {
            return tryConvertLike(call, assignments, columnOrdinals, columnNames);
        }

        // Handle IS NULL
        if (call.getFunctionName().equals(IS_NULL_FUNCTION_NAME)) {
            return tryConvertIsNull(call, assignments, columnOrdinals, columnNames);
        }

        // Handle NOT
        if (call.getFunctionName().equals(NOT_FUNCTION_NAME)) {
            return tryConvertNot(call, assignments, columnOrdinals, columnNames);
        }

        // Handle OR
        if (call.getFunctionName().equals(OR_FUNCTION_NAME)) {
            return tryConvertOr(call, assignments, columnOrdinals, columnNames);
        }

        // Handle comparison operators
        if (call.getFunctionName().equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
            return tryConvertComparison(call, "equal:any_any", assignments, columnOrdinals, columnNames);
        }
        if (call.getFunctionName().equals(NOT_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return tryConvertComparison(call, "not_equal:any_any", assignments, columnOrdinals, columnNames);
        }
        if (call.getFunctionName().equals(LESS_THAN_OPERATOR_FUNCTION_NAME)) {
            return tryConvertComparison(call, "lt:any_any", assignments, columnOrdinals, columnNames);
        }
        if (call.getFunctionName().equals(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return tryConvertComparison(call, "lte:any_any", assignments, columnOrdinals, columnNames);
        }
        if (call.getFunctionName().equals(GREATER_THAN_OPERATOR_FUNCTION_NAME)) {
            return tryConvertComparison(call, "gt:any_any", assignments, columnOrdinals, columnNames);
        }
        if (call.getFunctionName().equals(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return tryConvertComparison(call, "gte:any_any", assignments, columnOrdinals, columnNames);
        }

        // Handle IN predicate
        if (call.getFunctionName().equals(IN_PREDICATE_FUNCTION_NAME)) {
            return tryConvertIn(call, assignments, columnOrdinals, columnNames);
        }

        return Optional.empty();
    }

    private static Optional<Expression> tryConvertLike(
            Call call,
            Map<String, ColumnHandle> assignments,
            Map<String, Integer> columnOrdinals,
            List<String> columnNames)
    {
        List<ConnectorExpression> args = call.getArguments();
        if (args.size() < 2) {
            return Optional.empty();
        }

        ConnectorExpression columnExpr = args.get(0);
        ConnectorExpression patternExpr = args.get(1);

        if (columnExpr instanceof Variable variable && patternExpr instanceof Constant constant) {
            String columnName = variable.getName();
            ColumnHandle columnHandle = assignments.get(columnName);
            Integer ordinal = columnOrdinals.get(columnName);

            if (columnHandle instanceof LanceColumnHandle lanceColumn && ordinal != null) {
                Object patternValue = constant.getValue();
                if (patternValue instanceof Slice slice) {
                    String pattern = slice.toStringUtf8();
                    if (isPushableLikePattern(pattern)) {
                        Type substraitType = trinoTypeToSubstrait(lanceColumn.trinoType());
                        if (!columnNames.contains(columnName)) {
                            columnNames.add(columnName);
                        }
                        return Optional.of(likeExpression(ordinal, substraitType, pattern));
                    }
                }
            }
        }
        return Optional.empty();
    }

    private static Optional<Expression> tryConvertIsNull(
            Call call,
            Map<String, ColumnHandle> assignments,
            Map<String, Integer> columnOrdinals,
            List<String> columnNames)
    {
        List<ConnectorExpression> args = call.getArguments();
        if (args.size() != 1) {
            return Optional.empty();
        }

        ConnectorExpression arg = args.get(0);
        if (arg instanceof Variable variable) {
            String columnName = variable.getName();
            ColumnHandle columnHandle = assignments.get(columnName);
            Integer ordinal = columnOrdinals.get(columnName);

            if (columnHandle instanceof LanceColumnHandle lanceColumn && ordinal != null) {
                if (isSupportedType(lanceColumn.trinoType())) {
                    Type substraitType = trinoTypeToSubstrait(lanceColumn.trinoType());
                    if (!columnNames.contains(columnName)) {
                        columnNames.add(columnName);
                    }
                    return Optional.of(isNullExpression(ordinal, substraitType));
                }
            }
        }
        return Optional.empty();
    }

    private static Optional<Expression> tryConvertNot(
            Call call,
            Map<String, ColumnHandle> assignments,
            Map<String, Integer> columnOrdinals,
            List<String> columnNames)
    {
        List<ConnectorExpression> args = call.getArguments();
        if (args.size() != 1) {
            return Optional.empty();
        }

        ConnectorExpression arg = args.get(0);
        if (arg instanceof Call innerCall) {
            Optional<Expression> innerExpr = tryConvertToSubstrait(innerCall, assignments, columnOrdinals, columnNames);
            if (innerExpr.isPresent()) {
                return Optional.of(notExpression(innerExpr.get()));
            }
        }
        return Optional.empty();
    }

    private static Optional<Expression> tryConvertOr(
            Call call,
            Map<String, ColumnHandle> assignments,
            Map<String, Integer> columnOrdinals,
            List<String> columnNames)
    {
        List<ConnectorExpression> args = call.getArguments();
        if (args.isEmpty()) {
            return Optional.empty();
        }

        List<Expression> substraitArgs = new ArrayList<>();
        for (ConnectorExpression arg : args) {
            Optional<Expression> converted;
            if (arg instanceof Call argCall) {
                converted = tryConvertToSubstrait(argCall, assignments, columnOrdinals, columnNames);
            }
            else {
                return Optional.empty();
            }
            if (converted.isEmpty()) {
                return Optional.empty();
            }
            substraitArgs.add(converted.get());
        }
        return Optional.of(orExpressions(substraitArgs));
    }

    private static Optional<Expression> tryConvertComparison(
            Call call,
            String functionKey,
            Map<String, ColumnHandle> assignments,
            Map<String, Integer> columnOrdinals,
            List<String> columnNames)
    {
        List<ConnectorExpression> args = call.getArguments();
        if (args.size() != 2) {
            return Optional.empty();
        }

        ConnectorExpression left = args.get(0);
        ConnectorExpression right = args.get(1);

        // Handle: column op constant
        if (left instanceof Variable variable && right instanceof Constant constant) {
            return tryBuildComparison(variable, constant, functionKey, assignments, columnOrdinals, columnNames);
        }
        // Handle: constant op column (reverse operands for symmetric operators)
        if (left instanceof Constant constant && right instanceof Variable variable) {
            // Swap the comparison direction for asymmetric operators
            String reversedKey = reverseComparisonKey(functionKey);
            if (reversedKey != null) {
                return tryBuildComparison(variable, constant, reversedKey, assignments, columnOrdinals, columnNames);
            }
        }
        return Optional.empty();
    }

    private static Optional<Expression> tryBuildComparison(
            Variable variable,
            Constant constant,
            String functionKey,
            Map<String, ColumnHandle> assignments,
            Map<String, Integer> columnOrdinals,
            List<String> columnNames)
    {
        String columnName = variable.getName();
        ColumnHandle columnHandle = assignments.get(columnName);
        Integer ordinal = columnOrdinals.get(columnName);

        if (columnHandle instanceof LanceColumnHandle lanceColumn && ordinal != null) {
            io.trino.spi.type.Type trinoType = lanceColumn.trinoType();
            if (isSupportedType(trinoType)) {
                Type substraitType = trinoTypeToSubstrait(trinoType);
                Expression fieldRef = fieldReference(ordinal, substraitType);
                Expression literal = toLiteral(trinoType, constant.getValue(), substraitType);
                if (!columnNames.contains(columnName)) {
                    columnNames.add(columnName);
                }
                return Optional.of(scalarFunction(functionKey, R.BOOLEAN, fieldRef, literal));
            }
        }
        return Optional.empty();
    }

    private static String reverseComparisonKey(String key)
    {
        return switch (key) {
            case "equal:any_any", "not_equal:any_any" -> key;  // Symmetric
            case "lt:any_any" -> "gt:any_any";
            case "lte:any_any" -> "gte:any_any";
            case "gt:any_any" -> "lt:any_any";
            case "gte:any_any" -> "lte:any_any";
            default -> null;
        };
    }

    private static Optional<Expression> tryConvertIn(
            Call call,
            Map<String, ColumnHandle> assignments,
            Map<String, Integer> columnOrdinals,
            List<String> columnNames)
    {
        List<ConnectorExpression> args = call.getArguments();
        if (args.size() != 2) {
            return Optional.empty();
        }

        ConnectorExpression columnExpr = args.get(0);
        ConnectorExpression arrayExpr = args.get(1);

        if (!(columnExpr instanceof Variable variable)) {
            return Optional.empty();
        }

        String columnName = variable.getName();
        ColumnHandle columnHandle = assignments.get(columnName);
        Integer ordinal = columnOrdinals.get(columnName);

        if (!(columnHandle instanceof LanceColumnHandle lanceColumn) || ordinal == null) {
            return Optional.empty();
        }

        io.trino.spi.type.Type trinoType = lanceColumn.trinoType();
        if (!isSupportedType(trinoType)) {
            return Optional.empty();
        }

        // Extract values from $array(...) constructor
        if (arrayExpr instanceof Call arrayCall &&
                arrayCall.getFunctionName().getName().equals("$array")) {
            List<Expression> options = new ArrayList<>();
            Type substraitType = trinoTypeToSubstrait(trinoType);

            for (ConnectorExpression element : arrayCall.getArguments()) {
                if (element instanceof Constant constant) {
                    options.add(toLiteral(trinoType, constant.getValue(), substraitType));
                }
                else {
                    return Optional.empty();
                }
            }

            if (options.isEmpty()) {
                return Optional.empty();
            }

            Expression fieldRef = fieldReference(ordinal, substraitType);
            if (!columnNames.contains(columnName)) {
                columnNames.add(columnName);
            }
            return Optional.of(Expression.SingleOrList.builder()
                    .condition(fieldRef)
                    .options(options)
                    .build());
        }
        return Optional.empty();
    }

    private static ConnectorExpression extractLikePredicatesRecursive(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            List<LikePredicate> likePredicates)
    {
        if (expression instanceof Constant) {
            return expression;
        }

        if (!(expression instanceof Call call)) {
            return expression;
        }

        // Handle AND - process both sides and combine remaining
        if (call.getFunctionName().equals(AND_FUNCTION_NAME)) {
            List<ConnectorExpression> remainingArgs = new ArrayList<>();
            for (ConnectorExpression arg : call.getArguments()) {
                ConnectorExpression remaining = extractLikePredicatesRecursive(arg, assignments, likePredicates);
                if (!isConstantTrue(remaining)) {
                    remainingArgs.add(remaining);
                }
            }
            if (remainingArgs.isEmpty()) {
                return Constant.TRUE;
            }
            if (remainingArgs.size() == 1) {
                return remainingArgs.getFirst();
            }
            return new Call(call.getType(), AND_FUNCTION_NAME, remainingArgs);
        }

        // Handle LIKE
        if (call.getFunctionName().equals(LIKE_FUNCTION_NAME)) {
            List<ConnectorExpression> args = call.getArguments();
            // LIKE has 2 arguments: column, pattern (optionally 3 with escape char)
            if (args.size() >= 2) {
                ConnectorExpression columnExpr = args.get(0);
                ConnectorExpression patternExpr = args.get(1);

                if (columnExpr instanceof Variable variable && patternExpr instanceof Constant constant) {
                    String columnName = variable.getName();
                    ColumnHandle columnHandle = assignments.get(columnName);

                    if (columnHandle instanceof LanceColumnHandle lanceColumn) {
                        Object patternValue = constant.getValue();
                        if (patternValue instanceof Slice slice) {
                            String pattern = slice.toStringUtf8();
                            if (isPushableLikePattern(pattern)) {
                                likePredicates.add(new LikePredicate(columnName, lanceColumn, pattern));
                                return Constant.TRUE;
                            }
                        }
                    }
                }
            }
        }

        // Not a pushable expression - return as is
        return expression;
    }

    private static Expression notExpression(Expression arg)
    {
        SimpleExtension.ScalarFunctionVariant declaration =
                EXTENSIONS.getScalarFunction(SimpleExtension.FunctionAnchor.of(
                        DefaultExtensionCatalog.FUNCTIONS_BOOLEAN, "not:bool"));
        return Expression.ScalarFunctionInvocation.builder()
                .declaration(declaration)
                .outputType(R.BOOLEAN)
                .arguments(List.of(arg))
                .build();
    }

    /**
     * Check if a LIKE pattern is pushable to Lance.
     * All LIKE patterns are pushable - Lance will filter at the storage layer,
     * reducing data sent to Trino even if index acceleration isn't used.
     * Prefix patterns (e.g., 'foo%') can additionally use btree/zonemap indices.
     */
    public static boolean isPushableLikePattern(String pattern)
    {
        // All non-empty patterns are pushable
        return pattern != null && !pattern.isEmpty();
    }

    /**
     * Creates a Substrait LIKE expression for the given column and pattern.
     */
    public static Expression likeExpression(int ordinal, Type substraitType, String pattern)
    {
        Expression fieldRef = fieldReference(ordinal, substraitType);
        Expression patternLiteral = ExpressionCreator.string(false, pattern);

        SimpleExtension.ScalarFunctionVariant declaration =
                EXTENSIONS.getScalarFunction(SimpleExtension.FunctionAnchor.of(
                        DefaultExtensionCatalog.FUNCTIONS_STRING, "like:str_str"));
        return Expression.ScalarFunctionInvocation.builder()
                .declaration(declaration)
                .outputType(R.BOOLEAN)
                .arguments(List.of(fieldRef, patternLiteral))
                .build();
    }

    /**
     * Combines a TupleDomain expression with LIKE predicates into a single Substrait expression.
     */
    public static Optional<ByteBuffer> combineExpressionsToSubstrait(
            Optional<Expression> tupleDomainExpr,
            List<LikePredicate> likePredicates,
            List<LanceColumnHandle> allColumns,
            Map<String, Integer> columnOrdinals)
    {
        List<Expression> expressions = new ArrayList<>();

        // Add TupleDomain expression if present
        tupleDomainExpr.ifPresent(expressions::add);

        // Add LIKE expressions
        for (LikePredicate likePredicate : likePredicates) {
            Integer ordinal = columnOrdinals.get(likePredicate.columnName());
            if (ordinal != null) {
                Type substraitType = trinoTypeToSubstrait(likePredicate.column().trinoType());
                expressions.add(likeExpression(ordinal, substraitType, likePredicate.pattern()));
            }
        }

        if (expressions.isEmpty()) {
            return Optional.empty();
        }

        Expression combined = expressions.size() == 1
                ? expressions.getFirst()
                : andExpressions(expressions);

        // Sort columns by field ID to match the schema order
        List<LanceColumnHandle> sortedColumns = allColumns.stream()
                .sorted(Comparator.comparingInt(LanceColumnHandle::fieldId))
                .toList();

        return Optional.of(serializeAsExtendedExpression(combined, sortedColumns));
    }

    /**
     * Combines a TupleDomain expression with extracted Substrait expressions into a single Substrait filter.
     */
    public static Optional<ByteBuffer> combineAllExpressionsToSubstrait(
            Optional<Expression> tupleDomainExpr,
            List<Expression> pushedExpressions,
            List<LanceColumnHandle> allColumns)
    {
        List<Expression> expressions = new ArrayList<>();

        // Add TupleDomain expression if present
        tupleDomainExpr.ifPresent(expressions::add);

        // Add all pushed expressions
        expressions.addAll(pushedExpressions);

        if (expressions.isEmpty()) {
            return Optional.empty();
        }

        Expression combined = expressions.size() == 1
                ? expressions.getFirst()
                : andExpressions(expressions);

        // Sort columns by field ID to match the schema order
        List<LanceColumnHandle> sortedColumns = allColumns.stream()
                .sorted(Comparator.comparingInt(LanceColumnHandle::fieldId))
                .toList();

        return Optional.of(serializeAsExtendedExpression(combined, sortedColumns));
    }

    private static boolean isConstantTrue(ConnectorExpression expression)
    {
        return expression instanceof Constant constant && Boolean.TRUE.equals(constant.getValue());
    }
}
