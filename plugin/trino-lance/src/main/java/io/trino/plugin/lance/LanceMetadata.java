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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.DateType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Dataset;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.ManifestSummary;
import org.lance.ReadOptions;
import org.lance.Transaction;
import org.lance.WriteParams;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateEmptyTableRequest;
import org.lance.namespace.model.CreateEmptyTableResponse;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.model.DeclareTableResponse;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.operation.Append;
import org.lance.operation.Overwrite;
import org.lance.operation.Update;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.lance.RowAddress.LANCE_ROW_ADDRESS;
import static io.trino.plugin.lance.SubstraitExpressionBuilder.isDomainPushable;
import static io.trino.plugin.lance.SubstraitExpressionBuilder.isSupportedType;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class LanceMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(LanceMetadata.class);
    private static final ConcurrentMap<String, Dataset> transactionDatasets = new ConcurrentHashMap<>();

    private final LanceRuntime runtime;
    private final LanceConfig lanceConfig;
    private final JsonCodec<LanceCommitTaskData> commitTaskDataCodec;
    private final JsonCodec<LanceMergeCommitData> mergeCommitDataCodec;

    @Inject
    public LanceMetadata(
            LanceRuntime runtime,
            LanceConfig lanceConfig,
            JsonCodec<LanceCommitTaskData> commitTaskDataCodec,
            JsonCodec<LanceMergeCommitData> mergeCommitDataCodec)
    {
        this.runtime = requireNonNull(runtime, "runtime is null");
        this.lanceConfig = requireNonNull(lanceConfig, "lanceConfig is null");
        this.commitTaskDataCodec = requireNonNull(commitTaskDataCodec, "commitTaskDataCodec is null");
        this.mergeCommitDataCodec = requireNonNull(mergeCommitDataCodec, "mergeCommitDataCodec is null");
    }

    // ===== Schema/Namespace Operations =====

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        if (runtime.isSingleLevelNs()) {
            return List.of(LanceRuntime.DEFAULT_SCHEMA);
        }

        ListNamespacesRequest request = new ListNamespacesRequest();
        if (runtime.getParentPrefix().isPresent()) {
            request.setId(runtime.getParentPrefix().get());
        }
        ListNamespacesResponse response = getNamespace().listNamespaces(request);
        Set<String> namespaces = response.getNamespaces();
        if (namespaces == null || namespaces.isEmpty()) {
            return Collections.emptyList();
        }
        return namespaces.stream().collect(toImmutableList());
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        if (runtime.isSingleLevelNs()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
        }

        List<String> namespaceId = runtime.trinoSchemaToLanceNamespace(schemaName);
        log.debug("createSchema: creating namespace with id=%s for schema '%s'", namespaceId, schemaName);

        CreateNamespaceRequest request = new CreateNamespaceRequest();
        request.setId(namespaceId);

        if (properties != null && !properties.isEmpty()) {
            Map<String, String> propsMap = new HashMap<>();
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                if (entry.getValue() != null) {
                    propsMap.put(entry.getKey(), entry.getValue().toString());
                }
            }
            request.setProperties(propsMap);
        }

        getNamespace().createNamespace(request);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (runtime.isSingleLevelNs()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
        }

        if (cascade) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas with CASCADE option");
        }

        List<String> namespaceId = runtime.trinoSchemaToLanceNamespace(schemaName);

        DropNamespaceRequest request = new DropNamespaceRequest();
        request.setId(namespaceId);
        request.setBehavior("Restrict");
        getNamespace().dropNamespace(request);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        if (runtime.isSingleLevelNs() && LanceRuntime.DEFAULT_SCHEMA.equals(schemaName)) {
            return Collections.emptyMap();
        }

        DescribeNamespaceRequest request = new DescribeNamespaceRequest();
        request.setId(runtime.trinoSchemaToLanceNamespace(schemaName));
        DescribeNamespaceResponse response = getNamespace().describeNamespace(request);

        Map<String, String> props = response.getProperties();
        if (props == null || props.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Object> result = new HashMap<>();
        result.putAll(props);
        return result;
    }

    // ===== Table Operations =====

    @Override
    public LanceTableHandle getTableHandle(ConnectorSession session, SchemaTableName name,
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Lance connector does not support start version for time travel");
        }

        String tablePath = getTablePath(session, name);
        if (tablePath != null) {
            List<String> tableId = runtime.getTableId(name.getSchemaName(), name.getTableName());
            Map<String, String> storageOptions = getStorageOptionsForTable(tableId);
            String userIdentity = session.getUser();

            Long datasetVersion;
            if (endVersion.isPresent()) {
                datasetVersion = resolveVersion(session, tablePath, storageOptions, endVersion.get());
            }
            else {
                datasetVersion = runtime.getLatestVersion(userIdentity, tablePath, storageOptions);
            }

            return new LanceTableHandle(name.getSchemaName(), name.getTableName(), tablePath, tableId, storageOptions, datasetVersion);
        }
        return null;
    }

    private Long resolveVersion(ConnectorSession session, String tablePath,
            Map<String, String> storageOptions, ConnectorTableVersion version)
    {
        String userIdentity = session.getUser();
        Type versionType = version.getVersionType();

        return switch (version.getPointerType()) {
            case TARGET_ID -> resolveTargetIdVersion(tablePath, storageOptions, userIdentity, version, versionType);
            case TEMPORAL -> resolveTemporalVersion(session, tablePath, storageOptions, userIdentity, version, versionType);
        };
    }

    private Long resolveTargetIdVersion(String tablePath, Map<String, String> storageOptions,
            String userIdentity, ConnectorTableVersion version, Type versionType)
    {
        long versionNumber;
        if (versionType.equals(BIGINT)) {
            versionNumber = (long) version.getVersion();
        }
        else if (versionType.equals(INTEGER)) {
            versionNumber = ((Number) version.getVersion()).longValue();
        }
        else if (versionType.equals(SMALLINT)) {
            versionNumber = ((Number) version.getVersion()).longValue();
        }
        else if (versionType.equals(TINYINT)) {
            versionNumber = ((Number) version.getVersion()).longValue();
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported type for Lance version: " + versionType.getDisplayName() +
                    ". Only integer types (TINYINT, SMALLINT, INTEGER, BIGINT) are supported for FOR VERSION AS OF.");
        }

        if (versionNumber <= 0) {
            throw new TrinoException(INVALID_ARGUMENTS, "Lance version number must be positive: " + versionNumber);
        }

        // Verify the version exists
        if (!runtime.versionExists(userIdentity, tablePath, versionNumber, storageOptions)) {
            throw new TrinoException(INVALID_ARGUMENTS, "Lance version does not exist: " + versionNumber);
        }

        return versionNumber;
    }

    private Long resolveTemporalVersion(ConnectorSession session, String tablePath,
            Map<String, String> storageOptions, String userIdentity, ConnectorTableVersion version, Type versionType)
    {
        long timestampMillis = getTimestampMillis(session, version, versionType);

        Optional<Long> resolvedVersion = runtime.getVersionAtTimestamp(
                userIdentity, tablePath, timestampMillis, storageOptions);

        if (resolvedVersion.isEmpty()) {
            throw new TrinoException(INVALID_ARGUMENTS,
                    "No Lance version found at or before timestamp: " + Instant.ofEpochMilli(timestampMillis));
        }

        log.debug("Resolved temporal version for timestamp %s to version %d",
                Instant.ofEpochMilli(timestampMillis), resolvedVersion.get());
        return resolvedVersion.get();
    }

    private long getTimestampMillis(ConnectorSession session, ConnectorTableVersion version, Type versionType)
    {
        if (versionType.equals(DateType.DATE)) {
            // Convert date to start of day in session timezone
            long epochDay = (long) version.getVersion();
            return LocalDate.ofEpochDay(epochDay)
                    .atStartOfDay()
                    .atZone(session.getTimeZoneKey().getZoneId())
                    .toInstant()
                    .toEpochMilli();
        }

        if (versionType instanceof TimestampType timestampType) {
            long epochMicrosUtc = timestampType.isShort()
                    ? (long) version.getVersion()
                    : ((LongTimestamp) version.getVersion()).getEpochMicros();
            long epochMillisUtc = MICROSECONDS.toMillis(epochMicrosUtc);
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillisUtc), ZoneOffset.UTC)
                    .atZone(session.getTimeZoneKey().getZoneId())
                    .toInstant()
                    .toEpochMilli();
        }

        if (versionType instanceof TimestampWithTimeZoneType timeZonedVersionType) {
            return timeZonedVersionType.isShort()
                    ? unpackMillisUtc((long) version.getVersion())
                    : ((LongTimestampWithTimeZone) version.getVersion()).getEpochMillis();
        }

        throw new TrinoException(NOT_SUPPORTED,
                "Unsupported type for Lance temporal version: " + versionType.getDisplayName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) table;
        try {
            Map<String, String> storageOptions = getEffectiveStorageOptions(lanceTableHandle);
            String userIdentity = session.getUser();
            List<ColumnMetadata> columnsMetadata = runtime.getColumnMetadata(
                    userIdentity, lanceTableHandle.getTablePath(), lanceTableHandle.getDatasetVersion(), storageOptions);
            SchemaTableName schemaTableName =
                    new SchemaTableName(lanceTableHandle.getSchemaName(), lanceTableHandle.getTableName());
            return new ConnectorTableMetadata(schemaTableName, columnsMetadata);
        }
        catch (Exception e) {
            log.warn(e, "Failed to get table metadata for %s", lanceTableHandle.getTableName());
            return null;
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameOrNull)
    {
        String schema = schemaNameOrNull.orElse(LanceRuntime.DEFAULT_SCHEMA);

        if (!schemaExists(schema)) {
            return Collections.emptyList();
        }

        List<String> namespaceId = runtime.trinoSchemaToLanceNamespace(schema);
        ListTablesRequest request = new ListTablesRequest();
        request.setId(namespaceId);

        ListTablesResponse response = getNamespace().listTables(request);
        Set<String> tables = response.getTables();
        if (tables == null || tables.isEmpty()) {
            return Collections.emptyList();
        }
        return tables.stream()
                .map(tableName -> new SchemaTableName(schema, tableName))
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        try {
            Map<String, String> storageOptions = getEffectiveStorageOptions(lanceTableHandle);
            String userIdentity = session.getUser();
            return runtime.getColumnHandles(userIdentity, lanceTableHandle.getTablePath(),
                    lanceTableHandle.getDatasetVersion(), storageOptions);
        }
        catch (Exception e) {
            throw new TableNotFoundException(new SchemaTableName(lanceTableHandle.getSchemaName(), lanceTableHandle.getTableName()));
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        String schemaName = prefix.getSchema().orElse(LanceRuntime.DEFAULT_SCHEMA);
        String userIdentity = session.getUser();
        for (SchemaTableName tableName : listTables(session, Optional.of(schemaName))) {
            try {
                String tablePath = getTablePath(session, tableName);
                if (tablePath != null) {
                    List<String> tableId = runtime.getTableId(tableName.getSchemaName(), tableName.getTableName());
                    Map<String, String> storageOptions = getStorageOptionsForTable(tableId);
                    // Use null version for listing (always get latest)
                    List<ColumnMetadata> columnsMetadata = runtime.getColumnMetadata(userIdentity, tablePath, null, storageOptions);
                    columns.put(tableName, columnsMetadata);
                }
            }
            catch (Exception e) {
                // Table can disappear during listing operation, skip it
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((LanceColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        return Optional.empty();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;

        try {
            Map<String, String> storageOptions = getEffectiveStorageOptions(lanceTableHandle);
            String userIdentity = session.getUser();
            ManifestSummary summary = runtime.getManifestSummary(
                    userIdentity, lanceTableHandle.getTablePath(), lanceTableHandle.getDatasetVersion(), storageOptions);

            log.debug("getTableStatistics: table=%s, totalRows=%d, totalFilesSize=%d, totalFragments=%d",
                    lanceTableHandle.getTableName(),
                    summary.getTotalRows(),
                    summary.getTotalFilesSize(),
                    summary.getTotalFragments());

            // Note: TableStatistics is used for query planning/cost estimation only.
            // For COUNT(*) optimization, we would need to implement applyAggregation().
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(summary.getTotalRows()))
                    .build();
        }
        catch (Exception e) {
            log.warn(e, "Failed to get table statistics for %s", lanceTableHandle.getTableName());
            return TableStatistics.empty();
        }
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session,
            ConnectorTableHandle table, long limit)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) table;

        if (lanceTableHandle.getLimit().isPresent() && lanceTableHandle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        LanceTableHandle newHandle = lanceTableHandle.withLimit(limit);
        return Optional.of(new LimitApplicationResult<>(newHandle, false, false));
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) table;

        // Don't push if already has aggregate
        if (lanceTableHandle.isCountStar()) {
            return Optional.empty();
        }

        // Don't push COUNT(*) if there's a filter - let Trino aggregate distributed counts
        // We can only return a single row from aggregate pushdown, but with filter we need
        // to count each fragment separately which would return multiple rows
        if (lanceTableHandle.hasFilter()) {
            log.debug("applyAggregation: not pushing COUNT(*) with filter");
            return Optional.empty();
        }

        // Only support simple COUNT(*) without grouping
        if (groupingSets.size() != 1 || !groupingSets.get(0).isEmpty()) {
            return Optional.empty();
        }

        // Only support single COUNT(*) aggregate
        if (aggregates.size() != 1) {
            return Optional.empty();
        }

        AggregateFunction aggregate = aggregates.get(0);
        if (!isCountStar(aggregate)) {
            log.debug("applyAggregation: unsupported aggregate function: %s", aggregate.getFunctionName());
            return Optional.empty();
        }

        log.debug("applyAggregation: pushing COUNT(*) for table %s (no filter)",
                lanceTableHandle.getTableName());

        LanceTableHandle newHandle = lanceTableHandle.withCountStar();

        // Create synthetic column handle for COUNT(*) result
        LanceColumnHandle countColumnHandle = new LanceColumnHandle("_count", aggregate.getOutputType(), false, -1);
        Variable variable = new Variable("_count", aggregate.getOutputType());

        return Optional.of(new AggregationApplicationResult<>(
                newHandle,
                ImmutableList.of(variable),
                ImmutableList.of(new Assignment("_count", countColumnHandle, aggregate.getOutputType())),
                ImmutableMap.of(),
                false));
    }

    private boolean isCountStar(AggregateFunction aggregate)
    {
        return "count".equalsIgnoreCase(aggregate.getFunctionName()) &&
                aggregate.getArguments().isEmpty() &&
                !aggregate.isDistinct();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
            ConnectorTableHandle table, Constraint constraint)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) table;

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll()) {
            log.debug("applyFilter: constraint summary is ALL, returning empty");
            return Optional.empty();
        }

        TupleDomain<LanceColumnHandle> newConstraint = summary.transformKeys(LanceColumnHandle.class::cast);
        TupleDomain<LanceColumnHandle> supportedConstraint = filterToSupportedTypes(newConstraint);

        log.debug("applyFilter: newConstraint=%s, supportedConstraint=%s", newConstraint, supportedConstraint);

        if (supportedConstraint.isAll()) {
            log.debug("applyFilter: supportedConstraint is ALL, returning empty");
            return Optional.empty();
        }

        // Get all columns from the table for building the full schema
        Map<String, String> storageOptions = getEffectiveStorageOptions(lanceTableHandle);
        String userIdentity = session.getUser();
        List<LanceColumnHandle> allColumns = runtime.getColumnHandleList(
                userIdentity, lanceTableHandle.getTablePath(), lanceTableHandle.getDatasetVersion(), storageOptions);

        // Build field ID map from all column handles
        Map<String, Integer> fieldIdMap = new HashMap<>();
        for (LanceColumnHandle column : allColumns) {
            fieldIdMap.put(column.name(), column.fieldId());
        }

        // Filter out columns without valid field IDs (e.g., synthetic COUNT columns)
        Map<LanceColumnHandle, Domain> domains = supportedConstraint.getDomains().orElse(Map.of());
        Map<LanceColumnHandle, Domain> validDomains = new HashMap<>();
        for (Map.Entry<LanceColumnHandle, Domain> entry : domains.entrySet()) {
            LanceColumnHandle column = entry.getKey();
            if (column.fieldId() >= 0) {
                validDomains.put(column, entry.getValue());
            }
            else {
                log.debug("applyFilter: skipping synthetic column %s with invalid fieldId", column.name());
            }
        }

        if (validDomains.isEmpty()) {
            log.debug("applyFilter: no valid columns to filter on");
            return Optional.empty();
        }
        supportedConstraint = TupleDomain.withColumnDomains(validDomains);

        // Build Substrait filter with full schema
        Optional<ByteBuffer> substraitFilter = SubstraitExpressionBuilder.tupleDomainToSubstrait(
                supportedConstraint, allColumns, fieldIdMap);

        if (substraitFilter.isEmpty()) {
            log.debug("applyFilter: no substrait filter generated, returning empty");
            return Optional.empty();
        }

        // Combine with existing filter if present
        byte[] newFilterBytes = substraitFilter.get().array();
        byte[] existingFilter = lanceTableHandle.getSubstraitFilter();

        // If there's an existing filter, we can't easily combine Substrait expressions,
        // so we just use the new one (this matches the previous behavior with TupleDomain.intersect)
        // In practice, Trino calls applyFilter once with the full constraint
        if (existingFilter != null && existingFilter.length > 0 &&
                java.util.Arrays.equals(existingFilter, newFilterBytes)) {
            log.debug("applyFilter: filter unchanged, returning empty");
            return Optional.empty();
        }

        // Collect column names for display in EXPLAIN
        List<String> filterColumnNames = validDomains.keySet().stream()
                .map(LanceColumnHandle::name)
                .toList();

        // Collect columns with equality predicates (single value domains)
        // Only equality predicates can benefit from btree/bitmap index acceleration
        List<String> equalityColumnNames = new ArrayList<>();
        for (Map.Entry<LanceColumnHandle, Domain> entry : validDomains.entrySet()) {
            if (entry.getValue().isSingleValue()) {
                equalityColumnNames.add(entry.getKey().name());
            }
        }

        LanceTableHandle newHandle = lanceTableHandle.withSubstraitFilter(newFilterBytes, filterColumnNames, equalityColumnNames);
        TupleDomain<LanceColumnHandle> remainingFilter = filterToUnsupportedTypes(newConstraint);

        log.debug("applyFilter: pushing substrait filter (size=%d bytes), remaining=%s",
                newFilterBytes.length, remainingFilter);

        return Optional.of(new ConstraintApplicationResult<>(
                newHandle,
                remainingFilter.transformKeys(ColumnHandle.class::cast),
                constraint.getExpression(),
                false));
    }

    private TupleDomain<LanceColumnHandle> filterToSupportedTypes(TupleDomain<LanceColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll() || tupleDomain.isNone()) {
            return tupleDomain;
        }

        Map<LanceColumnHandle, Domain> domains = tupleDomain.getDomains().orElse(Map.of());
        Map<LanceColumnHandle, Domain> supportedDomains = new HashMap<>();

        for (Map.Entry<LanceColumnHandle, Domain> entry : domains.entrySet()) {
            LanceColumnHandle column = entry.getKey();
            Domain domain = entry.getValue();
            if (isSupportedType(column.trinoType()) && isDomainPushable(domain)) {
                supportedDomains.put(column, domain);
            }
        }

        if (supportedDomains.isEmpty()) {
            return TupleDomain.all();
        }

        return TupleDomain.withColumnDomains(supportedDomains);
    }

    private TupleDomain<LanceColumnHandle> filterToUnsupportedTypes(TupleDomain<LanceColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll() || tupleDomain.isNone()) {
            return TupleDomain.all();
        }

        Map<LanceColumnHandle, Domain> domains = tupleDomain.getDomains().orElse(Map.of());
        Map<LanceColumnHandle, Domain> unsupportedDomains = new HashMap<>();

        for (Map.Entry<LanceColumnHandle, Domain> entry : domains.entrySet()) {
            LanceColumnHandle column = entry.getKey();
            Domain domain = entry.getValue();
            if (!isSupportedType(column.trinoType()) || !isDomainPushable(domain)) {
                unsupportedDomains.put(column, domain);
            }
        }

        if (unsupportedDomains.isEmpty()) {
            return TupleDomain.all();
        }

        return TupleDomain.withColumnDomains(unsupportedDomains);
    }

    // ===== DROP TABLE =====

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        List<String> tableId = lanceTableHandle.getTableId();
        String tablePath = lanceTableHandle.getTablePath();

        log.debug("dropTable: table=%s, path=%s", lanceTableHandle.getTableName(), tablePath);

        DropTableRequest dropRequest = new DropTableRequest()
                .id(tableId);
        getNamespace().dropTable(dropRequest);

        String userIdentity = session.getUser();
        runtime.invalidate(userIdentity, tablePath);
    }

    // ===== CREATE TABLE =====

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        SchemaTableName tableName = tableMetadata.getTable();

        if (!schemaExists(tableName.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "Schema " + tableName.getSchemaName() + " not found");
        }

        // Get blob and vector columns from table properties
        Set<String> blobColumns = LanceTableProperties.getBlobColumns(tableMetadata.getProperties());
        Map<String, Integer> vectorColumns = LanceTableProperties.getVectorColumns(tableMetadata.getProperties());
        LancePageToArrowConverter.validateBlobColumns(tableMetadata.getColumns(), blobColumns);
        LancePageToArrowConverter.validateVectorColumns(tableMetadata.getColumns(), vectorColumns);

        List<String> tableId = runtime.getTableId(tableName.getSchemaName(), tableName.getTableName());
        String existingPath = getTablePath(session, tableName);

        if (existingPath != null) {
            if (saveMode == SaveMode.FAIL) {
                throw new TrinoException(ALREADY_EXISTS, "Table already exists: " + tableName);
            }
            else if (saveMode == SaveMode.IGNORE) {
                return;
            }
            // For REPLACE, overwrite with empty dataset
            Map<String, String> storageOptions = getStorageOptionsForTable(tableId);
            Schema arrowSchema = LancePageToArrowConverter.toArrowSchema(tableMetadata.getColumns(), blobColumns, vectorColumns);
            String userIdentity = session.getUser();
            // For write operations, open dataset directly (not cached)
            try (Dataset dataset = runtime.openDatasetDirect(userIdentity, existingPath, null, storageOptions)) {
                commitOverwrite(dataset, List.of(), arrowSchema, storageOptions);
            }
            runtime.invalidate(userIdentity, existingPath);
            log.debug("createTable: replaced table %s at %s", tableName, existingPath);
            return;
        }

        // Create new table via namespace API
        CreateEmptyTableRequest createRequest = new CreateEmptyTableRequest()
                .id(tableId);
        CreateEmptyTableResponse createResponse = getNamespace().createEmptyTable(createRequest);
        String tablePath = createResponse.getLocation();

        Map<String, String> storageOptions = getStorageOptionsForTable(tableId);
        Schema arrowSchema = LancePageToArrowConverter.toArrowSchema(tableMetadata.getColumns(), blobColumns, vectorColumns);

        WriteParams params = buildWriteParams(storageOptions);
        createEmptyDataset(tablePath, arrowSchema, params);

        log.debug("createTable: created empty table %s at %s", tableName, tablePath);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout,
            RetryMode retryMode,
            boolean replace)
    {
        SchemaTableName tableName = tableMetadata.getTable();

        if (!schemaExists(tableName.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "Schema " + tableName.getSchemaName() + " not found");
        }

        // Get blob and vector columns from table properties
        Set<String> blobColumns = LanceTableProperties.getBlobColumns(tableMetadata.getProperties());
        Map<String, Integer> vectorColumns = LanceTableProperties.getVectorColumns(tableMetadata.getProperties());
        LancePageToArrowConverter.validateBlobColumns(tableMetadata.getColumns(), blobColumns);
        LancePageToArrowConverter.validateVectorColumns(tableMetadata.getColumns(), vectorColumns);

        List<String> tableId = runtime.getTableId(tableName.getSchemaName(), tableName.getTableName());
        String existingPath = getTablePath(session, tableName);
        String tablePath;
        boolean tableExisted = existingPath != null;
        Map<String, String> storageOptions;

        if (tableExisted) {
            if (!replace) {
                throw new TrinoException(ALREADY_EXISTS, "Table already exists: " + tableName);
            }
            log.debug("beginCreateTable: replacing existing table at: %s", existingPath);
            tablePath = existingPath;
            storageOptions = getStorageOptionsForTable(tableId);
        }
        else {
            // Use declareTable to reserve the location without creating the actual table.
            // The table will only be created in finishCreateTable after fragments are written.
            DeclareTableRequest declareRequest = new DeclareTableRequest();
            tableId.forEach(declareRequest::addIdItem);
            DeclareTableResponse declareResponse = getNamespace().declareTable(declareRequest);
            tablePath = declareResponse.getLocation();
            // Get storage options from the declare response for new tables
            storageOptions = declareResponse.getStorageOptions();
            if (storageOptions == null) {
                storageOptions = new HashMap<>();
            }
        }

        List<LanceColumnHandle> columns = tableMetadata.getColumns().stream()
                .map(col -> new LanceColumnHandle(col.getName(), col.getType(), col.isNullable(),
                        -1, blobColumns.contains(col.getName())))
                .collect(toImmutableList());

        Schema arrowSchema = LancePageToArrowConverter.toArrowSchema(tableMetadata.getColumns(), blobColumns, vectorColumns);
        String schemaJson = arrowSchema.toJson();

        String transactionId = null;
        if (tableExisted) {
            transactionId = UUID.randomUUID().toString();
            ReadOptions readOptions = new ReadOptions.Builder()
                    .setStorageOptions(storageOptions)
                    .build();
            Dataset dataset = Dataset.open(tablePath, readOptions);
            transactionDatasets.put(transactionId, dataset);
        }

        log.debug("beginCreateTable: table=%s, path=%s, replace=%s, tableExisted=%s, transactionId=%s, blobColumns=%s",
                tableName, tablePath, replace, tableExisted, transactionId, blobColumns);

        return new LanceWritableTableHandle(
                tableName,
                tablePath,
                schemaJson,
                columns,
                tableId,
                storageOptions,
                true,
                replace,
                tableExisted,
                transactionId);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        LanceWritableTableHandle handle = (LanceWritableTableHandle) tableHandle;
        Schema arrowSchema;
        try {
            arrowSchema = Schema.fromJSON(handle.schemaJson());
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to parse Arrow schema", e);
        }

        String transactionId = handle.transactionId();
        log.debug("finishCreateTable: table=%s, fragments=%d, replace=%s, tableExisted=%s, transactionId=%s",
                handle.tableName(), fragments.size(), handle.replace(), handle.tableExisted(), transactionId);

        Map<String, String> storageOptions = handle.storageOptions();

        if (handle.tableExisted()) {
            Dataset dataset = transactionDatasets.remove(transactionId);
            if (dataset == null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "No dataset found for transaction: " + transactionId);
            }
            try {
                if (fragments.isEmpty()) {
                    commitOverwrite(dataset, List.of(), arrowSchema, storageOptions);
                }
                else {
                    List<String> allFragmentsJson = collectFragmentsFromSlices(fragments);
                    commitOverwrite(dataset, allFragmentsJson, arrowSchema, storageOptions);
                }
            }
            finally {
                dataset.close();
            }
        }
        else {
            if (fragments.isEmpty()) {
                WriteParams params = buildWriteParams(storageOptions);
                createEmptyDataset(handle.tablePath(), arrowSchema, params);
            }
            else {
                List<String> allFragmentsJson = collectFragmentsFromSlices(fragments);
                WriteParams params = buildWriteParams(storageOptions);
                createDatasetWithFragments(handle.tablePath(), allFragmentsJson, arrowSchema, params, storageOptions);
            }
        }

        String userIdentity = session.getUser();
        runtime.invalidate(userIdentity, handle.tablePath());
        return Optional.empty();
    }

    // ===== INSERT =====

    @Override
    public ConnectorInsertTableHandle beginInsert(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            RetryMode retryMode)
    {
        LanceTableHandle table = (LanceTableHandle) tableHandle;
        SchemaTableName tableName = new SchemaTableName(table.getSchemaName(), table.getTableName());

        String tablePath = table.getTablePath();
        List<String> tableId = table.getTableId();

        List<LanceColumnHandle> lanceColumns = columns.stream()
                .map(LanceColumnHandle.class::cast)
                .collect(toImmutableList());

        Map<String, String> storageOptions = getStorageOptionsForTable(tableId);
        String userIdentity = session.getUser();
        // For write operations, get schema but open dataset directly (not cached)
        Schema arrowSchema = runtime.getSchema(userIdentity, tablePath, null, storageOptions);
        String schemaJson = arrowSchema.toJson();

        String transactionId = UUID.randomUUID().toString();
        // For write operations, open dataset directly (not cached)
        Dataset dataset = runtime.openDatasetDirect(userIdentity, tablePath, null, storageOptions);
        transactionDatasets.put(transactionId, dataset);

        log.debug("beginInsert: table=%s, path=%s, columns=%d, transactionId=%s", tableName, tablePath, columns.size(), transactionId);

        return new LanceWritableTableHandle(
                tableName,
                tablePath,
                schemaJson,
                lanceColumns,
                tableId,
                storageOptions,
                false,
                false,
                true,
                transactionId);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        LanceWritableTableHandle handle = (LanceWritableTableHandle) insertHandle;
        String transactionId = handle.transactionId();

        log.debug("finishInsert: table=%s, fragments=%d, transactionId=%s", handle.tableName(), fragments.size(), transactionId);

        Dataset dataset = transactionDatasets.remove(transactionId);
        if (dataset == null) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "No dataset found for transaction: " + transactionId);
        }

        try {
            if (fragments.isEmpty()) {
                return Optional.empty();
            }

            List<String> allFragmentsJson = collectFragmentsFromSlices(fragments);
            Map<String, String> storageOptions = handle.storageOptions();
            commitAppend(dataset, allFragmentsJson, storageOptions);

            String userIdentity = session.getUser();
            runtime.invalidate(userIdentity, handle.tablePath());
            return Optional.empty();
        }
        finally {
            dataset.close();
        }
    }

    // ===== DELETE/UPDATE/MERGE Operations =====

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new LanceColumnHandle(LANCE_ROW_ADDRESS, BIGINT, false, -1);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        // Return empty to use merge-on-read pattern for all deletes
        return Optional.empty();
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Map<Integer, Collection<ColumnHandle>> updateCaseColumns,
            RetryMode retryMode)
    {
        LanceTableHandle table = (LanceTableHandle) tableHandle;
        SchemaTableName tableName = new SchemaTableName(table.getSchemaName(), table.getTableName());

        String tablePath = table.getTablePath();
        List<String> tableId = table.getTableId();
        Map<String, String> storageOptions = getStorageOptionsForTable(tableId);

        String transactionId = UUID.randomUUID().toString();
        String userIdentity = session.getUser();
        // For write operations, open dataset directly (not cached)
        Dataset dataset = runtime.openDatasetDirect(userIdentity, tablePath, null, storageOptions);
        long readVersion = dataset.version();
        transactionDatasets.put(transactionId, dataset);

        List<LanceColumnHandle> columns = runtime.getColumnHandleList(userIdentity, tablePath, null, storageOptions);
        Schema arrowSchema = runtime.getSchema(userIdentity, tablePath, null, storageOptions);
        String schemaJson = arrowSchema.toJson();

        log.debug("beginMerge: table=%s, path=%s, version=%d, transactionId=%s",
                tableName, tablePath, readVersion, transactionId);

        return new LanceMergeTableHandle(
                table.withStorageOptions(storageOptions),
                getMergeRowIdColumnHandle(session, tableHandle),
                readVersion,
                schemaJson,
                columns,
                transactionId);
    }

    @Override
    public void finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        LanceMergeTableHandle handle = (LanceMergeTableHandle) mergeTableHandle;
        String transactionId = handle.transactionId();

        log.debug("finishMerge: table=%s, fragments=%d, transactionId=%s",
                handle.tableHandle().getTableName(), fragments.size(), transactionId);

        Dataset dataset = transactionDatasets.remove(transactionId);
        if (dataset == null) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "No dataset found for transaction: " + transactionId);
        }

        try {
            List<Long> removedFragmentIds = new ArrayList<>();
            List<FragmentMetadata> updatedFragments = new ArrayList<>();
            List<FragmentMetadata> newFragments = new ArrayList<>();

            // First, collect all deletions per fragment from all workers
            // We must merge deletions for the same fragment before calling deleteRows,
            // otherwise only the last deletion vector would be applied
            Map<Integer, List<Integer>> allDeletions = new HashMap<>();
            for (Slice slice : fragments) {
                LanceMergeCommitData commitData = mergeCommitDataCodec.fromJson(slice.getBytes());

                for (FragmentDeletion deletion : commitData.deletions()) {
                    allDeletions.computeIfAbsent(deletion.fragmentId(), k -> new ArrayList<>())
                            .addAll(deletion.rowIndexes());
                }

                newFragments.addAll(deserializeFragments(commitData.newFragmentsJson()));
            }

            // Now process merged deletions per fragment
            for (Map.Entry<Integer, List<Integer>> entry : allDeletions.entrySet()) {
                int fragmentId = entry.getKey();
                List<Integer> rowIndexes = entry.getValue();

                log.debug("finishMerge: deleting %d rows from fragment %d, first few indices: %s",
                        rowIndexes.size(),
                        fragmentId,
                        rowIndexes.stream().limit(5).toList());

                FragmentMetadata updated = dataset.getFragment(fragmentId)
                        .deleteRows(rowIndexes);
                if (updated != null) {
                    log.debug("finishMerge: fragment %d updated with deletion vector, deletionFile=%s",
                            fragmentId, updated.getDeletionFile());
                    updatedFragments.add(updated);
                }
                else {
                    log.debug("finishMerge: fragment %d fully deleted", fragmentId);
                    removedFragmentIds.add((long) fragmentId);
                }
            }

            Map<String, String> storageOptions = handle.getStorageOptions();

            if (!removedFragmentIds.isEmpty() || !updatedFragments.isEmpty() || !newFragments.isEmpty()) {
                log.debug("finishMerge: committing update with %d removed fragments, %d updated fragments, %d new fragments",
                        removedFragmentIds.size(), updatedFragments.size(), newFragments.size());
                Update update = Update.builder()
                        .removedFragmentIds(removedFragmentIds)
                        .updatedFragments(updatedFragments)
                        .newFragments(newFragments)
                        .build();

                Transaction transaction = dataset
                        .newTransactionBuilder()
                        .writeParams(storageOptions)
                        .operation(update)
                        .build();
                transaction.commit().close();
            }

            String userIdentity = session.getUser();
            runtime.invalidate(userIdentity, handle.getTablePath());
        }
        catch (RuntimeException e) {
            if (isCommitConflict(e)) {
                throw new TrinoException(TRANSACTION_CONFLICT, "Concurrent modification conflict", e);
            }
            throw e;
        }
        finally {
            dataset.close();
        }
    }

    // ===== Helper Methods =====

    private LanceNamespace getNamespace()
    {
        return runtime.getNamespace();
    }

    private boolean schemaExists(String schema)
    {
        if (runtime.isSingleLevelNs() && LanceRuntime.DEFAULT_SCHEMA.equals(schema)) {
            return true;
        }
        if (runtime.isSingleLevelNs()) {
            return false;
        }

        try {
            NamespaceExistsRequest request = new NamespaceExistsRequest();
            request.setId(runtime.trinoSchemaToLanceNamespace(schema));
            getNamespace().namespaceExists(request);
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    private String getTablePath(ConnectorSession session, SchemaTableName schemaTableName)
    {
        if (runtime.isSingleLevelNs() && !LanceRuntime.DEFAULT_SCHEMA.equals(schemaTableName.getSchemaName())) {
            return null;
        }

        try {
            List<String> tableId = runtime.getTableId(schemaTableName.getSchemaName(), schemaTableName.getTableName());
            DescribeTableRequest request = new DescribeTableRequest()
                    .id(tableId);
            DescribeTableResponse response = getNamespace().describeTable(request);
            return response.getLocation();
        }
        catch (Exception e) {
            log.debug("Failed to describe table %s: %s", schemaTableName, e.getMessage());
            return null;
        }
    }

    private Map<String, String> getStorageOptionsForTable(List<String> tableId)
    {
        try {
            DescribeTableRequest request = new DescribeTableRequest().id(tableId);
            DescribeTableResponse response = getNamespace().describeTable(request);
            Map<String, String> storageOptions = response.getStorageOptions();
            if (storageOptions != null && !storageOptions.isEmpty()) {
                return storageOptions;
            }
        }
        catch (Exception e) {
            log.debug("Failed to get storage options from describeTable for %s: %s", tableId, e.getMessage());
        }

        Map<String, String> nsOptions = runtime.getNamespaceStorageOptions();
        if (!nsOptions.isEmpty()) {
            return nsOptions;
        }

        return new HashMap<>();
    }

    /**
     * Get effective storage options from the table handle, refreshing if expired.
     * This avoids repeated calls to getStorageOptionsForTable when the handle
     * already contains valid storage options.
     */
    private Map<String, String> getEffectiveStorageOptions(LanceTableHandle handle)
    {
        // If handle has storage options and they're not expired, use them directly
        if (!handle.getStorageOptions().isEmpty() && !handle.isStorageOptionsExpired()) {
            return handle.getStorageOptions();
        }
        // Otherwise, refresh from the namespace
        return getStorageOptionsForTable(handle.getTableId());
    }

    private List<String> collectFragmentsFromSlices(Collection<Slice> fragments)
    {
        List<String> allFragmentsJson = new ArrayList<>();
        for (Slice slice : fragments) {
            LanceCommitTaskData commitData = commitTaskDataCodec.fromJson(slice.getBytes());
            allFragmentsJson.addAll(commitData.getFragmentsJson());
        }
        return allFragmentsJson;
    }

    // ===== Write Operations (formerly in LanceWriter) =====

    private WriteParams buildWriteParams(Map<String, String> storageOptions)
    {
        WriteParams.Builder builder = new WriteParams.Builder();
        if (storageOptions != null && !storageOptions.isEmpty()) {
            builder.withStorageOptions(storageOptions);
        }
        return builder.build();
    }

    private void createEmptyDataset(String datasetUri, Schema schema, WriteParams params)
    {
        log.debug("Creating empty dataset at: %s", datasetUri);
        Dataset.create(runtime.getAllocator(), datasetUri, schema, params).close();
    }

    private void commitAppend(Dataset dataset, List<String> serializedFragments, Map<String, String> storageOptions)
    {
        log.debug("Committing %d fragments to dataset: %s (append)", serializedFragments.size(), dataset.uri());
        List<FragmentMetadata> fragments = deserializeFragments(serializedFragments);

        Transaction transaction = dataset
                .newTransactionBuilder()
                .writeParams(storageOptions)
                .operation(Append.builder().fragments(fragments).build())
                .build();
        transaction.commit().close();
    }

    private void commitOverwrite(Dataset dataset, List<String> serializedFragments, Schema schema, Map<String, String> storageOptions)
    {
        log.debug("Committing %d fragments to dataset: %s (overwrite)", serializedFragments.size(), dataset.uri());
        List<FragmentMetadata> fragments = deserializeFragments(serializedFragments);

        Transaction transaction = dataset
                .newTransactionBuilder()
                .writeParams(storageOptions)
                .operation(Overwrite.builder().fragments(fragments).schema(schema).build())
                .build();
        transaction.commit().close();
    }

    private void createDatasetWithFragments(String datasetUri, List<String> serializedFragments, Schema schema, WriteParams params, Map<String, String> storageOptions)
    {
        log.debug("Creating dataset with %d fragments at: %s", serializedFragments.size(), datasetUri);
        List<FragmentMetadata> fragments = deserializeFragments(serializedFragments);

        Dataset.create(runtime.getAllocator(), datasetUri, schema, params).close();

        FragmentOperation.Overwrite overwriteOp = new FragmentOperation.Overwrite(fragments, schema);

        ReadOptions readOptions = new ReadOptions.Builder()
                .setStorageOptions(storageOptions)
                .build();

        try (Dataset dataset = Dataset.open(datasetUri, readOptions)) {
            Dataset.commit(
                    runtime.getAllocator(),
                    datasetUri,
                    overwriteOp,
                    Optional.of(dataset.version()),
                    storageOptions).close();
        }
    }

    // ===== Fragment Serialization (static methods for use by LancePageSink) =====

    public static List<String> serializeFragments(List<FragmentMetadata> fragments)
    {
        List<String> result = new ArrayList<>();
        for (FragmentMetadata fragment : fragments) {
            result.add(serializeFragment(fragment));
        }
        return result;
    }

    public static String serializeFragment(FragmentMetadata fragment)
    {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(fragment);
            oos.close();
            return Base64.getEncoder().encodeToString(baos.toByteArray());
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to serialize FragmentMetadata", e);
        }
    }

    public static List<FragmentMetadata> deserializeFragments(List<String> serializedFragments)
    {
        List<FragmentMetadata> result = new ArrayList<>();
        for (String serialized : serializedFragments) {
            result.add(deserializeFragment(serialized));
        }
        return result;
    }

    public static FragmentMetadata deserializeFragment(String serialized)
    {
        try {
            byte[] bytes = Base64.getDecoder().decode(serialized);
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return (FragmentMetadata) ois.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize FragmentMetadata", e);
        }
    }

    @VisibleForTesting
    public LanceConfig getLanceConfig()
    {
        return lanceConfig;
    }

    @VisibleForTesting
    public LanceRuntime getRuntime()
    {
        return runtime;
    }

    /**
     * Check if the exception is a Lance commit conflict or concurrent modification error.
     */
    private static boolean isCommitConflict(RuntimeException e)
    {
        // Check entire causal chain for conflict messages
        Throwable current = e;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && (
                    message.toLowerCase().contains("commit conflict") ||
                    message.toLowerCase().contains("concurrent") ||
                    message.toLowerCase().contains("version") ||
                    message.toLowerCase().contains("conflict") ||
                    message.toLowerCase().contains("not found"))) {
                return true;
            }
            // NullPointerException in Fragment/Dataset operations is also a sign of concurrent modification
            if (current instanceof NullPointerException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
