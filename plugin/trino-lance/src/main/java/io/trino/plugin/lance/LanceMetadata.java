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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
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
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Dataset;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.ReadOptions;
import org.lance.Transaction;
import org.lance.WriteParams;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateEmptyTableRequest;
import org.lance.namespace.model.CreateEmptyTableResponse;
import org.lance.namespace.model.CreateNamespaceRequest;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
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
import static io.trino.plugin.lance.SubstraitExpressionBuilder.isDomainPushable;
import static io.trino.plugin.lance.SubstraitExpressionBuilder.isSupportedType;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class LanceMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(LanceMetadata.class);
    private static final ConcurrentMap<String, Dataset> transactionDatasets = new ConcurrentHashMap<>();

    private final LanceNamespaceHolder namespaceHolder;
    private final LanceConfig lanceConfig;
    private final JsonCodec<LanceCommitTaskData> commitTaskDataCodec;

    @Inject
    public LanceMetadata(
            LanceNamespaceHolder namespaceHolder,
            LanceConfig lanceConfig,
            JsonCodec<LanceCommitTaskData> commitTaskDataCodec)
    {
        this.namespaceHolder = requireNonNull(namespaceHolder, "namespaceHolder is null");
        this.lanceConfig = requireNonNull(lanceConfig, "lanceConfig is null");
        this.commitTaskDataCodec = requireNonNull(commitTaskDataCodec, "commitTaskDataCodec is null");
    }

    // ===== Schema/Namespace Operations =====

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        if (namespaceHolder.isSingleLevelNs()) {
            return List.of(LanceNamespaceHolder.DEFAULT_SCHEMA);
        }

        ListNamespacesRequest request = new ListNamespacesRequest();
        if (namespaceHolder.getParentPrefix().isPresent()) {
            request.setId(namespaceHolder.getParentPrefix().get());
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
        if (namespaceHolder.isSingleLevelNs()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
        }

        List<String> namespaceId = namespaceHolder.trinoSchemaToLanceNamespace(schemaName);
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
        if (namespaceHolder.isSingleLevelNs()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
        }

        if (cascade) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas with CASCADE option");
        }

        List<String> namespaceId = namespaceHolder.trinoSchemaToLanceNamespace(schemaName);

        DropNamespaceRequest request = new DropNamespaceRequest();
        request.setId(namespaceId);
        request.setBehavior("Restrict");
        getNamespace().dropNamespace(request);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        if (namespaceHolder.isSingleLevelNs() && LanceNamespaceHolder.DEFAULT_SCHEMA.equals(schemaName)) {
            return Collections.emptyMap();
        }

        DescribeNamespaceRequest request = new DescribeNamespaceRequest();
        request.setId(namespaceHolder.trinoSchemaToLanceNamespace(schemaName));
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
        String tablePath = getTablePath(session, name);
        if (tablePath != null) {
            List<String> tableId = namespaceHolder.getTableId(name.getSchemaName(), name.getTableName());
            Map<String, String> storageOptions = getStorageOptionsForTable(tableId);
            return new LanceTableHandle(name.getSchemaName(), name.getTableName(), tablePath, tableId, storageOptions);
        }
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) table;
        try {
            Map<String, String> storageOptions = getEffectiveStorageOptions(lanceTableHandle);
            List<ColumnMetadata> columnsMetadata = LanceDatasetCache.getColumnMetadata(
                    lanceTableHandle.getTablePath(), storageOptions);
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
        String schema = schemaNameOrNull.orElse(LanceNamespaceHolder.DEFAULT_SCHEMA);

        if (!schemaExists(schema)) {
            return Collections.emptyList();
        }

        List<String> namespaceId = namespaceHolder.trinoSchemaToLanceNamespace(schema);
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
            return LanceDatasetCache.getColumnHandles(lanceTableHandle.getTablePath(), storageOptions);
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

        String schemaName = prefix.getSchema().orElse(LanceNamespaceHolder.DEFAULT_SCHEMA);
        for (SchemaTableName tableName : listTables(session, Optional.of(schemaName))) {
            try {
                String tablePath = getTablePath(session, tableName);
                if (tablePath != null) {
                    List<String> tableId = namespaceHolder.getTableId(tableName.getSchemaName(), tableName.getTableName());
                    Map<String, String> storageOptions = getStorageOptionsForTable(tableId);
                    List<ColumnMetadata> columnsMetadata = LanceDatasetCache.getColumnMetadata(tablePath, storageOptions);
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
            long rowCount = LanceDatasetCache.countRows(lanceTableHandle.getTablePath(), storageOptions);
            log.debug("getTableStatistics: table=%s, rowCount=%d", lanceTableHandle.getTableName(), rowCount);

            return TableStatistics.builder()
                    .setRowCount(Estimate.of(rowCount))
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

        // Build field ID map from column handles
        Map<String, Integer> fieldIdMap = new HashMap<>();
        Map<LanceColumnHandle, Domain> domains = supportedConstraint.getDomains().orElse(Map.of());
        for (LanceColumnHandle column : domains.keySet()) {
            if (column.fieldId() < 0) {
                throw new IllegalStateException(
                        "Column " + column.name() + " has invalid field ID. " +
                        "This is unexpected - all columns should have valid field IDs from Lance schema.");
            }
            fieldIdMap.put(column.name(), column.fieldId());
        }

        // Build Substrait filter using field IDs
        Optional<ByteBuffer> substraitFilter = SubstraitExpressionBuilder.tupleDomainToSubstrait(
                supportedConstraint, fieldIdMap);

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

        LanceTableHandle newHandle = lanceTableHandle.withSubstraitFilter(newFilterBytes);
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

        LanceDatasetCache.invalidate(tablePath);
    }

    // ===== CREATE TABLE =====

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        SchemaTableName tableName = tableMetadata.getTable();

        if (!schemaExists(tableName.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "Schema " + tableName.getSchemaName() + " not found");
        }

        List<String> tableId = namespaceHolder.getTableId(tableName.getSchemaName(), tableName.getTableName());
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
            Schema arrowSchema = LancePageToArrowConverter.toArrowSchema(tableMetadata.getColumns());
            ReadOptions readOptions = new ReadOptions.Builder()
                    .setStorageOptions(storageOptions)
                    .build();
            try (Dataset dataset = Dataset.open(existingPath, readOptions)) {
                commitOverwrite(dataset, List.of(), arrowSchema, storageOptions);
            }
            LanceDatasetCache.invalidate(existingPath);
            log.debug("createTable: replaced table %s at %s", tableName, existingPath);
            return;
        }

        // Create new table via namespace API
        CreateEmptyTableRequest createRequest = new CreateEmptyTableRequest()
                .id(tableId);
        CreateEmptyTableResponse createResponse = getNamespace().createEmptyTable(createRequest);
        String tablePath = createResponse.getLocation();

        Map<String, String> storageOptions = getStorageOptionsForTable(tableId);
        Schema arrowSchema = LancePageToArrowConverter.toArrowSchema(tableMetadata.getColumns());

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

        List<String> tableId = namespaceHolder.getTableId(tableName.getSchemaName(), tableName.getTableName());
        String existingPath = getTablePath(session, tableName);
        String tablePath;
        boolean tableExisted = existingPath != null;

        if (tableExisted) {
            if (!replace) {
                throw new TrinoException(ALREADY_EXISTS, "Table already exists: " + tableName);
            }
            log.debug("beginCreateTable: replacing existing table at: %s", existingPath);
            tablePath = existingPath;
        }
        else {
            CreateEmptyTableRequest createRequest = new CreateEmptyTableRequest()
                    .id(tableId);
            CreateEmptyTableResponse createResponse = getNamespace().createEmptyTable(createRequest);
            tablePath = createResponse.getLocation();
        }

        List<LanceColumnHandle> columns = tableMetadata.getColumns().stream()
                .map(col -> new LanceColumnHandle(col.getName(), col.getType(), col.isNullable()))
                .collect(toImmutableList());

        Schema arrowSchema = LancePageToArrowConverter.toArrowSchema(tableMetadata.getColumns());
        String schemaJson = arrowSchema.toJson();

        Map<String, String> storageOptions = getStorageOptionsForTable(tableId);

        String transactionId = null;
        if (tableExisted) {
            transactionId = UUID.randomUUID().toString();
            ReadOptions readOptions = new ReadOptions.Builder()
                    .setStorageOptions(storageOptions)
                    .build();
            Dataset dataset = Dataset.open(tablePath, readOptions);
            transactionDatasets.put(transactionId, dataset);
        }

        log.debug("beginCreateTable: table=%s, path=%s, replace=%s, tableExisted=%s, transactionId=%s",
                tableName, tablePath, replace, tableExisted, transactionId);

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

        LanceDatasetCache.invalidate(handle.tablePath());
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
        Schema arrowSchema = LanceDatasetCache.getSchema(tablePath, storageOptions);
        String schemaJson = arrowSchema.toJson();

        String transactionId = UUID.randomUUID().toString();
        ReadOptions readOptions = new ReadOptions.Builder()
                .setStorageOptions(storageOptions)
                .build();
        Dataset dataset = Dataset.open(tablePath, readOptions);
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

            LanceDatasetCache.invalidate(handle.tablePath());
            return Optional.empty();
        }
        finally {
            dataset.close();
        }
    }

    // ===== Helper Methods =====

    private LanceNamespace getNamespace()
    {
        return namespaceHolder.getNamespace();
    }

    private boolean schemaExists(String schema)
    {
        if (namespaceHolder.isSingleLevelNs() && LanceNamespaceHolder.DEFAULT_SCHEMA.equals(schema)) {
            return true;
        }
        if (namespaceHolder.isSingleLevelNs()) {
            return false;
        }

        try {
            NamespaceExistsRequest request = new NamespaceExistsRequest();
            request.setId(namespaceHolder.trinoSchemaToLanceNamespace(schema));
            getNamespace().namespaceExists(request);
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    private String getTablePath(ConnectorSession session, SchemaTableName schemaTableName)
    {
        if (namespaceHolder.isSingleLevelNs() && !LanceNamespaceHolder.DEFAULT_SCHEMA.equals(schemaTableName.getSchemaName())) {
            return null;
        }

        try {
            List<String> tableId = namespaceHolder.getTableId(schemaTableName.getSchemaName(), schemaTableName.getTableName());
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

        Map<String, String> nsOptions = namespaceHolder.getNamespaceStorageOptions();
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
        Dataset.create(LanceNamespaceHolder.getAllocator(), datasetUri, schema, params).close();
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

        Dataset.create(LanceNamespaceHolder.getAllocator(), datasetUri, schema, params).close();

        FragmentOperation.Overwrite overwriteOp = new FragmentOperation.Overwrite(fragments, schema);

        ReadOptions readOptions = new ReadOptions.Builder()
                .setStorageOptions(storageOptions)
                .build();

        try (Dataset dataset = Dataset.open(datasetUri, readOptions)) {
            Dataset.commit(
                    LanceNamespaceHolder.getAllocator(),
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
    public LanceNamespaceHolder getNamespaceHolder()
    {
        return namespaceHolder;
    }
}
