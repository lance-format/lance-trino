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
import io.trino.plugin.lance.internal.LancePageToArrowConverter;
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.plugin.lance.internal.LanceWriter;
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
import io.trino.spi.statistics.ComputedStatistics;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class LanceMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(LanceMetadata.class);

    private final LanceReader lanceReader;
    private final LanceWriter lanceWriter;
    private final LanceConfig lanceConfig;
    private final JsonCodec<LanceCommitTaskData> commitTaskDataCodec;

    @Inject
    public LanceMetadata(
            LanceReader lanceReader,
            LanceWriter lanceWriter,
            LanceConfig lanceConfig,
            JsonCodec<LanceCommitTaskData> commitTaskDataCodec)
    {
        this.lanceReader = requireNonNull(lanceReader, "lanceReader is null");
        this.lanceWriter = requireNonNull(lanceWriter, "lanceWriter is null");
        this.lanceConfig = requireNonNull(lanceConfig, "lanceConfig is null");
        this.commitTaskDataCodec = requireNonNull(commitTaskDataCodec, "commitTaskDataCodec is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(LanceReader.SCHEMA);
    }

    @Override
    public LanceTableHandle getTableHandle(ConnectorSession session, SchemaTableName name,
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        String tablePath = lanceReader.getTablePath(session, name);
        if (tablePath != null) {
            return new LanceTableHandle(name.getSchemaName(), name.getTableName(), tablePath);
        }
        else {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) table;
        try {
            List<ColumnMetadata> columnsMetadata = lanceReader.getColumnsMetadata(((LanceTableHandle) table).getTableName());
            SchemaTableName schemaTableName =
                    new SchemaTableName(lanceTableHandle.getSchemaName(), lanceTableHandle.getTableName());
            return new ConnectorTableMetadata(schemaTableName, columnsMetadata);
        }
        catch (Exception e) {
            return null;
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameOrNull)
    {
        return lanceReader.listTables(session, schemaNameOrNull.orElse(LanceReader.SCHEMA));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        try {
            return lanceReader.getColumnHandle(lanceTableHandle.getTableName());
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
        for (SchemaTableName tableName : lanceReader.listTables(session, prefix.toString())) {
            ConnectorTableMetadata tableMetadata =
                    new ConnectorTableMetadata(tableName, lanceReader.getColumnsMetadata(tableName.getTableName()));
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
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
        // TODO: support projection pushdown
        return Optional.empty();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session,
            ConnectorTableHandle table, long limit)
    {
        // TODO: support limit pushdown
        return Optional.empty();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
            ConnectorTableHandle table, Constraint constraint)
    {
        // TODO: support filter pushdown
        return Optional.empty();
    }

    // ===== DROP TABLE =====

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LanceTableHandle lanceTableHandle = (LanceTableHandle) tableHandle;
        String tablePath = lanceTableHandle.getTablePath();

        log.debug("dropTable: table=%s, path=%s", lanceTableHandle.getTableName(), tablePath);

        // Delete the dataset files
        lanceWriter.dropDataset(tablePath);

        // Invalidate cache
        lanceReader.invalidateCache(tablePath);
    }

    // ===== CREATE TABLE =====

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        SchemaTableName tableName = tableMetadata.getTable();
        String tablePath = lanceReader.getTablePathForNewTable(tableName.getTableName());

        // Check if table already exists
        String existingPath = lanceReader.getTablePath(session, tableName);
        if (existingPath != null) {
            if (saveMode == SaveMode.FAIL) {
                throw new TrinoException(ALREADY_EXISTS, "Table already exists: " + tableName);
            }
            else if (saveMode == SaveMode.IGNORE) {
                return;
            }
            // For REPLACE, we continue and create new table
        }

        // Convert to Arrow schema and create empty dataset
        Schema arrowSchema = LancePageToArrowConverter.toArrowSchema(tableMetadata.getColumns());
        lanceWriter.createEmptyDataset(tablePath, arrowSchema, lanceWriter.getDefaultWriteParams());

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
        String tablePath = lanceReader.getTablePathForNewTable(tableName.getTableName());

        // Check if table already exists
        String existingPath = lanceReader.getTablePath(session, tableName);
        if (existingPath != null) {
            if (!replace) {
                throw new TrinoException(ALREADY_EXISTS, "Table already exists: " + tableName);
            }
            // Drop existing table for replace
            log.debug("beginCreateTable: dropping existing table for replace: %s", existingPath);
            lanceWriter.dropDataset(existingPath);
            lanceReader.invalidateCache(existingPath);
        }

        // Convert columns to LanceColumnHandle
        List<LanceColumnHandle> columns = tableMetadata.getColumns().stream()
                .map(col -> new LanceColumnHandle(col.getName(), col.getType(), col.isNullable()))
                .collect(toImmutableList());

        // Convert to Arrow schema
        Schema arrowSchema = LancePageToArrowConverter.toArrowSchema(tableMetadata.getColumns());
        String schemaJson = arrowSchema.toJson();

        log.debug("beginCreateTable: table=%s, path=%s, replace=%s", tableName, tablePath, replace);

        return new LanceWritableTableHandle(
                tableName,
                tablePath,
                schemaJson,
                columns,
                true,  // forCreateTable
                replace);
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

        log.debug("finishCreateTable: table=%s, fragments=%d", handle.tableName(), fragments.size());

        if (fragments.isEmpty()) {
            // Create empty dataset
            lanceWriter.createEmptyDataset(handle.tablePath(), arrowSchema, lanceWriter.getDefaultWriteParams());
        }
        else {
            // Deserialize all fragments and commit
            List<String> allFragmentsJson = new ArrayList<>();
            for (Slice slice : fragments) {
                LanceCommitTaskData commitData = commitTaskDataCodec.fromJson(slice.getBytes());
                allFragmentsJson.addAll(commitData.getFragmentsJson());
            }

            // Create dataset with fragments
            lanceWriter.createDatasetWithFragments(
                    handle.tablePath(),
                    allFragmentsJson,
                    arrowSchema,
                    lanceWriter.getDefaultWriteParams(),
                    new HashMap<>());
        }

        // Invalidate cache after write
        lanceReader.invalidateCache(handle.tablePath());

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

        // Get existing table path
        String tablePath = lanceReader.getTablePath(session, tableName);
        if (tablePath == null) {
            throw new TrinoException(NOT_FOUND, "Table not found: " + tableName);
        }

        // Convert columns to LanceColumnHandle
        List<LanceColumnHandle> lanceColumns = columns.stream()
                .map(LanceColumnHandle.class::cast)
                .collect(toImmutableList());

        // Get Arrow schema from existing table
        Schema arrowSchema = lanceReader.getArrowSchema(tablePath);
        String schemaJson = arrowSchema.toJson();

        log.debug("beginInsert: table=%s, path=%s, columns=%d", tableName, tablePath, columns.size());

        return new LanceWritableTableHandle(
                tableName,
                tablePath,
                schemaJson,
                lanceColumns,
                false, // forCreateTable
                false); // replace
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

        log.debug("finishInsert: table=%s, fragments=%d", handle.tableName(), fragments.size());

        if (fragments.isEmpty()) {
            // No data to insert
            return Optional.empty();
        }

        // Deserialize all fragments
        List<String> allFragmentsJson = new ArrayList<>();
        for (Slice slice : fragments) {
            LanceCommitTaskData commitData = commitTaskDataCodec.fromJson(slice.getBytes());
            allFragmentsJson.addAll(commitData.getFragmentsJson());
        }

        // Commit fragments (INSERT is always append mode)
        lanceWriter.commitAppend(handle.tablePath(), allFragmentsJson, new HashMap<>());

        // Invalidate cache after write
        lanceReader.invalidateCache(handle.tablePath());

        return Optional.empty();
    }

    @VisibleForTesting
    public LanceConfig getLanceConfig()
    {
        return lanceConfig;
    }

    @VisibleForTesting
    public LanceReader getLanceReader()
    {
        return lanceReader;
    }
}
