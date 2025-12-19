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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Handle for write operations (CREATE TABLE, INSERT).
 * Implements both ConnectorInsertTableHandle and ConnectorOutputTableHandle
 * so it can be used for both INSERT and CREATE TABLE AS SELECT operations.
 */
public record LanceWritableTableHandle(
        SchemaTableName tableName,
        String tablePath,
        String schemaJson,
        List<LanceColumnHandle> inputColumns,
        List<String> tableId,
        boolean forCreateTable,
        boolean replace,
        boolean tableExisted)
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle
{
    @JsonCreator
    public LanceWritableTableHandle(
            @JsonProperty("tableName") SchemaTableName tableName,
            @JsonProperty("tablePath") String tablePath,
            @JsonProperty("schemaJson") String schemaJson,
            @JsonProperty("inputColumns") List<LanceColumnHandle> inputColumns,
            @JsonProperty("tableId") List<String> tableId,
            @JsonProperty("forCreateTable") boolean forCreateTable,
            @JsonProperty("replace") boolean replace,
            @JsonProperty("tableExisted") boolean tableExisted)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.schemaJson = requireNonNull(schemaJson, "schemaJson is null");
        this.inputColumns = requireNonNull(inputColumns, "inputColumns is null");
        this.tableId = requireNonNull(tableId, "tableId is null");
        this.forCreateTable = forCreateTable;
        this.replace = replace;
        this.tableExisted = tableExisted;
    }

    @JsonProperty
    @Override
    public SchemaTableName tableName()
    {
        return tableName;
    }

    @JsonProperty
    @Override
    public String tablePath()
    {
        return tablePath;
    }

    @JsonProperty
    @Override
    public String schemaJson()
    {
        return schemaJson;
    }

    @JsonProperty
    @Override
    public List<LanceColumnHandle> inputColumns()
    {
        return inputColumns;
    }

    /**
     * Get the Lance table identifier for namespace operations.
     */
    @JsonProperty
    @Override
    public List<String> tableId()
    {
        return tableId;
    }

    @JsonProperty
    @Override
    public boolean forCreateTable()
    {
        return forCreateTable;
    }

    @JsonProperty
    @Override
    public boolean replace()
    {
        return replace;
    }

    /**
     * Returns true if the table existed before this operation.
     * Used to determine whether to use overwrite mode (existing table) or create mode (new table).
     */
    @JsonProperty
    @Override
    public boolean tableExisted()
    {
        return tableExisted;
    }
}
