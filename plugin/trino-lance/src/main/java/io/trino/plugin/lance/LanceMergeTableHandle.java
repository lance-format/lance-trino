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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Handle for MERGE/UPDATE/DELETE operations.
 * Contains the original table handle plus merge-specific metadata.
 */
public record LanceMergeTableHandle(
        @JsonProperty("tableHandle") LanceTableHandle tableHandle,
        @JsonProperty("mergeRowIdColumnHandle") ColumnHandle mergeRowIdColumnHandle,
        @JsonProperty("readVersion") long readVersion,
        @JsonProperty("schemaJson") String schemaJson,
        @JsonProperty("inputColumns") List<LanceColumnHandle> inputColumns,
        @JsonProperty("transactionId") String transactionId,
        @JsonProperty("fileFormatVersion") String fileFormatVersion)
        implements ConnectorMergeTableHandle
{
    @JsonCreator
    public LanceMergeTableHandle
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(mergeRowIdColumnHandle, "mergeRowIdColumnHandle is null");
        requireNonNull(schemaJson, "schemaJson is null");
        requireNonNull(inputColumns, "inputColumns is null");
        // fileFormatVersion can be null
    }

    @JsonProperty
    public LanceTableHandle tableHandle()
    {
        return tableHandle;
    }

    @Override
    public LanceTableHandle getTableHandle()
    {
        return tableHandle;
    }

    public String getTablePath()
    {
        return tableHandle.getTablePath();
    }

    public List<String> getTableId()
    {
        return tableHandle.getTableId();
    }

    public Map<String, String> getStorageOptions()
    {
        return tableHandle.getStorageOptions();
    }
}
