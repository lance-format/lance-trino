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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Handle for an in-flight {@code ALTER TABLE ... EXECUTE} table procedure on a Lance table.
 * {@code procedureHandle} carries the procedure-specific arguments, e.g. {@link LanceCreateIndexHandle}.
 */
public record LanceTableExecuteHandle(
        SchemaTableName schemaTableName,
        String tablePath,
        List<String> tableId,
        Map<String, String> storageOptions,
        LanceTableProcedureId procedureId,
        LanceProcedureHandle procedureHandle)
        implements ConnectorTableExecuteHandle
{
    public LanceTableExecuteHandle
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(tablePath, "tablePath is null");
        tableId = List.copyOf(requireNonNull(tableId, "tableId is null"));
        storageOptions = ImmutableMap.copyOf(requireNonNull(storageOptions, "storageOptions is null"));
        requireNonNull(procedureId, "procedureId is null");
        requireNonNull(procedureHandle, "procedureHandle is null");
    }

    @Override
    public String toString()
    {
        return "schemaTableName:%s, procedureId:%s, procedureHandle:{%s}".formatted(schemaTableName, procedureId, procedureHandle);
    }
}
