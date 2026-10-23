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

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class LanceRefsTable
        implements SystemTable
{
    private final LanceRuntime runtime;
    private final String tablePath;
    private final Map<String, String> storageOptions;
    private final ConnectorTableMetadata tableMetadata;

    public LanceRefsTable(LanceRuntime runtime, SchemaTableName tableName, String tablePath, Map<String, String> storageOptions)
    {
        this.runtime = requireNonNull(runtime, "runtime is null");
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.storageOptions = new HashMap<>(requireNonNull(storageOptions, "storageOptions is null"));
        this.tableMetadata = new ConnectorTableMetadata(
                requireNonNull(tableName, "tableName is null"),
                List.of(
                        new ColumnMetadata("name", VARCHAR),
                        new ColumnMetadata("type", VARCHAR),
                        new ColumnMetadata("version", BIGINT),
                        new ColumnMetadata("branch", VARCHAR)));
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            TupleDomain<Integer> constraint)
    {
        InMemoryRecordSet.Builder builder = InMemoryRecordSet.builder(tableMetadata);
        for (LanceRuntime.LanceRef ref : runtime.listRefs(session.getUser(), tablePath, storageOptions)) {
            builder.addRow(
                    ref.name(),
                    ref.type(),
                    ref.version(),
                    ref.branch());
        }
        return new RecordPageSource(builder.build());
    }
}
