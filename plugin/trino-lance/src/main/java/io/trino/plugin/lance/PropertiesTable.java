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

import com.google.common.collect.ImmutableList;
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
import org.lance.Dataset;

import java.util.HashMap;
import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

class PropertiesTable
        implements SystemTable
{
    private final LanceRuntime runtime;
    private final String tablePath;
    private final Map<String, String> storageOptions;
    private final ConnectorTableMetadata tableMetadata;

    PropertiesTable(LanceRuntime runtime, SchemaTableName tableName, String tablePath, Map<String, String> storageOptions)
    {
        this.runtime = requireNonNull(runtime, "runtime is null");
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.storageOptions = Map.copyOf(requireNonNull(storageOptions, "storageOptions is null"));
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("key", VARCHAR))
                .add(new ColumnMetadata("value", VARCHAR))
                .build());
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
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        // Same as Spark SHOW TBLPROPERTIES: dataset config plus location
        Map<String, String> properties;
        try (Dataset dataset = runtime.openDatasetDirect(session.getUser(), tablePath, null, storageOptions)) {
            properties = new HashMap<>(dataset.getConfig());
        }
        properties.put("location", tablePath);

        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(tableMetadata);
        properties.forEach(table::addRow);
        return new RecordPageSource(table.build());
    }
}
