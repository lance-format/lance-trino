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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
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
import io.trino.spi.type.ArrayType;
import org.lance.Dataset;
import org.lance.index.Index;
import org.lance.schema.LanceField;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

class IndexesTable
        implements SystemTable
{
    private static final String SYSTEM_INDEX_PREFIX = "__lance_";

    private final LanceRuntime runtime;
    private final String tablePath;
    private final Map<String, String> storageOptions;
    private final ConnectorTableMetadata tableMetadata;

    IndexesTable(LanceRuntime runtime, SchemaTableName tableName, String tablePath, Map<String, String> storageOptions)
    {
        this.runtime = requireNonNull(runtime, "runtime is null");
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.storageOptions = Map.copyOf(requireNonNull(storageOptions, "storageOptions is null"));
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), List.of(
                new ColumnMetadata("index_name", VARCHAR),
                new ColumnMetadata("columns", new ArrayType(VARCHAR)),
                new ColumnMetadata("index_type", VARCHAR),
                new ColumnMetadata("num_indexed_fragments", BIGINT),
                new ColumnMetadata("num_indexed_rows", BIGINT),
                new ColumnMetadata("num_unindexed_fragments", BIGINT),
                new ColumnMetadata("num_unindexed_rows", BIGINT),
                new ColumnMetadata("num_segments", BIGINT)));
        // TODO Add 'size_bytes' once lance-core exposes Index size (missing in 10.0.0)
        // TODO Add 'indexed_percent' (num_indexed_rows / total rows) as in lance-spark
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
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(tableMetadata);
        try (Dataset dataset = runtime.openDatasetDirect(session.getUser(), tablePath, null, storageOptions)) {
            Map<Integer, String> fieldPaths = new HashMap<>();
            for (LanceField field : dataset.getLanceSchema().fields()) {
                collectFieldPaths(field, null, fieldPaths);
            }

            Map<String, Index> indexes = new TreeMap<>();
            for (Index index : dataset.getIndexes()) {
                if (!index.name().startsWith(SYSTEM_INDEX_PREFIX)) {
                    indexes.putIfAbsent(index.name(), index);
                }
            }

            for (Index index : indexes.values()) {
                String indexName = index.name();
                Map<String, Object> stats = dataset.getIndexStatistics(indexName);
                table.addRow(
                        indexName,
                        toArrayBlock(index.fields().stream().map(id -> fieldPaths.getOrDefault(id, id.toString())).toList()),
                        stats.get("index_type"),
                        longStat(stats, "num_indexed_fragments"),
                        longStat(stats, "num_indexed_rows"),
                        longStat(stats, "num_unindexed_fragments"),
                        longStat(stats, "num_unindexed_rows"),
                        longStat(stats, "num_segments") != null ? longStat(stats, "num_segments") : longStat(stats, "num_indices"));
            }
        }
        return new RecordPageSource(table.build());
    }

    private static Long longStat(Map<String, Object> stats, String key)
    {
        return stats.get(key) instanceof Number number ? number.longValue() : null;
    }

    private static void collectFieldPaths(LanceField field, String parent, Map<Integer, String> paths)
    {
        String path = parent == null ? field.getName() : parent + "." + field.getName();
        paths.put(field.getId(), path);
        field.getChildren().forEach(child -> collectFieldPaths(child, path, paths));
    }

    private static Block toArrayBlock(List<String> values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, values.size());
        values.forEach(value -> VARCHAR.writeString(builder, value));
        return builder.build();
    }
}
