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
import com.google.common.collect.ImmutableMap;
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
import org.lance.index.IndexDescription;
import org.lance.schema.LanceField;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

class IndexesTable
        implements SystemTable
{
    private final LanceRuntime runtime;
    private final String tablePath;
    private final Map<String, String> storageOptions;
    private final ConnectorTableMetadata tableMetadata;

    IndexesTable(LanceRuntime runtime, SchemaTableName tableName, String tablePath, Map<String, String> storageOptions)
    {
        this.runtime = requireNonNull(runtime, "runtime is null");
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.storageOptions = Map.copyOf(requireNonNull(storageOptions, "storageOptions is null"));
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), ImmutableList.<ColumnMetadata>builder()
                .add(new ColumnMetadata("index_name", VARCHAR))
                .add(new ColumnMetadata("columns", new ArrayType(VARCHAR)))
                .add(new ColumnMetadata("index_type", VARCHAR))
                .add(new ColumnMetadata("num_indexed_rows", BIGINT))
                .add(new ColumnMetadata("num_segments", BIGINT))
                .build());
        // TODO Add 'num_indexed_fragments', 'num_unindexed_fragments', 'num_unindexed_rows' and 'indexed_percent' columns
        // TODO Add 'size_bytes' once lance-core exposes Index size (missing in 10.0.0)
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
            Map<Integer, String> idToFieldPath = idToFieldPath(dataset);
            for (IndexDescription index : dataset.describeIndices()) {
                table.addRow(
                        index.getName(),
                        toArrayBlock(index.getFieldIds().stream().map(id -> idToFieldPath.getOrDefault(id, id.toString())).toList()),
                        index.getIndexType(),
                        index.getRowsIndexed(),
                        (long) index.getSegments().size());
            }
        }
        return new RecordPageSource(table.build());
    }

    private static Map<Integer, String> idToFieldPath(Dataset dataset)
    {
        ImmutableMap.Builder<Integer, String> idToFieldPath = ImmutableMap.builder();
        for (LanceField field : dataset.getLanceSchema().fields()) {
            collectFieldPaths(field, null, idToFieldPath);
        }
        return idToFieldPath.buildOrThrow();
    }

    private static void collectFieldPaths(LanceField field, String parent, ImmutableMap.Builder<Integer, String> idToFieldPath)
    {
        String path = parent == null ? field.getName() : parent + "." + field.getName();
        idToFieldPath.put(field.getId(), path);
        field.getChildren().forEach(child -> collectFieldPaths(child, path, idToFieldPath));
    }

    private static Block toArrayBlock(List<String> values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, values.size());
        values.forEach(value -> VARCHAR.writeString(builder, value));
        return builder.build();
    }
}
