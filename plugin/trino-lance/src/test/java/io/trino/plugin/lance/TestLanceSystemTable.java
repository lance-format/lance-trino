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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;

import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.List;

import static io.trino.plugin.lance.LanceRuntime.TABLE_PATH_SUFFIX;
import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestLanceSystemTable
        extends AbstractTestQueryFramework
{
    @TempDir
    private static Path lanceRoot;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return LanceQueryRunner.builder()
                .addConnectorProperty("lance.root", lanceRoot.toUri().toString())
                .setInitialTables(REGION)
                .build();
    }

    @Test
    void testVersionsTable()
    {
        List<MaterializedRow> rows = computeActual("TABLE \"region$versions\"").getMaterializedRows();
        assertThat(rows).isNotEmpty();
        for (MaterializedRow row : rows) {
            assertThat(row.getFields()).hasSize(5);
            assertThat(row.getField(0)).isEqualTo(1L);
            assertThat((String) row.getField(1)).endsWith(".manifest");
            assertThat(row.getField(2)).isInstanceOf(Long.class);
            assertThat(row.getField(3)).isInstanceOf(String.class);
            assertThat(row.getField(4)).isInstanceOf(ZonedDateTime.class);
        }
    }

    @Test
    void testIndexesTable()
    {
        assertUpdate("CREATE TABLE test_indexes (id bigint, name varchar)");
        try {
            assertUpdate("INSERT INTO test_indexes VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
            assertThat(computeActual("TABLE \"test_indexes$indexes\"").getMaterializedRows()).isEmpty();

            try (BufferAllocator allocator = new RootAllocator();
                    Dataset dataset = Dataset.open()
                            .allocator(allocator)
                            .uri(lanceRoot.resolve("test_indexes" + TABLE_PATH_SUFFIX).toString())
                            .build()) {
                dataset.createIndex(IndexOptions.builder(
                                List.of("id"),
                                IndexType.BTREE,
                                IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create("btree")).build())
                        .withIndexName("id_idx")
                        .replace(true)
                        .build());
            }

            assertThat(query("SELECT index_name, columns, index_type, num_indexed_rows, num_unindexed_rows FROM \"test_indexes$indexes\""))
                    .matches("VALUES (VARCHAR 'id_idx', ARRAY[VARCHAR 'id'], VARCHAR 'BTree', BIGINT '3', BIGINT '0')");
        }
        finally {
            assertUpdate("DROP TABLE test_indexes");
        }
    }
}
