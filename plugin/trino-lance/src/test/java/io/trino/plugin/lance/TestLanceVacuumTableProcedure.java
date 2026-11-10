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
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.lance.Dataset;
import org.lance.Version;

import java.nio.file.Path;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.assertj.core.api.Assertions.assertThat;

final class TestLanceVacuumTableProcedure
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
                .build();
    }

    @ParameterizedTest
    @ValueSource(strings = {"legacy", "0.1", "2.0", "2.1", "2.2", "stable", "next"})
    void testVacuum(String fileFormatVersion)
    {
        try (TestTable table = newTrinoTable("test_version", "(id int) WITH (file_format_version = '" + fileFormatVersion + "')")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertThat(listVersions(table.getName())).contains(1L, 2L);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE vacuum");
            assertThat(listVersions(table.getName())).contains(2L);
        }
    }

    @Test
    void testVacuumBeforeVersion()
    {
        try (TestTable table = newTrinoTable("test_before_version", "(id int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 2", 1);
            assertThat(listVersions(table.getName())).contains(1L, 2L, 3L);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE vacuum(before_version => 0)");
            assertThat(listVersions(table.getName())).contains(1L, 2L, 3L);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE vacuum(before_version => 1)");
            assertThat(listVersions(table.getName())).contains(1L, 2L, 3L);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE vacuum(before_version => 2)");
            assertThat(listVersions(table.getName())).contains(2L, 3L);

            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE vacuum(before_version => 3)");
            assertThat(listVersions(table.getName())).contains(3L);

            assertThat(query("SELECT * FROM " + table.getName())).matches("VALUES 1, 2");
        }
    }

    @Test
    void testVacuumTaggedVersion()
    {
        try (TestTable table = newTrinoTable("test_tagged_version", "(id int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertThat(listVersions(table.getName())).contains(1L, 2L);

            createTag(table.getName(), "tag1", 1);

            assertQueryFails("ALTER TABLE " + table.getName() + " EXECUTE vacuum", ".* 1 tagged version\\(s\\) have been marked for cleanup.*");
            assertThat(listVersions(table.getName())).contains(1L, 2L);
        }
    }

    @Test
    void testVacuumInvalidProperty()
    {
        try (TestTable table = newTrinoTable("test_invalid_property", "(id int)")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " EXECUTE vacuum(before_version => -1)",
                    ".* before_version must be equal or greater than 0");
        }
    }

    private static void createTag(String tableName, String tag, long version)
    {
        try (Dataset dataset = openDataset(tableName)) {
            dataset.tags().create(tag, version);
        }
    }

    private static List<Long> listVersions(String tableName)
    {
        try (Dataset dataset = openDataset(tableName)) {
            return dataset.listVersions().stream()
                    .map(Version::getId)
                    .collect(toImmutableList());
        }
    }

    private static Dataset openDataset(String tableName)
    {
        String tablePath = lanceRoot.toUri() + tableName + ".lance";
        return Dataset.open().uri(tablePath).build();
    }
}
