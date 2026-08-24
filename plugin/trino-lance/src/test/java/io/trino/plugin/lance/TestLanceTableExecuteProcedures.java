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
import org.junit.jupiter.api.Test;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ReadOptions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Coverage for the {@code create_index} / {@code optimize_indices} / {@code compact} table
 * procedures (Phase 1, coordinator-only). These call directly into {@code org.lance.Dataset}
 * rather than Trino's split/page pipeline, so correctness is verified both through SQL and by
 * opening the underlying Lance dataset directly to inspect index state.
 */
public class TestLanceTableExecuteProcedures
        extends AbstractTestQueryFramework
{
    private Path tempDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        tempDir = Files.createTempDirectory("lance-trino-execute-test");
        tempDir.toFile().deleteOnExit();
        return LanceQueryRunner.builder()
                .addConnectorProperty("lance.root", tempDir.toUri().toString())
                .addConnectorProperty("lance.single_level_ns", "true")
                .build();
    }

    private Dataset openDataset(String tableName)
    {
        String tablePath = tempDir.toUri() + tableName + ".lance";
        return Dataset.open(tablePath, new ReadOptions.Builder().build());
    }

    @Test
    public void testCreateIndexBuildsFtsIndex()
    {
        String tableName = "test_create_index_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id bigint, body varchar)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'hello world'), (2, 'goodbye world')", 2);

            getQueryRunner().execute("ALTER TABLE " + tableName + " EXECUTE create_index(column => 'body', index_type => 'fts')");

            try (Dataset dataset = openDataset(tableName)) {
                List<String> indexNames = dataset.listIndexes();
                assertThat(indexNames)
                        .as("an index should have been created on 'body'")
                        .isNotEmpty();
            }

            // Re-running create_index for the same column without replace => true should fail:
            // the auto-generated index name collides with the one just created.
            assertThatThrownBy(() -> getQueryRunner().execute(
                    "ALTER TABLE " + tableName + " EXECUTE create_index(column => 'body', index_type => 'fts')"));

            // With replace => true it should succeed.
            getQueryRunner().execute(
                    "ALTER TABLE " + tableName + " EXECUTE create_index(column => 'body', index_type => 'fts', replace => true)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateIndexParallelizesAcrossMultipleFragments()
    {
        String tableName = "test_create_index_parallel_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id bigint, body varchar)");
            // Separate INSERT statements so the table has multiple fragments before create_index
            // runs, exercising the fragment-parallel build (org.lance.Dataset#createIndex with
            // withFragmentIds/withIndexUUID per batch) + commitExistingIndexSegments merge path,
            // instead of the single-fragment path that skips parallelization.
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'alpha document')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'beta document')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'gamma document')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'delta document')", 1);

            List<Integer> fragmentIdsBeforeIndex;
            try (Dataset dataset = openDataset(tableName)) {
                fragmentIdsBeforeIndex = dataset.getFragments().stream().map(Fragment::getId).toList();
            }
            assertThat(fragmentIdsBeforeIndex.size())
                    .as("test setup should produce multiple fragments so the parallel path is exercised")
                    .isGreaterThan(1);

            getQueryRunner().execute("ALTER TABLE " + tableName + " EXECUTE create_index(column => 'body', index_type => 'fts')");

            try (Dataset dataset = openDataset(tableName)) {
                assertThat(dataset.listIndexes()).contains("body_idx");
            }

            // The merge (commitExistingIndexSegments) must not have lost or duplicated any rows.
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 4");
            assertQuery("SELECT id FROM " + tableName + " ORDER BY id", "VALUES 1, 2, 3, 4");

            // replace => true must go through the same drop-then-commit path as the single-fragment case.
            getQueryRunner().execute(
                    "ALTER TABLE " + tableName + " EXECUTE create_index(column => 'body', index_type => 'fts', replace => true)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateIndexWithLanguageAgnosticTokenizers()
    {
        String tableName = "test_create_index_lang_agnostic_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id bigint, body varchar)");
            // Mixed-script content: whitespace-delimited English, plus Chinese and Japanese text
            // with no whitespace between words at all. base_tokenizer => 'simple' (the default)
            // would treat each CJK line as one giant token since it only splits on whitespace/punctuation.
            assertUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, '这是一个测试文档，用于验证语言无关的分词器'), " +
                    "(2, 'これはテストです、言語に依存しないトークナイザーを検証するためのものです'), " +
                    "(3, 'hello world, this is an english test document')",
                    3);

            // 'ngram' is character-level and works uniformly regardless of script; the most
            // conservative choice when the corpus mixes arbitrary/unknown scripts. Note: 'icu' -
            // Unicode's own generic word-boundary segmentation - is documented on the lance-format/lance
            // main branch but is NOT accepted by the lance-core:7.0.0 native library this connector
            // pins (fails with "unknown base tokenizer icu"); it isn't available until a newer release.
            getQueryRunner().execute(
                    "ALTER TABLE " + tableName + " EXECUTE create_index(column => 'body', index_type => 'fts', base_tokenizer => 'ngram')");
            try (Dataset dataset = openDataset(tableName)) {
                assertThat(dataset.listIndexes()).contains("body_idx");
            }

            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateIndexRequiresColumnArgument()
    {
        String tableName = "test_create_index_missing_col_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id bigint, body varchar)");

            assertThatThrownBy(() -> getQueryRunner().execute(
                    "ALTER TABLE " + tableName + " EXECUTE create_index(index_type => 'fts')"))
                    .hasMessageContaining("column");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateIndexRejectsUnsupportedIndexType()
    {
        String tableName = "test_create_index_bad_type_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id bigint, body varchar)");

            assertThatThrownBy(() -> getQueryRunner().execute(
                    "ALTER TABLE " + tableName + " EXECUTE create_index(column => 'body', index_type => 'btree')"))
                    .hasMessageContaining("fts");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testOptimizeIndicesAfterInsertAndCompactWithDeferredRemap()
    {
        String tableName = "test_optimize_compact_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id bigint, body varchar)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'hello world')", 1);

            getQueryRunner().execute("ALTER TABLE " + tableName + " EXECUTE create_index(column => 'body', index_type => 'fts')");

            // Append more data after the index was built; the index now covers only part of the table.
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'goodbye world'), (3, 'another document')", 2);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");

            // Incrementally catch up the index on the newly appended fragment, without a full rebuild.
            getQueryRunner().execute("ALTER TABLE " + tableName + " EXECUTE optimize_indices()");
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");

            // Compacting with defer_index_remap should not require rebuilding the index and should
            // leave the table's data intact.
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'yet another document')", 1);
            getQueryRunner().execute("ALTER TABLE " + tableName + " EXECUTE compact(defer_index_remap => true)");
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 4");

            try (Dataset dataset = openDataset(tableName)) {
                assertThat(dataset.listIndexes()).isNotEmpty();
            }
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
