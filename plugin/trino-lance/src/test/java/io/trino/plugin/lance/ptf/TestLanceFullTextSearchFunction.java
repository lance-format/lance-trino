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
package io.trino.plugin.lance.ptf;

import io.trino.plugin.lance.LanceQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
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
import org.lance.index.scalar.InvertedIndexParams;

import java.nio.file.Path;
import java.util.List;

import static io.trino.plugin.lance.LanceRuntime.TABLE_PATH_SUFFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestLanceFullTextSearchFunction
        extends AbstractTestQueryFramework
{
    @TempDir
    private static Path lanceRoot;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner runner = LanceQueryRunner.builder()
                .addConnectorProperty("lance.root", lanceRoot.toUri().toString())
                .build();
        runner.execute("CREATE TABLE docs (id bigint, title varchar, body varchar)");
        runner.execute("INSERT INTO docs VALUES " +
                "(1, 'ml', 'machine learning basics'), " +
                "(2, 'dl', 'deep learning networks'), " +
                "(3, 'db', 'relational database systems')");
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open()
                        .allocator(allocator)
                        .uri(lanceRoot.resolve("docs" + TABLE_PATH_SUFFIX).toString())
                        .build()) {
            for (String column : List.of("title", "body")) {
                dataset.createIndex(IndexOptions.builder(
                                List.of(column),
                                IndexType.INVERTED,
                                IndexParams.builder().setScalarIndexParams(InvertedIndexParams.builder().build()).build())
                        .withIndexName(column + "_fts")
                        .replace(true)
                        .build());
            }
        }
        return runner;
    }

    @Test
    void testMatch()
    {
        assertThat(query("SELECT id FROM TABLE(system.full_text_search('default', 'docs', DESCRIPTOR(body), 'learning'))"))
                .matches("VALUES BIGINT '1', BIGINT '2'");
    }

    @Test
    void testScore()
    {
        assertThat(query("SELECT id FROM TABLE(system.full_text_search('default', 'docs', DESCRIPTOR(body), 'learning')) WHERE _score > 0"))
                .matches("VALUES BIGINT '1', BIGINT '2'");
    }

    @Test
    void testOptions()
    {
        assertThat(query("SELECT id FROM TABLE(system.full_text_search('default', 'docs', DESCRIPTOR(body), 'machine networks', 'operator=AND'))"))
                .returnsEmptyResult();
        assertThat(query("SELECT id FROM TABLE(system.full_text_search('default', 'docs', DESCRIPTOR(body), 'machin', 'fuzziness=1'))"))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    void testMultipleColumns()
    {
        assertThat(query("SELECT id FROM TABLE(system.full_text_search('default', 'docs', DESCRIPTOR(title, body), 'database'))"))
                .matches("VALUES BIGINT '3'");
    }

    @Test
    void testFilterLimitAndCount()
    {
        assertThat(query("SELECT id FROM TABLE(system.full_text_search('default', 'docs', DESCRIPTOR(body), 'learning')) WHERE id > 1"))
                .matches("VALUES BIGINT '2'");
        assertThat(query("SELECT id FROM TABLE(system.full_text_search('default', 'docs', DESCRIPTOR(body), 'learning')) LIMIT 1"))
                .result().rowCount().isEqualTo(1);
        assertThat(query("SELECT count(*) FROM TABLE(system.full_text_search('default', 'docs', DESCRIPTOR(body), 'learning'))"))
                .matches("VALUES BIGINT '2'");
    }

    @Test
    void testInvalidArguments()
    {
        assertQueryFails("SELECT * FROM TABLE(system.full_text_search('default', 'docs', DESCRIPTOR(id), 'x'))", ".*Column is not a varchar: id");
        assertQueryFails("SELECT * FROM TABLE(system.full_text_search('default', 'docs', DESCRIPTOR(missing), 'x'))", ".*Column not found: missing");
        assertQueryFails("SELECT * FROM TABLE(system.full_text_search('default', 'missing', DESCRIPTOR(body), 'x'))", ".*Table 'default.missing' does not exist");
        assertQueryFails("SELECT * FROM TABLE(system.full_text_search('default', 'docs', DESCRIPTOR(body), 'x', 'foo=1'))", ".*Unknown option: foo");
        assertQueryFails("SELECT * FROM TABLE(system.full_text_search('default', 'docs', DESCRIPTOR(body varchar), 'x'))", ".*COLUMNS descriptor contains types");
    }

    @Test
    void testNoIndex()
    {
        assertUpdate("CREATE TABLE no_index (body varchar)");
        assertQueryFails("SELECT * FROM TABLE(system.full_text_search('default', 'no_index', DESCRIPTOR(body), 'x'))", ".*Column has no FTS index: body");
        assertUpdate("DROP TABLE no_index");
    }
}
