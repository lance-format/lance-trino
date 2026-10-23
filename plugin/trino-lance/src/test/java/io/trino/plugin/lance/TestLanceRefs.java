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
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.PointerType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Ref;
import org.lance.Transaction;
import org.lance.operation.Append;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.lance.LanceRuntime.TABLE_PATH_SUFFIX;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLanceRefs
        extends AbstractTestQueryFramework
{
    private Path lanceRoot;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        lanceRoot = Files.createTempDirectory("lance-refs");
        lanceRoot.toFile().deleteOnExit();
        return LanceQueryRunner.builder()
                .addConnectorProperty("lance.root", lanceRoot.toUri().toString())
                .build();
    }

    @Test
    public void testNamedRefReadsBranchAndTag()
            throws Exception
    {
        assertUpdate("CREATE TABLE refs_demo (id bigint)");
        assertUpdate("INSERT INTO refs_demo VALUES (1)", 1);
        assertUpdate("INSERT INTO refs_demo VALUES (2)", 1);

        Path datasetPath = tablePath("refs_demo");
        assertThat(datasetPath).isDirectory();
        divergeBranch(datasetPath);

        assertThat(computeScalar("SELECT id FROM refs_demo FOR VERSION AS OF 2")).isEqualTo(1L);
        assertThat(computeScalar("SELECT COUNT(*) FROM refs_demo FOR VERSION AS OF 2")).isEqualTo(1L);
        assertThat(computeActual("SELECT id FROM refs_demo FOR VERSION AS OF 'audit' ORDER BY id").getOnlyColumnAsSet())
                .containsExactly(99L, 100L);
        assertThat(computeScalar("SELECT id FROM refs_demo FOR VERSION AS OF 'audit' WHERE id = 99")).isEqualTo(99L);
        assertThat(computeScalar("SELECT COUNT(*) FROM refs_demo FOR VERSION AS OF 'audit'")).isEqualTo(2L);
        assertThat(computeScalar("SELECT id FROM refs_demo FOR VERSION AS OF 'v1'")).isEqualTo(1L);
        assertThat(computeActual("SELECT id FROM refs_demo FOR VERSION AS OF 'on_audit' ORDER BY id").getOnlyColumnAsSet())
                .containsExactly(99L, 100L);
        assertThat(computeScalar("SELECT COUNT(*) FROM refs_demo FOR VERSION AS OF 'main'")).isEqualTo(2L);
        assertThat(computeActual("SELECT id FROM refs_demo ORDER BY id").getOnlyColumnAsSet()).containsExactly(1L, 2L);

        assertThat(computeActual("SELECT name, type, version, branch FROM \"refs_demo$refs\" ORDER BY name")
                .getMaterializedRows())
                .extracting(row -> row.getField(0) + "," + row.getField(1) + "," + row.getField(2) + "," + row.getField(3))
                .containsExactly(
                        "audit,BRANCH,2,null",
                        "main,BRANCH,3,null",
                        "on_audit,TAG,2,audit",
                        "v1,TAG,2,null");

        assertQueryFails(
                "SELECT id FROM refs_demo FOR VERSION AS OF 'missing'",
                "Lance ref does not exist: missing");
        assertNamedRefWriteRejected();

        assertUpdate("INSERT INTO refs_demo SELECT id FROM refs_demo FOR VERSION AS OF 'audit'", 2);
        assertThat(computeActual("SELECT id FROM refs_demo ORDER BY id").getOnlyColumnAsSet()).containsExactly(1L, 2L, 99L, 100L);
        assertThat(computeActual("SELECT id FROM refs_demo FOR VERSION AS OF 'audit' ORDER BY id").getOnlyColumnAsSet())
                .containsExactly(99L, 100L);

        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open().allocator(allocator).uri(datasetPath.toString()).build()) {
            dataset.tags().create("audit", 2);
        }
        assertQueryFails(
                "SELECT id FROM refs_demo FOR VERSION AS OF 'audit'",
                "Lance ref 'audit' is both a branch and a tag");
    }

    @Test
    public void testSlashedBranchAndItsTag()
            throws Exception
    {
        assertUpdate("CREATE TABLE slash_demo (id bigint)");
        assertUpdate("INSERT INTO slash_demo VALUES (1)", 1);

        Path datasetPath = tablePath("slash_demo");
        createBranch(datasetPath, "feature/audit", 1);
        long version = appendIds(datasetPath, "feature/audit", 99);
        assertThat(version).isEqualTo(2);
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open().allocator(allocator).uri(datasetPath.toString()).build()) {
            dataset.tags().create("release", Ref.ofBranch("feature/audit", version));
        }

        assertThat(computeScalar("SELECT id FROM slash_demo FOR VERSION AS OF 'feature/audit'")).isEqualTo(99L);
        assertThat(computeScalar("SELECT id FROM slash_demo FOR VERSION AS OF 'release'")).isEqualTo(99L);
        assertThat(computeScalar("SELECT id FROM slash_demo FOR VERSION AS OF 2")).isEqualTo(1L);
        assertThat(computeActual("SELECT name, type, version, branch FROM \"slash_demo$refs\" ORDER BY name")
                .getMaterializedRows())
                .extracting(row -> row.getField(0) + "," + row.getField(1) + "," + row.getField(2) + "," + row.getField(3))
                .containsExactly(
                        "feature/audit,BRANCH,2,null",
                        "main,BRANCH,2,null",
                        "release,TAG,2,feature/audit");
    }

    @Test
    public void testPlannedBranchHandleIgnoresLaterCommit()
            throws Exception
    {
        assertUpdate("CREATE TABLE pin_demo (id bigint)");
        assertUpdate("INSERT INTO pin_demo VALUES (1)", 1);
        assertUpdate("INSERT INTO pin_demo VALUES (2)", 1);

        Path datasetPath = tablePath("pin_demo");
        createBranch(datasetPath, "audit", 1);
        assertThat(appendIds(datasetPath, "audit", 99, 100)).isEqualTo(2);

        LanceRuntime runtime = newRuntime();
        try {
            LanceMetadata metadata = newMetadata(runtime);
            LanceTableHandle pinned = handleFor(metadata, "pin_demo", "audit");
            assertThat(pinned.getDatasetVersion()).isEqualTo(2L);
            assertThat(rowCount(runtime, pinned)).isEqualTo(2);
            assertThat(columnNames(metadata, pinned)).containsExactly("id");

            addNoteColumn(datasetPath, "audit");
            assertThat(appendNote(datasetPath, "audit", 7, "later")).isGreaterThan(2L);

            assertThat(pinned.getDatasetVersion()).isEqualTo(2L);
            assertThat(rowCount(runtime, pinned)).isEqualTo(2);
            assertThat(columnNames(metadata, pinned)).containsExactly("id");

            LanceTableHandle fresh = handleFor(metadata, "pin_demo", "audit");
            assertThat(fresh.getDatasetVersion()).isGreaterThan(2L);
            assertThat(fresh.getEffectiveReadUri()).isEqualTo(pinned.getEffectiveReadUri());
            assertThat(rowCount(runtime, fresh)).isEqualTo(3);
            assertThat(columnNames(metadata, fresh)).containsExactly("id", "note");
        }
        finally {
            runtime.close();
        }

        assertThat(computeScalar("SELECT COUNT(*) FROM pin_demo FOR VERSION AS OF 'audit'")).isEqualTo(3L);
        assertThat(computeScalar("SELECT note FROM pin_demo FOR VERSION AS OF 'audit' WHERE id = 7")).isEqualTo("later");
        assertQueryFails("SELECT note FROM pin_demo", ".*Column 'note' cannot be resolved.*");
    }

    @Test
    public void testTagNamedMainConflictsWithMain()
            throws Exception
    {
        assertUpdate("CREATE TABLE main_shadow (id bigint)");
        assertUpdate("INSERT INTO main_shadow VALUES (1)", 1);
        assertThat(computeScalar("SELECT id FROM main_shadow FOR VERSION AS OF 'main'")).isEqualTo(1L);

        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open().allocator(allocator).uri(tablePath("main_shadow").toString()).build()) {
            dataset.tags().create("main", 1);
        }

        assertQueryFails(
                "SELECT id FROM main_shadow FOR VERSION AS OF 'main'",
                "Lance ref 'main' is both a branch and a tag");
        assertThat(computeActual("SELECT name, type, version, branch FROM \"main_shadow$refs\" ORDER BY type")
                .getMaterializedRows())
                .extracting(row -> row.getField(0) + "," + row.getField(1) + "," + row.getField(2) + "," + row.getField(3))
                .containsExactly(
                        "main,BRANCH,2,null",
                        "main,TAG,1,null");
    }

    @Test
    public void testBranchUriKeepsQuery()
            throws Exception
    {
        assertUpdate("CREATE TABLE uri_demo (id bigint)");
        Path datasetPath = tablePath("uri_demo");
        createBranch(datasetPath, "feature/audit", 1);
        String tableUri = datasetPath.toUri() + "?ddbTableName=commits";

        String expected;
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open().allocator(allocator).uri(tableUri).build();
                Dataset branch = dataset.checkout(Ref.ofBranch("feature/audit"))) {
            expected = branch.uri();
        }

        LanceRuntime runtime = newRuntime();
        try {
            LanceRuntime.ResolvedLanceRef resolved = runtime.resolveNamedRef("user", tableUri, Map.of(), "feature/audit");
            assertThat(resolved.readUri()).isEqualTo(expected);
            assertThat(resolved.branchName()).isEqualTo("feature/audit");
            assertThat(resolved.readUri()).endsWith("/tree/feature/audit?ddbTableName=commits");

            LanceTableHandle handle = new LanceTableHandle(
                    "default",
                    "orders",
                    tableUri,
                    List.of("orders"),
                    Map.of(),
                    resolved.version(),
                    resolved.refName(),
                    resolved.branchName(),
                    resolved.readUri());
            assertThat(handle.getEffectiveReadUri()).isEqualTo(expected);
            try (BufferAllocator allocator = new RootAllocator();
                    Dataset opened = Dataset.open().allocator(allocator).uri(handle.getEffectiveReadUri()).build()) {
                assertThat(opened.version()).isEqualTo(resolved.version());
            }
        }
        finally {
            runtime.close();
        }
    }

    private void assertNamedRefWriteRejected()
    {
        LanceRuntime runtime = newRuntime();
        try {
            LanceMetadata metadata = newMetadata(runtime);
            LanceTableHandle handle = handleFor(metadata, "refs_demo", "audit");
            assertThat(handle.getRefName()).isEqualTo("audit");
            assertThat(handle.getBranchName()).isEqualTo("audit");
            assertThat(handle.getEffectiveReadUri()).endsWith("/tree/audit");
            assertThatThrownBy(() -> metadata.beginInsert(SESSION, handle, List.of(), NO_RETRIES))
                    .hasMessage("Writing to Lance ref 'audit' is not supported");
            assertThatThrownBy(() -> metadata.beginMerge(SESSION, handle, Map.of(), NO_RETRIES))
                    .hasMessage("Writing to Lance ref 'audit' is not supported");
        }
        finally {
            runtime.close();
        }
    }

    private Path tablePath(String table)
    {
        return lanceRoot.resolve(table + TABLE_PATH_SUFFIX);
    }

    private LanceRuntime newRuntime()
    {
        return new LanceRuntime(
                new LanceConfig().setSingleLevelNs(true),
                Map.of("lance.root", lanceRoot.toUri().toString()));
    }

    private static LanceMetadata newMetadata(LanceRuntime runtime)
    {
        return new LanceMetadata(
                runtime,
                jsonCodec(LanceCommitTaskData.class),
                jsonCodec(LanceMergeCommitData.class));
    }

    private static LanceTableHandle handleFor(LanceMetadata metadata, String table, String ref)
    {
        ConnectorTableVersion version = new ConnectorTableVersion(PointerType.TARGET_ID, VARCHAR, utf8Slice(ref));
        return metadata.getTableHandle(
                SESSION,
                new SchemaTableName("default", table),
                Optional.empty(),
                Optional.of(version));
    }

    private static long rowCount(LanceRuntime runtime, LanceTableHandle handle)
    {
        return runtime.getManifestSummary(
                SESSION.getUser(),
                handle.getEffectiveReadUri(),
                handle.getDatasetVersion(),
                handle.getStorageOptions()).getTotalRows();
    }

    private static List<String> columnNames(LanceMetadata metadata, LanceTableHandle handle)
    {
        ConnectorTableMetadata table = metadata.getTableMetadata(SESSION, handle);
        assertThat(table).isNotNull();
        return table.getColumns().stream().map(ColumnMetadata::getName).toList();
    }

    private static void divergeBranch(Path datasetPath)
            throws Exception
    {
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open().allocator(allocator).uri(datasetPath.toString()).build()) {
            assertThat(dataset.version()).isEqualTo(3);
        }
        createBranch(datasetPath, "audit", 1);
        assertThat(appendIds(datasetPath, "audit", 99, 100)).isEqualTo(2);

        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open().allocator(allocator).uri(datasetPath.toString()).build()) {
            dataset.tags().create("v1", 2);
            dataset.tags().create("on_audit", Ref.ofBranch("audit", 2));
        }
    }

    private static void createBranch(Path datasetPath, String branch, long fromVersion)
            throws Exception
    {
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open().allocator(allocator).uri(datasetPath.toString()).build()) {
            dataset.createBranch(branch, Ref.ofMain(fromVersion)).close();
        }
    }

    private static long appendIds(Path datasetPath, String branch, long... ids)
            throws Exception
    {
        try (BufferAllocator allocator = new RootAllocator();
                Dataset parent = Dataset.open().allocator(allocator).uri(datasetPath.toString()).build();
                Dataset checkedOut = parent.checkout(Ref.ofBranch(branch));
                VectorSchemaRoot batch = VectorSchemaRoot.create(checkedOut.getSchema(), allocator)) {
            BigIntVector column = (BigIntVector) batch.getVector("id");
            column.allocateNew(ids.length);
            for (int i = 0; i < ids.length; i++) {
                column.set(i, ids[i]);
            }
            batch.setRowCount(ids.length);
            return commitAppend(checkedOut, batch, allocator);
        }
    }

    private static void addNoteColumn(Path datasetPath, String branch)
            throws Exception
    {
        try (BufferAllocator allocator = new RootAllocator();
                Dataset parent = Dataset.open().allocator(allocator).uri(datasetPath.toString()).build();
                Dataset checkedOut = parent.checkout(Ref.ofBranch(branch))) {
            checkedOut.addColumns(List.of(Field.nullable("note", new ArrowType.Utf8())));
        }
    }

    private static long appendNote(Path datasetPath, String branch, long id, String note)
            throws Exception
    {
        try (BufferAllocator allocator = new RootAllocator();
                Dataset parent = Dataset.open().allocator(allocator).uri(datasetPath.toString()).build();
                Dataset checkedOut = parent.checkout(Ref.ofBranch(branch));
                VectorSchemaRoot batch = VectorSchemaRoot.create(checkedOut.getSchema(), allocator)) {
            BigIntVector ids = (BigIntVector) batch.getVector("id");
            VarCharVector notes = (VarCharVector) batch.getVector("note");
            ids.allocateNew(1);
            notes.allocateNew(1);
            ids.set(0, id);
            notes.setSafe(0, note.getBytes(UTF_8));
            batch.setRowCount(1);
            return commitAppend(checkedOut, batch, allocator);
        }
    }

    private static long commitAppend(Dataset branch, VectorSchemaRoot batch, BufferAllocator allocator)
            throws Exception
    {
        List<FragmentMetadata> fragments = Fragment.write()
                .datasetUri(branch.uri())
                .allocator(allocator)
                .data(batch)
                .execute();
        try (Transaction transaction = new Transaction(branch.version(), Append.builder().fragments(fragments).build())) {
            Dataset committed = branch.commitTransaction(transaction);
            long version = committed.version();
            committed.close();
            return version;
        }
    }
}
