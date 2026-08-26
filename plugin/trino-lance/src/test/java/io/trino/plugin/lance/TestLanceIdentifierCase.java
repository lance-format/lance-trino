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

import io.airlift.json.JsonCodec;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.WriteParams;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.DeclareTableRequest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.lance.LanceRuntime.TABLE_PATH_SUFFIX;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLanceIdentifierCase
        extends AbstractTestQueryFramework
{
    private static final String MIXED_CASE_TABLE = "table-main-20260729T170548Z";
    private static final Schema ID_SCHEMA = new Schema(
            List.of(Field.nullable("id", new ArrowType.Int(64, true))),
            null);

    private Path lanceRoot;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        lanceRoot = Files.createTempDirectory("lance-identifier-case");
        lanceRoot.toFile().deleteOnExit();
        return LanceQueryRunner.builder()
                .addConnectorProperty("lance.root", lanceRoot.toUri().toString())
                .build();
    }

    @Test
    public void testShowTablesLowercaseNameIsQueryableForTimestampTable()
    {
        createDataset(MIXED_CASE_TABLE);

        String trinoName = MIXED_CASE_TABLE.toLowerCase(ENGLISH);
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).contains(trinoName);
        assertThat(tableHandle(trinoName).getTableId()).isEqualTo(List.of(MIXED_CASE_TABLE));
        assertThat(computeScalar("SELECT COUNT(*) FROM " + quoted(MIXED_CASE_TABLE))).isEqualTo(0L);
        assertThat(computeScalar("SELECT COUNT(*) FROM " + quoted(trinoName))).isEqualTo(0L);
    }

    @Test
    public void testSelectFooBarUsingLowercaseName()
    {
        createDataset("fooBar");

        assertThat(tableHandle("foobar").getTableId()).isEqualTo(List.of("fooBar"));
        assertThat(computeScalar("SELECT COUNT(*) FROM foobar")).isEqualTo(0L);
    }

    @Test
    public void testSelectAllLowercaseRemoteTable()
    {
        createDataset("region");

        assertThat(tableHandle("region").getTableId()).isEqualTo(List.of("region"));
        assertThat(computeScalar("SELECT COUNT(*) FROM region")).isEqualTo(0L);
    }

    @Test
    public void testDropTableUsesResolvedRemoteTableId()
    {
        String remoteName = "DropCaseTABLE";
        createDeclaredDataset(remoteName);
        assertThat(tableHandle(remoteName).getTableId()).isEqualTo(List.of(remoteName));

        assertUpdate("DROP TABLE " + quoted(remoteName.toLowerCase(ENGLISH)));

        assertThat(Files.exists(datasetPath(remoteName))).isFalse();
        assertThat(tableHandle(remoteName)).isNull();
    }

    @Test
    public void testInsertUsesResolvedRemoteTableId()
    {
        String remoteName = "InsertCaseTABLE";
        createDataset(remoteName);
        assertThat(tableHandle(remoteName).getTableId()).isEqualTo(List.of(remoteName));

        assertUpdate("INSERT INTO " + quoted(remoteName.toLowerCase(ENGLISH)) + " VALUES (BIGINT '1')", 1);

        assertThat(tableHandle(remoteName).getTableId()).isEqualTo(List.of(remoteName));
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open(allocator, datasetPath(remoteName).toString(), new ReadOptions.Builder().build())) {
            assertThat(dataset.countRows()).isEqualTo(1);
        }
        assertThat(computeScalar("SELECT id FROM " + quoted(remoteName.toLowerCase(ENGLISH)))).isEqualTo(1L);
    }

    @Test
    public void testCreateOrReplaceUsesResolvedRemoteTableId()
    {
        String remoteName = "ReplaceCaseTABLE";
        createDataset(remoteName);
        assertThat(tableHandle(remoteName).getTableId()).isEqualTo(List.of(remoteName));

        assertUpdate("CREATE OR REPLACE TABLE " + quoted(remoteName.toLowerCase(ENGLISH)) + " AS SELECT BIGINT '42' AS id", 1);

        assertThat(tableHandle(remoteName).getTableId()).isEqualTo(List.of(remoteName));
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open(allocator, datasetPath(remoteName).toString(), new ReadOptions.Builder().build())) {
            assertThat(dataset.countRows()).isEqualTo(1);
        }
        assertThat(computeScalar("SELECT id FROM " + quoted(remoteName.toLowerCase(ENGLISH)))).isEqualTo(42L);
    }

    @Test
    public void testCreateTableUsesTrinoLowercaseName()
    {
        String tableName = "created_lowercase";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT BIGINT '1' AS id", 1);

        assertThat(tableHandle(tableName).getTableId()).isEqualTo(List.of(tableName));
        assertThat(Files.exists(datasetPath(tableName))).isTrue();
        assertThat(computeScalar("SELECT id FROM " + tableName)).isEqualTo(1L);
    }

    @Test
    public void testResolvedTableIdIncludesParentPrefixAndSchema()
            throws Exception
    {
        Path root = Files.createTempDirectory("lance-identifier-case-parent");
        root.toFile().deleteOnExit();
        LanceConfig config = new LanceConfig()
                .setSingleLevelNs(false)
                .setParent("p1$p2");
        LanceRuntime runtime = new LanceRuntime(config, Map.of("lance.root", root.toUri().toString()));
        try {
            LanceNamespace namespace = runtime.getNamespace();
            createNamespace(namespace, List.of("p1"));
            createNamespace(namespace, List.of("p1", "p2"));
            createNamespace(namespace, List.of("p1", "p2", "analytics"));

            String remoteName = "fooBar";
            String location = namespace.declareTable(
                    new DeclareTableRequest().id(List.of("p1", "p2", "analytics", remoteName)))
                    .getLocation();
            try (BufferAllocator allocator = new RootAllocator()) {
                Dataset.create(allocator, location, ID_SCHEMA, new WriteParams.Builder().build()).close();
            }

            LanceMetadata metadata = new LanceMetadata(
                    runtime,
                    config,
                    JsonCodec.jsonCodec(LanceCommitTaskData.class),
                    JsonCodec.jsonCodec(LanceMergeCommitData.class));
            LanceTableHandle handle = metadata.getTableHandle(
                    SESSION,
                    new SchemaTableName("analytics", "foobar"),
                    Optional.empty(),
                    Optional.empty());
            assertThat(handle.getTableId()).isEqualTo(List.of("p1", "p2", "analytics", remoteName));
        }
        finally {
            runtime.close();
        }
    }

    private LanceTableHandle tableHandle(String name)
    {
        LanceRuntime runtime = newRuntime();
        try {
            LanceMetadata metadata = new LanceMetadata(
                    runtime,
                    new LanceConfig().setSingleLevelNs(true),
                    JsonCodec.jsonCodec(LanceCommitTaskData.class),
                    JsonCodec.jsonCodec(LanceMergeCommitData.class));
            return metadata.getTableHandle(SESSION, new SchemaTableName("default", name), Optional.empty(), Optional.empty());
        }
        finally {
            runtime.close();
        }
    }

    private LanceRuntime newRuntime()
    {
        return new LanceRuntime(
                new LanceConfig().setSingleLevelNs(true),
                Map.of("lance.root", lanceRoot.toUri().toString()));
    }

    private void createDataset(String remoteTableName)
    {
        try (BufferAllocator allocator = new RootAllocator()) {
            Dataset.create(allocator, datasetPath(remoteTableName).toString(), ID_SCHEMA, new WriteParams.Builder().build()).close();
        }
    }

    private void createDeclaredDataset(String remoteTableName)
    {
        LanceRuntime runtime = newRuntime();
        try {
            String location = runtime.getNamespace()
                    .declareTable(new DeclareTableRequest().id(List.of(remoteTableName)))
                    .getLocation();
            try (BufferAllocator allocator = new RootAllocator()) {
                Dataset.create(allocator, location, ID_SCHEMA, new WriteParams.Builder().build()).close();
            }
        }
        finally {
            runtime.close();
        }
    }

    private Path datasetPath(String remoteTableName)
    {
        return lanceRoot.resolve(remoteTableName + TABLE_PATH_SUFFIX);
    }

    private static String quoted(String name)
    {
        return "\"" + name + "\"";
    }

    private static void createNamespace(LanceNamespace namespace, List<String> namespaceId)
    {
        CreateNamespaceRequest request = new CreateNamespaceRequest();
        request.setId(namespaceId);
        namespace.createNamespace(request);
    }
}
