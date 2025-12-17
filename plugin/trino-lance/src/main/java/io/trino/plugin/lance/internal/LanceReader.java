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
package io.trino.plugin.lance.internal;

import com.google.inject.Inject;
import io.trino.plugin.lance.LanceColumnHandle;
import io.trino.plugin.lance.LanceConfig;
import io.trino.plugin.lance.LanceNamespaceProperties;
import io.trino.plugin.lance.LanceTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class LanceReader
        implements Closeable
{
    // TODO: support multiple schemas
    public static final String SCHEMA = "default";
    private static final String TABLE_PATH_SUFFIX = ".lance";
    private static final BufferAllocator allocator = new RootAllocator(
            RootAllocator.configBuilder().from(RootAllocator.defaultConfig()).maxAllocation(4 * 1024 * 1024).build());

    private final String root;
    private final LanceNamespace namespace;

    @Inject
    public LanceReader(LanceConfig lanceConfig, @LanceNamespaceProperties Map<String, String> namespaceProperties)
    {
        String impl = lanceConfig.getImpl();

        // Build namespace properties from the raw properties map
        // Filter to only include lance.* properties (minus the prefix) for namespace initialization
        Map<String, String> properties = new HashMap<>();
        for (Map.Entry<String, String> entry : namespaceProperties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("lance.")) {
                // Strip the "lance." prefix for namespace properties
                properties.put(key.substring(6), entry.getValue());
            }
        }

        // For DirectoryNamespace, ensure default settings are applied
        if ("dir".equals(impl)) {
            properties.putIfAbsent("manifest_enabled", "true");
            properties.putIfAbsent("dir_listing_enabled", "true");
        }

        // Use LanceNamespace.connect() to dynamically load and initialize the namespace
        this.namespace = LanceNamespace.connect(impl, properties, allocator);

        // Extract root for table path construction (used by DirectoryNamespace)
        this.root = properties.get("root");
    }

    public List<SchemaTableName> listTables(ConnectorSession session, String schema)
    {
        if (SCHEMA.equals(schema)) {
            ListTablesRequest request = new ListTablesRequest();
            ListTablesResponse response = namespace.listTables(request);
            Set<String> tables = response.getTables();
            if (tables == null || tables.isEmpty()) {
                return Collections.emptyList();
            }
            return tables.stream()
                    .map(tableName -> new SchemaTableName(schema, tableName))
                    .collect(Collectors.toList());
        }
        else {
            return Collections.emptyList();
        }
    }

    public List<ColumnMetadata> getColumnsMetadata(String tableName)
    {
        Map<String, ColumnHandle> columnHandlers = this.getColumnHandle(tableName);
        return columnHandlers.values().stream().map(c -> ((LanceColumnHandle) c).getColumnMetadata())
                .collect(toImmutableList());
    }

    public Map<String, ColumnHandle> getColumnHandle(String tableName)
    {
        String tablePath = getTablePath(tableName);
        Schema arrowSchema = getSchema(tablePath);
        // Use LinkedHashMap to preserve column order
        return arrowSchema.getFields().stream().collect(Collectors.toMap(
                Field::getName,
                f -> new LanceColumnHandle(f.getName(), LanceColumnHandle.toTrinoType(f.getFieldType().getType()),
                        f.getFieldType()),
                (v1, v2) -> v1,
                LinkedHashMap::new));
    }

    public String getTablePath(ConnectorSession session, SchemaTableName schemaTableName)
    {
        List<SchemaTableName> schemaTableNameList = listTables(session, schemaTableName.getSchemaName());
        if (schemaTableNameList.contains(schemaTableName)) {
            return getTablePath(schemaTableName.getTableName());
        }
        else {
            return null;
        }
    }

    public List<Fragment> getFragments(LanceTableHandle tableHandle)
    {
        return getFragments(getTablePath(tableHandle.getTableName()));
    }

    private static List<Fragment> getFragments(String tablePath)
    {
        try (Dataset dataset = Dataset.open(tablePath, allocator)) {
            return dataset.getFragments();
        }
    }

    private static Schema getSchema(String tablePath)
    {
        try (Dataset dataset = Dataset.open(tablePath, allocator)) {
            return dataset.getSchema();
        }
    }

    private String getTablePath(String tableName)
    {
        // Construct table path from root location and table name
        String path = root;
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        return path + tableName + TABLE_PATH_SUFFIX;
    }

    @Override
    public void close()
    {
        // LanceNamespace doesn't have a close method in the interface,
        // but implementations may be Closeable
        if (namespace instanceof Closeable) {
            try {
                ((Closeable) namespace).close();
            }
            catch (Exception e) {
                // ignore for now
            }
        }
    }
}
