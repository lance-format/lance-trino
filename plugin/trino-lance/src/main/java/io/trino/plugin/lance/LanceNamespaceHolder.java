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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.lance.namespace.LanceNamespace;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Holds the LanceNamespace instance and namespace-level configuration.
 * Created at dependency injection time and shared across all connector components.
 */
public class LanceNamespaceHolder
        implements Closeable
{
    private static final Logger log = Logger.get(LanceNamespaceHolder.class);

    // Virtual "default" schema for single-level namespace mode
    public static final String DEFAULT_SCHEMA = "default";
    public static final String TABLE_PATH_SUFFIX = ".lance";

    // Shared allocator for Arrow operations
    private static final BufferAllocator allocator = new RootAllocator(
            RootAllocator.configBuilder().from(RootAllocator.defaultConfig()).maxAllocation(Long.MAX_VALUE).build());

    private final LanceNamespace namespace;
    private final String root;
    private final boolean singleLevelNs;
    private final Optional<List<String>> parentPrefix;
    private final Map<String, String> namespaceStorageOptions;

    @Inject
    public LanceNamespaceHolder(LanceConfig lanceConfig, @LanceNamespaceProperties Map<String, String> namespaceProperties)
    {
        String impl = lanceConfig.getImpl();

        // Build namespace properties from the raw properties map
        // Filter to only include lance.* properties (minus the prefix) for namespace initialization
        Map<String, String> properties = new HashMap<>();
        Map<String, String> storageOpts = new HashMap<>();
        for (Map.Entry<String, String> entry : namespaceProperties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("lance.")) {
                // Strip the "lance." prefix for namespace properties
                String strippedKey = key.substring(6);
                properties.put(strippedKey, entry.getValue());

                // Also extract storage options (after removing "storage." prefix)
                if (strippedKey.startsWith("storage.")) {
                    storageOpts.put(strippedKey.substring(8), entry.getValue());
                }
            }
        }
        this.namespaceStorageOptions = storageOpts;

        // For DirectoryNamespace, ensure default settings are applied
        if ("dir".equals(impl)) {
            properties.putIfAbsent("manifest_enabled", "true");
            properties.putIfAbsent("dir_listing_enabled", "true");
        }

        // Use LanceNamespace.connect() to dynamically load and initialize the namespace
        this.namespace = LanceNamespace.connect(impl, properties, allocator);

        // Extract root for table path construction
        this.root = properties.get("root");

        // Initialize namespace level handling from config
        this.singleLevelNs = lanceConfig.isSingleLevelNs();

        // Parse parent prefix if specified (format: "prefix$path")
        String parent = lanceConfig.getParent();
        if (parent != null && !parent.isEmpty()) {
            this.parentPrefix = Optional.of(Arrays.asList(parent.split("\\$")));
        }
        else {
            this.parentPrefix = Optional.empty();
        }

        log.debug("LanceNamespaceHolder initialized: impl=%s, root=%s, singleLevelNs=%s, parentPrefix=%s",
                impl, root, singleLevelNs, parentPrefix);
    }

    public LanceNamespace getNamespace()
    {
        return namespace;
    }

    public String getRoot()
    {
        return root;
    }

    public boolean isSingleLevelNs()
    {
        return singleLevelNs;
    }

    public Optional<List<String>> getParentPrefix()
    {
        return parentPrefix;
    }

    public Map<String, String> getNamespaceStorageOptions()
    {
        return new HashMap<>(namespaceStorageOptions);
    }

    public static BufferAllocator getAllocator()
    {
        return allocator;
    }

    /**
     * Transform Trino schema name to Lance namespace identifier.
     * In single-level mode, "default" maps to empty (root).
     * Otherwise, adds parent prefix if configured.
     */
    public List<String> trinoSchemaToLanceNamespace(String schema)
    {
        if (singleLevelNs) {
            // Single-level mode: "default" maps to root (empty list)
            return Collections.emptyList();
        }

        // Standard 2-level mode: schema maps directly to namespace
        List<String> namespaceId = List.of(schema);

        // Add parent prefix if configured
        return addParentPrefix(namespaceId);
    }

    /**
     * Add parent prefix for 3+ level namespaces.
     */
    public List<String> addParentPrefix(List<String> namespaceId)
    {
        if (parentPrefix.isEmpty()) {
            return namespaceId;
        }

        // Prepend parent prefix
        List<String> result = new java.util.ArrayList<>(parentPrefix.get());
        result.addAll(namespaceId);
        return result;
    }

    /**
     * Convert a Trino SchemaTableName to a Lance table identifier.
     * The table ID is a list of strings representing the path from namespace root to the table.
     */
    public List<String> getTableId(String schemaName, String tableName)
    {
        List<String> tableId = new java.util.ArrayList<>();

        // Add parent prefix if configured
        if (parentPrefix.isPresent()) {
            tableId.addAll(parentPrefix.get());
        }

        // In single-level mode, schema is virtual "default" - don't add it to the table ID
        // The table is directly under the root/parent namespace
        if (!singleLevelNs) {
            tableId.add(schemaName);
        }

        // Add the table name
        tableId.add(tableName);

        return tableId;
    }

    @Override
    public void close()
    {
        if (namespace instanceof Closeable closeable) {
            try {
                closeable.close();
            }
            catch (Exception e) {
                log.warn(e, "Failed to close namespace");
            }
        }
    }
}
