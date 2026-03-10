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
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public final class LanceTableProperties
{
    public static final String BLOB_COLUMNS = "blob_columns";
    public static final String VECTOR_COLUMNS = "vector_columns";
    public static final String FILE_FORMAT_VERSION = "file_format_version";

    private static final Set<String> VALID_STORAGE_VERSIONS = ImmutableSet.of(
            "legacy", "0.1", "2.0", "2.1", "2.2", "stable", "next");

    private LanceTableProperties() {}

    public static List<PropertyMetadata<?>> getTableProperties()
    {
        return ImmutableList.of(
                stringProperty(
                        BLOB_COLUMNS,
                        "Comma-separated list of column names to use blob encoding (out-of-line storage for large binary data)",
                        null,
                        false),
                stringProperty(
                        VECTOR_COLUMNS,
                        "Comma-separated list of vector columns with dimensions, format: 'column1:dim1, column2:dim2' (e.g., 'embedding:768, features:256')",
                        null,
                        false),
                stringProperty(
                        FILE_FORMAT_VERSION,
                        "Lance file format version: 'legacy' (0.1), '2.0', '2.1', '2.2', 'stable' (default for new tables), or 'next'",
                        null,
                        false));
    }

    public static Set<String> getBlobColumns(Map<String, Object> properties)
    {
        if (properties == null || properties.isEmpty()) {
            return Set.of();
        }
        Object value = properties.get(BLOB_COLUMNS);
        if (value == null || value.toString().isEmpty()) {
            return Set.of();
        }
        String[] columns = value.toString().split(",");
        Set<String> result = new HashSet<>();
        for (String column : columns) {
            String trimmed = column.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }

    /**
     * Parse vector columns property to get a map of column name to dimension.
     * Format: "column1:dim1, column2:dim2"
     * Example: "embedding:768, features:256"
     *
     * @return Map of column name to dimension (fixed size list size)
     */
    public static Map<String, Integer> getVectorColumns(Map<String, Object> properties)
    {
        if (properties == null || properties.isEmpty()) {
            return Map.of();
        }
        Object value = properties.get(VECTOR_COLUMNS);
        if (value == null || value.toString().isEmpty()) {
            return Map.of();
        }
        String[] entries = value.toString().split(",");
        Map<String, Integer> result = new HashMap<>();
        for (String entry : entries) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int colonIndex = trimmed.lastIndexOf(':');
            if (colonIndex <= 0 || colonIndex == trimmed.length() - 1) {
                throw new IllegalArgumentException(
                        "Invalid vector_columns format: '" + trimmed + "'. Expected format: 'column_name:dimension'");
            }
            String columnName = trimmed.substring(0, colonIndex).trim();
            String dimensionStr = trimmed.substring(colonIndex + 1).trim();
            try {
                int dimension = Integer.parseInt(dimensionStr);
                if (dimension <= 0) {
                    throw new IllegalArgumentException(
                            "Vector dimension must be positive for column '" + columnName + "', got: " + dimension);
                }
                result.put(columnName, dimension);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid dimension for vector column '" + columnName + "': " + dimensionStr);
            }
        }
        return result;
    }

    /**
     * Get the file format version from table properties.
     * Returns null if not specified (will use existing table's version or default for new tables).
     *
     * @return the file format version string, or null if not specified
     */
    public static String getFileFormatVersion(Map<String, Object> properties)
    {
        if (properties == null || properties.isEmpty()) {
            return null;
        }
        Object value = properties.get(FILE_FORMAT_VERSION);
        if (value == null || value.toString().isEmpty()) {
            return null;
        }
        String version = value.toString().trim().toLowerCase();
        if (!VALID_STORAGE_VERSIONS.contains(version)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY,
                    "Invalid file_format_version: '" + value + "'. " +
                    "Valid values are: legacy, 0.1, 2.0, 2.1, 2.2, stable, next");
        }
        return version;
    }
}
