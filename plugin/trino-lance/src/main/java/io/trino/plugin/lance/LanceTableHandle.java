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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LanceTableHandle
        implements ConnectorTableHandle
{
    // Buffer time before actual expiration to refresh credentials (5 minutes)
    private static final long EXPIRATION_BUFFER_MILLIS = 5 * 60 * 1000;

    private final String schemaName;
    private final String tableName;
    private final String tablePath;
    private final List<String> tableId;
    private final Map<String, String> storageOptions;
    private final byte[] substraitFilter;
    private final List<String> filterColumns;  // Column names in the filter, for display purposes
    private final OptionalLong limit;
    private final boolean countStar;
    private final Long datasetVersion;  // Captured at planning time for snapshot isolation
    private final String refName;
    private final String branchName;
    // Branch dataset URI from checkout. Null on main.
    private final String readUri;

    public LanceTableHandle(
            String schemaName,
            String tableName,
            String tablePath,
            List<String> tableId,
            Map<String, String> storageOptions)
    {
        this(schemaName, tableName, tablePath, tableId, storageOptions, null);
    }

    public LanceTableHandle(
            String schemaName,
            String tableName,
            String tablePath,
            List<String> tableId,
            Map<String, String> storageOptions,
            Long datasetVersion)
    {
        this(schemaName, tableName, tablePath, tableId, storageOptions, datasetVersion, null, null, null);
    }

    public LanceTableHandle(
            String schemaName,
            String tableName,
            String tablePath,
            List<String> tableId,
            Map<String, String> storageOptions,
            Long datasetVersion,
            String refName,
            String branchName,
            String readUri)
    {
        this(schemaName, tableName, tablePath, tableId, storageOptions, null, List.of(), OptionalLong.empty(), false, datasetVersion, refName, branchName, readUri);
    }

    @JsonCreator
    public LanceTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tablePath") String tablePath,
            @JsonProperty("tableId") List<String> tableId,
            @JsonProperty("storageOptions") Map<String, String> storageOptions,
            @JsonProperty("substraitFilter") byte[] substraitFilter,
            @JsonProperty("filterColumns") List<String> filterColumns,
            @JsonProperty("limit") Long limit,
            @JsonProperty("countStar") Boolean countStar,
            @JsonProperty("datasetVersion") Long datasetVersion,
            @JsonProperty("refName") String refName,
            @JsonProperty("branchName") String branchName,
            @JsonProperty("readUri") String readUri)
    {
        this(schemaName, tableName, tablePath, tableId, storageOptions, substraitFilter,
                filterColumns != null ? filterColumns : List.of(),
                limit != null ? OptionalLong.of(limit) : OptionalLong.empty(),
                countStar != null && countStar,
                datasetVersion,
                refName,
                branchName,
                readUri);
    }

    public LanceTableHandle(
            String schemaName,
            String tableName,
            String tablePath,
            List<String> tableId,
            Map<String, String> storageOptions,
            byte[] substraitFilter,
            List<String> filterColumns,
            OptionalLong limit,
            boolean countStar,
            Long datasetVersion,
            String refName,
            String branchName,
            String readUri)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.tableId = requireNonNull(tableId, "tableId is null");
        this.storageOptions = storageOptions != null ? new HashMap<>(storageOptions) : new HashMap<>();
        this.substraitFilter = substraitFilter;
        this.filterColumns = filterColumns != null ? List.copyOf(filterColumns) : List.of();
        this.limit = requireNonNull(limit, "limit is null");
        this.countStar = countStar;
        this.datasetVersion = datasetVersion;
        this.refName = refName;
        this.branchName = branchName;
        if ((branchName == null) != (readUri == null)) {
            throw new IllegalArgumentException("branchName and readUri are set together");
        }
        this.readUri = readUri;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getTablePath()
    {
        return tablePath;
    }

    /**
     * Get the Lance table identifier.
     * This is the path from namespace root to the table, used for namespace operations.
     */
    @JsonProperty
    public List<String> getTableId()
    {
        return tableId;
    }

    /**
     * Get storage options for accessing the table (S3 credentials, endpoint, etc.).
     */
    @JsonProperty
    public Map<String, String> getStorageOptions()
    {
        return storageOptions;
    }

    /**
     * Check if the storage options have expired or are about to expire.
     * Returns true if credentials need to be refreshed.
     */
    @JsonIgnore
    public boolean isStorageOptionsExpired()
    {
        String expiresAtStr = storageOptions.get("expires_at_millis");
        if (expiresAtStr == null) {
            // No expiration set, credentials are static
            return false;
        }
        try {
            long expiresAt = Long.parseLong(expiresAtStr);
            // Refresh if we're within the buffer time of expiration
            return System.currentTimeMillis() >= (expiresAt - EXPIRATION_BUFFER_MILLIS);
        }
        catch (NumberFormatException e) {
            // Invalid expiration format, assume not expired
            return false;
        }
    }

    /**
     * Get the Substrait filter as a direct ByteBuffer for Lance scanning.
     * Lance JNI requires a direct buffer for native access.
     */
    @JsonIgnore
    public Optional<ByteBuffer> getSubstraitFilterBuffer()
    {
        if (substraitFilter == null || substraitFilter.length == 0) {
            return Optional.empty();
        }
        // Lance JNI requires a direct ByteBuffer
        ByteBuffer directBuffer = ByteBuffer.allocateDirect(substraitFilter.length);
        directBuffer.put(substraitFilter);
        directBuffer.flip();
        return Optional.of(directBuffer);
    }

    /**
     * Get the raw Substrait filter bytes for JSON serialization.
     */
    @JsonProperty("substraitFilter")
    public byte[] getSubstraitFilter()
    {
        return substraitFilter;
    }

    /**
     * Check if there is a filter applied.
     */
    @JsonIgnore
    public boolean hasFilter()
    {
        return substraitFilter != null && substraitFilter.length > 0;
    }

    /**
     * Get the column names used in the filter (for display purposes).
     */
    @JsonProperty("filterColumns")
    public List<String> getFilterColumns()
    {
        return filterColumns;
    }

    /**
     * Get the limit if set.
     */
    @JsonIgnore
    public OptionalLong getLimit()
    {
        return limit;
    }

    /**
     * Get limit as Long for JSON serialization.
     */
    @JsonProperty("limit")
    public Long getLimitForJson()
    {
        return limit.isPresent() ? limit.getAsLong() : null;
    }

    /**
     * Check if this handle uses the manifest-backed row-count aggregate fast path.
     */
    @JsonProperty("countStar")
    public boolean isCountStar()
    {
        return countStar;
    }

    /**
     * Get the dataset version captured at planning time for snapshot isolation.
     * May be null if version was not captured (e.g., for write operations).
     */
    @JsonProperty("datasetVersion")
    public Long getDatasetVersion()
    {
        return datasetVersion;
    }

    @JsonProperty("refName")
    public String getRefName()
    {
        return refName;
    }

    @JsonProperty("branchName")
    public String getBranchName()
    {
        return branchName;
    }

    @JsonProperty("readUri")
    public String getReadUri()
    {
        return readUri;
    }

    @JsonIgnore
    public String getEffectiveReadUri()
    {
        if (readUri == null) {
            return tablePath;
        }
        return readUri;
    }

    /**
     * Create a new handle with refreshed storage options.
     */
    public LanceTableHandle withStorageOptions(Map<String, String> newStorageOptions)
    {
        return new LanceTableHandle(schemaName, tableName, tablePath, tableId, newStorageOptions, substraitFilter, filterColumns, limit, countStar, datasetVersion, refName, branchName, readUri);
    }

    /**
     * Create a new handle with the given Substrait filter and column names.
     */
    public LanceTableHandle withSubstraitFilter(byte[] newSubstraitFilter, List<String> newFilterColumns)
    {
        return new LanceTableHandle(schemaName, tableName, tablePath, tableId, storageOptions, newSubstraitFilter, newFilterColumns, limit, countStar, datasetVersion, refName, branchName, readUri);
    }

    /**
     * Create a new handle with the given limit.
     */
    public LanceTableHandle withLimit(long newLimit)
    {
        return new LanceTableHandle(schemaName, tableName, tablePath, tableId, storageOptions, substraitFilter, filterColumns, OptionalLong.of(newLimit), countStar, datasetVersion, refName, branchName, readUri);
    }

    /**
     * Create a new handle for the manifest-backed row-count aggregate fast path.
     */
    public LanceTableHandle withCountStar()
    {
        return new LanceTableHandle(schemaName, tableName, tablePath, tableId, storageOptions, substraitFilter, filterColumns, limit, true, datasetVersion, refName, branchName, readUri);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LanceTableHandle that = (LanceTableHandle) o;
        return countStar == that.countStar &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(tablePath, that.tablePath) &&
                Objects.equals(tableId, that.tableId) &&
                Arrays.equals(substraitFilter, that.substraitFilter) &&
                Objects.equals(filterColumns, that.filterColumns) &&
                Objects.equals(limit, that.limit) &&
                Objects.equals(datasetVersion, that.datasetVersion) &&
                Objects.equals(refName, that.refName) &&
                Objects.equals(branchName, that.branchName) &&
                Objects.equals(readUri, that.readUri);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(tableName, tablePath, tableId, filterColumns, limit, countStar, datasetVersion, refName, branchName, readUri);
        result = 31 * result + Arrays.hashCode(substraitFilter);
        return result;
    }

    @Override
    public String toString()
    {
        var helper = toStringHelper(this)
                .add("tableName", tableName)
                .add("tablePath", tablePath)
                .add("tableId", tableId)
                .add("hasStorageOptions", !storageOptions.isEmpty());

        // Show predicate columns if filter is present
        if (hasFilter() && !filterColumns.isEmpty()) {
            helper.add("constraint", filterColumns);
        }
        else {
            helper.add("hasFilter", hasFilter());
        }

        if (refName != null) {
            helper.add("refName", refName);
        }
        if (branchName != null) {
            helper.add("branchName", branchName);
        }
        if (readUri != null) {
            helper.add("readUri", readUri);
        }

        return helper
                .add("limit", limit)
                .add("countStar", countStar)
                .toString();
    }
}
