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
    private final OptionalLong limit;
    private final Optional<String> aggregateSql;

    public LanceTableHandle(
            String schemaName,
            String tableName,
            String tablePath,
            List<String> tableId,
            Map<String, String> storageOptions)
    {
        this(schemaName, tableName, tablePath, tableId, storageOptions, null, OptionalLong.empty(), Optional.empty());
    }

    @JsonCreator
    public LanceTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tablePath") String tablePath,
            @JsonProperty("tableId") List<String> tableId,
            @JsonProperty("storageOptions") Map<String, String> storageOptions,
            @JsonProperty("substraitFilter") byte[] substraitFilter,
            @JsonProperty("limit") Long limit,
            @JsonProperty("aggregateSql") String aggregateSql)
    {
        this(schemaName, tableName, tablePath, tableId, storageOptions, substraitFilter,
                limit != null ? OptionalLong.of(limit) : OptionalLong.empty(),
                Optional.ofNullable(aggregateSql));
    }

    public LanceTableHandle(
            String schemaName,
            String tableName,
            String tablePath,
            List<String> tableId,
            Map<String, String> storageOptions,
            byte[] substraitFilter,
            OptionalLong limit,
            Optional<String> aggregateSql)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.tableId = requireNonNull(tableId, "tableId is null");
        this.storageOptions = storageOptions != null ? new HashMap<>(storageOptions) : new HashMap<>();
        this.substraitFilter = substraitFilter;
        this.limit = requireNonNull(limit, "limit is null");
        this.aggregateSql = requireNonNull(aggregateSql, "aggregateSql is null");
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
     * Get the Substrait filter as a ByteBuffer for Lance scanning.
     */
    @JsonIgnore
    public Optional<ByteBuffer> getSubstraitFilterBuffer()
    {
        if (substraitFilter == null || substraitFilter.length == 0) {
            return Optional.empty();
        }
        return Optional.of(ByteBuffer.wrap(substraitFilter).asReadOnlyBuffer());
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
     * Get the aggregate SQL query for DataFusion execution.
     */
    @JsonIgnore
    public Optional<String> getAggregateSql()
    {
        return aggregateSql;
    }

    /**
     * Get aggregate SQL as nullable String for JSON serialization.
     */
    @JsonProperty("aggregateSql")
    public String getAggregateSqlForJson()
    {
        return aggregateSql.orElse(null);
    }

    /**
     * Create a new handle with refreshed storage options.
     */
    public LanceTableHandle withStorageOptions(Map<String, String> newStorageOptions)
    {
        return new LanceTableHandle(schemaName, tableName, tablePath, tableId, newStorageOptions, substraitFilter, limit, aggregateSql);
    }

    /**
     * Create a new handle with the given Substrait filter.
     */
    public LanceTableHandle withSubstraitFilter(byte[] newSubstraitFilter)
    {
        return new LanceTableHandle(schemaName, tableName, tablePath, tableId, storageOptions, newSubstraitFilter, limit, aggregateSql);
    }

    /**
     * Create a new handle with the given limit.
     */
    public LanceTableHandle withLimit(long newLimit)
    {
        return new LanceTableHandle(schemaName, tableName, tablePath, tableId, storageOptions, substraitFilter, OptionalLong.of(newLimit), aggregateSql);
    }

    /**
     * Create a new handle with the given aggregate SQL query.
     */
    public LanceTableHandle withAggregateSql(String newAggregateSql)
    {
        return new LanceTableHandle(schemaName, tableName, tablePath, tableId, storageOptions, substraitFilter, limit, Optional.of(newAggregateSql));
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
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(tablePath, that.tablePath) &&
                Objects.equals(tableId, that.tableId) &&
                Arrays.equals(substraitFilter, that.substraitFilter) &&
                Objects.equals(limit, that.limit) &&
                Objects.equals(aggregateSql, that.aggregateSql);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(tableName, tablePath, tableId, limit, aggregateSql);
        result = 31 * result + Arrays.hashCode(substraitFilter);
        return result;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("tablePath", tablePath)
                .add("tableId", tableId)
                .add("hasStorageOptions", !storageOptions.isEmpty())
                .add("hasFilter", hasFilter())
                .add("limit", limit)
                .add("aggregateSql", aggregateSql)
                .toString();
    }
}
