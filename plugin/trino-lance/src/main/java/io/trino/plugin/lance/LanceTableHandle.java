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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    @JsonCreator
    public LanceTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tablePath") String tablePath,
            @JsonProperty("tableId") List<String> tableId,
            @JsonProperty("storageOptions") Map<String, String> storageOptions)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.tableId = requireNonNull(tableId, "tableId is null");
        this.storageOptions = storageOptions != null ? new HashMap<>(storageOptions) : new HashMap<>();
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
     * Create a new handle with refreshed storage options.
     */
    public LanceTableHandle withStorageOptions(Map<String, String> newStorageOptions)
    {
        return new LanceTableHandle(schemaName, tableName, tablePath, tableId, newStorageOptions);
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
                Objects.equals(tableId, that.tableId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, tablePath, tableId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("tablePath", tablePath)
                .add("tableId", tableId)
                .add("hasStorageOptions", !storageOptions.isEmpty())
                .toString();
    }
}
