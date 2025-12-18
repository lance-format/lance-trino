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
import io.airlift.json.JsonCodec;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Provider for creating LancePageSink instances.
 * Used for both CREATE TABLE AS SELECT and INSERT operations.
 */
public class LancePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final JsonCodec<LanceCommitTaskData> jsonCodec;

    @Inject
    public LancePageSinkProvider(JsonCodec<LanceCommitTaskData> jsonCodec)
    {
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        LanceWritableTableHandle handle = (LanceWritableTableHandle) outputTableHandle;
        return createPageSink(handle);
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        LanceWritableTableHandle handle = (LanceWritableTableHandle) insertTableHandle;
        return createPageSink(handle);
    }

    private ConnectorPageSink createPageSink(LanceWritableTableHandle handle)
    {
        Schema arrowSchema;
        try {
            arrowSchema = Schema.fromJSON(handle.schemaJson());
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to parse Arrow schema", e);
        }
        return new LancePageSink(
                handle.tablePath(),
                arrowSchema,
                handle.inputColumns(),
                jsonCodec);
    }
}
