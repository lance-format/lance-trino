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
package io.trino.plugin.lance.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Provider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.session.PropertyMetadata;

import static io.trino.plugin.lance.procedure.LanceTableProcedureId.VACUUM;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.session.PropertyMetadata.longProperty;

public class LanceVacuumTableProcedure
        implements Provider<TableProcedureMetadata>
{
    public static final String BEFORE_VERSION = "before_version";

    @Override
    public TableProcedureMetadata get()
    {
        return new TableProcedureMetadata(
                VACUUM.name(),
                coordinatorOnly(),
                ImmutableList.<PropertyMetadata<?>>builder()
                        .add(longProperty(
                                BEFORE_VERSION,
                                "Remove versions older than this version",
                                null,
                                value -> {
                                    if (value < 0) {
                                        throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "%s must be equal or greater than 0".formatted(BEFORE_VERSION));
                                    }
                                },
                                false))
                        // TODO Add support for other properties such as before_timestamp_millis, delete_unverified, error_if_tagged_old_versions
                        .build());
    }
}
