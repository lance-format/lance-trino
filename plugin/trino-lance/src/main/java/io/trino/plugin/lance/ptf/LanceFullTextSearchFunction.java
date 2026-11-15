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
package io.trino.plugin.lance.ptf;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.lance.LanceColumnHandle;
import io.trino.plugin.lance.LanceMetadata;
import io.trino.plugin.lance.LanceRuntime;
import io.trino.plugin.lance.LanceTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.DescriptorArgument;
import io.trino.spi.function.table.DescriptorArgumentSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.VarcharType;
import org.lance.Dataset;
import org.lance.index.IndexDescription;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.DescriptorArgument.NULL_DESCRIPTOR;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class LanceFullTextSearchFunction
        extends AbstractConnectorTableFunction
{
    public static final String NAME = "full_text_search";
    public static final String SCORE_COLUMN = "_score";

    private static final String SCHEMA_ARGUMENT_NAME = "SCHEMA_NAME";
    private static final String TABLE_ARGUMENT_NAME = "TABLE_NAME";
    private static final String COLUMNS_ARGUMENT_NAME = "COLUMNS";
    private static final String QUERY_ARGUMENT_NAME = "QUERY";
    private static final String OPTIONS_ARGUMENT_NAME = "OPTIONS";

    private final LanceMetadata metadata;
    private final LanceRuntime runtime;

    public LanceFullTextSearchFunction(LanceMetadata metadata, LanceRuntime runtime)
    {
        super(
                "system",
                NAME,
                ImmutableList.of(
                        ScalarArgumentSpecification.builder().name(SCHEMA_ARGUMENT_NAME).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(TABLE_ARGUMENT_NAME).type(VARCHAR).build(),
                        DescriptorArgumentSpecification.builder().name(COLUMNS_ARGUMENT_NAME).build(),
                        ScalarArgumentSpecification.builder().name(QUERY_ARGUMENT_NAME).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(OPTIONS_ARGUMENT_NAME).type(VARCHAR).defaultValue(null).build()),
                GENERIC_TABLE);
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.runtime = requireNonNull(runtime, "runtime is null");
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments,
            ConnectorAccessControl accessControl)
    {
        String schemaName = stringArgument(arguments, SCHEMA_ARGUMENT_NAME);
        String tableName = stringArgument(arguments, TABLE_ARGUMENT_NAME);
        String query = stringArgument(arguments, QUERY_ARGUMENT_NAME);
        String options = ((ScalarArgument) arguments.get(OPTIONS_ARGUMENT_NAME)).getValue() == null
                ? null
                : stringArgument(arguments, OPTIONS_ARGUMENT_NAME);

        DescriptorArgument columnsArgument = (DescriptorArgument) arguments.get(COLUMNS_ARGUMENT_NAME);
        if (columnsArgument.equals(NULL_DESCRIPTOR)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "COLUMNS descriptor is null");
        }
        List<Descriptor.Field> requestedFields = columnsArgument.getDescriptor().orElseThrow().getFields();
        if (requestedFields.stream().anyMatch(field -> field.getType().isPresent())) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "COLUMNS descriptor contains types");
        }

        LanceTableHandle tableHandle = metadata.getTableHandle(session, new SchemaTableName(schemaName, tableName), Optional.empty(), Optional.empty());
        if (tableHandle == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Table '%s.%s' does not exist", schemaName, tableName));
        }
        Map<String, ColumnHandle> tableColumns = metadata.getColumnHandles(session, tableHandle);
        if (tableColumns.containsKey(SCORE_COLUMN)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Table already has a '_score' column");
        }

        Set<Integer> ftsFieldIds = ftsFieldIds(session, tableHandle);
        ImmutableList.Builder<String> matchColumns = ImmutableList.builder();
        Set<String> seen = new HashSet<>();
        for (Descriptor.Field field : requestedFields) {
            String requested = field.getName().orElseThrow().toLowerCase(ENGLISH);
            if (!seen.add(requested)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Duplicate column: " + requested);
            }
            LanceColumnHandle column = tableColumns.values().stream()
                    .map(LanceColumnHandle.class::cast)
                    .filter(handle -> handle.name().toLowerCase(ENGLISH).equals(requested))
                    .findFirst()
                    .orElseThrow(() -> new TrinoException(INVALID_FUNCTION_ARGUMENT, "Column not found: " + requested));
            if (!(column.trinoType() instanceof VarcharType)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Column is not a varchar: " + column.name());
            }
            if (!ftsFieldIds.contains(column.fieldId())) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Column has no FTS index: " + column.name());
            }
            matchColumns.add(column.name());
        }
        List<String> columns = matchColumns.build();
        if (columns.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "COLUMNS descriptor is empty");
        }
        LanceFullTextQuery fullTextQuery = LanceFullTextQuery.parse(columns, query, options);

        ImmutableList.Builder<Descriptor.Field> returnedFields = ImmutableList.builder();
        for (ColumnHandle handle : tableColumns.values()) {
            LanceColumnHandle column = (LanceColumnHandle) handle;
            returnedFields.add(new Descriptor.Field(column.name(), Optional.of(column.trinoType())));
        }
        returnedFields.add(new Descriptor.Field(SCORE_COLUMN, Optional.of(REAL)));

        return TableFunctionAnalysis.builder()
                .returnedType(new Descriptor(returnedFields.build()))
                .handle(new LanceFullTextSearchFunctionHandle(tableHandle, fullTextQuery))
                .build();
    }

    private Set<Integer> ftsFieldIds(ConnectorSession session, LanceTableHandle tableHandle)
    {
        Set<Integer> result = new HashSet<>();
        try (Dataset dataset = runtime.openDatasetDirect(session.getUser(), tableHandle.getEffectiveReadUri(), tableHandle.getDatasetVersion(), tableHandle.getStorageOptions())) {
            for (IndexDescription index : dataset.describeIndices()) {
                if (index.getIndexType().equalsIgnoreCase("INVERTED")) {
                    result.addAll(index.getFieldIds());
                }
            }
        }
        return result;
    }

    private static String stringArgument(Map<String, Argument> arguments, String name)
    {
        Object value = ((ScalarArgument) arguments.get(name)).getValue();
        if (value == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, name + " must not be null");
        }
        return ((Slice) value).toStringUtf8();
    }

    public record LanceFullTextSearchFunctionHandle(LanceTableHandle table, LanceFullTextQuery query)
            implements ConnectorTableFunctionHandle
    {
        public LanceFullTextSearchFunctionHandle
        {
            requireNonNull(table, "table is null");
            requireNonNull(query, "query is null");
        }
    }
}
