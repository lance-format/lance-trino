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

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.lance.LanceMetadata;
import io.trino.plugin.lance.LanceRuntime;
import io.trino.spi.function.table.ConnectorTableFunction;

import static java.util.Objects.requireNonNull;

public class LanceFullTextSearchFunctionProvider
        implements Provider<ConnectorTableFunction>
{
    private final LanceMetadata metadata;
    private final LanceRuntime runtime;

    @Inject
    public LanceFullTextSearchFunctionProvider(LanceMetadata metadata, LanceRuntime runtime)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.runtime = requireNonNull(runtime, "runtime is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new LanceFullTextSearchFunction(metadata, runtime), getClass().getClassLoader());
    }
}
