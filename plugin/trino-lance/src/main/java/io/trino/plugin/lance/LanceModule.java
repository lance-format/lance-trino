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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.trino.plugin.lance.ptf.LanceFullTextSearchFunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class LanceModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(LanceConfig.class);

        // Core components
        binder.bind(LanceRuntime.class).in(Scopes.SINGLETON);
        binder.bind(LanceMetadata.class).in(Scopes.SINGLETON);

        // Read components
        binder.bind(LanceSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(LancePageSourceProvider.class).in(Scopes.SINGLETON);

        // Write components
        binder.bind(LancePageSinkProvider.class).in(Scopes.SINGLETON);

        // JSON codecs for serialization
        jsonCodecBinder(binder).bindJsonCodec(LanceCommitTaskData.class);
        jsonCodecBinder(binder).bindJsonCodec(LanceMergeCommitData.class);
        jsonCodecBinder(binder).bindJsonCodec(FragmentDeletion.class);

        // Table functions
        Multibinder<ConnectorTableFunction> tableFunctions = newSetBinder(binder, ConnectorTableFunction.class);
        tableFunctions.addBinding().toProvider(LanceFullTextSearchFunctionProvider.class).in(Scopes.SINGLETON);

        // Connector
        binder.bind(LanceConnector.class).in(Scopes.SINGLETON);
    }
}
