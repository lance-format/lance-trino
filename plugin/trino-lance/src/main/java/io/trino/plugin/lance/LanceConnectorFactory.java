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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class LanceConnectorFactory
        implements ConnectorFactory
{
    // Properties that are handled by LanceConfig via @Config annotations
    private static final Set<String> KNOWN_CONFIG_PROPERTIES = ImmutableSet.of(
            "lance.impl",
            "lance.connection-timeout",
            "lance.connection-retry-count",
            "lance.max-rows-per-file",
            "lance.max-rows-per-group",
            "lance.write-batch-size",
            "lance.single-level-ns",
            "lance.parent");

    private final Optional<Module> extension;

    public LanceConnectorFactory(Optional<Module> extension)
    {
        this.extension = requireNonNull(extension, "extension is null");
    }

    @Override
    public String getName()
    {
        return "lance";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");
        checkStrictSpiVersionMatch(context, this);

        // Make an immutable copy of all config for injection to LanceReader
        Map<String, String> catalogProperties = ImmutableMap.copyOf(config);

        // Filter to only known properties for the Bootstrap configuration
        // This allows free-form lance.* properties to be passed through without error
        Map<String, String> knownProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            if (KNOWN_CONFIG_PROPERTIES.contains(entry.getKey())) {
                knownProperties.put(entry.getKey(), entry.getValue());
            }
        }

        ImmutableList.Builder<Module> modulesBuilder =
                ImmutableList.<Module>builder().add(new JsonModule()).add(new MBeanModule())
                        .add(new MBeanServerModule()).add(new TypeDeserializerModule(context.getTypeManager()))
                        .add(new LanceModule())
                        // Bind the raw namespace properties map for free-form property access
                        .add(binder -> binder.bind(new TypeLiteral<Map<String, String>>() {})
                                .annotatedWith(LanceNamespaceProperties.class)
                                .toInstance(catalogProperties));
        // TODO: add auth module
        // .add(new AuthenticationModule());

        extension.ifPresent(modulesBuilder::add);

        Bootstrap app = new Bootstrap(modulesBuilder.build());

        // Use only the known properties for Bootstrap configuration binding
        Injector injector = app.doNotInitializeLogging().setRequiredConfigurationProperties(knownProperties).initialize();

        return injector.getInstance(LanceConnector.class);
    }
}
