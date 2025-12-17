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

import com.google.common.io.Resources;
import io.airlift.log.Logger;
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * Add '--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED' to JVM options
 */
public final class LanceQueryRunner
{
    private LanceQueryRunner() {}

    public static Builder builder()
    {
        return new Builder()
                .addConnectorProperty("lance.root", Resources.getResource(LanceReader.class, "/example_db").toString());
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("lance")
                    .setSchema("default")
                    .build());
        }

        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new LancePlugin());
                queryRunner.createCatalog("lance", "lance", connectorProperties);

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    @SuppressWarnings("resource")
    public static void main(String[] args)
            throws Exception
    {
        QueryRunner queryRunner = builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .build();
        Logger log = Logger.get(LanceQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
