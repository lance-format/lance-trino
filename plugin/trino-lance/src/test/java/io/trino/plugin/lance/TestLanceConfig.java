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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLanceConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LanceConfig.class)
                .setImpl("dir")
                .setRoot(null)
                .setUri(null)
                .setFetchRetryCount(5)
                .setConnectionTimeout(Duration.valueOf("1m")));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        // Test all properties together since assertFullMapping requires all properties
        // All values must be different from defaults
        String testRoot = "file://path/to/warehouse";
        String testUri = "https://api.lancedb.com";
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("lance.impl", "rest")
                .put("lance.root", testRoot)
                .put("lance.uri", testUri)
                .put("lance.connection-retry-count", "1")
                .put("lance.connection-timeout", "30s")
                .buildOrThrow();

        LanceConfig expected = new LanceConfig()
                .setImpl("rest")
                .setRoot(testRoot)
                .setUri(testUri)
                .setFetchRetryCount(1)
                .setConnectionTimeout(Duration.valueOf("30s"));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testNamespacePropertiesForDirectory()
    {
        LanceConfig config = new LanceConfig()
                .setImpl("dir")
                .setRoot("/path/to/warehouse");

        Map<String, String> props = config.getNamespaceProperties();
        assertThat(props).containsEntry("root", "/path/to/warehouse");
        assertThat(props).doesNotContainKey("uri");
    }

    @Test
    public void testNamespacePropertiesForRest()
    {
        LanceConfig config = new LanceConfig()
                .setImpl("rest")
                .setUri("https://api.lancedb.com");

        Map<String, String> props = config.getNamespaceProperties();
        assertThat(props).containsEntry("uri", "https://api.lancedb.com");
        assertThat(props).doesNotContainKey("root");
    }
}
