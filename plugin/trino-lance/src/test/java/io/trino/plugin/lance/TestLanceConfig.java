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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestLanceConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LanceConfig.class)
                .setImpl("dir")
                .setMaxRowsPerFile(1_000_000)
                .setMaxRowsPerGroup(100_000)
                .setWriteBatchSize(10_000)
                .setSingleLevelNs(false)
                .setParent(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        // Test all properties together since assertFullMapping requires all properties
        // All values must be different from defaults
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("lance.impl", "rest")
                .put("lance.max_rows_per_file", "500000")
                .put("lance.max_rows_per_group", "50000")
                .put("lance.write_batch_size", "5000")
                .put("lance.single_level_ns", "true")
                .put("lance.parent", "p1$p2")
                .buildOrThrow();

        LanceConfig expected = new LanceConfig()
                .setImpl("rest")
                .setMaxRowsPerFile(500_000)
                .setMaxRowsPerGroup(50_000)
                .setWriteBatchSize(5_000)
                .setSingleLevelNs(true)
                .setParent("p1$p2");

        assertFullMapping(properties, expected);
    }
}
