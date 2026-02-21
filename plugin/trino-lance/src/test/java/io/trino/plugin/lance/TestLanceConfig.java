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
                .setReadBatchSize(8192)
                .setMaxRowsPerFile(1_000_000)
                .setMaxRowsPerGroup(100_000)
                .setWriteBatchSize(10_000)
                .setSingleLevelNs(false)
                .setParent(null)
                .setBtreeIndexedRowsPerSplit(100_000_000L)
                .setBitmapIndexedRowsPerSplit(10_000_000L)
                .setCacheMaxSessions(100)
                .setCacheMaxDatasets(100)
                .setCacheSessionTtlMinutes(60)
                .setCacheDatasetTtlMinutes(30)
                .setSessionIndexCacheSizeBytes(null)
                .setSessionMetadataCacheSizeBytes(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        // Test all properties together since assertFullMapping requires all properties
        // All values must be different from defaults
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("lance.impl", "rest")
                .put("lance.read_batch_size", "4096")
                .put("lance.max_rows_per_file", "500000")
                .put("lance.max_rows_per_group", "50000")
                .put("lance.write_batch_size", "5000")
                .put("lance.single_level_ns", "true")
                .put("lance.parent", "p1$p2")
                .put("lance.index.btree.rows_per_split", "50000000")
                .put("lance.index.bitmap.rows_per_split", "5000000")
                .put("lance.cache.max_sessions", "200")
                .put("lance.cache.max_datasets", "200")
                .put("lance.cache.session_ttl_minutes", "120")
                .put("lance.cache.dataset_ttl_minutes", "60")
                .put("lance.session.index_cache_size_bytes", "268435456")
                .put("lance.session.metadata_cache_size_bytes", "268435456")
                .buildOrThrow();

        LanceConfig expected = new LanceConfig()
                .setImpl("rest")
                .setReadBatchSize(4096)
                .setMaxRowsPerFile(500_000)
                .setMaxRowsPerGroup(50_000)
                .setWriteBatchSize(5_000)
                .setSingleLevelNs(true)
                .setParent("p1$p2")
                .setBtreeIndexedRowsPerSplit(50_000_000L)
                .setBitmapIndexedRowsPerSplit(5_000_000L)
                .setCacheMaxSessions(200)
                .setCacheMaxDatasets(200)
                .setCacheSessionTtlMinutes(120)
                .setCacheDatasetTtlMinutes(60)
                .setSessionIndexCacheSizeBytes(268435456L)
                .setSessionMetadataCacheSizeBytes(268435456L);

        assertFullMapping(properties, expected);
    }
}
