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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Configuration for different Lance namespace test scenarios.
 * Supports directory namespace (local and S3) and REST namespace configurations.
 */
public enum LanceNamespaceTestConfig
{
    /**
     * Directory namespace with single_level_ns=true (1st level access, virtual "default" schema).
     * CREATE SCHEMA is not allowed in this mode.
     */
    DIRECTORY_SINGLE_LEVEL("dir", true, Optional.empty()),

    /**
     * Directory namespace with parent prefix (3+ level access).
     * Full schema support with parent namespace prefix.
     * Parent namespaces p1 and p2 must be created before running tests.
     */
    DIRECTORY_WITH_PARENT("dir", false, Optional.of("p1$p2")),

    /**
     * Directory namespace default mode (2nd level access).
     * Full schema support with direct schema-to-namespace mapping.
     */
    DIRECTORY_DEFAULT("dir", false, Optional.empty()),

    /**
     * REST namespace with single_level_ns=true backend.
     */
    REST_SINGLE_LEVEL("rest", true, Optional.empty()),

    /**
     * REST namespace with parent prefix backend.
     * Parent namespaces p1 and p2 must be created before running tests.
     */
    REST_WITH_PARENT("rest", false, Optional.of("p1$p2")),

    /**
     * REST namespace default mode backend.
     */
    REST_DEFAULT("rest", false, Optional.empty()),

    /**
     * S3 directory namespace with single_level_ns=true.
     */
    S3_SINGLE_LEVEL("dir", true, Optional.empty()),

    /**
     * S3 directory namespace with parent prefix.
     * Parent namespaces p1 and p2 must be created before running tests.
     */
    S3_WITH_PARENT("dir", false, Optional.of("p1$p2")),

    /**
     * S3 directory namespace default mode.
     */
    S3_DEFAULT("dir", false, Optional.empty());

    private final String impl;
    private final boolean singleLevelNs;
    private final Optional<String> parent;

    LanceNamespaceTestConfig(String impl, boolean singleLevelNs, Optional<String> parent)
    {
        this.impl = impl;
        this.singleLevelNs = singleLevelNs;
        this.parent = parent;
    }

    public String getImpl()
    {
        return impl;
    }

    public boolean isSingleLevelNs()
    {
        return singleLevelNs;
    }

    public Optional<String> getParent()
    {
        return parent;
    }

    public boolean isS3Config()
    {
        return this.name().startsWith("S3_");
    }

    public boolean isRestConfig()
    {
        return this.name().startsWith("REST_");
    }

    public boolean isDirectoryConfig()
    {
        return this.name().startsWith("DIRECTORY_");
    }

    public boolean supportsCreateSchema()
    {
        return !singleLevelNs;
    }

    public boolean hasParent()
    {
        return parent.isPresent();
    }

    /**
     * Get the parent namespace levels as a list.
     * For example, "p1$p2" returns ["p1", "p2"].
     *
     * @return list of parent namespace levels, or empty list if no parent
     */
    public List<String> getParentLevels()
    {
        return parent
                .map(p -> Arrays.asList(p.split("\\$")))
                .orElse(Collections.emptyList());
    }

    /**
     * Build connector properties for this configuration.
     *
     * @param rootPath the root path for the namespace (local path or S3 URI)
     * @return map of connector properties
     */
    public Map<String, String> buildConnectorProperties(String rootPath)
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("lance.impl", impl);
        properties.put("lance.root", rootPath);

        if (singleLevelNs) {
            properties.put("lance.single_level_ns", "true");
        }

        parent.ifPresent(p -> properties.put("lance.parent", p));

        return properties;
    }

    /**
     * Build connector properties for REST namespace.
     *
     * @param restUri the REST server URI
     * @return map of connector properties
     */
    public Map<String, String> buildRestConnectorProperties(String restUri)
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("lance.impl", "rest");
        properties.put("lance.uri", restUri);

        if (singleLevelNs) {
            properties.put("lance.single_level_ns", "true");
        }

        parent.ifPresent(p -> properties.put("lance.parent", p));

        return properties;
    }

    /**
     * Build S3 storage options for LocalStack.
     *
     * @param s3Endpoint the S3 endpoint URL
     * @param bucketName the S3 bucket name
     * @return map of connector properties with S3 configuration
     */
    public Map<String, String> buildS3ConnectorProperties(String s3Endpoint, String bucketName)
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("lance.impl", impl);
        properties.put("lance.root", "s3://" + bucketName + "/lance-test");

        // S3 storage options
        properties.put("lance.storage.allow_http", "true");
        properties.put("lance.storage.aws_access_key_id", "ACCESSKEY");
        properties.put("lance.storage.aws_secret_access_key", "SECRETKEY");
        properties.put("lance.storage.aws_endpoint", s3Endpoint);
        properties.put("lance.storage.aws_region", "us-east-1");

        if (singleLevelNs) {
            properties.put("lance.single_level_ns", "true");
        }

        parent.ifPresent(p -> properties.put("lance.parent", p));

        return properties;
    }

    /**
     * Default LocalStack endpoint for local testing.
     */
    public static final String DEFAULT_LOCALSTACK_ENDPOINT = "http://localhost:4566";

    /**
     * Default bucket name for S3 tests.
     */
    public static final String DEFAULT_S3_BUCKET = "lance-test-bucket";

    /**
     * Build S3 storage options for local LocalStack (docker-compose).
     * Uses default endpoint (http://localhost:4566) and bucket (lance-test-bucket).
     * Each call generates a unique prefix to avoid interference between parallel test classes.
     *
     * @return map of connector properties with S3 configuration
     */
    public Map<String, String> buildLocalS3ConnectorProperties()
    {
        // Generate unique prefix per call to avoid conflicts between parallel test classes
        String uniquePrefix = UUID.randomUUID().toString().substring(0, 8);
        return buildS3ConnectorPropertiesWithPrefix(DEFAULT_LOCALSTACK_ENDPOINT, DEFAULT_S3_BUCKET, uniquePrefix);
    }

    /**
     * Build S3 storage options with a unique prefix per test run.
     *
     * @param s3Endpoint the S3 endpoint URL
     * @param bucketName the S3 bucket name
     * @param runPrefix unique prefix for this test run
     * @return map of connector properties with S3 configuration
     */
    public Map<String, String> buildS3ConnectorPropertiesWithPrefix(String s3Endpoint, String bucketName, String runPrefix)
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("lance.impl", impl);
        properties.put("lance.root", "s3://" + bucketName + "/lance-test-" + runPrefix);

        // S3 storage options
        properties.put("lance.storage.allow_http", "true");
        properties.put("lance.storage.aws_access_key_id", "ACCESSKEY");
        properties.put("lance.storage.aws_secret_access_key", "SECRETKEY");
        properties.put("lance.storage.aws_endpoint", s3Endpoint);
        properties.put("lance.storage.aws_region", "us-east-1");

        if (singleLevelNs) {
            properties.put("lance.single_level_ns", "true");
        }

        parent.ifPresent(p -> properties.put("lance.parent", p));

        return properties;
    }
}
