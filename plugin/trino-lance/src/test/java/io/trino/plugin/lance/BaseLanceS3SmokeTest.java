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

import io.airlift.log.Logger;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

/**
 * Abstract base class for S3/LocalStack smoke tests.
 * Manages the LocalStack container lifecycle and S3 bucket setup.
 */
public abstract class BaseLanceS3SmokeTest
        extends BaseLanceConnectorSmokeTest
{
    private static final Logger log = Logger.get(BaseLanceS3SmokeTest.class);

    protected static final String BUCKET_NAME = "lance-smoke-test-bucket";
    protected static final String ACCESS_KEY = "ACCESSKEY";
    protected static final String SECRET_KEY = "SECRETKEY";

    protected static LocalStackContainer localStackContainer;
    protected static S3Client s3Client;

    /**
     * Initialize LocalStack if not already started.
     * Called from createQueryRunner() to ensure proper initialization order.
     */
    protected static synchronized void ensureLocalStackStarted()
    {
        if (localStackContainer != null && localStackContainer.isRunning()) {
            return;
        }

        localStackContainer = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.0"))
                .withServices(S3);
        localStackContainer.start();

        // Create S3 client
        s3Client = S3Client.builder()
                .endpointOverride(localStackContainer.getEndpointOverride(S3))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(Region.of(localStackContainer.getRegion()))
                .forcePathStyle(true)
                .build();

        // Create test bucket
        s3Client.createBucket(b -> b.bucket(BUCKET_NAME));
        log.info("LocalStack started with S3 endpoint: %s", getS3Endpoint());
    }

    @AfterAll
    public static void stopLocalStack()
    {
        if (s3Client != null) {
            s3Client.close();
            s3Client = null;
        }
        if (localStackContainer != null) {
            localStackContainer.stop();
            localStackContainer = null;
        }
    }

    protected static String getS3Endpoint()
    {
        return localStackContainer.getEndpointOverride(S3).toString();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Ensure LocalStack is started before creating QueryRunner
        ensureLocalStackStarted();

        return LanceQueryRunner.builderForS3(getNamespaceTestConfig(), getS3Endpoint(), BUCKET_NAME)
                .setInitialTables(NATION, REGION)
                .build();
    }
}
