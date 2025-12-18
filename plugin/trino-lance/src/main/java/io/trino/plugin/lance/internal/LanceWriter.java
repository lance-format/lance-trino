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
package io.trino.plugin.lance.internal;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.lance.LanceConfig;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.WriteParams;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Encapsulates Lance write operations.
 * This class wraps the Lance Java API for creating datasets and committing fragments.
 */
public class LanceWriter
        implements Closeable
{
    private static final Logger log = Logger.get(LanceWriter.class);

    private static final BufferAllocator allocator = new RootAllocator(
            RootAllocator.configBuilder().from(RootAllocator.defaultConfig()).maxAllocation(Long.MAX_VALUE).build());

    private final LanceConfig config;

    @Inject
    public LanceWriter(LanceConfig config)
    {
        this.config = config;
    }

    /**
     * Create an empty dataset with the given schema.
     */
    public void createEmptyDataset(String datasetUri, Schema schema, WriteParams params)
    {
        log.debug("Creating empty dataset at: %s", datasetUri);
        Dataset.create(allocator, datasetUri, schema, params).close();
    }

    /**
     * Drop a dataset by deleting its directory.
     */
    public void dropDataset(String datasetUri)
    {
        log.debug("Dropping dataset at: %s", datasetUri);
        try {
            Path datasetPath;
            if (datasetUri.startsWith("file:")) {
                datasetPath = Path.of(URI.create(datasetUri));
            }
            else {
                datasetPath = Path.of(datasetUri);
            }

            if (Files.exists(datasetPath)) {
                // Delete directory recursively
                Files.walk(datasetPath)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to drop dataset: " + datasetUri, e);
        }
    }

    /**
     * Create fragments from an ArrowReader.
     * Returns a list of serialized fragment metadata strings.
     */
    public List<String> createFragments(String datasetUri, ArrowReader reader, WriteParams params)
    {
        log.debug("Creating fragments for dataset: %s", datasetUri);
        try (ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator)) {
            Data.exportArrayStream(allocator, reader, arrowStream);
            List<FragmentMetadata> fragments = Fragment.create(datasetUri, arrowStream, params);
            return serializeFragments(fragments);
        }
    }

    /**
     * Commit fragments to an existing dataset using append operation.
     */
    public void commitAppend(String datasetUri, List<String> serializedFragments, Map<String, String> storageOptions)
    {
        log.debug("Committing %d fragments to dataset: %s (append)", serializedFragments.size(), datasetUri);
        List<FragmentMetadata> fragments = deserializeFragments(serializedFragments);

        FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments);
        try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
            Dataset.commit(
                    allocator,
                    datasetUri,
                    appendOp,
                    Optional.of(dataset.version()),
                    storageOptions).close();
        }
    }

    /**
     * Commit fragments with overwrite operation (for CREATE TABLE AS SELECT or INSERT OVERWRITE).
     */
    public void commitOverwrite(String datasetUri, List<String> serializedFragments, Schema schema, Map<String, String> storageOptions)
    {
        log.debug("Committing %d fragments to dataset: %s (overwrite)", serializedFragments.size(), datasetUri);
        List<FragmentMetadata> fragments = deserializeFragments(serializedFragments);

        FragmentOperation.Overwrite overwriteOp = new FragmentOperation.Overwrite(fragments, schema);
        try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
            Dataset.commit(
                    allocator,
                    datasetUri,
                    overwriteOp,
                    Optional.of(dataset.version()),
                    storageOptions).close();
        }
    }

    /**
     * Create a new dataset and commit initial fragments.
     * Used for CREATE TABLE AS SELECT when table doesn't exist.
     */
    public void createDatasetWithFragments(String datasetUri, List<String> serializedFragments, Schema schema, WriteParams params, Map<String, String> storageOptions)
    {
        log.debug("Creating dataset with %d fragments at: %s", serializedFragments.size(), datasetUri);
        List<FragmentMetadata> fragments = deserializeFragments(serializedFragments);

        // Create empty dataset first
        Dataset.create(allocator, datasetUri, schema, params).close();

        // Then commit fragments with overwrite
        FragmentOperation.Overwrite overwriteOp = new FragmentOperation.Overwrite(fragments, schema);
        try (Dataset dataset = Dataset.open(datasetUri, allocator)) {
            Dataset.commit(
                    allocator,
                    datasetUri,
                    overwriteOp,
                    Optional.of(dataset.version()),
                    storageOptions).close();
        }
    }

    /**
     * Serialize FragmentMetadata list to Base64 strings for transport.
     */
    public static List<String> serializeFragments(List<FragmentMetadata> fragments)
    {
        List<String> result = new ArrayList<>();
        for (FragmentMetadata fragment : fragments) {
            result.add(serializeFragment(fragment));
        }
        return result;
    }

    /**
     * Serialize a single FragmentMetadata to Base64 string.
     */
    public static String serializeFragment(FragmentMetadata fragment)
    {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(fragment);
            oos.close();
            return Base64.getEncoder().encodeToString(baos.toByteArray());
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to serialize FragmentMetadata", e);
        }
    }

    /**
     * Deserialize Base64 strings back to FragmentMetadata list.
     */
    public static List<FragmentMetadata> deserializeFragments(List<String> serializedFragments)
    {
        List<FragmentMetadata> result = new ArrayList<>();
        for (String serialized : serializedFragments) {
            result.add(deserializeFragment(serialized));
        }
        return result;
    }

    /**
     * Deserialize a single Base64 string back to FragmentMetadata.
     */
    public static FragmentMetadata deserializeFragment(String serialized)
    {
        try {
            byte[] bytes = Base64.getDecoder().decode(serialized);
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return (FragmentMetadata) ois.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize FragmentMetadata", e);
        }
    }

    /**
     * Get the buffer allocator for creating Arrow structures.
     */
    public static BufferAllocator getAllocator()
    {
        return allocator;
    }

    /**
     * Create default WriteParams.
     */
    public WriteParams getDefaultWriteParams()
    {
        return new WriteParams.Builder().build();
    }

    @Override
    public void close()
    {
        // Nothing to close - allocator is static and shared
    }
}
