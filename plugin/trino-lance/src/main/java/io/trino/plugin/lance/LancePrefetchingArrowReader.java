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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A prefetching wrapper around ArrowReader that reads batches in a background thread.
 *
 * <p>This enables pipelining between I/O (loading Arrow batches from Lance/S3) and
 * CPU (converting Arrow to Trino Pages). The background thread continuously prefetches
 * batches into a bounded queue, while the main thread consumes from the queue.
 *
 * <p>Architecture:
 * <pre>
 * Background Thread (Producer):        Main Thread (Consumer):
 * - Calls underlying.loadNextBatch()   - Takes batches from queue
 * - Transfers vectors to queue batch   - Processes batches (Arrow → Page)
 * - Puts batch in queue                - Only blocks if queue is empty
 * - Only blocks if queue is full
 * </pre>
 */
public class LancePrefetchingArrowReader
        implements AutoCloseable
{
    private static final Logger log = Logger.get(LancePrefetchingArrowReader.class);

    /** Default queue depth - number of batches that can be prefetched. */
    private static final int DEFAULT_QUEUE_DEPTH = 4;

    /** Sentinel batch to signal end of stream. */
    private static final VectorSchemaRoot END_OF_STREAM = null;

    private final ArrowReader underlying;
    private final BufferAllocator allocator;
    private final int queueDepth;

    /** Queue holding prefetched batches ready for consumption. */
    private final BlockingQueue<VectorSchemaRoot> batchQueue;

    /** Background prefetch executor. */
    private final ExecutorService prefetchExecutor;

    /** Current batch being consumed. */
    private VectorSchemaRoot currentBatch;

    /** Flag indicating prefetch thread has finished (either completed or error). */
    private final AtomicBoolean prefetchFinished = new AtomicBoolean(false);

    /** Error from prefetch thread, if any. */
    private final AtomicReference<Throwable> prefetchError = new AtomicReference<>();

    /** Flag indicating consumer has finished. */
    private volatile boolean consumerFinished;

    /** Total bytes read from Arrow buffers. */
    private volatile long bytesRead;

    public LancePrefetchingArrowReader(ArrowReader underlying, BufferAllocator allocator)
    {
        this(underlying, allocator, DEFAULT_QUEUE_DEPTH);
    }

    public LancePrefetchingArrowReader(ArrowReader underlying, BufferAllocator allocator, int queueDepth)
    {
        this.underlying = underlying;
        this.allocator = allocator;
        this.queueDepth = queueDepth;
        this.batchQueue = new ArrayBlockingQueue<>(queueDepth);

        // Start background prefetch thread
        this.prefetchExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "lance-prefetch");
            t.setDaemon(true);
            return t;
        });
        this.prefetchExecutor.submit(this::prefetchLoop);
    }

    /**
     * Background loop that continuously prefetches batches from the underlying reader.
     */
    private void prefetchLoop()
    {
        try {
            VectorSchemaRoot underlyingRoot = underlying.getVectorSchemaRoot();

            while (!Thread.currentThread().isInterrupted() && !consumerFinished) {
                if (!underlying.loadNextBatch()) {
                    // No more batches
                    break;
                }

                // Transfer vectors to a new root (so underlying can reuse its buffers)
                VectorSchemaRoot transferredBatch = transferBatch(underlyingRoot);

                // Put in queue (blocks if full)
                while (!consumerFinished) {
                    if (batchQueue.offer(transferredBatch, 100, TimeUnit.MILLISECONDS)) {
                        break;
                    }
                }

                if (consumerFinished) {
                    // Consumer stopped, close the transferred batch
                    transferredBatch.close();
                    break;
                }
            }
        }
        catch (Throwable e) {
            prefetchError.set(e);
            log.warn(e, "Error in prefetch thread");
        }
        finally {
            prefetchFinished.set(true);
        }
    }

    /**
     * Transfer vectors from the underlying root to a new root.
     * This allows the underlying reader to reuse its buffers for the next batch.
     */
    private VectorSchemaRoot transferBatch(VectorSchemaRoot source)
    {
        List<FieldVector> transferredVectors = new ArrayList<>(source.getFieldVectors().size());
        for (FieldVector sourceVector : source.getFieldVectors()) {
            TransferPair transfer = sourceVector.getTransferPair(allocator);
            transfer.transfer();
            transferredVectors.add((FieldVector) transfer.getTo());
        }
        return new VectorSchemaRoot(transferredVectors);
    }

    /**
     * Load the next batch from the prefetch queue.
     *
     * @return true if a batch was loaded, false if no more batches
     */
    public boolean loadNextBatch()
            throws IOException
    {
        if (consumerFinished) {
            return false;
        }

        // Close previous batch
        if (currentBatch != null) {
            currentBatch.close();
            currentBatch = null;
        }

        try {
            while (true) {
                // Try to get next batch from queue with timeout
                VectorSchemaRoot batch = batchQueue.poll(100, TimeUnit.MILLISECONDS);

                if (batch != null) {
                    currentBatch = batch;
                    // Track bytes read
                    bytesRead += calculateBatchBytes(batch);
                    return true;
                }

                // Check for prefetch error
                Throwable error = prefetchError.get();
                if (error != null) {
                    throw new IOException("Error in prefetch thread", error);
                }

                // Check if prefetch is done and queue is empty
                if (prefetchFinished.get() && batchQueue.isEmpty()) {
                    consumerFinished = true;
                    return false;
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for batch", e);
        }
    }

    /**
     * Get the current batch (after loadNextBatch returns true).
     */
    public VectorSchemaRoot getVectorSchemaRoot()
    {
        return currentBatch;
    }

    /**
     * Get total bytes read from Arrow buffers.
     */
    public long getBytesRead()
    {
        return bytesRead;
    }

    private long calculateBatchBytes(VectorSchemaRoot batch)
    {
        long bytes = 0;
        for (FieldVector vector : batch.getFieldVectors()) {
            bytes += getVectorBytes(vector);
        }
        return bytes;
    }

    private long getVectorBytes(FieldVector vector)
    {
        long bytes = 0;
        for (var buffer : vector.getBuffers(false)) {
            bytes += buffer.capacity();
        }
        // Handle nested vectors
        for (FieldVector child : vector.getChildrenFromFields()) {
            bytes += getVectorBytes(child);
        }
        return bytes;
    }

    @Override
    public void close()
            throws IOException
    {
        consumerFinished = true;

        // Shutdown prefetch executor
        prefetchExecutor.shutdownNow();
        try {
            prefetchExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Close current batch
        if (currentBatch != null) {
            currentBatch.close();
            currentBatch = null;
        }

        // Drain and close any batches left in queue
        VectorSchemaRoot batch;
        while ((batch = batchQueue.poll()) != null) {
            batch.close();
        }

        // Close underlying reader
        underlying.close();
    }
}
