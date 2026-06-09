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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

public class TestArrowAllocationManager
{
    private static final String ARROW_ALLOCATION_MANAGER_TYPE = "arrow.allocation.manager.type";

    @Test
    public void testDirectRootAllocator()
    {
        try (BufferAllocator allocator = new RootAllocator();
                ArrowBuf buffer = allocator.buffer(Long.BYTES)) {
            buffer.setLong(0, 42);
            assertThat(buffer.getLong(0)).isEqualTo(42);
        }
    }

    // Exercises LanceRuntime.configureArrowAllocationManager() directly. It mutates the global
    // arrow.allocation.manager.type system property, so it locks SYSTEM_PROPERTIES to avoid racing
    // other property-sensitive tests.
    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    public void testConfigureArrowAllocationManager()
    {
        // The method is a no-op when the env var is set; skip if an operator override is present.
        assumeThat(System.getenv("ARROW_ALLOCATION_MANAGER_TYPE"))
                .as("operator override via environment variable")
                .isNull();

        // Force Arrow's allocation-manager factory to resolve now (it is read once, lazily), so
        // clearing the property below cannot affect a concurrently-created allocator.
        try (BufferAllocator allocator = new RootAllocator();
                ArrowBuf buffer = allocator.buffer(Long.BYTES)) {
            assertThat(buffer.capacity()).isGreaterThanOrEqualTo(Long.BYTES);
        }

        String original = System.getProperty(ARROW_ALLOCATION_MANAGER_TYPE);
        try {
            // When unset, the connector pins the Arrow "Unsafe" allocator because Arrow 19's Netty
            // allocator is incompatible with Netty 4.2 on Java 25.
            System.clearProperty(ARROW_ALLOCATION_MANAGER_TYPE);
            LanceRuntime.configureArrowAllocationManager();
            assertThat(System.getProperty(ARROW_ALLOCATION_MANAGER_TYPE)).isEqualTo("Unsafe");

            // An explicit override must be left untouched.
            System.setProperty(ARROW_ALLOCATION_MANAGER_TYPE, "Netty");
            LanceRuntime.configureArrowAllocationManager();
            assertThat(System.getProperty(ARROW_ALLOCATION_MANAGER_TYPE)).isEqualTo("Netty");
        }
        finally {
            if (original == null) {
                System.clearProperty(ARROW_ALLOCATION_MANAGER_TYPE);
            }
            else {
                System.setProperty(ARROW_ALLOCATION_MANAGER_TYPE, original);
            }
        }
    }
}
