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
import io.trino.plugin.lance.internal.LanceReader;
import io.trino.plugin.lance.internal.ScannerFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.lance.Fragment;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class LanceFragmentPageSource
        extends LanceBasePageSource
{
    private static final Logger log = Logger.get(LanceFragmentPageSource.class);

    public LanceFragmentPageSource(LanceReader lanceReader, LanceTableHandle tableHandle, List<LanceColumnHandle> columns, List<Integer> fragments, int maxReadRowsRetries)
    {
        super(lanceReader, tableHandle, columns, maxReadRowsRetries, createScannerFactory(fragments));
    }

    private static ScannerFactory createScannerFactory(List<Integer> fragments)
    {
        checkState(fragments.size() == 1, "only one fragment is allowed, found: " + fragments.size());
        return new FragmentScannerFactory(fragments.getFirst());
    }

    public static class FragmentScannerFactory
            implements ScannerFactory
    {
        private final int fragmentId;
        private Fragment lanceFragment;
        private LanceScanner lanceScanner;

        public FragmentScannerFactory(int fragmentId)
        {
            this.fragmentId = fragmentId;
        }

        @Override
        public LanceScanner open(String tablePath, BufferAllocator allocator, List<String> columns)
        {
            // Use cached fragment lookup instead of opening dataset and filtering
            this.lanceFragment = LanceReader.getFragment(tablePath, this.fragmentId);
            if (this.lanceFragment == null) {
                throw new RuntimeException("Fragment not found: " + this.fragmentId);
            }
            ScanOptions.Builder optionsBuilder = new ScanOptions.Builder();
            // Only set columns if non-empty; empty list means read all columns
            if (!columns.isEmpty()) {
                optionsBuilder.columns(columns);
            }
            this.lanceScanner = lanceFragment.newScan(optionsBuilder.build());
            return lanceScanner;
        }

        @Override
        public void close()
        {
            // Only close the scanner; the dataset is managed by LanceReader's cache
            try {
                if (lanceScanner != null) {
                    lanceScanner.close();
                }
            }
            catch (Exception e) {
                log.warn("error while closing lance scanner, Exception: %s", e.getMessage());
            }
        }
    }
}
