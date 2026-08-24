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

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Procedure handle for {@code ALTER TABLE ... EXECUTE optimize_indices(...)}.
 * Incrementally indexes fragments that were appended since an index was last built or optimized,
 * without a full rebuild. {@code indexNames} empty means all indices on the table.
 */
public record LanceOptimizeIndicesHandle(List<String> indexNames, Optional<Integer> numIndicesToMerge, boolean retrain)
        implements LanceProcedureHandle
{
    public LanceOptimizeIndicesHandle
    {
        indexNames = List.copyOf(requireNonNull(indexNames, "indexNames is null"));
        requireNonNull(numIndicesToMerge, "numIndicesToMerge is null");
    }
}
