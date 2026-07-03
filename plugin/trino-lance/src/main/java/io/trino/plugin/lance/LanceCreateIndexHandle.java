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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Procedure handle for {@code ALTER TABLE ... EXECUTE create_index(...)}.
 * Currently only the {@code fts} (inverted) index type is supported.
 */
public record LanceCreateIndexHandle(
        String column,
        Optional<String> indexName,
        String indexType,
        boolean replace,
        boolean train,
        String baseTokenizer,
        String language,
        boolean withPosition,
        boolean lowerCase,
        boolean stem,
        boolean removeStopWords,
        boolean asciiFolding,
        Optional<Integer> maxTokenLength)
        implements LanceProcedureHandle
{
    public LanceCreateIndexHandle
    {
        requireNonNull(column, "column is null");
        requireNonNull(indexName, "indexName is null");
        requireNonNull(indexType, "indexType is null");
        requireNonNull(baseTokenizer, "baseTokenizer is null");
        requireNonNull(language, "language is null");
        requireNonNull(maxTokenLength, "maxTokenLength is null");
    }
}
