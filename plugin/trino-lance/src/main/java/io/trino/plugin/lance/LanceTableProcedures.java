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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.TableProcedureMetadata;

import java.util.Set;

import static io.trino.spi.connector.TableProcedureExecutionMode.coordinatorOnly;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

/**
 * Table procedures exposed via {@code ALTER TABLE ... EXECUTE}. All procedures here build or
 * maintain Lance indexes directly through {@code org.lance.Dataset} (index build, incremental
 * optimize, compaction) and never read or write table data through Trino's split/page pipeline,
 * so they all run coordinator-only.
 */
public final class LanceTableProcedures
{
    // CREATE_INDEX properties
    public static final String COLUMN = "column";
    public static final String INDEX_NAME = "index_name";
    public static final String INDEX_TYPE = "index_type";
    public static final String REPLACE = "replace";
    public static final String TRAIN = "train";
    public static final String BASE_TOKENIZER = "base_tokenizer";
    public static final String LANGUAGE = "language";
    public static final String WITH_POSITION = "with_position";
    public static final String LOWER_CASE = "lower_case";
    public static final String STEM = "stem";
    public static final String REMOVE_STOP_WORDS = "remove_stop_words";
    public static final String ASCII_FOLDING = "ascii_folding";
    public static final String MAX_TOKEN_LENGTH = "max_token_length";

    // OPTIMIZE_INDICES properties
    public static final String INDEX_NAMES = "index_names";
    public static final String NUM_INDICES_TO_MERGE = "num_indices_to_merge";
    public static final String RETRAIN = "retrain";

    // COMPACT properties
    public static final String DEFER_INDEX_REMAP = "defer_index_remap";

    private LanceTableProcedures() {}

    public static Set<TableProcedureMetadata> getTableProcedures()
    {
        return ImmutableSet.of(createIndex(), optimizeIndices(), compact());
    }

    private static TableProcedureMetadata createIndex()
    {
        return new TableProcedureMetadata(
                LanceTableProcedureId.CREATE_INDEX.name(),
                coordinatorOnly(),
                ImmutableList.of(
                        stringProperty(COLUMN, "Column to build the index on", null, false),
                        stringProperty(INDEX_NAME, "Name for the index; auto-generated if not provided", null, false),
                        stringProperty(INDEX_TYPE, "Index type to create; currently only 'fts' is supported", "fts", false),
                        booleanProperty(REPLACE, "Replace an existing index with the same name", false, false),
                        booleanProperty(TRAIN, "Train the index on existing data now; if false, registers an empty index to populate later via optimize_indices", true, false),
                        stringProperty(BASE_TOKENIZER, "FTS tokenizer: simple, whitespace, raw, ngram, icu, icu/split, lindera/*, jieba/*", "simple", false),
                        stringProperty(LANGUAGE, "Language used for stemming and stop words", "English", false),
                        booleanProperty(WITH_POSITION, "Store token positions to support phrase queries", false, false),
                        booleanProperty(LOWER_CASE, "Lower-case tokens", true, false),
                        booleanProperty(STEM, "Apply stemming", false, false),
                        booleanProperty(REMOVE_STOP_WORDS, "Remove stop words", false, false),
                        booleanProperty(ASCII_FOLDING, "Apply ASCII folding", false, false),
                        integerProperty(MAX_TOKEN_LENGTH, "Maximum token length", null, false)));
    }

    private static TableProcedureMetadata optimizeIndices()
    {
        return new TableProcedureMetadata(
                LanceTableProcedureId.OPTIMIZE_INDICES.name(),
                coordinatorOnly(),
                ImmutableList.of(
                        stringProperty(INDEX_NAMES, "Comma-separated index names to optimize; all indices on the table if not specified", null, false),
                        integerProperty(NUM_INDICES_TO_MERGE, "Number of index segments to merge while optimizing", null, false),
                        booleanProperty(RETRAIN, "Retrain the index from scratch instead of incrementally indexing newly appended fragments", false, false)));
    }

    private static TableProcedureMetadata compact()
    {
        return new TableProcedureMetadata(
                LanceTableProcedureId.COMPACT.name(),
                coordinatorOnly(),
                ImmutableList.of(
                        booleanProperty(
                                DEFER_INDEX_REMAP,
                                "Defer remapping row addresses in indices to next index load, so compaction does not conflict with concurrent index builds",
                                null,
                                false)));
    }
}
