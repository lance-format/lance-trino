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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Data structure for serializing merge commit information from workers to coordinator.
 * Contains both deletions (rows to delete) and new fragments (rows to insert).
 */
public record LanceMergeCommitData(
        @JsonProperty("deletions") List<FragmentDeletion> deletions,
        @JsonProperty("newFragmentsJson") List<String> newFragmentsJson,
        @JsonProperty("writtenBytes") long writtenBytes,
        @JsonProperty("rowCount") long rowCount)
{
    @JsonCreator
    public LanceMergeCommitData
    {
        requireNonNull(deletions, "deletions is null");
        requireNonNull(newFragmentsJson, "newFragmentsJson is null");
    }
}
