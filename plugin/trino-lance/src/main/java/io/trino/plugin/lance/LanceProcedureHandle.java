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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Marker interface for procedure-specific {@link LanceTableExecuteHandle} payloads.
 * {@code LanceTableExecuteHandle} is serialized as part of the query plan sent from the
 * coordinator to itself for execution, so the concrete subtype needs explicit type info for
 * Jackson to deserialize it back correctly.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = LanceCreateIndexHandle.class, name = "create_index"),
        @JsonSubTypes.Type(value = LanceOptimizeIndicesHandle.class, name = "optimize_indices"),
        @JsonSubTypes.Type(value = LanceCompactHandle.class, name = "compact"),
})
public interface LanceProcedureHandle {}
