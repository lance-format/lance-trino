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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class LanceSplit
        implements ConnectorSplit
{
    private static final Joiner JOINER = Joiner.on(",");
    private static final int INSTANCE_SIZE = instanceSize(LanceSplit.class);

    private final List<Integer> fragments;
    private final boolean allFragments;

    @JsonCreator
    public static LanceSplit fromJson(
            @JsonProperty("fragments") List<Integer> fragments,
            @JsonProperty("allFragments") Boolean allFragments)
    {
        if (Boolean.TRUE.equals(allFragments)) {
            checkArgument(fragments == null, "allFragments split cannot include explicit fragments");
            return allFragments();
        }
        return new LanceSplit(fragments);
    }

    public LanceSplit(List<Integer> fragments)
    {
        this(fragments, false);
    }

    private LanceSplit(List<Integer> fragments, boolean allFragments)
    {
        this.fragments = ImmutableList.copyOf(requireNonNull(fragments, "fragments is null"));
        this.allFragments = allFragments;
        checkArgument(!this.allFragments || this.fragments.isEmpty(), "allFragments split cannot include explicit fragments");
    }

    public static LanceSplit allFragments()
    {
        return new LanceSplit(List.of(), true);
    }

    @JsonIgnore
    public List<Integer> getFragments()
    {
        return fragments;
    }

    @JsonProperty("fragments")
    @JsonInclude(JsonInclude.Include.ALWAYS)
    public List<Integer> getJsonFragments()
    {
        if (allFragments) {
            return null;
        }
        return fragments;
    }

    @JsonProperty("allFragments")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isAllFragments()
    {
        return allFragments;
    }

    @JsonIgnore
    public Optional<List<Integer>> getFragmentIdsForScan()
    {
        if (allFragments) {
            return Optional.empty();
        }
        return Optional.of(fragments);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fragments", fragments)
                .add("allFragments", allFragments)
                .toString();
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        if (allFragments) {
            return ImmutableMap.of("fragments", "ALL");
        }
        return ImmutableMap.of("fragments", JOINER.join(fragments));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(fragments, e -> sizeOf(Integer.SIZE));
    }
}
