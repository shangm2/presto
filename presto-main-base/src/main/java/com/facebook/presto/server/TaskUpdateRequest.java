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
package com.facebook.presto.server;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.util.PrestoJsonObjectMapperUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_ABSENT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class TaskUpdateRequest
{
    private final SessionRepresentation session;
    // extraCredentials is stored separately from SessionRepresentation to avoid being leaked
    private final Map<String, String> extraCredentials;
    private final Optional<byte[]> fragment;
    private final List<TaskSource> sources;
    private final OutputBuffers outputIds;
    private final Optional<TableWriteInfo> tableWriteInfo;

    @JsonCreator
    public TaskUpdateRequest(
            @JsonProperty("session") SessionRepresentation session,
            @JsonProperty("extraCredentials") Map<String, String> extraCredentials,
            @JsonProperty("fragment") Optional<byte[]> fragment,
            @JsonProperty("sources") List<TaskSource> sources,
            @JsonProperty("outputIds") OutputBuffers outputIds,
            @JsonProperty("tableWriteInfo") Optional<TableWriteInfo> tableWriteInfo)
    {
        requireNonNull(session, "session is null");
        requireNonNull(extraCredentials, "credentials is null");
        requireNonNull(fragment, "fragment is null");
        requireNonNull(sources, "sources is null");
        requireNonNull(outputIds, "outputIds is null");
        requireNonNull(tableWriteInfo, "tableWriteInfo is null");

        this.session = session;
        this.extraCredentials = extraCredentials;
        this.fragment = fragment;
        this.sources = ImmutableList.copyOf(sources);
        this.outputIds = outputIds;
        this.tableWriteInfo = tableWriteInfo;
    }

    @ThriftConstructor
    public TaskUpdateRequest(SessionRepresentation session,
            Map<String, String> extraCredentials,
            Optional<byte[]> fragment,
            List<TaskSource> sources,
            OutputBuffers outputIds,
            byte[] tableWriteInfo)
    {
        this.session = requireNonNull(session, "session is null");
        this.extraCredentials = requireNonNull(extraCredentials, "credentials is null");
        this.fragment = requireNonNull(fragment, "fragment is null");
        this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
        this.outputIds = requireNonNull(outputIds, "outputIds is null");
        this.tableWriteInfo = Optional.ofNullable(tableWriteInfo).map(info -> (TableWriteInfo) PrestoJsonObjectMapperUtil.deserialize(info, TableWriteInfo.class));
    }

    @JsonProperty
    @ThriftField(1)
    public SessionRepresentation getSession()
    {
        return session;
    }

    @JsonProperty
    @ThriftField(2)
    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    @JsonInclude(NON_ABSENT)
    @JsonProperty
    @ThriftField(3)
    public Optional<byte[]> getFragment()
    {
        return fragment;
    }

    @JsonProperty
    @ThriftField(4)
    public List<TaskSource> getSources()
    {
        return sources;
    }

    @JsonProperty
    @ThriftField(5)
    public OutputBuffers getOutputIds()
    {
        return outputIds;
    }

    @JsonProperty
    public Optional<TableWriteInfo> getTableWriteInfo()
    {
        return tableWriteInfo;
    }

    @ThriftField(value = 6, name = "tableWriteInfo", requiredness = OPTIONAL)
    public byte[] getJsonTableWriteInfo()
    {
        return tableWriteInfo.map(PrestoJsonObjectMapperUtil::serialize).orElse(null);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("session", session)
                .add("extraCredentials", extraCredentials.keySet())
                .add("fragment", fragment)
                .add("sources", sources)
                .add("outputIds", outputIds)
                .toString();
    }
}
