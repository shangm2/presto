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
package com.facebook.presto.split;

import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorSplit;
import com.facebook.presto.common.experimental.auto_gen.ThriftRemoteSplit;
import com.facebook.presto.execution.Location;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.List;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RemoteSplit
        implements ConnectorSplit
{
    private final Location location;
    private final TaskId remoteSourceTaskId;

    static {
        ThriftSerializationRegistry.registerSerializer(RemoteSplit.class, RemoteSplit::serialize);
        ThriftSerializationRegistry.registerDeserializer("REMOTE_SPLIT", RemoteSplit::deserialize);
    }

    public RemoteSplit(ThriftRemoteSplit thriftRemoteSplit)
    {
        this(new Location(thriftRemoteSplit.getLocation()), new TaskId(thriftRemoteSplit.getRemoteSourceTaskId()));
    }

    @JsonCreator
    public RemoteSplit(@JsonProperty("location") Location location, @JsonProperty("remoteSourceTaskId") TaskId remoteSourceTaskId)
    {
        this.location = requireNonNull(location, "location is null");
        this.remoteSourceTaskId = requireNonNull(remoteSourceTaskId, "remoteSourceTaskId is null");
    }

    @JsonProperty
    public Location getLocation()
    {
        return location;
    }

    @JsonProperty
    public TaskId getRemoteSourceTaskId()
    {
        return remoteSourceTaskId;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("location", location)
                .add("remoteSourceTaskId", remoteSourceTaskId)
                .toString();
    }

    @Override
    public ThriftConnectorSplit toThriftInterface()
    {
        try {
            TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            ThriftConnectorSplit thriftSplit = new ThriftConnectorSplit();
            thriftSplit.setType(getImplementationType());
            thriftSplit.setSerializedSplit(serializer.serialize(this.toThrift()));
            return thriftSplit;
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ThriftRemoteSplit toThrift()
    {
        return new ThriftRemoteSplit(
                location.toString(),
                remoteSourceTaskId.toThrift());
    }

    @Override
    public byte[] serialize()
    {
        try {
            TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            return serializer.serialize(this.toThriftInterface());
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static RemoteSplit deserialize(byte[] bytes)
    {
        try {
            ThriftRemoteSplit thriftSplit = new ThriftRemoteSplit();
            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            deserializer.deserialize(thriftSplit, bytes);
            return new RemoteSplit(thriftSplit);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getImplementationType()
    {
        return "REMOTE_SPLIT";
    }
}
