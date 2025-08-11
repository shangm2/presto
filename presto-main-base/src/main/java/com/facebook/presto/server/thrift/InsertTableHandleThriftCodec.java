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
package com.facebook.presto.server.thrift;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.drift.buffer.ByteBufferPool;
import com.facebook.drift.codec.CodecThriftType;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.presto.connector.ConnectorCodecManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorInsertTableHandle;

import javax.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.server.thrift.ThriftCodecUtils.deserializeFromBufferList;
import static com.facebook.presto.server.thrift.ThriftCodecUtils.serializeToBufferList;
import static java.util.Objects.requireNonNull;

public class InsertTableHandleThriftCodec
        extends AbstractTypedThriftCodec<ConnectorInsertTableHandle>
{
    private static final ThriftType THRIFT_TYPE = createThriftType(ConnectorInsertTableHandle.class);
    private final ConnectorCodecManager connectorCodecManager;
    private final ByteBufferPool pool;

    @Inject
    public InsertTableHandleThriftCodec(HandleResolver handleResolver,
            ConnectorCodecManager connectorCodecManager,
            JsonCodec<ConnectorInsertTableHandle> jsonCodec,
            @ForPooledByteBuffer ByteBufferPool pool)
    {
        super(ConnectorInsertTableHandle.class,
                requireNonNull(jsonCodec, "jsonCodec is null"),
                requireNonNull(handleResolver, "handleResolver is null")::getId,
                handleResolver::getInsertTableHandleClass);
        this.connectorCodecManager = requireNonNull(connectorCodecManager, "connectorThriftCodecManager is null");
        this.pool = requireNonNull(pool, "pool is null");
    }

    @CodecThriftType
    public static ThriftType getThriftType()
    {
        return THRIFT_TYPE;
    }

    @Override
    public ThriftType getType()
    {
        return THRIFT_TYPE;
    }

    @Override
    public ConnectorInsertTableHandle readConcreteValue(String connectorId, TProtocolReader reader)
            throws Exception
    {
        Optional<ConnectorCodec<ConnectorInsertTableHandle>> codec = connectorCodecManager.getInsertTableHandleCodec(connectorId);
        if (!codec.isPresent()) {
            return null;
        }

        return deserializeFromBufferList(codec.get(), reader, pool);
    }

    @Override
    public void writeConcreteValue(String connectorId, ConnectorInsertTableHandle value, TProtocolWriter writer)
            throws Exception
    {
        requireNonNull(value, "value is null");
        Optional<ConnectorCodec<ConnectorInsertTableHandle>> codec = connectorCodecManager.getInsertTableHandleCodec(connectorId);
        if (!codec.isPresent()) {
            return;
        }

        serializeToBufferList(codec.get(), value, writer);
    }

    @Override
    public boolean isThriftCodecAvailable(String connectorId)
    {
        return connectorCodecManager.getInsertTableHandleCodec(connectorId).isPresent();
    }
}
