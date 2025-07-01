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
import com.facebook.drift.TException;
import com.facebook.drift.buffer.ByteBufferPool;
import com.facebook.drift.codec.CodecThriftType;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.drift.protocol.bytebuffer.ForPooledByteBuffer;
import com.facebook.presto.connector.ConnectorCodecManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TransactionHandleThriftCodec
        extends AbstractTypedThriftCodec<ConnectorTransactionHandle>
{
    private static final ThriftType THRIFT_TYPE = createThriftType(ConnectorTransactionHandle.class);
    private final ConnectorCodecManager connectorCodecManager;
    private final ByteBufferPool pool;

    @Inject
    public TransactionHandleThriftCodec(HandleResolver handleResolver,
            ConnectorCodecManager connectorCodecManager,
            JsonCodec<ConnectorTransactionHandle> jsonCodec,
            @ForPooledByteBuffer ByteBufferPool pool)
    {
        super(ConnectorTransactionHandle.class,
                requireNonNull(jsonCodec, "jsonCodec is null"),
                requireNonNull(handleResolver, "handleResolver is null")::getId,
                handleResolver::getTransactionHandleClass);
        this.connectorCodecManager = requireNonNull(connectorCodecManager, "connectorThriftCodecManager is null");
        this.pool = requireNonNull(pool, "pool is null");
        System.out.println("=====> TransactionHandleThriftCodec, id: " + pool.getId());
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
    public ConnectorTransactionHandle readConcreteValue(String connectorId, TProtocolReader reader)
            throws Exception
    {
        List<ByteBufferPool.ReusableByteBuffer> byteBufferList = reader.readBinaryToBufferList(pool);

        if (byteBufferList.isEmpty()) {
            return null;
        }
        return connectorCodecManager.getTransactionHandleCodec(connectorId)
                .map(codec -> {
                    try {
                        return codec.deserialize(byteBufferList);
                    }
                    catch (Exception e) {
                        throw new IllegalStateException("Failed to deserialize connector transaction handle", e);
                    }
                    finally {
                        for (ByteBufferPool.ReusableByteBuffer buffer : byteBufferList) {
                            buffer.release();
                        }
                    }
                })
                .orElse(null);
    }

    @Override
    public void writeConcreteValue(String connectorId, ConnectorTransactionHandle value, TProtocolWriter writer)
            throws Exception
    {
        requireNonNull(value, "value is null");
        connectorCodecManager.getTransactionHandleCodec(connectorId)
                .ifPresent(codec -> {
                    try {
                        codec.serialize(value, byteBufferList -> {
                            try {
                                writer.writeBinaryFromBufferList(byteBufferList);
                            }
                            catch (TException e) {
                                throw new IllegalStateException("Failed to serialize connector handle", e);
                            }
                            finally {
                                for (ByteBufferPool.ReusableByteBuffer buffer : byteBufferList) {
                                    buffer.release();
                                }
                            }
                        });
                    }
                    catch (Exception e) {
                        throw new IllegalStateException("Failed to serialize connector handle", e);
                    }
                });
    }

    @Override
    public boolean isThriftCodecAvailable(String connectorId)
    {
        return connectorCodecManager.getTransactionHandleCodec(connectorId).isPresent();
    }
}
