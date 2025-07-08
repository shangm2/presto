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
import com.facebook.drift.codec.CodecThriftType;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.drift.protocol.bytebuffer.ForChunkedProtocol;
import com.facebook.presto.connector.ConnectorThriftCodecManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import javax.inject.Inject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class TransactionHandleThriftCodec
        extends AbstractTypedThriftCodec<ConnectorTransactionHandle>
{
    private static final ThriftType THRIFT_TYPE = createThriftType(ConnectorTransactionHandle.class);
    private final ConnectorThriftCodecManager connectorThriftCodecManager;
    private final ByteBufAllocator allocator;

    @Inject
    public TransactionHandleThriftCodec(HandleResolver handleResolver,
            ConnectorThriftCodecManager connectorThriftCodecManager,
            JsonCodec<ConnectorTransactionHandle> jsonCodec,
            @ForChunkedProtocol ByteBufAllocator allocator)
    {
        super(ConnectorTransactionHandle.class,
                requireNonNull(jsonCodec, "jsonCodec is null"),
                requireNonNull(handleResolver, "handleResolver is null")::getId,
                handleResolver::getTransactionHandleClass);
        this.connectorThriftCodecManager = requireNonNull(connectorThriftCodecManager, "connectorThriftCodecManager is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
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
        List<ByteBuf> byteBufs = reader.readBinaryAsByteBufList();
        try {
            List<ByteBuffer> buffers = new ArrayList<>(byteBufs.size());
            for (ByteBuf byteBuf : byteBufs) {
                if (byteBuf.nioBufferCount() > 0) {
                    buffers.add(byteBuf.nioBuffer());
                }
                else {
                    // Fallback - should rarely happen
                    ByteBuffer buffer = ByteBuffer.allocate(byteBuf.readableBytes());
                    byteBuf.readBytes(buffer);
                    buffer.flip();
                    buffers.add(buffer);
                }
            }
            return connectorThriftCodecManager.getConnectorTransactionHandleThriftCodec(connectorId)
                    .map(codec -> {
                        try {
                            return codec.deserialize(buffers);
                        }
                        catch (Exception e) {
                            throw new RuntimeException("Failed to deserialize connector transaction handle", e);
                        }
                    })
                    .orElse(null);
        }
        finally {
            byteBufs.forEach(ByteBuf::release);
        }
    }

    @Override
    public void writeConcreteValue(String connectorId, ConnectorTransactionHandle value, TProtocolWriter writer)
            throws Exception
    {
        requireNonNull(value, "value is null");

        List<ByteBuffer> buffers = new ArrayList<>();
        List<ByteBuf> byteBufs = new ArrayList<>();
        connectorThriftCodecManager.getConnectorTransactionHandleThriftCodec(connectorId)
                .ifPresent(codec -> {
                    try {
                        codec.serialize(value, buffers::addAll);
                        for (ByteBuffer buffer : buffers) {
                            ByteBuf byteBuf;
                            if (buffer.isDirect()) {
                                // Wrap direct ByteBuffer without copying
                                byteBuf = allocator.directBuffer(buffer.remaining());
                                byteBuf.writeBytes(buffer);
                            }
                            else {
                                // Heap ByteBuffer
                                byteBuf = allocator.heapBuffer(buffer.remaining());
                                byteBuf.writeBytes(buffer);
                            }
                            byteBufs.add(byteBuf);
                        }
                        if (byteBufs.isEmpty()) {
                            throw new RuntimeException("Empty buffer list. Failed to serialize connector split");
                        }
                        writer.writeBinary(byteBufs);
                    }
                    catch (Exception e) {
                        throw new RuntimeException("Failed to serialize connector transaction handle", e);
                    }
                    finally {
                        byteBufs.forEach(ByteBuf::release);
                    }
                });
    }

    @Override
    public boolean isThriftCodecAvailable(String connectorId)
    {
        return connectorThriftCodecManager.getConnectorTransactionHandleThriftCodec(connectorId).isPresent();
    }
}
