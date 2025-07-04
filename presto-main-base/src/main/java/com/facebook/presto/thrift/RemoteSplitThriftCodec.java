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
package com.facebook.presto.thrift;

import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.protocol.TChunkedBinaryProtocol;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorThriftCodec;
import com.facebook.presto.split.RemoteSplit;
import com.google.inject.Provider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class RemoteSplitThriftCodec
        implements ConnectorThriftCodec<ConnectorSplit>
{
    private final Provider<ThriftCodecManager> thriftCodecManagerProvider;
    private final ByteBufAllocator allocator;

    public RemoteSplitThriftCodec(Provider<ThriftCodecManager> thriftCodecManagerProvider, ByteBufAllocator allocator)
    {
        this.thriftCodecManagerProvider = requireNonNull(thriftCodecManagerProvider, "thriftCodecManagerProvider is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
    }

    @Override
    public void serialize(ConnectorSplit connectorSplit, Consumer<List<ByteBuf>> bufferConsumer)
    {
        requireNonNull(connectorSplit, "split is null");
        requireNonNull(bufferConsumer, "bufferConsumer is null");

        RemoteSplit remoteSplit = (RemoteSplit) connectorSplit;

        try {
            TChunkedBinaryProtocol.serialize(
                    allocator,
                    remoteSplit,
                    thriftCodecManagerProvider.get().getCodec(RemoteSplit.class)::write,
                    bufferConsumer);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to serialize RemoteSplit", e);
        }
    }

    @Override
    public ConnectorSplit deserialize(List<ByteBuf> buffers)
    {
        requireNonNull(buffers, "buffers is null");
        try {
            return TChunkedBinaryProtocol.deserialize(buffers, thriftCodecManagerProvider.get().getCodec(RemoteSplit.class)::read);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to deserialize RemoteSplit", e);
        }
    }
}
