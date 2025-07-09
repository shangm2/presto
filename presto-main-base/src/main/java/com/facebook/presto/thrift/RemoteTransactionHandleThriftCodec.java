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
import com.facebook.drift.protocol.bytebuffer.BufferPool;
import com.facebook.presto.metadata.RemoteTransactionHandle;
import com.facebook.presto.server.thrift.ThriftCodecUtils;
import com.facebook.presto.spi.ConnectorThriftCodec;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Provider;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class RemoteTransactionHandleThriftCodec
        implements ConnectorThriftCodec<ConnectorTransactionHandle>
{
    private final Provider<ThriftCodecManager> thriftCodecManagerProvider;
    private final BufferPool pool;

    public RemoteTransactionHandleThriftCodec(Provider<ThriftCodecManager> thriftCodecManagerProvider, BufferPool pool)
    {
        this.thriftCodecManagerProvider = requireNonNull(thriftCodecManagerProvider, "thriftCodecManagerProvider is null");
        this.pool = requireNonNull(pool, "pool is null");
    }

    @Override
    public void serialize(ConnectorTransactionHandle handle, Consumer<List<ByteBuffer>> consumer)
    {
        requireNonNull(handle, "split is null");
        requireNonNull(consumer, "consumer is null");

        RemoteTransactionHandle remoteHandle = (RemoteTransactionHandle) handle;

        try {
            ThriftCodecUtils.serializeToBufferList(remoteHandle, thriftCodecManagerProvider.get().getCodec(RemoteTransactionHandle.class), pool, consumer);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to serialize RemoteSplit", e);
        }
    }

    @Override
    public ConnectorTransactionHandle deserialize(List<ByteBuffer> buffers)
    {
        requireNonNull(buffers, "buffers is null");
        try {
            return ThriftCodecUtils.deserializeFromBufferList(buffers, pool, thriftCodecManagerProvider.get().getCodec(RemoteTransactionHandle.class), RemoteTransactionHandleThriftCodec.class);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to deserialize RemoteTransactionHandle", e);
        }
    }
}
