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
package com.facebook.presto.tpcds.thrift;

import com.facebook.drift.buffer.ByteBufferPool;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.tpcds.TpcdsTableHandle;
import com.google.inject.Provider;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

import static com.facebook.presto.tpcds.thrift.ThriftCodecUtils.deserializeConcreteValue;
import static com.facebook.presto.tpcds.thrift.ThriftCodecUtils.serializeConcreteValue;
import static java.util.Objects.requireNonNull;

public class TpcdsTableHandleCodec
        implements ConnectorCodec<ConnectorTableHandle>
{
    private final Provider<ThriftCodecManager> thriftCodecManagerProvider;
    private final ByteBufferPool pool;

    public TpcdsTableHandleCodec(Provider<ThriftCodecManager> thriftCodecManagerProvider, ByteBufferPool pool)
    {
        this.thriftCodecManagerProvider = requireNonNull(thriftCodecManagerProvider, "thriftCodecManagerProvider is null");
        this.pool = requireNonNull(pool, "pool is null");
    }

    @Override
    public void serialize(ConnectorTableHandle handle, Consumer<List<ByteBuffer>> consumer)
    {
        requireNonNull(handle, "split is null");
        requireNonNull(consumer, "consumer is null");

        TpcdsTableHandle tableHandle = (TpcdsTableHandle) handle;

        try {
            serializeConcreteValue(tableHandle, thriftCodecManagerProvider.get().getCodec(TpcdsTableHandle.class), pool, consumer);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to serialize TpcdsTableHandle", e);
        }
    }

    @Override
    public ConnectorTableHandle deserialize(List<ByteBuffer> byteBuffers)
    {
        requireNonNull(byteBuffers, "byteBuffers is null");
        try {
            return deserializeConcreteValue(byteBuffers, thriftCodecManagerProvider.get().getCodec(TpcdsTableHandle.class));
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to deserialize TpcdsTableHandle", e);
        }
    }
}
