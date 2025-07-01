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
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.tpcds.TpcdsTableLayoutHandle;
import com.google.inject.Provider;

import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class TpcdsTableLayoutHandleCodec
        implements ConnectorCodec<ConnectorTableLayoutHandle>
{
    private final Provider<ThriftCodecManager> thriftCodecManagerProvider;
    private final ByteBufferPool pool;

    public TpcdsTableLayoutHandleCodec(Provider<ThriftCodecManager> thriftCodecManagerProvider, ByteBufferPool pool)
    {
        this.thriftCodecManagerProvider = requireNonNull(thriftCodecManagerProvider, "thriftCodecManagerProvider is null");
        this.pool = requireNonNull(pool, "pool is null");
    }

    @Override
    public void serialize(ConnectorTableLayoutHandle tableLayoutHandle, Consumer<List<ByteBufferPool.ReusableByteBuffer>> consumer)
            throws Exception
    {
        requireNonNull(tableLayoutHandle, "tableLayoutHandle is null");
        requireNonNull(consumer, "consumer is null");

        TpcdsTableLayoutHandle handle = (TpcdsTableLayoutHandle) tableLayoutHandle;

        try {
            ThriftCodecUtils.serializeToBufferList(handle, thriftCodecManagerProvider.get().getCodec(TpcdsTableLayoutHandle.class), pool, consumer);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to serialize TpcdsTableLayoutHandle", e);
        }
    }

    @Override
    public ConnectorTableLayoutHandle deserialize(List<ByteBufferPool.ReusableByteBuffer> byteBufferList)
            throws Exception
    {
        requireNonNull(byteBufferList, "byteBufferList is null");

        TpcdsTableLayoutHandle tableLayoutHandle;
        try {
            tableLayoutHandle = ThriftCodecUtils.deserializeFromBufferList(byteBufferList, thriftCodecManagerProvider.get().getCodec(TpcdsTableLayoutHandle.class));
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to deserialize TpcdsTableLayoutHandle", e);
        }
        return tableLayoutHandle;
    }
}
