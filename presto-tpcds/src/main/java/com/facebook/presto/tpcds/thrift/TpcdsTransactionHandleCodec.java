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
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.tpcds.TpcdsTransactionHandle;
import com.google.inject.Provider;

import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class TpcdsTransactionHandleCodec
        implements ConnectorCodec<ConnectorTransactionHandle>
{
    private final Provider<ThriftCodecManager> thriftCodecManagerProvider;
    private final ByteBufferPool pool;

    public TpcdsTransactionHandleCodec(Provider<ThriftCodecManager> thriftCodecManagerProvider, ByteBufferPool pool)
    {
        this.thriftCodecManagerProvider = requireNonNull(thriftCodecManagerProvider, "thriftCodecManagerProvider is null");
        this.pool = requireNonNull(pool, "pool is null");
    }

    @Override
    public void serialize(ConnectorTransactionHandle transactionHandle, Consumer<List<ByteBufferPool.ReusableByteBuffer>> consumer)
            throws Exception
    {
        requireNonNull(transactionHandle, "transactionHandle is null");
        requireNonNull(consumer, "consumer is null");

        TpcdsTransactionHandle handle = (TpcdsTransactionHandle) transactionHandle;
        ThriftCodecUtils.serializeToBufferList(handle, thriftCodecManagerProvider.get().getCodec(TpcdsTransactionHandle.class), pool, consumer);
    }

    @Override
    public ConnectorTransactionHandle deserialize(List<ByteBufferPool.ReusableByteBuffer> byteBufferList)
            throws Exception
    {
        requireNonNull(byteBufferList, "byteBufferList is null");
        return ThriftCodecUtils.deserializeFromBufferList(byteBufferList, thriftCodecManagerProvider.get().getCodec(TpcdsTransactionHandle.class));
    }
}
