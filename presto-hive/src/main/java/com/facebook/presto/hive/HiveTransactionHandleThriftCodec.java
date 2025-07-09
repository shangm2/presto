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
package com.facebook.presto.hive;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.protocol.bytebuffer.BufferPool;
import com.facebook.presto.hive.thrift.ThriftCodecUtils;
import com.facebook.presto.spi.ConnectorThriftCodec;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class HiveTransactionHandleThriftCodec
        implements ConnectorThriftCodec<ConnectorTransactionHandle>
{
    private final ThriftCodec<HiveTransactionHandle> thriftCodec;
    private final BufferPool pool;

    public HiveTransactionHandleThriftCodec(ThriftCodecManager thriftCodecManager, BufferPool pool)
    {
        this.thriftCodec = requireNonNull(thriftCodecManager, "thriftCodecManager is null").getCodec(HiveTransactionHandle.class);
        this.pool = requireNonNull(pool, "pool is null");
    }

    @Override
    public void serialize(ConnectorTransactionHandle transactionHandle, Consumer<List<ByteBuffer>> consumer)
    {
        requireNonNull(transactionHandle, "transactionHandle is null");
        requireNonNull(consumer, "consumer is null");

        HiveTransactionHandle hiveTransactionHandle = (HiveTransactionHandle) transactionHandle;

        try {
            ThriftCodecUtils.serializeToBufferList(hiveTransactionHandle, thriftCodec, pool, consumer);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to serialize HiveTransactionHandle", e);
        }
    }

    @Override
    public ConnectorTransactionHandle deserialize(List<ByteBuffer> buffers)
    {
        requireNonNull(buffers, "buffers is null");
        try {
            return ThriftCodecUtils.deserializeFromBufferList(buffers, pool, thriftCodec);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to deserialize HiveTransactionHandle", e);
        }
    }
}
