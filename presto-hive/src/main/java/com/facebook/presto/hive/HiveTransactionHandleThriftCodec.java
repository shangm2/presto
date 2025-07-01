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
import com.facebook.drift.protocol.TChunkedBinaryProtocol;
import com.facebook.presto.spi.ConnectorThriftCodec;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class HiveTransactionHandleThriftCodec
        implements ConnectorThriftCodec<ConnectorTransactionHandle>
{
    private final ThriftCodec<HiveTransactionHandle> thriftCodec;
    private final ByteBufAllocator allocator;

    public HiveTransactionHandleThriftCodec(ThriftCodecManager thriftCodecManager, ByteBufAllocator allocator)
    {
        this.thriftCodec = requireNonNull(thriftCodecManager, "thriftCodecManager is null").getCodec(HiveTransactionHandle.class);
        this.allocator = requireNonNull(allocator, "allocator is null");
    }

    @Override
    public void serialize(ConnectorTransactionHandle transactionHandle, Consumer<List<ByteBuf>> bufferConsumer)
    {
        requireNonNull(transactionHandle, "transactionHandle is null");
        requireNonNull(bufferConsumer, "bufferConsumer is null");

        HiveTransactionHandle hiveTransactionHandle = (HiveTransactionHandle) transactionHandle;

        try {
            TChunkedBinaryProtocol.serialize(
                    allocator,
                    hiveTransactionHandle,
                    thriftCodec::write,
                    bufferConsumer);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to serialize HiveTransactionHandle", e);
        }
    }

    @Override
    public ConnectorTransactionHandle deserialize(List<ByteBuf> buffers)
    {
        requireNonNull(buffers, "buffers is null");
        try {
            return TChunkedBinaryProtocol.deserialize(buffers, thriftCodec::read);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to deserialize HiveTransactionHandle", e);
        }
    }
}
