package com.facebook.presto.server.thrift;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.protocol.bytebuffer.BufferPool;
import com.facebook.presto.execution.Location;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.split.RemoteSplit;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

public class TestThriftSerde
{
    @Test
    public void testThriftSerde()
            throws Exception
    {
        ThriftCodecManager codecManager = new ThriftCodecManager();
        ThriftCodec<RemoteSplit> thriftCodec = codecManager.getCodec(RemoteSplit.class);
        BufferPool bufferPool = new BufferPool(8, 1024);

        RemoteSplit expectedSplit = new RemoteSplit(new Location("http://127.0.0.1:56080/v1/task/20250709_104120_00002_wekgx.1.0.0.0/results/0"), new TaskId("20250709_104120_00002_wekgx", 1, 0, 0, 0));

        System.out.println(expectedSplit);

        int bufferCount = 0;
        for (int i = 0; i < 1; i++) {
            List<ByteBuffer> buffers = new ArrayList<>();
            ThriftCodecUtils.serializeToBufferList(expectedSplit,
                    thriftCodec,
                    bufferPool,
                    buffers::addAll);
            bufferCount = buffers.size();

            System.out.println(bufferCount);
            StringBuilder sb = new StringBuilder();
            for (ByteBuffer buffer : buffers) {
                ByteBuffer duplicate = buffer.duplicate();
                while (duplicate.hasRemaining()) {
                    byte b = duplicate.get();
                    sb.append(String.format("%02X", b & 0xFF));
                }
            }

            System.out.println("in test: " + sb);

            RemoteSplit split = ThriftCodecUtils.deserializeFromBufferList(buffers, bufferPool, thriftCodec, RemoteSplit.class);

            assertEquals(expectedSplit.getLocation().getLocation(), split.getLocation().getLocation());
            assertEquals(expectedSplit.getRemoteSourceTaskId(), split.getRemoteSourceTaskId());
        }

        assertEquals(bufferCount, bufferPool.getCount());
    }
}
