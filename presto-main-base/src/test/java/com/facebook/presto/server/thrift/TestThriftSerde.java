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

import com.facebook.drift.buffer.ByteBufferPool;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.presto.execution.Location;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.split.RemoteSplit;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

public class TestThriftSerde
{
    @Test
    public void testThriftSerde()
            throws Exception
    {
        ThriftCodecManager codecManager = new ThriftCodecManager();
        ThriftCodec<RemoteSplit> thriftCodec = codecManager.getCodec(RemoteSplit.class);
        ByteBufferPool bufferPool = new ByteBufferPool(8, 1024);
        RemoteSplit split1 = new RemoteSplit(new Location("http://127.0.0.1:56080/v1/task/20250709_104120_00002_wekgx.1.0.0.0/results/0"), new TaskId("20250709_104120_00002_wekgx", 1, 0, 0, 0));

        RemoteSplit split2 = new RemoteSplit(new Location("https://www.example.com/this/is/a/very/long/url/that/keeps/going/on/and/on/and/on/and/it/just/does/not/stop/it/keeps/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going/and/going"), new TaskId("20250709_104120_00002_wekgx", 1, 2, 3, 4));

        final AtomicInteger maxSize = new AtomicInteger();

        new Thread(() ->
        {
            try {
                ThriftCodecUtils.serializeConcreteValue(split1,
                        thriftCodec,
                        bufferPool,
                        byteBufferList -> {
                            RemoteSplit split;
                            try {
                                split = ThriftCodecUtils.deserializeConcreteValue(byteBufferList, thriftCodec);
                            }
                            catch (Exception e) {
                                throw new RuntimeException(e);
                            }

                            maxSize.set(Math.max(maxSize.get(), byteBufferList.size()));
                            assertEquals(split1.getLocation().getLocation(), split.getLocation().getLocation());
                            assertEquals(split1.getRemoteSourceTaskId(), split.getRemoteSourceTaskId());
                        });
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() ->
        {
            try {
                // Give the other thread enough time to finish
                Thread.sleep(200);

                ThriftCodecUtils.serializeConcreteValue(split2,
                        thriftCodec,
                        bufferPool,
                        byteBufferList -> {
                            RemoteSplit split;
                            try {
                                split = ThriftCodecUtils.deserializeConcreteValue(byteBufferList, thriftCodec);
                            }
                            catch (Exception e) {
                                throw new RuntimeException(e);
                            }

                            maxSize.set(Math.max(maxSize.get(), byteBufferList.size()));
                            assertEquals(split2.getLocation().getLocation(), split.getLocation().getLocation());
                            assertEquals(split2.getRemoteSourceTaskId(), split.getRemoteSourceTaskId());
                        });
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();

        assertEquals(maxSize.get(), bufferPool.getPoolSize());
    }
}
