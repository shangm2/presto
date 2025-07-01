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
import org.weakref.jmx.Managed;

import javax.inject.Inject;

public class ByteBufferPoolStats
{
    private final ByteBufferPool pool;

    @Inject
    public ByteBufferPoolStats(ConnectorSplitThriftCodec splitCodec)
    {
        this.pool = splitCodec.getPool();
    }

    @Managed
    public int getPoolSize()
    {
        return pool.getPoolSize();
    }

    @Managed
    public long getPoolAcquire()
    {
        return pool.getAcquire();
    }

    @Managed
    public long getPoolReuse()
    {
        return pool.getReuse();
    }

    @Managed
    public long getPoolRecycle()
    {
        return pool.getRecycle();
    }

    @Managed
    public long getPoolNewAllocation()
    {
        return pool.getNewAllocate();
    }
}
