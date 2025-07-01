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
package com.facebook.presto.spi;

import com.facebook.presto.spi.api.Experimental;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.function.Consumer;

@Experimental
public interface ConnectorThriftCodec<T>
{
    void serialize(T value, Consumer<List<ByteBuf>> bufferConsumer)
            throws Exception;

    T deserialize(List<ByteBuf> buffers)
            throws Exception;
}
