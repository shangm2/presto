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
package com.facebook.presto.experimental.utils;

import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.experimental.auto_gen.ThriftOutputBuffers;

import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.experimental.utils.ThriftBufferTypeUtils.fromBufferType;
import static com.facebook.presto.experimental.utils.ThriftBufferTypeUtils.toBufferType;

public class ThriftOutputBuffersUtils
{
    private ThriftOutputBuffersUtils() {}

    public static OutputBuffers toOutputBuffers(ThriftOutputBuffers thriftOutputBuffers)
    {
        if (thriftOutputBuffers == null) {
            return null;
        }
        Map<OutputBuffers.OutputBufferId, Integer> buffers = thriftOutputBuffers.getBuffers().entrySet()
                .stream().collect(Collectors.toMap(
                        entry -> new OutputBuffers.OutputBufferId(entry.getKey()),
                        Map.Entry::getValue));
        return new OutputBuffers(
                toBufferType(thriftOutputBuffers.getType()),
                thriftOutputBuffers.getVersion(),
                thriftOutputBuffers.isNoMoreBufferIds(),
                buffers);
    }

    public static ThriftOutputBuffers fromOutputBuffers(OutputBuffers outputBuffers)
    {
        if (outputBuffers == null) {
            return null;
        }
        Map<Integer, Integer> buffers = outputBuffers.getBuffers().entrySet()
                .stream().collect(Collectors.toMap(
                        entry -> entry.getKey().getId(),
                        Map.Entry::getValue));
        return new ThriftOutputBuffers(
                fromBufferType(outputBuffers.getType()),
                outputBuffers.getVersion(),
                outputBuffers.isNoMoreBufferIds(), buffers);
    }
}
