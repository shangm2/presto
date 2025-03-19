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

import com.facebook.presto.experimental.auto_gen.ThriftResourceEstimates;
import com.facebook.presto.spi.session.ResourceEstimates;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class ThriftResourceEstimatesUtils
{
    private ThriftResourceEstimatesUtils() {}

    public static ThriftResourceEstimates fromResourceEstimates(ResourceEstimates resourceEstimates)
    {
        if (resourceEstimates == null) {
            return null;
        }
        ThriftResourceEstimates thriftResourceEstimates = new ThriftResourceEstimates();
        thriftResourceEstimates.setExecutionTimeInMillis(resourceEstimates.getExecutionTime().map(Duration::toMillis).orElse(0L))
                .setCpuTimeInNanos(resourceEstimates.getCpuTime().map(d -> d.roundTo(TimeUnit.NANOSECONDS)).orElse(0L))
                .setPeakMemoryInBytes(resourceEstimates.getPeakMemory().map(DataSize::toBytes).orElse(0L))
                .setPeakTaskMemoryInBytes(resourceEstimates.getPeakTaskMemory().map(DataSize::toBytes).orElse(0L));
        return thriftResourceEstimates;
    }

    public static ResourceEstimates toResourceEstimates(ThriftResourceEstimates thriftResourceEstimates)
    {
        if (thriftResourceEstimates == null) {
            return null;
        }
        return new ResourceEstimates(
                thriftResourceEstimates.getExecutionTimeInMillis().map(TimeUnit.MILLISECONDS::toNanos).map(Duration::succinctNanos),
                thriftResourceEstimates.getCpuTimeInNanos().map(Duration::succinctNanos),
                thriftResourceEstimates.getPeakMemoryInBytes().map(DataSize::succinctBytes),
                thriftResourceEstimates.getPeakTaskMemoryInBytes().map(DataSize::succinctBytes));
    }
}
