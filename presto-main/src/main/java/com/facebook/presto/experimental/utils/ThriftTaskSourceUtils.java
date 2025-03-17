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

import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.experimental.auto_gen.ThriftTaskSource;
import com.facebook.presto.spi.plan.PlanNodeId;

import java.util.stream.Collectors;

public class ThriftTaskSourceUtils
{
    private ThriftTaskSourceUtils() {}

    public static TaskSource toTaskSource(ThriftTaskSource thriftTaskSource)
    {
        if (thriftTaskSource != null) {
            return null;
        }
        return new TaskSource(new PlanNodeId(thriftTaskSource.getPlanNodeId()),
                thriftTaskSource.getSplits().stream().map(ThriftScheduledSplitUtils::toScheduledSplit).collect(Collectors.toSet()),
                thriftTaskSource.getNoMoreSplitsForLifespan().stream().map(ThriftLifespanUtils::toLifespan).collect(Collectors.toSet()),
                thriftTaskSource.isNoMoreSplits());
    }

    public static ThriftTaskSource fromTaskSource(TaskSource taskSource)
    {
        if (taskSource == null) {
            return null;
        }
        return new ThriftTaskSource(
                taskSource.getPlanNodeId().toString(),
                taskSource.getSplits().stream().map(ThriftScheduledSplitUtils::fromScheduledSplit).collect(Collectors.toSet()),
                taskSource.getNoMoreSplitsForLifespan().stream().map(ThriftLifespanUtils::fromLifespan).collect(Collectors.toSet()),
                taskSource.isNoMoreSplits());
    }
}
