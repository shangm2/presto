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

import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.experimental.auto_gen.ThriftScheduledSplit;
import com.facebook.presto.spi.plan.PlanNodeId;

import static com.facebook.presto.experimental.utils.ThriftSplitUtils.fromSplit;
import static com.facebook.presto.experimental.utils.ThriftSplitUtils.toSplit;

public class ThriftScheduledSplitUtils
{
    private ThriftScheduledSplitUtils() {}

    public static ThriftScheduledSplit fromScheduledSplit(ScheduledSplit scheduledSplit)
    {
        if (scheduledSplit == null) {
            return null;
        }
        return new ThriftScheduledSplit(
                scheduledSplit.getSequenceId(),
                scheduledSplit.getPlanNodeId().toString(),
                fromSplit(scheduledSplit.getSplit()));
    }

    public static ScheduledSplit toScheduledSplit(ThriftScheduledSplit thriftScheduledSplit)
    {
        if (thriftScheduledSplit == null) {
            return null;
        }
        return new ScheduledSplit(
                thriftScheduledSplit.getSequenceId(),
                new PlanNodeId(thriftScheduledSplit.getPlanNodeId()),
                toSplit(thriftScheduledSplit.getSplit()));
    }
}
