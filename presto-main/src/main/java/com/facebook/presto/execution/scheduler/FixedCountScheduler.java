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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.metadata.InternalNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.facebook.presto.execution.scheduler.ScheduleResult.BlockedReason.WAITING_FOR_TASK_CREATION;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.allAsList;
import static java.util.Objects.requireNonNull;

public class FixedCountScheduler
        implements StageScheduler
{
    public interface TaskScheduler
    {
        ListenableFuture<?> scheduleTask(InternalNode node, int partition);
    }

    private final TaskScheduler taskScheduler;
    private final List<InternalNode> partitionToNode;

    public FixedCountScheduler(SqlStageExecution stage, List<InternalNode> partitionToNode)
    {
        requireNonNull(stage, "stage is null");
        this.taskScheduler = stage::scheduleTask;
        this.partitionToNode = requireNonNull(partitionToNode, "partitionToNode is null");
    }

    @VisibleForTesting
    public FixedCountScheduler(TaskScheduler taskScheduler, List<InternalNode> partitionToNode)
    {
        this.taskScheduler = requireNonNull(taskScheduler, "taskScheduler is null");
        this.partitionToNode = requireNonNull(partitionToNode, "partitionToNode is null");
    }

    @Override
    public ScheduleResult schedule()
    {
        List<ListenableFuture<?>> newTasks = IntStream.range(0, partitionToNode.size())
                .mapToObj(partition -> taskScheduler.scheduleTask(partitionToNode.get(partition), partition))
                .filter(Objects::nonNull)
                .collect(toImmutableList());

        // no need to call stage.transitionToSchedulingSplits() since there is no table splits

        return ScheduleResult.blocked(true, ImmutableSet.of(), allAsList(newTasks), WAITING_FOR_TASK_CREATION, 0);
    }
}
