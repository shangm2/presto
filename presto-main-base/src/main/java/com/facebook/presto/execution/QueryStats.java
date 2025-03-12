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
package com.facebook.presto.execution;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.operator.ExchangeOperator;
import com.facebook.presto.operator.MergeOperator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.ScanFilterAndProjectOperator;
import com.facebook.presto.operator.TableScanOperator;
import com.facebook.presto.operator.TableWriterOperator;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.facebook.presto.sql.planner.PlanFragment;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkNonNegative;
import static com.facebook.presto.util.DateTimeUtils.toTimeStampInMillis;
import static com.facebook.presto.util.DurationUtils.toTimeStampInNanos;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctNanos;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class QueryStats
{
    private final long createTimeInMillis;

    private final long executionStartTimeInMillis;
    private final long lastHeartbeatInMillis;
    private final long endTimeInMillis;

    private final long elapsedTimeInNanos;
    private final long waitingForPrerequisitesTimeInNanos;
    private final long queuedTimeInNanos;
    private final long resourceWaitingTimeInNanos;
    private final long semanticAnalyzingTimeInNanos;
    private final long columnAccessPermissionCheckingTimeInNanos;
    private final long dispatchingTimeInNanos;
    private final long executionTimeInNanos;
    private final long analysisTimeInNanos;
    private final long totalPlanningTimeInNanos;
    private final long finishingTimeInNanos;

    private final int totalTasks;
    private final int runningTasks;
    private final int peakRunningTasks;
    private final int completedTasks;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int blockedDrivers;
    private final int completedDrivers;

    private final double cumulativeUserMemory;
    private final double cumulativeTotalMemory;
    private final DataSize userMemoryReservation;
    private final DataSize totalMemoryReservation;
    private final DataSize peakUserMemoryReservation;
    private final DataSize peakTotalMemoryReservation;
    private final DataSize peakTaskTotalMemory;
    private final DataSize peakTaskUserMemory;
    private final DataSize peakNodeTotalMemory;

    private final boolean scheduled;
    private final long totalScheduledTimeInNanos;
    private final long totalCpuTimeInNanos;
    private final long retriedCpuTimeInNanos;
    private final long totalBlockedTimeInNanos;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final DataSize totalAllocation;

    private final DataSize rawInputDataSize;
    private final long rawInputPositions;

    private final DataSize processedInputDataSize;
    private final long processedInputPositions;

    private final DataSize shuffledDataSize;
    private final long shuffledPositions;

    private final DataSize outputDataSize;
    private final long outputPositions;

    private final long writtenOutputPositions;
    private final DataSize writtenOutputLogicalDataSize;
    private final DataSize writtenOutputPhysicalDataSize;

    private final DataSize writtenIntermediatePhysicalDataSize;

    private final List<StageGcStatistics> stageGcStatistics;

    private final List<OperatorStats> operatorSummaries;

    // RuntimeStats aggregated at the query level including the metrics exposed in every task and every operator.
    private final RuntimeStats runtimeStats;

    public QueryStats(
            long createTimeInMillis,
            long executionStartTimeInMillis,
            long lastHeartbeatInMillis,
            long endTimeInMillis,

            long elapsedTimeInNanos,
            long waitingForPrerequisitesTimeInNanos,
            long queuedTimeInNanos,
            long resourceWaitingTimeInNanos,
            long semanticAnalyzingTimeInNanos,
            long columnAccessPermissionCheckingTimeInNanos,
            long dispatchingTimeInNanos,
            long executionTimeInNanos,
            long analysisTimeInNanos,
            long totalPlanningTimeInNanos,
            long finishingTimeInNanos,

            int totalTasks,
            int runningTasks,
            int peakRunningTasks,
            int completedTasks,

            int totalDrivers,
            int queuedDrivers,
            int runningDrivers,
            int blockedDrivers,
            int completedDrivers,

            double cumulativeUserMemory,
            double cumulativeTotalMemory,
            DataSize userMemoryReservation,
            DataSize totalMemoryReservation,
            DataSize peakUserMemoryReservation,
            DataSize peakTotalMemoryReservation,
            DataSize peakTaskUserMemory,
            DataSize peakTaskTotalMemory,
            DataSize peakNodeTotalMemory,

            boolean scheduled,
            long totalScheduledTimeInNanos,
            long totalCpuTimeInNanos,
            long retriedCpuTimeInNanos,
            long totalBlockedTimeInNanos,
            boolean fullyBlocked,
            Set<BlockedReason> blockedReasons,

            DataSize totalAllocation,

            DataSize rawInputDataSize,
            long rawInputPositions,

            DataSize processedInputDataSize,
            long processedInputPositions,

            DataSize shuffledDataSize,
            long shuffledPositions,

            DataSize outputDataSize,
            long outputPositions,

            long writtenOutputPositions,
            DataSize writtenOutputLogicalDataSize,
            DataSize writtenOutputPhysicalDataSize,

            DataSize writtenIntermediatePhysicalDataSize,

            List<StageGcStatistics> stageGcStatistics,

            List<OperatorStats> operatorSummaries,

            RuntimeStats runtimeStats)
    {
        checkArgument(createTimeInMillis >= 0, "createTimeInMillis is negative");
        this.createTimeInMillis = createTimeInMillis;
        this.executionStartTimeInMillis = executionStartTimeInMillis;
        checkArgument(lastHeartbeatInMillis >= 0, "lastHeartbeatInMillis is negative");
        this.lastHeartbeatInMillis = lastHeartbeatInMillis;
        this.endTimeInMillis = endTimeInMillis;

        this.elapsedTimeInNanos = checkNonNegative(elapsedTimeInNanos, "elapsedTimeInNanos is negative");
        this.waitingForPrerequisitesTimeInNanos = checkNonNegative(waitingForPrerequisitesTimeInNanos, "waitingForPrerequisitesTimeInNanos is negative");
        this.queuedTimeInNanos = checkNonNegative(queuedTimeInNanos, "queuedTimeInNanos is negative");
        this.resourceWaitingTimeInNanos = checkNonNegative(resourceWaitingTimeInNanos, "resourceWaitingTimeInNanos is negative");
        this.semanticAnalyzingTimeInNanos = checkNonNegative(semanticAnalyzingTimeInNanos, "semanticAnalyzingTimeInNanos is negative");
        this.columnAccessPermissionCheckingTimeInNanos = checkNonNegative(columnAccessPermissionCheckingTimeInNanos, "columnAccessPermissionCheckingTimeInNanos is negative");
        this.dispatchingTimeInNanos = checkNonNegative(dispatchingTimeInNanos, "dispatchingTimeInNanos is negative");
        this.executionTimeInNanos = checkNonNegative(executionTimeInNanos, "executionTimeInNanos is negative");
        this.analysisTimeInNanos = checkNonNegative(analysisTimeInNanos, "analysisTimeInNanos is negative");
        this.totalPlanningTimeInNanos = checkNonNegative(totalPlanningTimeInNanos, "totalPlanningTimeInNanos is negative");
        this.finishingTimeInNanos = checkNonNegative(finishingTimeInNanos, "finishingTimeInNanos is negative");

        checkArgument(totalTasks >= 0, "totalTasks is negative");
        this.totalTasks = totalTasks;
        checkArgument(runningTasks >= 0, "runningTasks is negative");
        this.runningTasks = runningTasks;
        checkArgument(peakRunningTasks >= 0, "peakRunningTasks is negative");
        this.peakRunningTasks = peakRunningTasks;
        checkArgument(completedTasks >= 0, "completedTasks is negative");
        this.completedTasks = completedTasks;

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        this.totalDrivers = totalDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        this.queuedDrivers = queuedDrivers;
        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        this.runningDrivers = runningDrivers;
        checkArgument(blockedDrivers >= 0, "blockedDrivers is negative");
        this.blockedDrivers = blockedDrivers;
        checkArgument(completedDrivers >= 0, "completedDrivers is negative");
        this.completedDrivers = completedDrivers;
        checkArgument(cumulativeUserMemory >= 0, "cumulativeUserMemory is negative");
        this.cumulativeUserMemory = cumulativeUserMemory;
        checkArgument(cumulativeTotalMemory >= 0, "cumulativeTotalMemory is negative");
        this.cumulativeTotalMemory = cumulativeTotalMemory;
        this.userMemoryReservation = requireNonNull(userMemoryReservation, "userMemoryReservation is null");
        this.totalMemoryReservation = requireNonNull(totalMemoryReservation, "totalMemoryReservation is null");
        this.peakUserMemoryReservation = requireNonNull(peakUserMemoryReservation, "peakUserMemoryReservation is null");
        this.peakTotalMemoryReservation = requireNonNull(peakTotalMemoryReservation, "peakTotalMemoryReservation is null");
        this.peakTaskTotalMemory = requireNonNull(peakTaskTotalMemory, "peakTaskTotalMemory is null");
        this.peakTaskUserMemory = requireNonNull(peakTaskUserMemory, "peakTaskUserMemory is null");
        this.peakNodeTotalMemory = requireNonNull(peakNodeTotalMemory, "peakNodeTotalMemory is null");
        this.scheduled = scheduled;
        this.totalScheduledTimeInNanos = checkNonNegative(totalScheduledTimeInNanos, "totalScheduledTimeInNanos is negative");
        this.totalCpuTimeInNanos = checkNonNegative(totalCpuTimeInNanos, "totalCpuTimeInNanos is negative");
        this.retriedCpuTimeInNanos = checkNonNegative(retriedCpuTimeInNanos, "totalCpuTimeInNanos is negative");
        this.totalBlockedTimeInNanos = checkNonNegative(totalBlockedTimeInNanos, "totalBlockedTimeInNanos is negative");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.totalAllocation = requireNonNull(totalAllocation, "totalAllocation is null");

        this.rawInputDataSize = requireNonNull(rawInputDataSize, "rawInputDataSize is null");
        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;

        this.processedInputDataSize = requireNonNull(processedInputDataSize, "processedInputDataSize is null");
        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        this.shuffledDataSize = requireNonNull(shuffledDataSize, "shuffledDataSize is null");
        checkArgument(shuffledPositions >= 0, "shuffledPositions is negative");
        this.shuffledPositions = shuffledPositions;

        this.outputDataSize = requireNonNull(outputDataSize, "outputDataSize is null");
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        checkArgument(writtenOutputPositions >= 0, "writtenOutputPositions is negative: %s", writtenOutputPositions);
        this.writtenOutputPositions = writtenOutputPositions;
        this.writtenOutputLogicalDataSize = requireNonNull(writtenOutputLogicalDataSize, "writtenOutputLogicalDataSize is null");
        this.writtenOutputPhysicalDataSize = requireNonNull(writtenOutputPhysicalDataSize, "writtenOutputPhysicalDataSize is null");
        this.writtenIntermediatePhysicalDataSize = requireNonNull(writtenIntermediatePhysicalDataSize, "writtenIntermediatePhysicalDataSize is null");

        this.stageGcStatistics = ImmutableList.copyOf(requireNonNull(stageGcStatistics, "stageGcStatistics is null"));

        this.operatorSummaries = ImmutableList.copyOf(requireNonNull(operatorSummaries, "operatorSummaries is null"));

        this.runtimeStats = (runtimeStats == null) ? new RuntimeStats() : runtimeStats;
    }

    @JsonCreator
    public QueryStats(
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("executionStartTime") DateTime executionStartTime,
            @JsonProperty("lastHeartbeat") DateTime lastHeartbeat,
            @JsonProperty("endTime") DateTime endTime,

            @JsonProperty("elapsedTime") Duration elapsedTime,
            @JsonProperty("waitingForPrerequisitesTime") Duration waitingForPrerequisitesTime,
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("resourceWaitingTime") Duration resourceWaitingTime,
            @JsonProperty("semanticAnalyzingTime") Duration semanticAnalyzingTime,
            @JsonProperty("columnAccessPermissionCheckingTime") Duration columnAccessPermissionCheckingTime,
            @JsonProperty("dispatchingTime") Duration dispatchingTime,
            @JsonProperty("executionTime") Duration executionTime,
            @JsonProperty("analysisTime") Duration analysisTime,
            @JsonProperty("totalPlanningTime") Duration totalPlanningTime,
            @JsonProperty("finishingTime") Duration finishingTime,

            @JsonProperty("totalTasks") int totalTasks,
            @JsonProperty("runningTasks") int runningTasks,
            @JsonProperty("peakRunningTasks") int peakRunningTasks,
            @JsonProperty("completedTasks") int completedTasks,

            @JsonProperty("totalDrivers") int totalDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("blockedDrivers") int blockedDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,

            @JsonProperty("cumulativeUserMemory") double cumulativeUserMemory,
            @JsonProperty("cumulativeTotalMemory") double cumulativeTotalMemory,
            @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,
            @JsonProperty("totalMemoryReservation") DataSize totalMemoryReservation,
            @JsonProperty("peakUserMemoryReservation") DataSize peakUserMemoryReservation,
            @JsonProperty("peakTotalMemoryReservation") DataSize peakTotalMemoryReservation,
            @JsonProperty("peakTaskUserMemory") DataSize peakTaskUserMemory,
            @JsonProperty("peakTaskTotalMemory") DataSize peakTaskTotalMemory,
            @JsonProperty("peakNodeTotalMemory") DataSize peakNodeTotalMemory,

            @JsonProperty("scheduled") boolean scheduled,
            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("retriedCpuTime") Duration retriedCpuTime,
            @JsonProperty("totalBlockedTime") Duration totalBlockedTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,

            @JsonProperty("totalAllocation") DataSize totalAllocation,

            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,

            @JsonProperty("processedInputDataSize") DataSize processedInputDataSize,
            @JsonProperty("processedInputPositions") long processedInputPositions,

            @JsonProperty("shuffledDataSize") DataSize shuffledDataSize,
            @JsonProperty("shuffledPositions") long shuffledPositions,

            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("writtenOutputPositions") long writtenOutputPositions,
            @JsonProperty("writtenOutputLogicalDataSize") DataSize writtenOutputLogicalDataSize,
            @JsonProperty("writtenOutputPhysicalDataSize") DataSize writtenOutputPhysicalDataSize,

            @JsonProperty("writtenIntermediatePhysicalDataSize") DataSize writtenIntermediatePhysicalDataSize,

            @JsonProperty("stageGcStatistics") List<StageGcStatistics> stageGcStatistics,

            @JsonProperty("operatorSummaries") List<OperatorStats> operatorSummaries,

            @JsonProperty("runtimeStats") RuntimeStats runtimeStats)
    {
        this(toTimeStampInMillis(createTime),
                toTimeStampInMillis(executionStartTime),
                toTimeStampInMillis(lastHeartbeat),
                toTimeStampInMillis(endTime),

                toTimeStampInNanos(elapsedTime),
                toTimeStampInNanos(waitingForPrerequisitesTime),
                toTimeStampInNanos(queuedTime),
                toTimeStampInNanos(resourceWaitingTime),
                toTimeStampInNanos(semanticAnalyzingTime),
                toTimeStampInNanos(columnAccessPermissionCheckingTime),
                toTimeStampInNanos(dispatchingTime),
                toTimeStampInNanos(executionTime),
                toTimeStampInNanos(analysisTime),
                toTimeStampInNanos(totalPlanningTime),
                toTimeStampInNanos(finishingTime),

                totalTasks,
                runningTasks,
                peakRunningTasks,
                completedTasks,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                blockedDrivers,
                completedDrivers,

                cumulativeUserMemory,
                cumulativeTotalMemory,
                userMemoryReservation,
                totalMemoryReservation,
                peakUserMemoryReservation,
                peakTotalMemoryReservation,
                peakTaskUserMemory,
                peakTaskTotalMemory,
                peakNodeTotalMemory,

                scheduled,
                toTimeStampInNanos(totalScheduledTime),
                toTimeStampInNanos(totalCpuTime),
                toTimeStampInNanos(retriedCpuTime),
                toTimeStampInNanos(totalBlockedTime),
                fullyBlocked,
                blockedReasons,

                totalAllocation,

                rawInputDataSize,
                rawInputPositions,

                processedInputDataSize,
                processedInputPositions,

                shuffledDataSize,
                shuffledPositions,

                outputDataSize,
                outputPositions,

                writtenOutputPositions,
                writtenOutputLogicalDataSize,
                writtenOutputPhysicalDataSize,

                writtenIntermediatePhysicalDataSize,

                stageGcStatistics,
                operatorSummaries,

                runtimeStats);
    }

    public static QueryStats create(
            QueryStateTimer queryStateTimer,
            Optional<StageInfo> rootStage,
            List<StageInfo> allStages,
            int peakRunningTasks,
            long peakUserMemoryReservation,
            long peakTotalMemoryReservation,
            long peakTaskUserMemory,
            long peakTaskTotalMemory,
            long peakNodeTotalMemory,
            RuntimeStats runtimeStats)
    {
        int totalTasks = 0;
        int runningTasks = 0;
        int completedTasks = 0;

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int runningDrivers = 0;
        int blockedDrivers = 0;
        int completedDrivers = 0;

        double cumulativeUserMemory = 0;
        double cumulativeTotalMemory = 0;
        long userMemoryReservation = 0;
        long totalMemoryReservation = 0;

        long totalScheduledTime = 0;
        long totalCpuTime = 0;
        long retriedCpuTime = 0;
        long totalBlockedTime = 0;

        long totalAllocation = 0;

        long rawInputDataSize = 0;
        long rawInputPositions = 0;

        long processedInputDataSize = 0;
        long processedInputPositions = 0;

        long shuffledDataSize = 0;
        long shuffledPositions = 0;

        long outputDataSize = 0;
        long outputPositions = 0;

        long writtenOutputPositions = 0;
        long writtenOutputLogicalDataSize = 0;
        long writtenOutputPhysicalDataSize = 0;

        long writtenIntermediatePhysicalDataSize = 0;

        ImmutableList.Builder<StageGcStatistics> stageGcStatistics = ImmutableList.builderWithExpectedSize(allStages.size());

        boolean fullyBlocked = rootStage.isPresent();
        Set<BlockedReason> blockedReasons = new HashSet<>();

        ImmutableList.Builder<OperatorStats> operatorStatsSummary = ImmutableList.builder();
        RuntimeStats mergedRuntimeStats = RuntimeStats.copyOf(runtimeStats);
        for (StageInfo stageInfo : allStages) {
            StageExecutionStats stageExecutionStats = stageInfo.getLatestAttemptExecutionInfo().getStats();
            totalTasks += stageExecutionStats.getTotalTasks();
            runningTasks += stageExecutionStats.getRunningTasks();
            completedTasks += stageExecutionStats.getCompletedTasks();

            totalDrivers += stageExecutionStats.getTotalDrivers();
            queuedDrivers += stageExecutionStats.getQueuedDrivers();
            runningDrivers += stageExecutionStats.getRunningDrivers();
            blockedDrivers += stageExecutionStats.getBlockedDrivers();
            completedDrivers += stageExecutionStats.getCompletedDrivers();

            cumulativeUserMemory += stageExecutionStats.getCumulativeUserMemory();
            cumulativeTotalMemory += stageExecutionStats.getCumulativeTotalMemory();
            userMemoryReservation += stageExecutionStats.getUserMemoryReservationInBytes();
            totalMemoryReservation += stageExecutionStats.getTotalMemoryReservationInBytes();
            totalScheduledTime += stageExecutionStats.getTotalScheduledTime().roundTo(MILLISECONDS);
            totalCpuTime += stageExecutionStats.getTotalCpuTime().roundTo(MILLISECONDS);
            retriedCpuTime += computeRetriedCpuTime(stageInfo);
            totalBlockedTime += stageExecutionStats.getTotalBlockedTime().roundTo(MILLISECONDS);
            if (!stageInfo.getLatestAttemptExecutionInfo().getState().isDone()) {
                fullyBlocked &= stageExecutionStats.isFullyBlocked();
                blockedReasons.addAll(stageExecutionStats.getBlockedReasons());
            }

            totalAllocation += stageExecutionStats.getTotalAllocationInBytes();

            if (stageInfo.getPlan().isPresent()) {
                PlanFragment plan = stageInfo.getPlan().get();
                for (OperatorStats operatorStats : stageExecutionStats.getOperatorSummaries()) {
                    // NOTE: we need to literally check each operator type to tell if the source is from table input or shuffled input. A stage can have input from both types of source.
                    String operatorType = operatorStats.getOperatorType();
                    if (operatorType.equals(ExchangeOperator.class.getSimpleName()) || operatorType.equals(MergeOperator.class.getSimpleName())) {
                        shuffledPositions += operatorStats.getRawInputPositions();
                        shuffledDataSize += operatorStats.getRawInputDataSizeInBytes();
                    }
                    else if (operatorType.equals(TableScanOperator.class.getSimpleName()) || operatorType.equals(ScanFilterAndProjectOperator.class.getSimpleName())) {
                        rawInputDataSize += operatorStats.getRawInputDataSizeInBytes();
                        rawInputPositions += operatorStats.getRawInputPositions();
                    }
                }
                processedInputDataSize += stageExecutionStats.getProcessedInputDataSizeInBytes();
                processedInputPositions += stageExecutionStats.getProcessedInputPositions();

                if (plan.isOutputTableWriterFragment()) {
                    writtenOutputPositions += stageExecutionStats.getOperatorSummaries().stream()
                            .filter(stats -> stats.getOperatorType().equals(TableWriterOperator.OPERATOR_TYPE))
                            .mapToLong(OperatorStats::getInputPositions)
                            .sum();
                    writtenOutputLogicalDataSize += stageExecutionStats.getOperatorSummaries().stream()
                            .filter(stats -> stats.getOperatorType().equals(TableWriterOperator.OPERATOR_TYPE))
                            .mapToLong(OperatorStats::getInputDataSizeInBytes)
                            .sum();
                    writtenOutputPhysicalDataSize += stageExecutionStats.getPhysicalWrittenDataSizeInBytes();
                }
                else {
                    writtenIntermediatePhysicalDataSize += stageExecutionStats.getPhysicalWrittenDataSizeInBytes();
                }
            }

            stageGcStatistics.add(stageExecutionStats.getGcInfo());

            operatorStatsSummary.addAll(stageExecutionStats.getOperatorSummaries());
            // We prepend each metric name with the stage id to avoid merging metrics across stages.
            int stageId = stageInfo.getStageId().getId();
            stageExecutionStats.getRuntimeStats().getMetrics().forEach((name, metric) -> {
                String metricName = String.format("S%d-%s", stageId, name);
                mergedRuntimeStats.mergeMetric(metricName, metric);
            });
        }

        if (rootStage.isPresent()) {
            StageExecutionStats outputStageStats = rootStage.get().getLatestAttemptExecutionInfo().getStats();
            outputDataSize += outputStageStats.getOutputDataSizeInBytes();
            outputPositions += outputStageStats.getOutputPositions();
        }

        boolean isScheduled = rootStage.isPresent() && allStages.stream()
                .map(StageInfo::getLatestAttemptExecutionInfo)
                .map(StageExecutionInfo::getState)
                .allMatch(state -> (state == StageExecutionState.RUNNING) || state.isDone());

        return new QueryStats(
                queryStateTimer.getCreateTimeInMillis(),
                queryStateTimer.getExecutionStartTimeInMillis(),
                queryStateTimer.getLastHeartbeatInMillis(),
                queryStateTimer.getEndTimeInMillis(),

                queryStateTimer.getElapsedTimeInNanos(),
                queryStateTimer.getWaitingForPrerequisitesTimeInNanos(),
                queryStateTimer.getQueuedTimeInNanos(),
                queryStateTimer.getResourceWaitingTimeInNanos(),
                queryStateTimer.getSemanticAnalyzingTimeInNanos(),
                queryStateTimer.getColumnAccessPermissionCheckingTimeInNanos(),
                queryStateTimer.getDispatchingTimeInNanos(),
                queryStateTimer.getExecutionTimeInNanos(),
                queryStateTimer.getAnalysisTimeInNanos(),
                queryStateTimer.getPlanningTimeInNanos(),
                queryStateTimer.getFinishingTimeInNanos(),

                totalTasks,
                runningTasks,
                peakRunningTasks,
                completedTasks,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                blockedDrivers,
                completedDrivers,

                cumulativeUserMemory,
                cumulativeTotalMemory,
                succinctBytes(userMemoryReservation),
                succinctBytes(totalMemoryReservation),
                succinctBytes(peakUserMemoryReservation),
                succinctBytes(peakTotalMemoryReservation),
                succinctBytes(peakTaskUserMemory),
                succinctBytes(peakTaskTotalMemory),
                succinctBytes(peakNodeTotalMemory),

                isScheduled,

                MILLISECONDS.toNanos(totalScheduledTime),
                MILLISECONDS.toNanos(totalCpuTime),
                MILLISECONDS.toNanos(retriedCpuTime),
                MILLISECONDS.toNanos(totalBlockedTime),
                fullyBlocked,
                blockedReasons,

                succinctBytes(totalAllocation),

                succinctBytes(rawInputDataSize),
                rawInputPositions,
                succinctBytes(processedInputDataSize),
                processedInputPositions,
                succinctBytes(shuffledDataSize),
                shuffledPositions,
                succinctBytes(outputDataSize),
                outputPositions,

                writtenOutputPositions,
                succinctBytes(writtenOutputLogicalDataSize),
                succinctBytes(writtenOutputPhysicalDataSize),

                succinctBytes(writtenIntermediatePhysicalDataSize),

                stageGcStatistics.build(),

                operatorStatsSummary.build(),
                mergedRuntimeStats);
    }

    private static long computeRetriedCpuTime(StageInfo stageInfo)
    {
        long stageRetriedCpuTime = stageInfo.getPreviousAttemptsExecutionInfos().stream()
                .mapToLong(executionInfo -> executionInfo.getStats().getTotalCpuTime().roundTo(MILLISECONDS))
                .sum();
        long taskRetriedCpuTime = stageInfo.getLatestAttemptExecutionInfo().getStats().getRetriedCpuTime().roundTo(MILLISECONDS);
        return stageRetriedCpuTime + taskRetriedCpuTime;
    }

    public static QueryStats immediateFailureQueryStats()
    {
        long now = System.currentTimeMillis();
        return new QueryStats(
                now,
                now,
                now,
                now,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                succinctBytes(0),
                succinctBytes(0),
                succinctBytes(0),
                succinctBytes(0),
                succinctBytes(0),
                succinctBytes(0),
                succinctBytes(0),
                false,
                0L,
                0L,
                0L,
                0L,
                false,
                ImmutableSet.of(),
                succinctBytes(0),
                succinctBytes(0),
                0,
                succinctBytes(0),
                0,
                succinctBytes(0),
                0,
                succinctBytes(0),
                0,
                0,
                succinctBytes(0),
                succinctBytes(0),
                succinctBytes(0),
                ImmutableList.of(),
                ImmutableList.of(),
                new RuntimeStats());
    }

    @JsonProperty
    public DateTime getCreateTime()
    {
        return new DateTime(createTimeInMillis);
    }

    public long getCreateTimeInMillis()
    {
        return createTimeInMillis;
    }

    @JsonProperty
    public DateTime getExecutionStartTime()
    {
        return new DateTime(executionStartTimeInMillis);
    }

    public long getExecutionStartTimeInMillis()
    {
        return executionStartTimeInMillis;
    }

    @JsonProperty
    public DateTime getLastHeartbeat()
    {
        return new DateTime(lastHeartbeatInMillis);
    }

    public long getLastHeartbeatInMillis()
    {
        return lastHeartbeatInMillis;
    }

    @Nullable
    @JsonProperty
    public DateTime getEndTime()
    {
        return new DateTime(endTimeInMillis);
    }

    public long getEndTimeInMillis()
    {
        return endTimeInMillis;
    }

    @JsonProperty
    public Duration getElapsedTime()
    {
        return succinctNanos(elapsedTimeInNanos);
    }

    public long getElapsedTimeInNanos()
    {
        return elapsedTimeInNanos;
    }

    @JsonProperty
    public Duration getWaitingForPrerequisitesTime()
    {
        return succinctNanos(waitingForPrerequisitesTimeInNanos);
    }

    public long getWaitingForPrerequisitesTimeInNanos()
    {
        return waitingForPrerequisitesTimeInNanos;
    }

    @JsonProperty
    public Duration getResourceWaitingTime()
    {
        return succinctNanos(resourceWaitingTimeInNanos);
    }

    public long getResourceWaitingTimeInNanos()
    {
        return resourceWaitingTimeInNanos;
    }

    @JsonProperty
    public Duration getSemanticAnalyzingTime()
    {
        return succinctNanos(semanticAnalyzingTimeInNanos);
    }

    public long getSemanticAnalyzingTimeInNanos()
    {
        return semanticAnalyzingTimeInNanos;
    }

    @JsonProperty
    public Duration getColumnAccessPermissionCheckingTime()
    {
        return succinctNanos(columnAccessPermissionCheckingTimeInNanos);
    }

    public long getColumnAccessPermissionCheckingTimeInNanos()
    {
        return columnAccessPermissionCheckingTimeInNanos;
    }

    @JsonProperty
    public Duration getDispatchingTime()
    {
        return succinctNanos(dispatchingTimeInNanos);
    }

    public long getDispatchingTimeInNanos()
    {
        return dispatchingTimeInNanos;
    }

    @JsonProperty
    public Duration getQueuedTime()
    {
        return succinctNanos(queuedTimeInNanos);
    }

    public long getQueuedTimeInNanos()
    {
        return queuedTimeInNanos;
    }

    @JsonProperty
    public Duration getExecutionTime()
    {
        return succinctNanos(executionTimeInNanos);
    }

    public long getExecutionTimeInNanos()
    {
        return executionTimeInNanos;
    }

    @JsonProperty
    public Duration getAnalysisTime()
    {
        return succinctNanos(analysisTimeInNanos);
    }

    public long getAnalysisTimeInNanos()
    {
        return analysisTimeInNanos;
    }

    @JsonProperty
    public Duration getTotalPlanningTime()
    {
        return succinctNanos(totalPlanningTimeInNanos);
    }

    public long getTotalPlanningTimeInNanos()
    {
        return totalPlanningTimeInNanos;
    }

    @JsonProperty
    public Duration getFinishingTime()
    {
        return succinctNanos(finishingTimeInNanos);
    }

    public long getFinishingTimeInNanos()
    {
        return finishingTimeInNanos;
    }

    @JsonProperty
    public int getTotalTasks()
    {
        return totalTasks;
    }

    @JsonProperty
    public int getRunningTasks()
    {
        return runningTasks;
    }

    @JsonProperty
    public int getPeakRunningTasks()
    {
        return peakRunningTasks;
    }

    @JsonProperty
    public int getCompletedTasks()
    {
        return completedTasks;
    }

    @JsonProperty
    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    @JsonProperty
    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    @JsonProperty
    public int getBlockedDrivers()
    {
        return blockedDrivers;
    }

    @JsonProperty
    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    @JsonProperty
    public double getCumulativeUserMemory()
    {
        return cumulativeUserMemory;
    }

    @JsonProperty
    public double getCumulativeTotalMemory()
    {
        return cumulativeTotalMemory;
    }

    @JsonProperty
    public DataSize getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    @JsonProperty
    public DataSize getTotalMemoryReservation()
    {
        return totalMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakUserMemoryReservation()
    {
        return peakUserMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakTotalMemoryReservation()
    {
        return peakTotalMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakTaskTotalMemory()
    {
        return peakTaskTotalMemory;
    }

    @JsonProperty
    public DataSize getPeakNodeTotalMemory()
    {
        return peakNodeTotalMemory;
    }

    @JsonProperty
    public DataSize getPeakTaskUserMemory()
    {
        return peakTaskUserMemory;
    }

    @JsonProperty
    public boolean isScheduled()
    {
        return scheduled;
    }

    @JsonProperty
    public Duration getTotalScheduledTime()
    {
        return succinctNanos(totalScheduledTimeInNanos);
    }

    public long getTotalScheduledTimeInNanos()
    {
        return totalScheduledTimeInNanos;
    }

    @JsonProperty
    public Duration getTotalCpuTime()
    {
        return succinctNanos(totalCpuTimeInNanos);
    }

    public long getTotalCpuTimeInNanos()
    {
        return totalCpuTimeInNanos;
    }

    @JsonProperty
    public Duration getRetriedCpuTime()
    {
        return succinctNanos(retriedCpuTimeInNanos);
    }

    public long getRetriedCpuTimeInNanos()
    {
        return retriedCpuTimeInNanos;
    }

    @JsonProperty
    public Duration getTotalBlockedTime()
    {
        return succinctNanos(totalBlockedTimeInNanos);
    }

    public long getTotalBlockedTimeInNanos()
    {
        return totalBlockedTimeInNanos;
    }

    @JsonProperty
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    @JsonProperty
    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    @JsonProperty
    public DataSize getTotalAllocation()
    {
        return totalAllocation;
    }

    @JsonProperty
    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    @JsonProperty
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    public DataSize getProcessedInputDataSize()
    {
        return processedInputDataSize;
    }

    @JsonProperty
    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

    @JsonProperty
    public DataSize getShuffledDataSize()
    {
        return shuffledDataSize;
    }

    @JsonProperty
    public long getShuffledPositions()
    {
        return shuffledPositions;
    }

    @JsonProperty
    public DataSize getOutputDataSize()
    {
        return outputDataSize;
    }

    @JsonProperty
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    public long getWrittenOutputPositions()
    {
        return writtenOutputPositions;
    }

    @JsonProperty
    public DataSize getWrittenOutputLogicalDataSize()
    {
        return writtenOutputLogicalDataSize;
    }

    @JsonProperty
    public DataSize getWrittenOutputPhysicalDataSize()
    {
        return writtenOutputPhysicalDataSize;
    }

    @JsonProperty
    public DataSize getWrittenIntermediatePhysicalDataSize()
    {
        return writtenIntermediatePhysicalDataSize;
    }

    @JsonProperty
    public List<StageGcStatistics> getStageGcStatistics()
    {
        return stageGcStatistics;
    }

    @JsonProperty
    public List<OperatorStats> getOperatorSummaries()
    {
        return operatorSummaries;
    }

    @JsonProperty
    public OptionalDouble getProgressPercentage()
    {
        if (!scheduled || totalDrivers == 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(min(100, (completedDrivers * 100.0) / totalDrivers));
    }

    @JsonProperty
    public DataSize getSpilledDataSize()
    {
        return succinctBytes(operatorSummaries.stream()
                .mapToLong(OperatorStats::getSpilledDataSizeInBytes)
                .sum());
    }

    @JsonProperty
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }
}
