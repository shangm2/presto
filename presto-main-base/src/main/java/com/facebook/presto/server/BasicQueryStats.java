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
package com.facebook.presto.server;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.operator.BlockedReason;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;

import java.util.OptionalDouble;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkNonNegative;
import static com.facebook.presto.util.DateTimeUtils.toTimeStampInMillis;
import static com.facebook.presto.util.DurationUtils.toTimeStampInNanos;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.Duration.succinctNanos;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;

/**
 * Lightweight version of QueryStats. Parts of the web UI depend on the fields
 * being named consistently across these classes.
 */
@ThriftStruct
@Immutable
public class BasicQueryStats
{
    private final long createTimeInMillis;
    private final long endTimeInMillis;

    private final long waitingForPrerequisitesTimeInNanos;
    private final long queuedTimeInNanos;
    private final long elapsedTimeInNanos;
    private final long executionTimeInNanos;
    private final long analysisTimeInNanos;

    private final int runningTasks;
    private final int peakRunningTasks;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int completedDrivers;

    private final DataSize rawInputDataSize;
    private final long rawInputPositions;

    private final double cumulativeUserMemory;
    private final double cumulativeTotalMemory;
    private final DataSize userMemoryReservation;
    private final DataSize totalMemoryReservation;
    private final DataSize peakUserMemoryReservation;
    private final DataSize peakTotalMemoryReservation;
    private final DataSize peakTaskTotalMemoryReservation;
    private final DataSize peakNodeTotalMemoryReservation;
    private final long totalCpuTimeInNanos;
    private final long totalScheduledTimeInNanos;

    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final DataSize totalAllocation;

    private final OptionalDouble progressPercentage;

    public BasicQueryStats(
            long createTimeInMillis,
            long endTimeInMillis,
            long waitingForPrerequisitesTimeInNanos,
            long queuedTimeInNanos,
            long elapsedTimeInNanos,
            long executionTimeInNanos,
            long analysisTimeInNanos,
            int runningTasks,
            int peakRunningTasks,
            int totalDrivers,
            int queuedDrivers,
            int runningDrivers,
            int completedDrivers,
            DataSize rawInputDataSize,
            long rawInputPositions,
            double cumulativeUserMemory,
            double cumulativeTotalMemory,
            DataSize userMemoryReservation,
            DataSize totalMemoryReservation,
            DataSize peakUserMemoryReservation,
            DataSize peakTotalMemoryReservation,
            DataSize peakTaskTotalMemoryReservation,
            DataSize peakNodeTotalMemoryReservation,
            long totalCpuTimeInNanos,
            long totalScheduledTimeInNanos,
            boolean fullyBlocked,
            Set<BlockedReason> blockedReasons,
            DataSize totalAllocation,
            OptionalDouble progressPercentage)
    {
        this.createTimeInMillis = createTimeInMillis;
        this.endTimeInMillis = endTimeInMillis;

        this.waitingForPrerequisitesTimeInNanos = checkNonNegative(waitingForPrerequisitesTimeInNanos, "waitingForPrerequisitesTimeInNanos is negative");
        this.queuedTimeInNanos = checkNonNegative(queuedTimeInNanos, "queuedTimeInNanos is negative");
        this.elapsedTimeInNanos = checkNonNegative(elapsedTimeInNanos, "elapsedTimeInNanos is negative");
        this.executionTimeInNanos = checkNonNegative(executionTimeInNanos, "executionTimeInNanos is negative");
        this.analysisTimeInNanos = checkNonNegative(analysisTimeInNanos, "analysisTimeInNanos is negative");
        this.runningTasks = runningTasks;
        this.peakRunningTasks = peakRunningTasks;

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        this.totalDrivers = totalDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        this.queuedDrivers = queuedDrivers;
        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        this.runningDrivers = runningDrivers;
        checkArgument(completedDrivers >= 0, "completedDrivers is negative");
        this.completedDrivers = completedDrivers;

        this.rawInputDataSize = requireNonNull(rawInputDataSize);
        this.rawInputPositions = rawInputPositions;

        this.cumulativeUserMemory = cumulativeUserMemory;
        this.cumulativeTotalMemory = cumulativeTotalMemory;
        this.userMemoryReservation = userMemoryReservation;
        this.totalMemoryReservation = totalMemoryReservation;
        this.peakUserMemoryReservation = peakUserMemoryReservation;
        this.peakTotalMemoryReservation = peakTotalMemoryReservation;
        this.peakTaskTotalMemoryReservation = peakTaskTotalMemoryReservation;
        this.peakNodeTotalMemoryReservation = peakNodeTotalMemoryReservation;
        this.totalCpuTimeInNanos = checkNonNegative(totalCpuTimeInNanos, "totalCpuTimeInNanos is negative");
        this.totalScheduledTimeInNanos = checkNonNegative(totalScheduledTimeInNanos, "totalScheduledTimeInNanos is negative");

        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.totalAllocation = requireNonNull(totalAllocation, "totalAllocation is null");

        this.progressPercentage = requireNonNull(progressPercentage, "progressPercentage is null");
    }

    @ThriftConstructor
    @JsonCreator
    public BasicQueryStats(
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("endTime") DateTime endTime,
            @JsonProperty("waitingForPrerequisitesTime") Duration waitingForPrerequisitesTime,
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("elapsedTime") Duration elapsedTime,
            @JsonProperty("executionTime") Duration executionTime,
            @JsonProperty("analysisTime") Duration analysisTime,
            @JsonProperty("runningTasks") int runningTasks,
            @JsonProperty("peakRunningTasks") int peakRunningTasks,
            @JsonProperty("totalDrivers") int totalDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,
            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,
            @JsonProperty("cumulativeUserMemory") double cumulativeUserMemory,
            @JsonProperty("cumulativeTotalMemory") double cumulativeTotalMemory,
            @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,
            @JsonProperty("totalMemoryReservation") DataSize totalMemoryReservation,
            @JsonProperty("peakUserMemoryReservation") DataSize peakUserMemoryReservation,
            @JsonProperty("peakTotalMemoryReservation") DataSize peakTotalMemoryReservation,
            @JsonProperty("peakTaskTotalMemoryReservation") DataSize peakTaskTotalMemoryReservation,
            @JsonProperty("peakNodeTotalMemoryReservation") DataSize peakNodeTotalMemoryReservation,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,
            @JsonProperty("totalAllocation") DataSize totalAllocation,
            @JsonProperty("progressPercentage") OptionalDouble progressPercentage)
    {
        this(toTimeStampInMillis(createTime),
                toTimeStampInMillis(endTime),
                toTimeStampInNanos(waitingForPrerequisitesTime),
                toTimeStampInNanos(queuedTime),
                toTimeStampInNanos(elapsedTime),
                toTimeStampInNanos(executionTime),
                toTimeStampInNanos(analysisTime),
                runningTasks,
                peakRunningTasks,
                totalDrivers,
                queuedDrivers,
                runningDrivers,
                completedDrivers,
                rawInputDataSize,
                rawInputPositions,
                cumulativeUserMemory,
                cumulativeTotalMemory,
                userMemoryReservation,
                totalMemoryReservation,
                peakUserMemoryReservation,
                peakTotalMemoryReservation,
                peakTaskTotalMemoryReservation,
                peakNodeTotalMemoryReservation,
                toTimeStampInNanos(totalCpuTime),
                toTimeStampInNanos(totalScheduledTime),
                fullyBlocked,
                blockedReasons,
                totalAllocation,
                progressPercentage);
    }

    public BasicQueryStats(QueryStats queryStats)
    {
        this(queryStats.getCreateTimeInMillis(),
                queryStats.getEndTimeInMillis(),
                queryStats.getWaitingForPrerequisitesTimeInNanos(),
                queryStats.getQueuedTimeInNanos(),
                queryStats.getElapsedTimeInNanos(),
                queryStats.getExecutionTimeInNanos(),
                queryStats.getAnalysisTimeInNanos(),
                queryStats.getRunningTasks(),
                queryStats.getPeakRunningTasks(),
                queryStats.getTotalDrivers(),
                queryStats.getQueuedDrivers(),
                queryStats.getRunningDrivers(),
                queryStats.getCompletedDrivers(),
                queryStats.getRawInputDataSize(),
                queryStats.getRawInputPositions(),
                queryStats.getCumulativeUserMemory(),
                queryStats.getCumulativeTotalMemory(),
                queryStats.getUserMemoryReservation(),
                queryStats.getTotalMemoryReservation(),
                queryStats.getPeakUserMemoryReservation(),
                queryStats.getPeakTotalMemoryReservation(),
                queryStats.getPeakTaskTotalMemory(),
                queryStats.getPeakNodeTotalMemory(),
                queryStats.getTotalCpuTimeInNanos(),
                queryStats.getTotalScheduledTimeInNanos(),
                queryStats.isFullyBlocked(),
                queryStats.getBlockedReasons(),
                queryStats.getTotalAllocation(),
                queryStats.getProgressPercentage());
    }

    public static BasicQueryStats immediateFailureQueryStats()
    {
        long now = currentTimeMillis();
        return new BasicQueryStats(
                now,
                now,
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
                new DataSize(0, BYTE),
                0,
                0,
                0,
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                new DataSize(0, BYTE),
                0L,
                0L,
                false,
                ImmutableSet.of(),
                new DataSize(0, BYTE),
                OptionalDouble.empty());
    }

    @ThriftField(1)
    @JsonProperty
    public DateTime getCreateTime()
    {
        return new DateTime(createTimeInMillis);
    }

    public long getCreateTimeInMillis()
    {
        return createTimeInMillis;
    }

    @ThriftField(2)
    @JsonProperty
    public DateTime getEndTime()
    {
        return new DateTime(endTimeInMillis);
    }

    public long getEndTimeInMillis()
    {
        return endTimeInMillis;
    }

    @ThriftField(3)
    @JsonProperty
    public Duration getQueuedTime()
    {
        return succinctNanos(queuedTimeInNanos);
    }

    public long getQueuedTimeInNanos()
    {
        return queuedTimeInNanos;
    }

    @ThriftField(4)
    @JsonProperty
    public Duration getElapsedTime()
    {
        return succinctNanos(elapsedTimeInNanos);
    }

    public long getElapsedTimeInNanos()
    {
        return elapsedTimeInNanos;
    }

    @ThriftField(5)
    @JsonProperty
    public Duration getExecutionTime()
    {
        return succinctNanos(executionTimeInNanos);
    }

    public long getExecutionTimeInNanos()
    {
        return executionTimeInNanos;
    }

    @ThriftField(6)
    @JsonProperty
    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    @ThriftField(7)
    @JsonProperty
    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    @ThriftField(8)
    @JsonProperty
    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    @ThriftField(9)
    @JsonProperty
    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    @ThriftField(10)
    @JsonProperty
    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    @ThriftField(11)
    @JsonProperty
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @ThriftField(12)
    @JsonProperty
    public double getCumulativeUserMemory()
    {
        return cumulativeUserMemory;
    }

    @ThriftField(13)
    @JsonProperty
    public DataSize getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    @ThriftField(14)
    @JsonProperty
    public DataSize getTotalMemoryReservation()
    {
        return totalMemoryReservation;
    }

    @ThriftField(15)
    public int getPeakRunningTasks()
    {
        return peakRunningTasks;
    }

    @ThriftField(16)
    @JsonProperty
    public DataSize getPeakUserMemoryReservation()
    {
        return peakUserMemoryReservation;
    }

    @ThriftField(17)
    @JsonProperty
    public DataSize getPeakTotalMemoryReservation()
    {
        return peakTotalMemoryReservation;
    }

    @ThriftField(18)
    @JsonProperty
    public DataSize getPeakTaskTotalMemoryReservation()
    {
        return peakTaskTotalMemoryReservation;
    }

    @ThriftField(value = 19, name = "peakNodeTotalMemoryReservation")
    @JsonProperty
    public DataSize getPeakNodeTotalMemoryReservation()
    {
        return peakNodeTotalMemoryReservation;
    }

    @ThriftField(20)
    @JsonProperty
    public Duration getTotalCpuTime()
    {
        return succinctNanos(totalCpuTimeInNanos);
    }

    public long getTotalCpuTimeInNanos()
    {
        return totalCpuTimeInNanos;
    }

    @ThriftField(21)
    @JsonProperty
    public Duration getTotalScheduledTime()
    {
        return succinctNanos(totalScheduledTimeInNanos);
    }

    public long getTotalScheduledTimeInNanos()
    {
        return totalScheduledTimeInNanos;
    }

    @ThriftField(22)
    @JsonProperty
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    @ThriftField(23)
    @JsonProperty
    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    @ThriftField(24)
    @JsonProperty
    public DataSize getTotalAllocation()
    {
        return totalAllocation;
    }

    @ThriftField(25)
    @JsonProperty
    public OptionalDouble getProgressPercentage()
    {
        return progressPercentage;
    }

    @ThriftField(26)
    @JsonProperty
    public Duration getWaitingForPrerequisitesTime()
    {
        return succinctNanos(waitingForPrerequisitesTimeInNanos);
    }

    public long getWaitingForPrerequisitesTimeInNanos()
    {
        return waitingForPrerequisitesTimeInNanos;
    }

    @ThriftField(27)
    @JsonProperty
    public double getCumulativeTotalMemory()
    {
        return cumulativeTotalMemory;
    }

    @ThriftField(28)
    @JsonProperty
    public int getRunningTasks()
    {
        return runningTasks;
    }

    @ThriftField(29)
    @JsonProperty
    public Duration getAnalysisTime()
    {
        return succinctNanos(analysisTimeInNanos);
    }

    public long getAnalysisTimeInNanos()
    {
        return analysisTimeInNanos;
    }
}
