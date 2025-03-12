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

import com.facebook.presto.operator.BlockedReason;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.OptionalDouble;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkNonNegative;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class BasicStageExecutionStats
{
    public static final BasicStageExecutionStats EMPTY_STAGE_STATS = new BasicStageExecutionStats(
            false,

            0,
            0,
            0,
            0,

            0L,
            0,

            0.0,
            0.0,
            0L,
            0L,

            0L,
            0L,

            false,
            ImmutableSet.of(),

            0L,

            OptionalDouble.empty());

    private final boolean isScheduled;
    private final int totalDrivers;
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int completedDrivers;
    private final long rawInputDataSizeInBytes;
    private final long rawInputPositions;
    private final double cumulativeUserMemory;
    private final double cumulativeTotalMemory;
    private final long userMemoryReservationInBytes;
    private final long totalMemoryReservationInBytes;
    private final long totalCpuTimeInNanos;
    private final long totalScheduledTimeInNanos;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;
    private final long totalAllocationInBytes;
    private final OptionalDouble progressPercentage;

    public BasicStageExecutionStats(
            boolean isScheduled,

            int totalDrivers,
            int queuedDrivers,
            int runningDrivers,
            int completedDrivers,

            long rawInputDataSizeInBytes,
            long rawInputPositions,

            double cumulativeUserMemory,
            double cumulativeTotalMemory,
            long userMemoryReservationInBytes,
            long totalMemoryReservationInBytes,

            long totalCpuTimeInNanos,
            long totalScheduledTimeInNanos,

            boolean fullyBlocked,
            Set<BlockedReason> blockedReasons,

            long totalAllocationInBytes,

            OptionalDouble progressPercentage)
    {
        this.isScheduled = isScheduled;
        this.totalDrivers = totalDrivers;
        this.queuedDrivers = queuedDrivers;
        this.runningDrivers = runningDrivers;
        this.completedDrivers = completedDrivers;
        checkArgument(rawInputDataSizeInBytes >= 0, "rawInputDataSizeInBytes is negative");
        this.rawInputDataSizeInBytes = rawInputDataSizeInBytes;
        this.rawInputPositions = rawInputPositions;
        this.cumulativeUserMemory = cumulativeUserMemory;
        this.cumulativeTotalMemory = cumulativeTotalMemory;
        checkArgument(userMemoryReservationInBytes >= 0, "userMemoryReservationInBytes is negative");
        this.userMemoryReservationInBytes = userMemoryReservationInBytes;
        checkArgument(totalMemoryReservationInBytes >= 0, "totalMemoryReservationInBytes is negative");
        this.totalMemoryReservationInBytes = totalMemoryReservationInBytes;
        this.totalCpuTimeInNanos = checkNonNegative(totalCpuTimeInNanos, "totalCpuTime is negative");
        this.totalScheduledTimeInNanos = checkNonNegative(totalScheduledTimeInNanos, "totalScheduledTime is negative");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));
        checkArgument(totalAllocationInBytes >= 0, "totalAllocationInBytes is negative");
        this.totalAllocationInBytes = totalAllocationInBytes;
        this.progressPercentage = requireNonNull(progressPercentage, "progressPercentage is null");
    }

    public boolean isScheduled()
    {
        return isScheduled;
    }

    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    public long getRawInputDataSizeInBytes()
    {
        return rawInputDataSizeInBytes;
    }

    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    public double getCumulativeUserMemory()
    {
        return cumulativeUserMemory;
    }

    public double getCumulativeTotalMemory()
    {
        return cumulativeTotalMemory;
    }

    public long getUserMemoryReservationInBytes()
    {
        return userMemoryReservationInBytes;
    }

    public long getTotalMemoryReservationInBytes()
    {
        return totalMemoryReservationInBytes;
    }

    public long getTotalCpuTimeInNanos()
    {
        return totalCpuTimeInNanos;
    }

    public long getTotalScheduledTimeInNanos()
    {
        return totalScheduledTimeInNanos;
    }

    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    public long getTotalAllocationInBytes()
    {
        return totalAllocationInBytes;
    }

    public OptionalDouble getProgressPercentage()
    {
        return progressPercentage;
    }

    public static BasicStageExecutionStats aggregateBasicStageStats(Iterable<BasicStageExecutionStats> stages)
    {
        int totalDrivers = 0;
        int queuedDrivers = 0;
        int runningDrivers = 0;
        int completedDrivers = 0;

        double cumulativeUserMemory = 0;
        double cumulativeTotalMemory = 0;
        long userMemoryReservation = 0;
        long totalMemoryReservation = 0;

        long totalScheduledTimeInNanos = 0;
        long totalCpuTimeInNanos = 0;

        long rawInputDataSize = 0;
        long rawInputPositions = 0;

        boolean isScheduled = true;

        boolean fullyBlocked = true;
        Set<BlockedReason> blockedReasons = new HashSet<>();

        long totalAllocation = 0;

        for (BasicStageExecutionStats stageStats : stages) {
            totalDrivers += stageStats.getTotalDrivers();
            queuedDrivers += stageStats.getQueuedDrivers();
            runningDrivers += stageStats.getRunningDrivers();
            completedDrivers += stageStats.getCompletedDrivers();

            cumulativeUserMemory += stageStats.getCumulativeUserMemory();
            cumulativeTotalMemory += stageStats.getCumulativeTotalMemory();
            userMemoryReservation += stageStats.getUserMemoryReservationInBytes();
            totalMemoryReservation += stageStats.getTotalMemoryReservationInBytes();

            totalScheduledTimeInNanos += stageStats.getTotalScheduledTimeInNanos();
            totalCpuTimeInNanos += stageStats.getTotalCpuTimeInNanos();

            isScheduled &= stageStats.isScheduled();

            fullyBlocked &= stageStats.isFullyBlocked();
            blockedReasons.addAll(stageStats.getBlockedReasons());

            totalAllocation += stageStats.getTotalAllocationInBytes();

            rawInputDataSize += stageStats.getRawInputDataSizeInBytes();
            rawInputPositions += stageStats.getRawInputPositions();
        }

        OptionalDouble progressPercentage = OptionalDouble.empty();
        if (isScheduled && totalDrivers != 0) {
            progressPercentage = OptionalDouble.of(min(100, (completedDrivers * 100.0) / totalDrivers));
        }

        return new BasicStageExecutionStats(
                isScheduled,

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

                totalCpuTimeInNanos,
                totalScheduledTimeInNanos,

                fullyBlocked,
                blockedReasons,

                totalAllocation,

                progressPercentage);
    }
}
