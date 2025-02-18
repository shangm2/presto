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
package com.facebook.presto.operator;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.execution.Lifespan;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Set;

import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Immutable
@ThriftStruct
public class DriverStats
{
    private final Lifespan lifespan;

    private final DateTime createTime;
    private final DateTime startTime;
    private final DateTime endTime;

    private final long queuedTimeInMillis;
    private final long elapsedTimeInMillis;

    private final long userMemoryReservationInBytes;
    private final long revocableMemoryReservationInBytes;
    private final long systemMemoryReservationInBytes;

    private final long totalScheduledTimeInMillis;
    private final long totalCpuTimeInMillis;
    private final long totalBlockedTimeInMillis;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final long totalAllocationInBytes;

    private final long rawInputDataSizeInBytes;
    private final long rawInputPositions;
    private final long rawInputReadTimeInMillis;

    private final long processedInputDataSizeInBytes;
    private final long processedInputPositions;

    private final long outputDataSizeInBytes;
    private final long outputPositions;

    private final long physicalWrittenDataSizeInBytes;

    private final List<OperatorStats> operatorStats;

    public DriverStats()
    {
        this.lifespan = null;

        this.createTime = DateTime.now();
        this.startTime = null;
        this.endTime = null;
        this.queuedTimeInMillis = 0L;
        this.elapsedTimeInMillis = 0L;

        this.userMemoryReservationInBytes = 0L;
        this.revocableMemoryReservationInBytes = 0L;
        this.systemMemoryReservationInBytes = 0L;

        this.totalScheduledTimeInMillis = 0L;
        this.totalCpuTimeInMillis = 0L;
        this.totalBlockedTimeInMillis = 0L;
        this.fullyBlocked = false;
        this.blockedReasons = ImmutableSet.of();

        this.totalAllocationInBytes = 0L;

        this.rawInputDataSizeInBytes = 0L;
        this.rawInputPositions = 0;
        this.rawInputReadTimeInMillis = 0L;

        this.processedInputDataSizeInBytes = 0L;
        this.processedInputPositions = 0;

        this.outputDataSizeInBytes = 0L;
        this.outputPositions = 0;

        this.physicalWrittenDataSizeInBytes = 0L;

        this.operatorStats = ImmutableList.of();
    }

    @JsonCreator
    @ThriftConstructor
    public DriverStats(
            @JsonProperty("lifespan") Lifespan lifespan,

            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("startTime") DateTime startTime,
            @JsonProperty("endTime") DateTime endTime,
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("elapsedTime") Duration elapsedTime,

            @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,
            @JsonProperty("revocableMemoryReservation") DataSize revocableMemoryReservation,
            @JsonProperty("systemMemoryReservation") DataSize systemMemoryReservation,

            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("totalBlockedTime") Duration totalBlockedTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,

            @JsonProperty("totalAllocation") DataSize totalAllocation,

            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,
            @JsonProperty("rawInputReadTime") Duration rawInputReadTime,

            @JsonProperty("processedInputDataSize") DataSize processedInputDataSize,
            @JsonProperty("processedInputPositions") long processedInputPositions,

            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("physicalWrittenDataSize") DataSize physicalWrittenDataSize,

            @JsonProperty("operatorStats") List<OperatorStats> operatorStats)
    {
        this.lifespan = lifespan;

        this.createTime = requireNonNull(createTime, "createTime is null");
        this.startTime = startTime;
        this.endTime = endTime;
        this.queuedTimeInMillis = requireNonNull(queuedTime, "queuedTime is null").toMillis();
        this.elapsedTimeInMillis = requireNonNull(elapsedTime, "elapsedTime is null").toMillis();

        this.userMemoryReservationInBytes = requireNonNull(userMemoryReservation, "userMemoryReservation is null").toBytes();
        this.revocableMemoryReservationInBytes = requireNonNull(revocableMemoryReservation, "revocableMemoryReservation is null").toBytes();
        this.systemMemoryReservationInBytes = requireNonNull(systemMemoryReservation, "systemMemoryReservation is null").toBytes();

        this.totalScheduledTimeInMillis = requireNonNull(totalScheduledTime, "totalScheduledTime is null").toMillis();
        this.totalCpuTimeInMillis = requireNonNull(totalCpuTime, "totalCpuTime is null").toMillis();
        this.totalBlockedTimeInMillis = requireNonNull(totalBlockedTime, "totalBlockedTime is null").toMillis();
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.totalAllocationInBytes = requireNonNull(totalAllocation, "totalAllocation is null").toBytes();

        this.rawInputDataSizeInBytes = requireNonNull(rawInputDataSize, "rawInputDataSize is null").toBytes();
        Preconditions.checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;
        this.rawInputReadTimeInMillis = requireNonNull(rawInputReadTime, "rawInputReadTime is null").toMillis();

        this.processedInputDataSizeInBytes = requireNonNull(processedInputDataSize, "processedInputDataSize is null").toBytes();
        Preconditions.checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;

        this.outputDataSizeInBytes = requireNonNull(outputDataSize, "outputDataSize is null").toBytes();
        Preconditions.checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        this.physicalWrittenDataSizeInBytes = requireNonNull(physicalWrittenDataSize, "writtenDataSize is null").toBytes();

        this.operatorStats = ImmutableList.copyOf(requireNonNull(operatorStats, "operatorStats is null"));
    }

    @JsonProperty
    @ThriftField(1)
    public Lifespan getLifespan()
    {
        return lifespan;
    }

    @JsonProperty
    @ThriftField(2)
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @Nullable
    @JsonProperty
    @ThriftField(3)
    public DateTime getStartTime()
    {
        return startTime;
    }

    @Nullable
    @JsonProperty
    @ThriftField(4)
    public DateTime getEndTime()
    {
        return endTime;
    }

    @JsonProperty
    @ThriftField(5)
    public Duration getQueuedTime()
    {
        return succinctDuration(queuedTimeInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(6)
    public Duration getElapsedTime()
    {
        return succinctDuration(elapsedTimeInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(7)
    public DataSize getUserMemoryReservation()
    {
        return succinctBytes(userMemoryReservationInBytes);
    }

    @JsonProperty
    @ThriftField(8)
    public DataSize getRevocableMemoryReservation()
    {
        return succinctBytes(revocableMemoryReservationInBytes);
    }

    @JsonProperty
    @ThriftField(9)
    public DataSize getSystemMemoryReservation()
    {
        return succinctBytes(systemMemoryReservationInBytes);
    }

    @JsonProperty
    @ThriftField(10)
    public Duration getTotalScheduledTime()
    {
        return succinctDuration(totalScheduledTimeInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(11)
    public Duration getTotalCpuTime()
    {
        return succinctDuration(totalCpuTimeInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(12)
    public Duration getTotalBlockedTime()
    {
        return succinctDuration(totalBlockedTimeInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(13)
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    @JsonProperty
    @ThriftField(14)
    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    @JsonProperty
    @ThriftField(15)
    public DataSize getTotalAllocation()
    {
        return succinctBytes(totalAllocationInBytes);
    }

    @JsonProperty
    @ThriftField(16)
    public DataSize getRawInputDataSize()
    {
        return succinctBytes(rawInputDataSizeInBytes);
    }

    @JsonProperty
    @ThriftField(17)
    public Duration getRawInputReadTime()
    {
        return succinctDuration(rawInputReadTimeInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(18)
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    @ThriftField(19)
    public DataSize getProcessedInputDataSize()
    {
        return succinctBytes(processedInputDataSizeInBytes);
    }

    @JsonProperty
    @ThriftField(20)
    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

    @JsonProperty
    @ThriftField(21)
    public DataSize getOutputDataSize()
    {
        return succinctBytes(outputDataSizeInBytes);
    }

    @JsonProperty
    @ThriftField(22)
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    @ThriftField(23)
    public DataSize getPhysicalWrittenDataSize()
    {
        return succinctBytes(physicalWrittenDataSizeInBytes);
    }

    @JsonProperty
    @ThriftField(24)
    public List<OperatorStats> getOperatorStats()
    {
        return operatorStats;
    }
}
