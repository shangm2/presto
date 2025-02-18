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
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static io.airlift.units.Duration.succinctNanos;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Immutable
@ThriftStruct
public class OperatorStats
{
    private final int stageId;
    private final int stageExecutionId;
    private final int pipelineId;
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final String operatorType;

    private final long totalDrivers;

    private final long isBlockedCalls;
    private final long isBlockedWallInMillis;
    private final long isBlockedCpuInMillis;
    private final long isBlockedAllocation;

    private final long addInputCalls;
    private final long addInputWallInMillis;
    private final long addInputCpuInMillis;
    private final long addInputAllocationInBytes;
    private final long rawInputDataSizeInBytes;
    private final long rawInputPositions;
    private final long inputDataSizeInBytes;
    private final long inputPositions;
    private final double sumSquaredInputPositions;

    private final long getOutputCalls;
    private final long getOutputWallInMillis;
    private final long getOutputCpuInMillis;
    private final long getOutputAllocationInBytes;
    private final long outputDataSizeInBytes;
    private final long outputPositions;

    private final long physicalWrittenDataSizeInBytes;

    private final long additionalCpuInMillis;
    private final long blockedWallInMillis;

    private final long finishCalls;
    private final long finishWallInMillis;
    private final long finishCpuInMillis;
    private final long finishAllocationInBytes;

    private final long userMemoryReservationInBytes;
    private final long revocableMemoryReservationInBytes;
    private final long systemMemoryReservationInBytes;
    private final long peakUserMemoryReservationInBytes;
    private final long peakSystemMemoryReservationInBytes;
    private final long peakTotalMemoryReservationInBytes;

    private final long spilledDataSizeInBytes;

    private final Optional<BlockedReason> blockedReason;

    @Nullable
    private final OperatorInfo info;
    @Nullable
    private final OperatorInfoUnion infoUnion;

    private final RuntimeStats runtimeStats;

    private final DynamicFilterStats dynamicFilterStats;

    private final long nullJoinBuildKeyCount;
    private final long joinBuildKeyCount;
    private final long nullJoinProbeKeyCount;
    private final long joinProbeKeyCount;

    @JsonCreator
    public OperatorStats(
            @JsonProperty("stageId") int stageId,
            @JsonProperty("stageExecutionId") int stageExecutionId,
            @JsonProperty("pipelineId") int pipelineId,
            @JsonProperty("operatorId") int operatorId,
            @JsonProperty("planNodeId") PlanNodeId planNodeId,
            @JsonProperty("operatorType") String operatorType,

            @JsonProperty("totalDrivers") long totalDrivers,

            @JsonProperty("isBlockedCalls") long isBlockedCalls,
            @JsonProperty("isBlockedWall") Duration isBlockedWall,
            @JsonProperty("isBlockedCpu") Duration isBlockedCpu,
            @JsonProperty("isBlockedAllocation") DataSize isBlockedAllocation,

            @JsonProperty("addInputCalls") long addInputCalls,
            @JsonProperty("addInputWall") Duration addInputWall,
            @JsonProperty("addInputCpu") Duration addInputCpu,
            @JsonProperty("addInputAllocation") DataSize addInputAllocation,
            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,
            @JsonProperty("inputDataSize") DataSize inputDataSize,
            @JsonProperty("inputPositions") long inputPositions,
            @JsonProperty("sumSquaredInputPositions") double sumSquaredInputPositions,

            @JsonProperty("getOutputCalls") long getOutputCalls,
            @JsonProperty("getOutputWall") Duration getOutputWall,
            @JsonProperty("getOutputCpu") Duration getOutputCpu,
            @JsonProperty("getOutputAllocation") DataSize getOutputAllocation,
            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("outputPositions") long outputPositions,

            @JsonProperty("physicalWrittenDataSize") DataSize physicalWrittenDataSize,

            @JsonProperty("additionalCpu") Duration additionalCpu,
            @JsonProperty("blockedWall") Duration blockedWall,

            @JsonProperty("finishCalls") long finishCalls,
            @JsonProperty("finishWall") Duration finishWall,
            @JsonProperty("finishCpu") Duration finishCpu,
            @JsonProperty("finishAllocation") DataSize finishAllocation,

            @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,
            @JsonProperty("revocableMemoryReservation") DataSize revocableMemoryReservation,
            @JsonProperty("systemMemoryReservation") DataSize systemMemoryReservation,
            @JsonProperty("peakUserMemoryReservation") DataSize peakUserMemoryReservation,
            @JsonProperty("peakSystemMemoryReservation") DataSize peakSystemMemoryReservation,
            @JsonProperty("peakTotalMemoryReservation") DataSize peakTotalMemoryReservation,

            @JsonProperty("spilledDataSize") DataSize spilledDataSize,

            @JsonProperty("blockedReason") Optional<BlockedReason> blockedReason,

            @Nullable
            @JsonProperty("info") OperatorInfo info,
            @JsonProperty("runtimeStats") RuntimeStats runtimeStats,
            @JsonProperty("dynamicFilterStats") DynamicFilterStats dynamicFilterStats,
            @JsonProperty("nullJoinBuildKeyCount") long nullJoinBuildKeyCount,
            @JsonProperty("joinBuildKeyCount") long joinBuildKeyCount,
            @JsonProperty("nullJoinProbeKeyCount") long nullJoinProbeKeyCount,
            @JsonProperty("joinProbeKeyCount") long joinProbeKeyCount)
    {
        this.stageId = stageId;
        this.stageExecutionId = stageExecutionId;
        this.pipelineId = pipelineId;

        checkArgument(operatorId >= 0, "operatorId is negative");
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.operatorType = requireNonNull(operatorType, "operatorType is null");

        this.totalDrivers = totalDrivers;

        this.isBlockedCalls = isBlockedCalls;
        this.isBlockedWallInMillis = requireNonNull(isBlockedWall, "isBlockedWall is null").toMillis();
        this.isBlockedCpuInMillis = requireNonNull(isBlockedCpu, "isBlockedCpu is null").toMillis();
        this.isBlockedAllocation = requireNonNull(isBlockedAllocation, "isBlockedAllocation is null").toBytes();

        this.addInputCalls = addInputCalls;
        this.addInputWallInMillis = requireNonNull(addInputWall, "addInputWall is null").toMillis();
        this.addInputCpuInMillis = requireNonNull(addInputCpu, "addInputCpu is null").toMillis();
        this.addInputAllocationInBytes = requireNonNull(addInputAllocation, "addInputAllocation is null").toBytes();
        this.rawInputDataSizeInBytes = requireNonNull(rawInputDataSize, "rawInputDataSize is null").toBytes();
        this.rawInputPositions = requireNonNull(rawInputPositions, "rawInputPositions is null");
        this.inputDataSizeInBytes = requireNonNull(inputDataSize, "inputDataSize is null").toBytes();
        checkArgument(inputPositions >= 0, "inputPositions is negative");
        this.inputPositions = inputPositions;
        this.sumSquaredInputPositions = sumSquaredInputPositions;

        this.getOutputCalls = getOutputCalls;
        this.getOutputWallInMillis = requireNonNull(getOutputWall, "getOutputWall is null").toMillis();
        this.getOutputCpuInMillis = requireNonNull(getOutputCpu, "getOutputCpu is null").toMillis();
        this.getOutputAllocationInBytes = requireNonNull(getOutputAllocation, "getOutputAllocation is null").toBytes();
        this.outputDataSizeInBytes = requireNonNull(outputDataSize, "outputDataSize is null").toBytes();
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        this.physicalWrittenDataSizeInBytes = requireNonNull(physicalWrittenDataSize, "writtenDataSize is null").toBytes();

        this.additionalCpuInMillis = requireNonNull(additionalCpu, "additionalCpu is null").toMillis();
        this.blockedWallInMillis = requireNonNull(blockedWall, "blockedWall is null").toMillis();

        this.finishCalls = finishCalls;
        this.finishWallInMillis = requireNonNull(finishWall, "finishWall is null").toMillis();
        this.finishCpuInMillis = requireNonNull(finishCpu, "finishCpu is null").toMillis();
        this.finishAllocationInBytes = requireNonNull(finishAllocation, "finishAllocation is null").toBytes();

        this.userMemoryReservationInBytes = requireNonNull(userMemoryReservation, "userMemoryReservation is null").toBytes();
        this.revocableMemoryReservationInBytes = requireNonNull(revocableMemoryReservation, "revocableMemoryReservation is null").toBytes();
        this.systemMemoryReservationInBytes = requireNonNull(systemMemoryReservation, "systemMemoryReservation is null").toBytes();

        this.peakUserMemoryReservationInBytes = requireNonNull(peakUserMemoryReservation, "peakUserMemoryReservation is null").toBytes();
        this.peakSystemMemoryReservationInBytes = requireNonNull(peakSystemMemoryReservation, "peakSystemMemoryReservation is null").toBytes();
        this.peakTotalMemoryReservationInBytes = requireNonNull(peakTotalMemoryReservation, "peakTotalMemoryReservation is null").toBytes();

        this.spilledDataSizeInBytes = requireNonNull(spilledDataSize, "spilledDataSize is null").toBytes();

        this.runtimeStats = runtimeStats;

        this.dynamicFilterStats = dynamicFilterStats;

        this.blockedReason = blockedReason;

        this.info = info;
        this.infoUnion = null;
        this.nullJoinBuildKeyCount = nullJoinBuildKeyCount;
        this.joinBuildKeyCount = joinBuildKeyCount;
        this.nullJoinProbeKeyCount = nullJoinProbeKeyCount;
        this.joinProbeKeyCount = joinProbeKeyCount;
    }

    @ThriftConstructor
    public OperatorStats(
            int stageId,
            int stageExecutionId,
            int pipelineId,
            int operatorId,
            PlanNodeId planNodeId,
            String operatorType,

            long totalDrivers,

            long isBlockedCalls,
            Duration isBlockedWall,
            Duration isBlockedCpu,
            DataSize isBlockedAllocation,

            long addInputCalls,
            Duration addInputWall,
            Duration addInputCpu,
            DataSize addInputAllocation,
            DataSize rawInputDataSize,
            long rawInputPositions,
            DataSize inputDataSize,
            long inputPositions,
            double sumSquaredInputPositions,

            long getOutputCalls,
            Duration getOutputWall,
            Duration getOutputCpu,
            DataSize getOutputAllocation,
            DataSize outputDataSize,
            long outputPositions,

            DataSize physicalWrittenDataSize,

            Duration additionalCpu,
            Duration blockedWall,

            long finishCalls,
            Duration finishWall,
            Duration finishCpu,
            DataSize finishAllocation,

            DataSize userMemoryReservation,
            DataSize revocableMemoryReservation,
            DataSize systemMemoryReservation,
            DataSize peakUserMemoryReservation,
            DataSize peakSystemMemoryReservation,
            DataSize peakTotalMemoryReservation,

            DataSize spilledDataSize,

            Optional<BlockedReason> blockedReason,

            RuntimeStats runtimeStats,
            DynamicFilterStats dynamicFilterStats,
            @Nullable
            OperatorInfoUnion infoUnion,
            long nullJoinBuildKeyCount,
            long joinBuildKeyCount,
            long nullJoinProbeKeyCount,
            long joinProbeKeyCount)
    {
        this.stageId = stageId;
        this.stageExecutionId = stageExecutionId;
        this.pipelineId = pipelineId;

        checkArgument(operatorId >= 0, "operatorId is negative");
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.operatorType = requireNonNull(operatorType, "operatorType is null");

        this.totalDrivers = totalDrivers;

        this.isBlockedCalls = isBlockedCalls;
        this.isBlockedWallInMillis = requireNonNull(isBlockedWall, "isBlockedWall is null").toMillis();
        this.isBlockedCpuInMillis = requireNonNull(isBlockedCpu, "isBlockedCpu is null").toMillis();
        this.isBlockedAllocation = requireNonNull(isBlockedAllocation, "isBlockedAllocation is null").toBytes();

        this.addInputCalls = addInputCalls;
        this.addInputWallInMillis = requireNonNull(addInputWall, "addInputWall is null").toMillis();
        this.addInputCpuInMillis = requireNonNull(addInputCpu, "addInputCpu is null").toMillis();
        this.addInputAllocationInBytes = requireNonNull(addInputAllocation, "addInputAllocation is null").toBytes();
        this.rawInputDataSizeInBytes = requireNonNull(rawInputDataSize, "rawInputDataSize is null").toBytes();
        this.rawInputPositions = requireNonNull(rawInputPositions, "rawInputPositions is null");
        this.inputDataSizeInBytes = requireNonNull(inputDataSize, "inputDataSize is null").toBytes();
        checkArgument(inputPositions >= 0, "inputPositions is negative");
        this.inputPositions = inputPositions;
        this.sumSquaredInputPositions = sumSquaredInputPositions;

        this.getOutputCalls = getOutputCalls;
        this.getOutputWallInMillis = requireNonNull(getOutputWall, "getOutputWall is null").toMillis();
        this.getOutputCpuInMillis = requireNonNull(getOutputCpu, "getOutputCpu is null").toMillis();
        this.getOutputAllocationInBytes = requireNonNull(getOutputAllocation, "getOutputAllocation is null").toBytes();
        this.outputDataSizeInBytes = requireNonNull(outputDataSize, "outputDataSize is null").toBytes();
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;

        this.physicalWrittenDataSizeInBytes = requireNonNull(physicalWrittenDataSize, "writtenDataSize is null").toBytes();

        this.additionalCpuInMillis = requireNonNull(additionalCpu, "additionalCpu is null").toMillis();
        this.blockedWallInMillis = requireNonNull(blockedWall, "blockedWall is null").toMillis();

        this.finishCalls = finishCalls;
        this.finishWallInMillis = requireNonNull(finishWall, "finishWall is null").toMillis();
        this.finishCpuInMillis = requireNonNull(finishCpu, "finishCpu is null").toMillis();
        this.finishAllocationInBytes = requireNonNull(finishAllocation, "finishAllocation is null").toBytes();

        this.userMemoryReservationInBytes = requireNonNull(userMemoryReservation, "userMemoryReservation is null").toBytes();
        this.revocableMemoryReservationInBytes = requireNonNull(revocableMemoryReservation, "revocableMemoryReservation is null").toBytes();
        this.systemMemoryReservationInBytes = requireNonNull(systemMemoryReservation, "systemMemoryReservation is null").toBytes();

        this.peakUserMemoryReservationInBytes = requireNonNull(peakUserMemoryReservation, "peakUserMemoryReservation is null").toBytes();
        this.peakSystemMemoryReservationInBytes = requireNonNull(peakSystemMemoryReservation, "peakSystemMemoryReservation is null").toBytes();
        this.peakTotalMemoryReservationInBytes = requireNonNull(peakTotalMemoryReservation, "peakTotalMemoryReservation is null").toBytes();

        this.spilledDataSizeInBytes = requireNonNull(spilledDataSize, "spilledDataSize is null").toBytes();

        this.runtimeStats = runtimeStats;

        this.dynamicFilterStats = dynamicFilterStats;

        this.blockedReason = blockedReason;

        this.infoUnion = infoUnion;
        this.info = null;
        this.nullJoinBuildKeyCount = nullJoinBuildKeyCount;
        this.joinBuildKeyCount = joinBuildKeyCount;
        this.nullJoinProbeKeyCount = nullJoinProbeKeyCount;
        this.joinProbeKeyCount = joinProbeKeyCount;
    }

    @JsonProperty
    @ThriftField(1)
    public int getStageId()
    {
        return stageId;
    }

    @JsonProperty
    @ThriftField(2)
    public int getStageExecutionId()
    {
        return stageExecutionId;
    }

    @JsonProperty
    @ThriftField(3)
    public int getPipelineId()
    {
        return pipelineId;
    }

    @JsonProperty
    @ThriftField(4)
    public int getOperatorId()
    {
        return operatorId;
    }

    @JsonProperty
    @ThriftField(5)
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @JsonProperty
    @ThriftField(6)
    public String getOperatorType()
    {
        return operatorType;
    }

    @JsonProperty
    @ThriftField(7)
    public long getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    @ThriftField(8)
    public long getAddInputCalls()
    {
        return addInputCalls;
    }

    @JsonProperty
    @ThriftField(9)
    public Duration getAddInputWall()
    {
        return succinctDuration(addInputWallInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(10)
    public Duration getAddInputCpu()
    {
        return succinctDuration(addInputCpuInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(11)
    public DataSize getAddInputAllocation()
    {
        return succinctBytes(addInputAllocationInBytes);
    }

    @JsonProperty
    @ThriftField(12)
    public DataSize getRawInputDataSize()
    {
        return succinctBytes(rawInputDataSizeInBytes);
    }

    @JsonProperty
    @ThriftField(13)
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    @ThriftField(14)
    public DataSize getInputDataSize()
    {
        return succinctBytes(inputDataSizeInBytes);
    }

    @JsonProperty
    @ThriftField(15)
    public long getInputPositions()
    {
        return inputPositions;
    }

    @JsonProperty
    @ThriftField(16)
    public double getSumSquaredInputPositions()
    {
        return sumSquaredInputPositions;
    }

    @JsonProperty
    @ThriftField(17)
    public long getGetOutputCalls()
    {
        return getOutputCalls;
    }

    @JsonProperty
    @ThriftField(18)
    public Duration getGetOutputWall()
    {
        return succinctDuration(getOutputWallInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(19)
    public Duration getGetOutputCpu()
    {
        return succinctDuration(getOutputCpuInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(20)
    public DataSize getGetOutputAllocation()
    {
        return succinctBytes(getOutputAllocationInBytes);
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
    public Duration getAdditionalCpu()
    {
        return succinctDuration(additionalCpuInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(25)
    public Duration getBlockedWall()
    {
        return succinctDuration(blockedWallInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(26)
    public long getFinishCalls()
    {
        return finishCalls;
    }

    @JsonProperty
    @ThriftField(27)
    public Duration getFinishWall()
    {
        return succinctDuration(finishWallInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(28)
    public Duration getFinishCpu()
    {
        return succinctDuration(finishCpuInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(29)
    public DataSize getFinishAllocation()
    {
        return succinctBytes(finishAllocationInBytes);
    }

    @JsonProperty
    @ThriftField(30)
    public DataSize getUserMemoryReservation()
    {
        return succinctBytes(userMemoryReservationInBytes);
    }

    @JsonProperty
    @ThriftField(31)
    public DataSize getRevocableMemoryReservation()
    {
        return succinctBytes(revocableMemoryReservationInBytes);
    }

    @JsonProperty
    @ThriftField(32)
    public DataSize getSystemMemoryReservation()
    {
        return succinctBytes(systemMemoryReservationInBytes);
    }

    @JsonProperty
    @ThriftField(33)
    public DataSize getPeakUserMemoryReservation()
    {
        return succinctBytes(peakUserMemoryReservationInBytes);
    }

    @JsonProperty
    @ThriftField(34)
    public DataSize getPeakSystemMemoryReservation()
    {
        return succinctBytes(peakSystemMemoryReservationInBytes);
    }

    @JsonProperty
    @ThriftField(35)
    public DataSize getPeakTotalMemoryReservation()
    {
        return succinctBytes(peakTotalMemoryReservationInBytes);
    }

    @JsonProperty
    @ThriftField(36)
    public DataSize getSpilledDataSize()
    {
        return succinctBytes(spilledDataSizeInBytes);
    }

    @Nullable
    @JsonProperty
    @ThriftField(37)
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    @JsonProperty
    @ThriftField(38)
    public Optional<BlockedReason> getBlockedReason()
    {
        return blockedReason;
    }

    @Nullable
    @JsonProperty
    public OperatorInfo getInfo()
    {
        return info;
    }

    @Nullable
    @ThriftField(39)
    public OperatorInfoUnion getInfoUnion()
    {
        return infoUnion;
    }

    @JsonProperty
    @ThriftField(40)
    public long getNullJoinBuildKeyCount()
    {
        return nullJoinBuildKeyCount;
    }

    @JsonProperty
    @ThriftField(41)
    public long getJoinBuildKeyCount()
    {
        return joinBuildKeyCount;
    }

    @JsonProperty
    @ThriftField(42)
    public long getNullJoinProbeKeyCount()
    {
        return nullJoinProbeKeyCount;
    }

    @JsonProperty
    @ThriftField(43)
    public long getJoinProbeKeyCount()
    {
        return joinProbeKeyCount;
    }

    @Nullable
    @JsonProperty
    @ThriftField(44)
    public DynamicFilterStats getDynamicFilterStats()
    {
        return dynamicFilterStats;
    }

    @JsonProperty
    @ThriftField(45)
    public long getIsBlockedCalls()
    {
        return isBlockedCalls;
    }

    @JsonProperty
    @ThriftField(46)
    public Duration getIsBlockedWall()
    {
        return succinctDuration(isBlockedWallInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(47)
    public Duration getIsBlockedCpu()
    {
        return succinctDuration(isBlockedCpuInMillis, MILLISECONDS);
    }

    @JsonProperty
    @ThriftField(48)
    public DataSize getIsBlockedAllocation()
    {
        return succinctBytes(isBlockedAllocation);
    }

    public static Optional<OperatorStats> merge(List<OperatorStats> operators)
    {
        if (operators.isEmpty()) {
            return Optional.empty();
        }

        OperatorStats first = operators.stream().findFirst().get();
        int stageId = first.getStageId();
        int operatorId = first.getOperatorId();
        int stageExecutionId = first.getStageExecutionId();
        int pipelineId = first.getPipelineId();
        PlanNodeId planNodeId = first.getPlanNodeId();
        String operatorType = first.getOperatorType();

        long totalDrivers = 0;

        long isBlockedCalls = 0;
        long isBlockedWall = 0;
        long isBlockedCpu = 0;
        long isBlockedAllocation = 0;

        long addInputCalls = 0;
        long addInputWall = 0;
        long addInputCpu = 0;
        long addInputAllocation = 0;
        long rawInputDataSize = 0;
        long rawInputPositions = 0;
        long inputDataSize = 0;
        long inputPositions = 0;

        double sumSquaredInputPositions = 0.0;

        long getOutputCalls = 0;
        long getOutputWall = 0;
        long getOutputCpu = 0;
        long getOutputAllocation = 0;
        long outputDataSize = 0;
        long outputPositions = 0;

        long physicalWrittenDataSize = 0;

        long finishCalls = 0;
        long finishWall = 0;
        long finishCpu = 0;
        long finishAllocation = 0;

        long additionalCpu = 0;
        long blockedWall = 0;

        long memoryReservation = 0;
        long revocableMemoryReservation = 0;
        long systemMemoryReservation = 0;

        long peakUserMemory = 0;
        long peakSystemMemory = 0;
        long peakTotalMemory = 0;

        long spilledDataSize = 0;

        long nullJoinBuildKeyCount = 0;
        long joinBuildKeyCount = 0;
        long nullJoinProbeKeyCount = 0;
        long joinProbeKeyCount = 0;

        RuntimeStats runtimeStats = new RuntimeStats();
        DynamicFilterStats dynamicFilterStats = new DynamicFilterStats(new HashSet<>());

        Optional<BlockedReason> blockedReason = Optional.empty();

        Mergeable<OperatorInfo> base = null;

        for (OperatorStats operator : operators) {
            checkArgument(operator.getOperatorId() == operatorId, "Expected operatorId to be %s but was %s", operatorId, operator.getOperatorId());

            totalDrivers += operator.totalDrivers;

            isBlockedCalls += operator.getGetOutputCalls();
            isBlockedWall += operator.getGetOutputWall().roundTo(NANOSECONDS);
            isBlockedCpu += operator.getGetOutputCpu().roundTo(NANOSECONDS);
            isBlockedAllocation += operator.getIsBlockedAllocation().toBytes();

            addInputCalls += operator.getAddInputCalls();
            addInputWall += operator.getAddInputWall().roundTo(NANOSECONDS);
            addInputCpu += operator.getAddInputCpu().roundTo(NANOSECONDS);
            addInputAllocation += operator.getAddInputAllocation().toBytes();
            rawInputDataSize += operator.getRawInputDataSize().toBytes();
            rawInputPositions += operator.getRawInputPositions();
            inputDataSize += operator.getInputDataSize().toBytes();
            inputPositions += operator.getInputPositions();
            sumSquaredInputPositions += operator.getSumSquaredInputPositions();

            getOutputCalls += operator.getGetOutputCalls();
            getOutputWall += operator.getGetOutputWall().roundTo(NANOSECONDS);
            getOutputCpu += operator.getGetOutputCpu().roundTo(NANOSECONDS);
            getOutputAllocation += operator.getGetOutputAllocation().toBytes();
            outputDataSize += operator.getOutputDataSize().toBytes();
            outputPositions += operator.getOutputPositions();

            physicalWrittenDataSize += operator.getPhysicalWrittenDataSize().toBytes();

            finishCalls += operator.getFinishCalls();
            finishWall += operator.getFinishWall().roundTo(NANOSECONDS);
            finishCpu += operator.getFinishCpu().roundTo(NANOSECONDS);
            finishAllocation += operator.getFinishAllocation().toBytes();

            additionalCpu += operator.getAdditionalCpu().roundTo(NANOSECONDS);
            blockedWall += operator.getBlockedWall().roundTo(NANOSECONDS);

            memoryReservation += operator.getUserMemoryReservation().toBytes();
            revocableMemoryReservation += operator.getRevocableMemoryReservation().toBytes();
            systemMemoryReservation += operator.getSystemMemoryReservation().toBytes();

            peakUserMemory = max(peakUserMemory, operator.getPeakUserMemoryReservation().toBytes());
            peakSystemMemory = max(peakSystemMemory, operator.getPeakSystemMemoryReservation().toBytes());
            peakTotalMemory = max(peakTotalMemory, operator.getPeakTotalMemoryReservation().toBytes());

            spilledDataSize += operator.getSpilledDataSize().toBytes();

            if (operator.getBlockedReason().isPresent()) {
                blockedReason = operator.getBlockedReason();
            }

            OperatorInfo info = operator.getInfo();
            if (base == null) {
                base = getMergeableInfoOrNull(info);
            }
            else if (info != null && base.getClass() == info.getClass()) {
                base = mergeInfo(base, info);
            }

            runtimeStats.mergeWith(operator.getRuntimeStats());
            dynamicFilterStats.mergeWith(operator.getDynamicFilterStats());

            nullJoinBuildKeyCount += operator.getNullJoinBuildKeyCount();
            joinBuildKeyCount += operator.getJoinBuildKeyCount();
            nullJoinProbeKeyCount += operator.getNullJoinProbeKeyCount();
            joinProbeKeyCount += operator.getJoinProbeKeyCount();
        }

        return Optional.of(new OperatorStats(
                stageId,
                stageExecutionId,
                pipelineId,
                operatorId,
                planNodeId,
                operatorType,

                totalDrivers,

                isBlockedCalls,
                succinctNanos(isBlockedWall),
                succinctNanos(isBlockedCpu),
                succinctBytes(isBlockedAllocation),

                addInputCalls,
                succinctNanos(addInputWall),
                succinctNanos(addInputCpu),
                succinctBytes((long) addInputAllocation),
                succinctBytes((long) rawInputDataSize),
                rawInputPositions,
                succinctBytes((long) inputDataSize),
                inputPositions,
                sumSquaredInputPositions,

                getOutputCalls,
                succinctNanos(getOutputWall),
                succinctNanos(getOutputCpu),
                succinctBytes((long) getOutputAllocation),
                succinctBytes((long) outputDataSize),
                outputPositions,

                succinctBytes((long) physicalWrittenDataSize),

                succinctNanos(additionalCpu),
                succinctNanos(blockedWall),

                finishCalls,
                succinctNanos(finishWall),
                succinctNanos(finishCpu),
                succinctBytes(finishAllocation),

                succinctBytes((long) memoryReservation),
                succinctBytes((long) revocableMemoryReservation),
                succinctBytes((long) systemMemoryReservation),
                succinctBytes((long) peakUserMemory),
                succinctBytes((long) peakSystemMemory),
                succinctBytes((long) peakTotalMemory),

                succinctBytes((long) spilledDataSize),

                blockedReason,

                (OperatorInfo) base,
                runtimeStats,
                dynamicFilterStats,
                nullJoinBuildKeyCount,
                joinBuildKeyCount,
                nullJoinProbeKeyCount,
                joinProbeKeyCount));
    }

    @SuppressWarnings("unchecked")
    private static Mergeable<OperatorInfo> getMergeableInfoOrNull(OperatorInfo info)
    {
        Mergeable<OperatorInfo> base = null;
        if (info instanceof Mergeable) {
            base = (Mergeable<OperatorInfo>) info;
        }
        return base;
    }

    @SuppressWarnings("unchecked")
    private static <T> Mergeable<T> mergeInfo(Mergeable<T> base, T other)
    {
        return (Mergeable<T>) base.mergeWith(other);
    }

    public OperatorStats summarize()
    {
        if (info == null || info.isFinal()) {
            return this;
        }
        OperatorInfo info = null;
        return new OperatorStats(
                stageId,
                stageExecutionId,
                pipelineId,
                operatorId,
                planNodeId,
                operatorType,
                totalDrivers,
                isBlockedCalls,
                succinctDuration(isBlockedWallInMillis, MILLISECONDS),
                succinctDuration(isBlockedCpuInMillis, MILLISECONDS),
                succinctBytes(isBlockedAllocation),
                addInputCalls,
                succinctDuration(addInputWallInMillis, MILLISECONDS),
                succinctDuration(addInputCpuInMillis, MILLISECONDS),
                succinctBytes(addInputAllocationInBytes),
                succinctBytes(rawInputDataSizeInBytes),
                rawInputPositions,
                succinctBytes(inputDataSizeInBytes),
                inputPositions,
                sumSquaredInputPositions,
                getOutputCalls,
                succinctDuration(getOutputWallInMillis, MILLISECONDS),
                succinctDuration(getOutputCpuInMillis, MILLISECONDS),
                succinctBytes(getOutputAllocationInBytes),
                succinctBytes(outputDataSizeInBytes),
                outputPositions,
                succinctBytes(physicalWrittenDataSizeInBytes),
                succinctDuration(additionalCpuInMillis, MILLISECONDS),
                succinctDuration(blockedWallInMillis, MILLISECONDS),
                finishCalls,
                succinctDuration(finishWallInMillis, MILLISECONDS),
                succinctDuration(finishCpuInMillis, MILLISECONDS),
                succinctBytes(finishAllocationInBytes),
                succinctBytes(userMemoryReservationInBytes),
                succinctBytes(revocableMemoryReservationInBytes),
                succinctBytes(systemMemoryReservationInBytes),
                succinctBytes(peakUserMemoryReservationInBytes),
                succinctBytes(peakSystemMemoryReservationInBytes),
                succinctBytes(peakTotalMemoryReservationInBytes),
                succinctBytes(spilledDataSizeInBytes),
                blockedReason,
                info,
                runtimeStats,
                dynamicFilterStats,
                nullJoinBuildKeyCount,
                joinBuildKeyCount,
                nullJoinProbeKeyCount,
                joinProbeKeyCount);
    }
}
