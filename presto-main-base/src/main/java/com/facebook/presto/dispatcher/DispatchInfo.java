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
package com.facebook.presto.dispatcher;

import com.facebook.presto.execution.ExecutionFailureInfo;

import java.util.Optional;

import static com.facebook.presto.common.Utils.checkNonNegative;
import static java.util.Objects.requireNonNull;

public class DispatchInfo
{
    private final Optional<CoordinatorLocation> coordinatorLocation;
    private final Optional<ExecutionFailureInfo> failureInfo;
    private final long elapsedTimeInNanos;
    private final long waitingForPrerequisitesTimeInNanos;
    private final long queuedTimeInNanos;

    public static DispatchInfo waitingForPrerequisites(long elapsedTimeInNanos, long waitingForPrerequisitesTimeInNanos)
    {
        return new DispatchInfo(Optional.empty(), Optional.empty(), elapsedTimeInNanos, waitingForPrerequisitesTimeInNanos, 0);
    }

    public static DispatchInfo queued(long elapsedTimeInNanos, long waitingForPrerequisitesTimeInNanos, long queuedTimeInNanos)
    {
        checkNonNegative(queuedTimeInNanos, "queuedTime is negative");
        return new DispatchInfo(Optional.empty(), Optional.empty(), elapsedTimeInNanos, waitingForPrerequisitesTimeInNanos, queuedTimeInNanos);
    }

    public static DispatchInfo dispatched(CoordinatorLocation coordinatorLocation, long elapsedTimeInNanos, long waitingForPrerequisitesTimeInNanos, long queuedTimeInNanos)
    {
        requireNonNull(coordinatorLocation, "coordinatorLocation is null");
        checkNonNegative(queuedTimeInNanos, "queuedTime is negative");
        return new DispatchInfo(Optional.of(coordinatorLocation), Optional.empty(), elapsedTimeInNanos, waitingForPrerequisitesTimeInNanos, queuedTimeInNanos);
    }

    public static DispatchInfo failed(ExecutionFailureInfo failureInfo, long elapsedTimeInNanos, long waitingForPrerequisitesTimeInNanos, long queuedTimeInNanos)
    {
        requireNonNull(failureInfo, "coordinatorLocation is null");
        checkNonNegative(queuedTimeInNanos, "queuedTime is negative");
        return new DispatchInfo(Optional.empty(), Optional.of(failureInfo), elapsedTimeInNanos, waitingForPrerequisitesTimeInNanos, queuedTimeInNanos);
    }

    private DispatchInfo(
            Optional<CoordinatorLocation> coordinatorLocation,
            Optional<ExecutionFailureInfo> failureInfo,
            long elapsedTimeInNanos,
            long waitingForPrerequisitesTimeInNanos,
            long queuedTimeInNanos)
    {
        this.coordinatorLocation = requireNonNull(coordinatorLocation, "coordinatorLocation is null");
        this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
        this.elapsedTimeInNanos = checkNonNegative(elapsedTimeInNanos, "elapsedTime is negative");
        this.waitingForPrerequisitesTimeInNanos = checkNonNegative(waitingForPrerequisitesTimeInNanos, "waitingForPrerequisitesTime is negative");
        this.queuedTimeInNanos = checkNonNegative(queuedTimeInNanos, "queuedTime is negative");
    }

    public Optional<CoordinatorLocation> getCoordinatorLocation()
    {
        return coordinatorLocation;
    }

    public Optional<ExecutionFailureInfo> getFailureInfo()
    {
        return failureInfo;
    }

    public long getElapsedTimeInNanos()
    {
        return elapsedTimeInNanos;
    }

    public long getWaitingForPrerequisitesTimeInNanos()
    {
        return waitingForPrerequisitesTimeInNanos;
    }

    public long getQueuedTimeInNanos()
    {
        return queuedTimeInNanos;
    }
}
