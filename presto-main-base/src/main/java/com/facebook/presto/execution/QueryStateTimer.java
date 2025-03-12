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

import com.google.common.base.Ticker;

import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

// Query time workflow chart. Left side shows query workflow. Right side shows
// associated time durations with a query.
//
//      Create                                                                                      -----------
//        |                                                                                         | waitingForPrerequisitesTime
//        |                       Semantic Analyzing      -----------                               |
//        |                               |               | semanticAnalyzingTime                   |
//        V                               V               V                                         V
//      Queued                       ACL Checking         -----------                               -----------
//        |                               |               | columnAccessPermissionCheckingTime      | queuedTime
//        V                               |               |                                         V
//    Wait for Resources                  |               V                                         -----------
//        |<------------------------------+               -----------                               | waitingForResourcesTime
//        V                                                                                         V
//    Dispatching                                                                                   -----------
//        |                                                                                         | dispatchingTime
//        V                                                                                         V
//     Planning                                                                                     ----------------------------------
//        |                                                                                         | executionTime     | planningTime
//        |      Analysis Start                                                                     |                   |        -----------
//        |         |                                                                               |                   |        | analysisTime
//        |         V                                                                               |                   |        V
//        |      Analysis End                                                                       |                   |        -----------
//        V                                                                                         |                   V
//     Starting                                                                                     |                   -----------
//        |                                                                                         |
//        V                                                                                         |
//     Running                                                                                      |
//        |                                                                                         |
//        V                                                                                         |
//    Finishing                                                                                     |                   -----------
//        |                                                                                         |                   | finishingTime
//        V                                                                                         V                   V
//       End                                                                                        ----------------------------------
public class QueryStateTimer
{
    private final Ticker ticker;

    private final long createTimeInMillis = System.currentTimeMillis();

    private final long createNanos;
    private final AtomicLong beginQueuedNanos = new AtomicLong();
    private final AtomicLong beginResourceWaitingNanos = new AtomicLong();
    private final AtomicLong beginSemanticAnalyzingNanos = new AtomicLong();
    private final AtomicLong beginColumnAccessPermissionCheckingNanos = new AtomicLong();
    private final AtomicLong beginDispatchingNanos = new AtomicLong();
    private final AtomicLong beginPlanningNanos = new AtomicLong();
    private final AtomicLong beginFinishingNanos = new AtomicLong();
    private final AtomicLong endNanos = new AtomicLong();

    private final AtomicLong waitingForPrerequisitesTime = new AtomicLong();
    private final AtomicLong queuedTime = new AtomicLong();
    private final AtomicLong resourceWaitingTime = new AtomicLong();
    private final AtomicLong semanticAnalyzingTime = new AtomicLong();
    private final AtomicLong columnAccessPermissionCheckingTime = new AtomicLong();
    private final AtomicLong dispatchingTime = new AtomicLong();
    private final AtomicLong executionTime = new AtomicLong();
    private final AtomicLong planningTime = new AtomicLong();
    private final AtomicLong finishingTime = new AtomicLong();

    private final AtomicLong beginAnalysisNanos = new AtomicLong();
    private final AtomicLong analysisTime = new AtomicLong();

    private final AtomicLong lastHeartbeatNanos;

    public QueryStateTimer(Ticker ticker)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.createNanos = tickerNanos();
        this.lastHeartbeatNanos = new AtomicLong(createNanos);
    }

    //
    // State transitions
    //

    public void beginQueued()
    {
        beginQueued(tickerNanos());
    }

    private void beginQueued(long now)
    {
        waitingForPrerequisitesTime.compareAndSet(0, nanosSince(createNanos, now));
        beginQueuedNanos.compareAndSet(0, now);
    }

    public void beginWaitingForResources()
    {
        beginWaitingForResources(tickerNanos());
    }

    private void beginWaitingForResources(long now)
    {
        beginQueued(now);
        queuedTime.compareAndSet(0, nanosSince(beginQueuedNanos, now));
        beginResourceWaitingNanos.compareAndSet(0, now);
    }

    public void beginSemanticAnalyzing()
    {
        beginSemanticAnalyzing(tickerNanos());
    }

    private void beginSemanticAnalyzing(long now)
    {
        beginSemanticAnalyzingNanos.compareAndSet(0, now);
    }

    public void beginColumnAccessPermissionChecking()
    {
        beginColumnAccessPermissionChecking(tickerNanos());
    }

    private void beginColumnAccessPermissionChecking(long now)
    {
        beginSemanticAnalyzing(now);
        semanticAnalyzingTime.compareAndSet(0, nanosSince(beginSemanticAnalyzingNanos, now));
        beginColumnAccessPermissionCheckingNanos.compareAndSet(0, now);
    }

    public void endColumnAccessPermissionChecking()
    {
        endColumnAccessPermissionChecking(tickerNanos());
    }

    private void endColumnAccessPermissionChecking(long now)
    {
        beginColumnAccessPermissionChecking(now);
        columnAccessPermissionCheckingTime.compareAndSet(0, nanosSince(beginColumnAccessPermissionCheckingNanos, now));
    }

    public void beginDispatching()
    {
        beginDispatching(tickerNanos());
    }

    private void beginDispatching(long now)
    {
        beginWaitingForResources(now);
        resourceWaitingTime.compareAndSet(0, nanosSince(beginResourceWaitingNanos, now));
        beginDispatchingNanos.compareAndSet(0, now);
    }

    public void beginPlanning()
    {
        beginPlanning(tickerNanos());
    }

    private void beginPlanning(long now)
    {
        beginDispatching(now);
        dispatchingTime.compareAndSet(0, nanosSince(beginDispatchingNanos, now));
        beginPlanningNanos.compareAndSet(0, now);
    }

    public void beginStarting()
    {
        beginStarting(tickerNanos());
    }

    private void beginStarting(long now)
    {
        beginPlanning(now);
        planningTime.compareAndSet(0, nanosSince(beginPlanningNanos, now));
    }

    public void beginRunning()
    {
        beginRunning(tickerNanos());
    }

    private void beginRunning(long now)
    {
        beginStarting(now);
    }

    public void beginFinishing()
    {
        beginFinishing(tickerNanos());
    }

    private void beginFinishing(long now)
    {
        beginRunning(now);
        beginFinishingNanos.compareAndSet(0, now);
    }

    public void endQuery()
    {
        endQuery(tickerNanos());
    }

    private void endQuery(long now)
    {
        beginFinishing(now);
        finishingTime.compareAndSet(0, nanosSince(beginFinishingNanos, now));
        executionTime.compareAndSet(0, nanosSince(beginPlanningNanos, now));
        endNanos.compareAndSet(0, now);
    }

    //
    //  Additional timings
    //

    public void beginAnalyzing()
    {
        beginAnalysisNanos.compareAndSet(0, tickerNanos());
    }

    public void endAnalysis()
    {
        analysisTime.compareAndSet(0, nanosSince(beginAnalysisNanos, tickerNanos()));
    }

    public void recordHeartbeat()
    {
        lastHeartbeatNanos.set(tickerNanos());
    }

    //
    // Stats
    //

    public long getCreateTimeInMillis()
    {
        return createTimeInMillis;
    }

    public long getExecutionStartTimeInMillis()
    {
        return toMillis(beginPlanningNanos);
    }

    public long getElapsedTimeInNanos()
    {
        if (endNanos.get() != 0) {
            return endNanos.get() - createNanos;
        }
        return nanosSince(createNanos, tickerNanos());
    }

    public long getWaitingForPrerequisitesTimeInNanos()
    {
        long waitingForPrerequisitesTime = this.waitingForPrerequisitesTime.get();
        if (waitingForPrerequisitesTime != 0) {
            return waitingForPrerequisitesTime;
        }

        // if prerequisite wait time is not set, the query is still waiting for prerequisites to finish
        return getElapsedTimeInNanos();
    }

    public long getQueuedTimeInNanos()
    {
        return getDurationInNanos(queuedTime, beginQueuedNanos);
    }

    public long getResourceWaitingTimeInNanos()
    {
        return getDurationInNanos(resourceWaitingTime, beginResourceWaitingNanos);
    }

    public long getSemanticAnalyzingTimeInNanos()
    {
        return getDurationInNanos(semanticAnalyzingTime, beginSemanticAnalyzingNanos);
    }

    public long getColumnAccessPermissionCheckingTimeInNanos()
    {
        return getDurationInNanos(columnAccessPermissionCheckingTime, beginColumnAccessPermissionCheckingNanos);
    }

    public long getDispatchingTimeInNanos()
    {
        return getDurationInNanos(dispatchingTime, beginDispatchingNanos);
    }

    public long getPlanningTimeInNanos()
    {
        return getDurationInNanos(planningTime, beginPlanningNanos);
    }

    public long getFinishingTimeInNanos()
    {
        return getDurationInNanos(finishingTime, beginFinishingNanos);
    }

    public long getExecutionTimeInNanos()
    {
        return getDurationInNanos(executionTime, beginPlanningNanos);
    }

    public long getEndTimeInMillis()
    {
        return toMillis(endNanos);
    }

    public long getAnalysisTimeInNanos()
    {
        return getDurationInNanos(analysisTime, beginAnalysisNanos);
    }

    public long getLastHeartbeatInMillis()
    {
        return toMillis(lastHeartbeatNanos);
    }

    //
    // Helper methods
    //

    private long tickerNanos()
    {
        return ticker.read();
    }

    private static long nanosSince(AtomicLong start, long end)
    {
        long startNanos = start.get();
        if (startNanos == 0) {
            throw new IllegalStateException("Start time not set");
        }
        return nanosSince(startNanos, end);
    }

    private static long nanosSince(long start, long now)
    {
        return max(0, now - start);
    }

    private long getDurationInNanos(AtomicLong finalDuration, AtomicLong start)
    {
        long duration = finalDuration.get();
        if (duration != 0) {
            return duration;
        }
        long startNanos = start.get();
        if (startNanos != 0) {
            return nanosSince(startNanos, tickerNanos());
        }
        return 0L;
    }

    private long toMillis(AtomicLong instantNanos)
    {
        long nanos = instantNanos.get();
        return nanos != 0 ? createTimeInMillis + NANOSECONDS.toMillis(nanos - createNanos) : 0L;
    }
}
