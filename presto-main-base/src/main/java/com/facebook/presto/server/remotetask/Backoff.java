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
package com.facebook.presto.server.remotetask;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;

import javax.annotation.concurrent.ThreadSafe;

import static com.facebook.presto.common.Utils.checkNonNegative;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class Backoff
{
    private static final int MIN_RETRIES = 3;
    private static final long[] DEFAULT_BACKOFF_DELAY_INTERVALS_IN_MILLIS = {0L, 50L, 100L, 200L, 500L};

    private final int minTries;
    private final long maxFailureIntervalNanos;
    private final Ticker ticker;
    private final long[] backoffDelayIntervalsInMillis;

    private long firstFailureTimeInNanos;
    private long lastFailureTime;
    private long failureCount;
    private long failureRequestTimeTotalInNanos;

    private long lastRequestStartInNanos;

    public Backoff(long maxFailureIntervalInNanos)
    {
        this(maxFailureIntervalInNanos, Ticker.systemTicker());
    }

    public Backoff(long maxFailureIntervalInNanos, Ticker ticker)
    {
        this(MIN_RETRIES, maxFailureIntervalInNanos, ticker, DEFAULT_BACKOFF_DELAY_INTERVALS_IN_MILLIS);
    }

    @VisibleForTesting
    public Backoff(int minTries, long maxFailureIntervalInNanos, Ticker ticker, long[] backoffDelayIntervals)
    {
        checkArgument(minTries > 0, "minTries must be at least 1");
        checkNonNegative(maxFailureIntervalInNanos, "maxFailureInterval is negative");
        requireNonNull(ticker, "ticker is null");
        requireNonNull(backoffDelayIntervals, "backoffDelayIntervals is null");
        checkArgument(backoffDelayIntervals.length > 0, "backoffDelayIntervals must contain at least one entry");

        this.minTries = minTries;
        this.maxFailureIntervalNanos = maxFailureIntervalInNanos;
        this.ticker = ticker;
        this.backoffDelayIntervalsInMillis = backoffDelayIntervals;
    }

    public synchronized long getFailureCount()
    {
        return failureCount;
    }

    public synchronized long getFailureDurationInSeconds()
    {
        if (firstFailureTimeInNanos == 0) {
            return 0;
        }
        long value = ticker.read() - firstFailureTimeInNanos;
        return NANOSECONDS.toSeconds(value);
    }

    public synchronized long getFailureRequestTimeTotalInSeconds()
    {
        return NANOSECONDS.toSeconds(max(0, failureRequestTimeTotalInNanos));
    }

    public synchronized void startRequest()
    {
        lastRequestStartInNanos = ticker.read();
    }

    public synchronized void success()
    {
        lastRequestStartInNanos = 0;
        firstFailureTimeInNanos = 0;
        failureCount = 0;
        lastFailureTime = 0;
    }

    /**
     * @return true if the failure is considered permanent
     */
    public synchronized boolean failure()
    {
        long now = ticker.read();

        lastFailureTime = now;
        failureCount++;
        if (lastRequestStartInNanos != 0) {
            failureRequestTimeTotalInNanos += now - lastRequestStartInNanos;
            lastRequestStartInNanos = 0;
        }

        if (firstFailureTimeInNanos == 0) {
            firstFailureTimeInNanos = now;
            // can not fail on first failure
            return false;
        }

        if (failureCount < minTries) {
            return false;
        }

        long failureDuration = now - firstFailureTimeInNanos;
        return failureDuration >= maxFailureIntervalNanos;
    }

    public synchronized long getBackoffDelayNanos()
    {
        int failureCount = (int) min(backoffDelayIntervalsInMillis.length, this.failureCount);
        if (failureCount == 0) {
            return 0;
        }
        // expected amount of time to delay from the last failure time
        long currentDelayInMillis = backoffDelayIntervalsInMillis[failureCount - 1];

        // calculate expected delay from now
        long nanosSinceLastFailure = ticker.read() - lastFailureTime;
        return max(0, currentDelayInMillis * 1_000_000 - nanosSinceLastFailure);
    }
}
