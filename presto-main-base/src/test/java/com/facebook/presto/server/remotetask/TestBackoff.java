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

import com.facebook.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBackoff
{
    @Test
    public void testFailureInterval()
    {
        TestingTicker ticker = new TestingTicker();
        ticker.increment(1, NANOSECONDS);

        Backoff backoff = new Backoff(1, new Duration(15, SECONDS).roundTo(NANOSECONDS), ticker, new long[] {10L});
        ticker.increment(10, MICROSECONDS);

        // verify initial state
        assertEquals(backoff.getFailureCount(), 0);
        assertEquals(backoff.getFailureDurationInSeconds(), 0);

        // first failure, should never fail
        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 1);
        assertEquals(backoff.getFailureDurationInSeconds(), 0);

        ticker.increment(14, SECONDS);

        // second failure within the limit, should not fail
        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 2);
        assertEquals(backoff.getFailureDurationInSeconds(), 14);

        ticker.increment(1, SECONDS);

        // final failure after the limit causes failure
        assertTrue(backoff.failure());
        assertEquals(backoff.getFailureCount(), 3);
        assertEquals(backoff.getFailureDurationInSeconds(), 15);
    }

    @Test
    public void testMinTries()
    {
        TestingTicker ticker = new TestingTicker();
        ticker.increment(1, NANOSECONDS);

        Backoff backoff = new Backoff(3, new Duration(1, NANOSECONDS).roundTo(NANOSECONDS), ticker, new long[] {10L});
        ticker.increment(10, MICROSECONDS);

        // verify initial state
        assertEquals(backoff.getFailureCount(), 0);
        assertEquals(backoff.getFailureDurationInSeconds(), 0);

        // first failure, should never fail
        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 1);
        assertEquals(backoff.getFailureDurationInSeconds(), 0);

        ticker.increment(14, SECONDS);

        // second failure under min failures, should not fail
        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 2);
        assertEquals(backoff.getFailureDurationInSeconds(), 14);

        ticker.increment(1, SECONDS);

        // last try failed
        assertTrue(backoff.failure());
        assertEquals(backoff.getFailureCount(), 3);
        assertEquals(backoff.getFailureDurationInSeconds(), 15);
    }

    @Test
    public void testStartRequest()
    {
        TestingTicker ticker = new TestingTicker();
        ticker.increment(1, NANOSECONDS);

        Backoff backoff = new Backoff(1, new Duration(15, SECONDS).roundTo(NANOSECONDS), ticker, new long[] {10L});
        ticker.increment(10, MICROSECONDS);

        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 1);
        assertEquals(backoff.getFailureDurationInSeconds(), 0);
        assertEquals(backoff.getFailureRequestTimeTotalInSeconds(), 0);

        ticker.increment(7, SECONDS);
        backoff.startRequest();
        ticker.increment(7, SECONDS);

        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 2);
        assertEquals(backoff.getFailureDurationInSeconds(), 14);
        // failed request took 7 seconds.
        assertEquals(backoff.getFailureRequestTimeTotalInSeconds(), 7);

        ticker.increment(1, SECONDS);
        backoff.startRequest();
        ticker.increment(1, SECONDS);

        assertTrue(backoff.failure());
        assertEquals(backoff.getFailureCount(), 3);
        assertEquals(backoff.getFailureDurationInSeconds(), 16);
        // failed requests took 7+1 seconds.
        assertEquals(backoff.getFailureRequestTimeTotalInSeconds(), 8);
    }

    @Test
    public void testDelay()
    {
        // 1, 2, 4, 8
        TestingTicker ticker = new TestingTicker();
        ticker.increment(1, NANOSECONDS);

        Backoff backoff = new Backoff(1, new Duration(15, SECONDS).roundTo(NANOSECONDS), ticker, new long[] {0L, 1_000L, 2_000L, 4_000L, 8_000L});

        assertEquals(backoff.getFailureCount(), 0);
        assertEquals(backoff.getFailureDurationInSeconds(), 0);

        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 1);
        assertEquals(backoff.getFailureDurationInSeconds(), 0);
        long backoffDelay = backoff.getBackoffDelayNanos();
        assertEquals(NANOSECONDS.toSeconds(backoffDelay), 0);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 2);
        assertEquals(backoff.getFailureDurationInSeconds(), 0);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertEquals(NANOSECONDS.toSeconds(backoffDelay), 1);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 3);
        assertEquals(backoff.getFailureDurationInSeconds(), 1);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertEquals(NANOSECONDS.toSeconds(backoffDelay), 2);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 4);
        assertEquals(backoff.getFailureDurationInSeconds(), 3);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertEquals(NANOSECONDS.toSeconds(backoffDelay), 4);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertFalse(backoff.failure());
        assertEquals(backoff.getFailureCount(), 5);
        assertEquals(backoff.getFailureDurationInSeconds(), 7);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertEquals(NANOSECONDS.toSeconds(backoffDelay), 8);

        ticker.increment(backoffDelay, NANOSECONDS);

        assertTrue(backoff.failure());
        assertEquals(backoff.getFailureCount(), 6);
        assertEquals(backoff.getFailureDurationInSeconds(), 15);
        backoffDelay = backoff.getBackoffDelayNanos();
        assertEquals(NANOSECONDS.toSeconds(backoffDelay), 8);
    }
}
