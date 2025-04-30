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
package com.facebook.presto.util;

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoTransportException;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.tree.NodeLocation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.metadata.MetadataUpdates.DEFAULT_METADATA_UPDATES;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static java.lang.System.currentTimeMillis;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestPrestoJsonObjectMapperUtil
{
    public static final long TASK_INSTANCE_ID_LEAST_SIGNIFICANT_BITS = 123L;
    public static final long TASK_INSTANCE_ID_MOST_SIGNIFICANT_BITS = 456L;
    public static final long VERSION = 789L;
    public static final TaskState RUNNING = TaskState.RUNNING;
    public static final URI SELF_URI = java.net.URI.create("fake://task/" + "1");
    public static final Set<Lifespan> LIFESPANS = ImmutableSet.of(Lifespan.taskWide(), Lifespan.driverGroup(100));
    public static final int QUEUED_PARTITIONED_DRIVERS = 100;
    public static final long QUEUED_PARTITIONED_WEIGHT = SplitWeight.rawValueForStandardSplitCount(QUEUED_PARTITIONED_DRIVERS);
    public static final int RUNNING_PARTITIONED_DRIVERS = 200;
    public static final long RUNNING_PARTITIONED_WEIGHT = SplitWeight.rawValueForStandardSplitCount(RUNNING_PARTITIONED_DRIVERS);
    public static final double OUTPUT_BUFFER_UTILIZATION = 99.9;
    public static final boolean OUTPUT_BUFFER_OVERUTILIZED = true;
    public static final int PHYSICAL_WRITTEN_DATA_SIZE_IN_BYTES = 1024 * 1024;
    public static final int MEMORY_RESERVATION_IN_BYTES = 1024 * 1024 * 1024;
    public static final int SYSTEM_MEMORY_RESERVATION_IN_BYTES = 2 * 1024 * 1024 * 1024;
    public static final int PEAK_NODE_TOTAL_MEMORY_RESERVATION_IN_BYTES = 42 * 1024 * 1024;
    public static final int FULL_GC_COUNT = 10;
    public static final int FULL_GC_TIME_IN_MILLIS = 1001;
    public static final int TOTAL_CPU_TIME_IN_NANOS = 1002;
    public static final int TASK_AGE = 1003;
    public static final HostAddress REMOTE_HOST = HostAddress.fromParts("www.fake.invalid", 8080);

    private ObjectMapper objectMapper;
    private PrestoJsonObjectMapperUtil prestoJsonObjectMapperUtil;

    @BeforeMethod
    public void startUp()
    {
        Injector injector = Guice.createInjector(binder -> {
            binder.bind(ObjectMapper.class).toProvider(JsonObjectMapperProvider.class);
            binder.bind(PrestoJsonObjectMapperUtil.class).in(Scopes.SINGLETON);
        });
        prestoJsonObjectMapperUtil = injector.getInstance(PrestoJsonObjectMapperUtil.class);
    }

    @Test
    public void testTaskStatus()
    {
        TaskStatus taskStatus = createTaskStatus();

        byte[] serializedTaskStatus = prestoJsonObjectMapperUtil.serialize(taskStatus);
        TaskStatus deserializedTaskStatus = (TaskStatus) prestoJsonObjectMapperUtil.deserialize(serializedTaskStatus, TaskStatus.class);

        assertEquals(deserializedTaskStatus.toString(), taskStatus.toString());
    }

    @Test
    public void testTaskInfo()
    {
        TaskInfo taskInfo = createTaskInfo();

        byte[] serializedTaskInfo = prestoJsonObjectMapperUtil.serialize(taskInfo);
        TaskInfo deserializedTaskInfo = (TaskInfo) prestoJsonObjectMapperUtil.deserialize(serializedTaskInfo, TaskInfo.class);

        assertEquals(deserializedTaskInfo.toString(), taskInfo.toString());
    }

    private TaskInfo createTaskInfo()
    {
        return new TaskInfo(
                new TaskId("query", 0, 0, 0, 0),
                createTaskStatus(),
                currentTimeMillis(),
                new OutputBufferInfo("UNINITIALIZED", OPEN, true, true, 0, 0, 0, 0, Collections.emptyList()),
                ImmutableSet.of(),
                new TaskStats(System.currentTimeMillis() - 1000, System.currentTimeMillis() - 100),
                true,
                DEFAULT_METADATA_UPDATES,
                "test-node");
    }

    private TaskStatus createTaskStatus()
    {
        List<ExecutionFailureInfo> executionFailureInfos = getExecutionFailureInfos();
        return new TaskStatus(
                TASK_INSTANCE_ID_LEAST_SIGNIFICANT_BITS,
                TASK_INSTANCE_ID_MOST_SIGNIFICANT_BITS,
                VERSION,
                RUNNING,
                SELF_URI,
                LIFESPANS,
                executionFailureInfos,
                QUEUED_PARTITIONED_DRIVERS,
                RUNNING_PARTITIONED_DRIVERS,
                OUTPUT_BUFFER_UTILIZATION,
                OUTPUT_BUFFER_OVERUTILIZED,
                PHYSICAL_WRITTEN_DATA_SIZE_IN_BYTES,
                MEMORY_RESERVATION_IN_BYTES,
                SYSTEM_MEMORY_RESERVATION_IN_BYTES,
                PEAK_NODE_TOTAL_MEMORY_RESERVATION_IN_BYTES,
                FULL_GC_COUNT,
                FULL_GC_TIME_IN_MILLIS,
                TOTAL_CPU_TIME_IN_NANOS,
                TASK_AGE,
                QUEUED_PARTITIONED_WEIGHT,
                RUNNING_PARTITIONED_WEIGHT);
    }

    private List<ExecutionFailureInfo> getExecutionFailureInfos()
    {
        IOException ioException = new IOException("Remote call timed out");
        ioException.addSuppressed(new IOException("Thrift call timed out"));
        PrestoTransportException prestoTransportException = new PrestoTransportException(TOO_MANY_REQUESTS_FAILED,
                REMOTE_HOST,
                "Too many requests failed",
                new PrestoException(REMOTE_TASK_ERROR, "Remote Task Error"));
        ParsingException parsingException = new ParsingException("Parsing Exception", new NodeLocation(100, 1));
        return Failures.toFailures(ImmutableList.of(ioException, prestoTransportException, parsingException));
    }
}
