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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.concurrent.ThreadPoolExecutorMBean;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.DecayCounter;
import com.facebook.airlift.stats.ExponentialDecay;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorTypeSerdeManager;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SchedulerStatsTracker;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MetadataUpdates;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.Multimap;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.server.thrift.ThriftCodecWrapper.wrapThriftCodec;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class HttpRemoteTaskFactory
        implements RemoteTaskFactory
{
    private static final Logger log = Logger.get(HttpRemoteTaskFactory.class);
    private final HttpClient httpClient;
    private final LocationFactory locationFactory;
    private final Codec<TaskStatus> taskStatusCodec;
    private final Codec<TaskInfo> taskInfoCodec;
    //Json codec required for TaskUpdateRequest endpoint which uses JSON and returns a TaskInfo
    private final Codec<TaskInfo> taskInfoJsonCodec;
    private final Codec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final Codec<PlanFragment> planFragmentCodec;
    private final Codec<MetadataUpdates> metadataUpdatesCodec;
    private final Duration maxErrorDuration;
    private final Duration taskStatusRefreshMaxWait;
    private final Duration taskInfoRefreshMaxWait;
    private final HandleResolver handleResolver;
    private final ConnectorTypeSerdeManager connectorTypeSerdeManager;

    private final Duration taskInfoUpdateInterval;
    private final ExecutorService coreExecutor;
    private final Executor executor;
    private final ThreadPoolExecutorMBean executorMBean;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final ScheduledExecutorService errorScheduledExecutor;
    private final RemoteTaskStats stats;
    private final boolean binaryTransportEnabled;
    private final boolean thriftTransportEnabled;
    private final boolean taskInfoThriftTransportEnabled;
    private final Protocol thriftProtocol;
    private final int maxTaskUpdateSizeInBytes;
    private final MetadataManager metadataManager;
    private final QueryManager queryManager;
    private final DecayCounter taskUpdateRequestSize;
    private final ConcurrentHashMap<TaskId, ExecutorService> activeTaskExecutor = new ConcurrentHashMap<>();
    //TODO: use config file to set this value
    private final AtomicInteger maxAllowedTaskExecutor = new AtomicInteger(1000);

    @Inject
    public HttpRemoteTaskFactory(
            QueryManagerConfig config,
            TaskManagerConfig taskConfig,
            @ForScheduler HttpClient httpClient,
            LocationFactory locationFactory,
            JsonCodec<TaskStatus> taskStatusJsonCodec,
            SmileCodec<TaskStatus> taskStatusSmileCodec,
            ThriftCodec<TaskStatus> taskStatusThriftCodec,
            JsonCodec<TaskInfo> taskInfoJsonCodec,
            SmileCodec<TaskInfo> taskInfoSmileCodec,
            ThriftCodec<TaskInfo> taskInfoThriftCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestJsonCodec,
            SmileCodec<TaskUpdateRequest> taskUpdateRequestSmileCodec,
            JsonCodec<PlanFragment> planFragmentJsonCodec,
            SmileCodec<PlanFragment> planFragmentSmileCodec,
            JsonCodec<MetadataUpdates> metadataUpdatesJsonCodec,
            SmileCodec<MetadataUpdates> metadataUpdatesSmileCodec,
            RemoteTaskStats stats,
            InternalCommunicationConfig communicationConfig,
            MetadataManager metadataManager,
            QueryManager queryManager,
            HandleResolver handleResolver,
            ConnectorTypeSerdeManager connectorTypeSerdeManager)
    {
        this.httpClient = httpClient;
        this.locationFactory = locationFactory;
        this.maxErrorDuration = config.getRemoteTaskMaxErrorDuration();
        this.taskStatusRefreshMaxWait = taskConfig.getStatusRefreshMaxWait();
        this.taskInfoUpdateInterval = taskConfig.getInfoUpdateInterval();
        this.taskInfoRefreshMaxWait = taskConfig.getInfoRefreshMaxWait();
        this.handleResolver = handleResolver;
        this.connectorTypeSerdeManager = connectorTypeSerdeManager;

        this.coreExecutor = newCachedThreadPool(daemonThreadsNamed("remote-task-callback-%s"));
        this.executor = new BoundedExecutor(coreExecutor, config.getRemoteTaskMaxCallbackThreads());
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) coreExecutor);
        this.stats = requireNonNull(stats, "stats is null");
        requireNonNull(communicationConfig, "communicationConfig is null");
        binaryTransportEnabled = communicationConfig.isBinaryTransportEnabled();
        thriftTransportEnabled = communicationConfig.isThriftTransportEnabled();
        taskInfoThriftTransportEnabled = communicationConfig.isTaskInfoThriftTransportEnabled();
        thriftProtocol = communicationConfig.getThriftProtocol();
        this.maxTaskUpdateSizeInBytes = toIntExact(requireNonNull(communicationConfig, "communicationConfig is null").getMaxTaskUpdateSize().toBytes());

        if (thriftTransportEnabled) {
            this.taskStatusCodec = wrapThriftCodec(taskStatusThriftCodec);
        }
        else if (binaryTransportEnabled) {
            this.taskStatusCodec = taskStatusSmileCodec;
        }
        else {
            this.taskStatusCodec = taskStatusJsonCodec;
        }

        if (taskInfoThriftTransportEnabled) {
            this.taskInfoCodec = wrapThriftCodec(taskInfoThriftCodec);
        }
        else if (binaryTransportEnabled) {
            this.taskInfoCodec = taskInfoSmileCodec;
        }
        else {
            this.taskInfoCodec = taskInfoJsonCodec;
        }

        this.taskInfoJsonCodec = taskInfoJsonCodec;
        if (binaryTransportEnabled) {
            this.taskUpdateRequestCodec = taskUpdateRequestSmileCodec;
            this.metadataUpdatesCodec = metadataUpdatesSmileCodec;
        }
        else {
            this.taskUpdateRequestCodec = taskUpdateRequestJsonCodec;
            this.metadataUpdatesCodec = metadataUpdatesJsonCodec;
        }
        this.planFragmentCodec = planFragmentJsonCodec;

        this.metadataManager = metadataManager;
        this.queryManager = queryManager;

        this.updateScheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("task-info-update-scheduler-%s"));
        this.errorScheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("remote-task-error-delay-%s"));
        this.taskUpdateRequestSize = new DecayCounter(ExponentialDecay.oneMinute());
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }

    @Managed
    public double getTaskUpdateRequestSize()
    {
        return taskUpdateRequestSize.getCount();
    }

    @PreDestroy
    public void stop()
    {
        coreExecutor.shutdownNow();
        updateScheduledExecutor.shutdownNow();
        errorScheduledExecutor.shutdownNow();

        for (ExecutorService executorService : activeTaskExecutor.values()) {
            executorService.shutdownNow();
        }
        activeTaskExecutor.clear();
    }

    @Override
    public RemoteTask createRemoteTask(
            Session session,
            TaskId taskId,
            InternalNode node,
            PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            NodeTaskMap.NodeStatsTracker nodeStatsTracker,
            boolean summarizeTaskInfo,
            TableWriteInfo tableWriteInfo,
            SchedulerStatsTracker schedulerStatsTracker)
    {
        while (maxAllowedTaskExecutor.get() < 1) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
                log.warn(e, "interruped while waiting for a executor to run");
            }
        }
        if (!maxAllowedTaskExecutor.compareAndSet(maxAllowedTaskExecutor.get(), maxAllowedTaskExecutor.get() - 1)) {
            return createRemoteTask(session, taskId, node, fragment, initialSplits, outputBuffers, nodeStatsTracker, summarizeTaskInfo, tableWriteInfo, schedulerStatsTracker);
        }

        ExecutorService singleThreadExecutor = newSingleThreadExecutor(daemonThreadsNamed("remote-task-executor-%s"));
        activeTaskExecutor.put(taskId, singleThreadExecutor);

        return new HttpRemoteTask(
                session,
                taskId,
                node.getNodeIdentifier(),
                locationFactory.createLegacyTaskLocation(node, taskId),
                locationFactory.createTaskLocation(node, taskId),
                fragment,
                initialSplits,
                outputBuffers,
                httpClient,
                executor,
                singleThreadExecutor,
                () -> releaseTaskExecutor(taskId),
                updateScheduledExecutor,
                errorScheduledExecutor,
                maxErrorDuration,
                taskStatusRefreshMaxWait,
                taskInfoRefreshMaxWait,
                taskInfoUpdateInterval,
                summarizeTaskInfo,
                taskStatusCodec,
                taskInfoCodec,
                taskInfoJsonCodec,
                taskUpdateRequestCodec,
                planFragmentCodec,
                metadataUpdatesCodec,
                nodeStatsTracker,
                stats,
                binaryTransportEnabled,
                thriftTransportEnabled,
                taskInfoThriftTransportEnabled,
                thriftProtocol,
                tableWriteInfo,
                maxTaskUpdateSizeInBytes,
                metadataManager,
                queryManager,
                taskUpdateRequestSize,
                handleResolver,
                connectorTypeSerdeManager,
                schedulerStatsTracker);
    }

    private void releaseTaskExecutor(TaskId taskId)
    {
        ExecutorService executorService = activeTaskExecutor.remove(taskId);
        if (executorService != null) {
            executorService.shutdownNow();
            maxAllowedTaskExecutor.incrementAndGet();
        }
    }
}
