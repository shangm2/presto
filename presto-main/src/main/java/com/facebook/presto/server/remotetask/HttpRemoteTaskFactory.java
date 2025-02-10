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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.airlift.stats.CounterStat;
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
import com.facebook.presto.execution.SafeEventLoopGroup;
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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.units.Duration;
import io.netty.channel.EventLoopGroup;
import org.weakref.jmx.Managed;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import static com.facebook.presto.server.thrift.ThriftCodecWrapper.wrapThriftCodec;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class HttpRemoteTaskFactory
        implements RemoteTaskFactory
{
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
    private final RemoteTaskStats stats;
    private final boolean binaryTransportEnabled;
    private final boolean thriftTransportEnabled;
    private final boolean taskInfoThriftTransportEnabled;
    private final Protocol thriftProtocol;
    private final int maxTaskUpdateSizeInBytes;
    private final MetadataManager metadataManager;
    private final QueryManager queryManager;
    private final DecayCounter taskUpdateRequestSize;

    private final CounterStat taskFailuresFromFailedConnection;

    private final EventLoopGroup eventLoopGroup = new SafeEventLoopGroup(Runtime.getRuntime().availableProcessors(),
            new ThreadFactoryBuilder().setNameFormat("task-event-loop-%s").setDaemon(true).build());

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

        this.taskUpdateRequestSize = new DecayCounter(ExponentialDecay.oneMinute());
        this.taskFailuresFromFailedConnection = new CounterStat();
    }

    @Managed
    public double getTaskUpdateRequestSize()
    {
        return taskUpdateRequestSize.getCount();
    }

    @Managed
    public long getTaskFailuresFromFailedConnectionCount()
    {
        return max(taskFailuresFromFailedConnection.getTotalCount(), 0);
    }

    @Managed
    public long getOpenConnectionCount()
    {
        if (httpClient instanceof JettyHttpClient) {
            return max(((JettyHttpClient) httpClient).getConnectionStats().getOpenConnectionCount(), 0);
        }
        return 0;
    }

    @Managed
    public long getTotalConnectionCount()
    {
        if (httpClient instanceof JettyHttpClient) {
            return max(((JettyHttpClient) httpClient).getConnectionStats().getTotalConnectionCount(), 0);
        }
        return 0;
    }

    @Managed
    public long getMaxConnectionPerDestinationCount()
    {
        if (httpClient instanceof JettyHttpClient) {
            return max(((JettyHttpClient) httpClient).getActiveConnectionsPerDestination().getMax(), 0);
        }
        return 0;
    }

    @PreDestroy
    public void stop()
    {
        eventLoopGroup.shutdownGracefully();
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
        return HttpRemoteTask.createHttpRemoteTask(
                session,
                taskId,
                node.getNodeIdentifier(),
                locationFactory.createLegacyTaskLocation(node, taskId),
                locationFactory.createTaskLocation(node, taskId),
                fragment,
                initialSplits,
                outputBuffers,
                httpClient,
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
                taskFailuresFromFailedConnection,
                handleResolver,
                connectorTypeSerdeManager,
                schedulerStatsTracker,
                (SafeEventLoopGroup.SafeEventLoop) eventLoopGroup.next());
    }
}
