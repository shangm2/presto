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
package com.facebook.presto.experimental.utils;

import com.facebook.presto.experimental.auto_gen.ThriftTaskUpdateRequest;
import com.facebook.presto.server.TaskUpdateRequest;

import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.experimental.utils.ThriftOutputBuffersUtils.fromOutputBuffers;
import static com.facebook.presto.experimental.utils.ThriftOutputBuffersUtils.toOutputBuffers;
import static com.facebook.presto.experimental.utils.ThriftSessionRepresentationUtils.fromSessionRepresentation;
import static com.facebook.presto.experimental.utils.ThriftSessionRepresentationUtils.toSessionRepresentation;
import static com.facebook.presto.experimental.utils.ThriftTableWriteInfoUtils.fromTableWriteInfo;

public class ThriftTaskUpdateRequestUtils
{
    private ThriftTaskUpdateRequestUtils() {}

    public static TaskUpdateRequest toTaskUpdateRequest(ThriftTaskUpdateRequest thriftTaskUpdateRequest)
    {
        if (thriftTaskUpdateRequest == null) {
            return null;
        }
        return new TaskUpdateRequest(
                toSessionRepresentation(thriftTaskUpdateRequest.getSession()),
                thriftTaskUpdateRequest.getExtraCredentials(),
                Optional.ofNullable(thriftTaskUpdateRequest.getFragment()),
                thriftTaskUpdateRequest.getSources().stream().map(ThriftTaskSourceUtils::toTaskSource).collect(Collectors.toList()),
                toOutputBuffers(thriftTaskUpdateRequest.getOutputIds()),
                thriftTaskUpdateRequest.getTableWriteInfo().map(ThriftTableWriteInfoUtils::toTableWriteInfo));
    }

    public static ThriftTaskUpdateRequest fromTaskUpdateRequest(TaskUpdateRequest taskUpdateRequest)
    {
        if (taskUpdateRequest == null) {
            return null;
        }
        ThriftTaskUpdateRequest thriftTaskUpdateRequest = new ThriftTaskUpdateRequest(
                fromSessionRepresentation(taskUpdateRequest.getSession()),
                taskUpdateRequest.getExtraCredentials(),
                taskUpdateRequest.getSources().stream().map(ThriftTaskSourceUtils::fromTaskSource).collect(Collectors.toList()),
                fromOutputBuffers(taskUpdateRequest.getOutputIds()));
        taskUpdateRequest.getTableWriteInfo().ifPresent(tableInfo -> thriftTaskUpdateRequest.setTableWriteInfo(fromTableWriteInfo(tableInfo)));
        taskUpdateRequest.getFragment().ifPresent(thriftTaskUpdateRequest::setFragment);
        return thriftTaskUpdateRequest;
    }
}
