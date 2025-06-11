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
package com.facebook.presto.server.thrift;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public class ConnectorSplitWrapper
{
    private final ConnectorId connectorId;
    private final ConnectorSplit connectorSplit;

    @JsonCreator
    @ThriftConstructor
    public ConnectorSplitWrapper(@JsonProperty("connectorId") ConnectorId connectorId,
            @JsonProperty("connectorSplit") ConnectorSplit connectorSplit)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.connectorSplit = requireNonNull(connectorSplit, "connectorSplit is null");
    }

    @JsonProperty
    @ThriftField(1)
    public ConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    @ThriftField(2)
    public ConnectorSplit getConnectorSplit()
    {
        return connectorSplit;
    }
}
