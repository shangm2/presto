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
package com.facebook.presto.thrift;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.drift.codec.CodecThriftType;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.spi.ConnectorOutputTableHandle;

import javax.inject.Inject;

public class OutputTableHandleThriftCodec
        extends AbstractTypedThriftCodec<ConnectorOutputTableHandle>
{
    private static final ThriftType thriftType = createThriftType(ConnectorOutputTableHandle.class);

    @Inject
    public OutputTableHandleThriftCodec(HandleResolver handleResolver, ThriftCatalog thriftCatalog, GlobalThriftCodecManager globalThriftCodecManager, JsonCodec<ConnectorOutputTableHandle> jsonCodec)
    {
        super(ConnectorOutputTableHandle.class,
                jsonCodec,
                handleResolver::getId,
                handleResolver::getOutputTableHandleClass,
                globalThriftCodecManager);
        thriftCatalog.addThriftType(thriftType);
    }

    @CodecThriftType
    public static ThriftType getThriftType()
    {
        return thriftType;
    }

    @Override
    public ThriftType getType()
    {
        return thriftType;
    }
}
