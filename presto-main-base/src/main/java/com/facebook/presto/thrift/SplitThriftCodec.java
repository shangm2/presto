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
import com.facebook.presto.spi.ConnectorSplit;

import javax.inject.Inject;

public class SplitThriftCodec
        extends AbstractTypedThriftCodec<ConnectorSplit>
{
    private static final ThriftType thriftType = createThriftType(ConnectorSplit.class);

    @Inject
    public SplitThriftCodec(HandleResolver handleResolver, ThriftCatalog thriftCatalog, GlobalThriftCodecManager globalThriftCodecManager, JsonCodec<ConnectorSplit> jsonCodec)
    {
        super(ConnectorSplit.class,
                jsonCodec,
                handleResolver::getId,
                handleResolver::getSplitClass,
                globalThriftCodecManager);
        System.out.println("==========> SplitThriftCodec thriftcatalog  " + System.identityHashCode(thriftCatalog));
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
