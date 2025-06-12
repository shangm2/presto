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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.drift.codec.CodecThriftType;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.presto.common.predicate.Domain;

import javax.inject.Inject;

import static com.facebook.presto.server.thrift.CustomCodecUtils.createSyntheticMetadata;
import static com.facebook.presto.server.thrift.CustomCodecUtils.readSingleJsonField;
import static com.facebook.presto.server.thrift.CustomCodecUtils.writeSingleJsonField;
import static java.util.Objects.requireNonNull;

public class DomainThriftCodec
        implements ThriftCodec<Domain>
{
    private static final short DOMAIN_DATA_FIELD_ID = 1;
    private static final String DOMAIN_DATA_FIELD_NAME = "domain";
    private static final String DOMAIN_STRUCT_NAME = "Domain";
    private static final ThriftType SYNTHETIC_STRUCT_TYPE = ThriftType.struct(createSyntheticMetadata(DOMAIN_DATA_FIELD_ID, DOMAIN_DATA_FIELD_NAME, Domain.class, String.class, ThriftType.STRING));

    private final JsonCodec<Domain> jsonCodec;

    @Inject
    public DomainThriftCodec(JsonCodec<Domain> jsonCodec, ThriftCatalog thriftCatalog)
    {
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        thriftCatalog.addThriftType(SYNTHETIC_STRUCT_TYPE);
    }

    @CodecThriftType
    public static ThriftType getThriftType()
    {
        return SYNTHETIC_STRUCT_TYPE;
    }

    @Override
    public ThriftType getType()
    {
        return SYNTHETIC_STRUCT_TYPE;
    }

    @Override
    public Domain read(TProtocolReader protocol)
            throws Exception
    {
        return readSingleJsonField(protocol, jsonCodec, DOMAIN_DATA_FIELD_ID, DOMAIN_DATA_FIELD_NAME);
    }

    @Override
    public void write(Domain value, TProtocolWriter protocol)
            throws Exception
    {
        writeSingleJsonField(value, protocol, jsonCodec, DOMAIN_DATA_FIELD_ID, DOMAIN_DATA_FIELD_NAME, DOMAIN_STRUCT_NAME);
    }
}
