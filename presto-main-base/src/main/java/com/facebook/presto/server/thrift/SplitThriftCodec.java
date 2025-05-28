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
import com.facebook.drift.TException;
import com.facebook.drift.codec.CodecThriftType;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TField;
import com.facebook.drift.protocol.TProtocolException;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.drift.protocol.TStruct;
import com.facebook.drift.protocol.TType;
import com.facebook.presto.connector.ConnectorSpecificCodecManager;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSpecificCodec;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.nio.ByteBuffer;

import static com.facebook.presto.server.thrift.CustomCodecUtils.createSyntheticMetadata;
import static com.facebook.presto.server.thrift.CustomCodecUtils.writeSingleJsonField;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SplitThriftCodec
        implements ThriftCodec<Split>
{
    private static final short SPLIT_DATA_FIELD_ID = 111;
    private static final String SPLIT_DATA_FIELD_NAME = "split";
    private static final String SPLIT_DATA_STRUCT_NAME = "Split";
    private static final ThriftType SYNTHETIC_STRUCT_TYPE = ThriftType.struct(createSyntheticMetadata(SPLIT_DATA_FIELD_ID, SPLIT_DATA_FIELD_NAME, Split.class, String.class, ThriftType.STRING));

    private final JsonCodec<Split> jsonCodec;

    private final ConnectorSpecificCodecManager codecManager;

    @Inject
    public SplitThriftCodec(JsonCodec<Split> jsonCodec, ThriftCatalog thriftCatalog, ConnectorSpecificCodecManager codecManager)
    {
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        thriftCatalog.addThriftType(SYNTHETIC_STRUCT_TYPE);
        this.codecManager = requireNonNull(codecManager, "codecManager is null");
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
    public Split read(TProtocolReader protocol)
            throws Exception
    {
        String jsonSplit = null;

        ConnectorId connectorId = null;
        ConnectorTransactionHandle transactionHandle = null;
        ConnectorSplit connectorSplit = null;
        Lifespan lifespan = null;
        SplitContext splitContext = null;

        protocol.readStructBegin();
        while (true) {
            TField field = protocol.readFieldBegin();
            if (field.getType() == TType.STOP || jsonSplit != null) {
                break;
            }
            switch (field.getId()) {
                case SPLIT_DATA_FIELD_ID:
                    if (field.getType() == TType.STRING) {
                        jsonSplit = protocol.readString();
                    }
                    else {
                        throw new TProtocolException(format("Unexpected field type: %s for field %s", field.getType(), field.getName()));
                    }
                    break;
                case 1:
                    connectorId = new ConnectorId(protocol.readString());
                    break;
                case 2:
                    ConnectorSpecificCodec<ConnectorTransactionHandle> transactionHandleCodec = codecManager.getTransactionHandleCodec(connectorId);
                    transactionHandle = transactionHandleCodec.deserialize(protocol.readBinary().array());
                    break;
                case 3:
                    ConnectorSpecificCodec<ConnectorSplit> splitCodec = codecManager.getSplitCodec(connectorId);
                    connectorSplit = splitCodec.deserialize(protocol.readBinary().array());
                    break;
                case 4:
                    lifespan = readLifespan(protocol);
                    break;
                case 5:
                    splitContext = readSplitContext(protocol);
                    break;
                default:
                    throw new TProtocolException(format("Unexpected field id: %s", field.getId()));
            }

            protocol.readFieldEnd();
        }
        protocol.readStructEnd();

        if (jsonSplit != null) {
            return jsonCodec.fromJson(jsonSplit);
        }

        return new Split(connectorId, transactionHandle, connectorSplit, lifespan, splitContext);
    }

    private SplitContext readSplitContext(TProtocolReader protocol)
    {
        try {
            protocol.readStructBegin();
            boolean cacheable = false;

            while (true) {
                TField field = protocol.readFieldBegin();
                if (field.getType() == TType.STOP) {
                    break;
                }

                switch (field.getId()) {
                    case 1:
                        cacheable = protocol.readBool();
                        break;
                    default:
                        throw new TProtocolException(format("Unexpected field id: %s", field.getId()));
                }
                protocol.readFieldEnd();
            }
            protocol.readStructEnd();
            return new SplitContext(cacheable);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    private Lifespan readLifespan(TProtocolReader protocol)
    {
        try {
            protocol.readStructBegin();

            boolean grouped = false;
            int groupId = 0;

            while (true) {
                TField field = protocol.readFieldBegin();
                if (field.getType() == TType.STOP) {
                    break;
                }
                switch (field.getId()) {
                    case 1:
                        grouped = protocol.readBool();
                        break;
                    case 2:
                        groupId = protocol.readI32();
                        break;
                    default:
                        throw new TProtocolException(format("Unexpected field id: %s", field.getId()));
                }
                protocol.readFieldEnd();
            }
            protocol.readStructEnd();
            return new Lifespan(grouped, groupId);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Split split, TProtocolWriter protocol)
            throws Exception
    {
        if (split.getConnectorId().getCatalogName().equals("prism")) {
            protocol.writeStructBegin(new TStruct("Split"));

            protocol.writeFieldBegin(new TField("connectorId", TType.STRING, (short) 1));
            protocol.writeString(split.getConnectorId().getCatalogName());
            protocol.writeFieldEnd();

            ConnectorSpecificCodec<ConnectorTransactionHandle> transactionHandleCodec = codecManager.getTransactionHandleCodec(split.getConnectorId());
            protocol.writeFieldBegin(new TField("transactionHandle", TType.STRING, (short) 2));
            protocol.writeBinary(ByteBuffer.wrap(transactionHandleCodec.serialize(split.getTransactionHandle())));
            protocol.writeFieldEnd();

            ConnectorSpecificCodec<ConnectorSplit> connectorSplitCodec = codecManager.getSplitCodec(split.getConnectorId());
            protocol.writeFieldBegin(new TField("connectorSplit", TType.STRING, (short) 3));
            protocol.writeBinary(ByteBuffer.wrap(connectorSplitCodec.serialize(split.getConnectorSplit())));
            protocol.writeFieldEnd();

            protocol.writeFieldBegin(new TField("lifespan", TType.STRUCT, (short) 4));
            protocol.writeStructBegin(new TStruct("Lifespan"));
            protocol.writeFieldBegin(new TField("grouped", TType.BOOL, (short) 1));
            protocol.writeBool(split.getLifespan().isGrouped());
            protocol.writeFieldEnd();
            protocol.writeFieldBegin(new TField("groupId", TType.I32, (short) 2));
            protocol.writeI32(split.getLifespan().getId());
            protocol.writeFieldEnd();
            protocol.writeFieldStop();
            protocol.writeStructEnd();
            protocol.writeFieldEnd();

            protocol.writeFieldBegin(new TField("splitContext", TType.STRUCT, (short) 5));
            protocol.writeStructBegin(new TStruct("SplitContext"));
            protocol.writeFieldBegin(new TField("cacheable", TType.BOOL, (short) 1));
            protocol.writeBool(split.getSplitContext().isCacheable());
            protocol.writeFieldEnd();
            protocol.writeFieldStop();
            protocol.writeStructEnd();
            protocol.writeFieldEnd();

            protocol.writeFieldStop();
            protocol.writeStructEnd();
            return;
        }

        writeSingleJsonField(split, protocol, jsonCodec, SPLIT_DATA_FIELD_ID, SPLIT_DATA_FIELD_NAME, SPLIT_DATA_STRUCT_NAME);
    }
}
