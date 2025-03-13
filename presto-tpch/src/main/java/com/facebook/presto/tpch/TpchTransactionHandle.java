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
package com.facebook.presto.tpch;

import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorTransactionHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftTpchTransactionHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

public enum TpchTransactionHandle
        implements ConnectorTransactionHandle
{
    INSTANCE;

    static {
//        ThriftSerializationRegistry.registerSerializer(TpchTransactionHandle.class, TpchTransactionHandle::serialize);
        ThriftSerializationRegistry.registerDeserializer("TPCH_TRANSACTION_HANDLE", TpchTransactionHandle::deserialize);
    }

    public static TpchTransactionHandle createTpchTransactionHandle(ThriftTpchTransactionHandle thriftHandle)
    {
        return TpchTransactionHandle.valueOf(thriftHandle.getValue());
    }

    @Override
    public ThriftConnectorTransactionHandle toThriftInterface()
    {
        try {
            TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            ThriftConnectorTransactionHandle thriftHandle = new ThriftConnectorTransactionHandle();
            thriftHandle.setType(getImplementationType());
            thriftHandle.setSerializedConnectorTransactionHandle(serializer.serialize(new ThriftTpchTransactionHandle(this.name())));
            return thriftHandle;
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static TpchTransactionHandle deserialize(byte[] bytes)
    {
        try {
            ThriftTpchTransactionHandle thriftHandle = new ThriftTpchTransactionHandle();
            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            deserializer.deserialize(thriftHandle, bytes);
            return createTpchTransactionHandle(thriftHandle);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getImplementationType()
    {
        return "TPCH_TRANSACTION_HANDLE";
    }
}
