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
package com.facebook.presto.metadata;

import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorTransactionHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftRemoteTransactionHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

public class RemoteTransactionHandle
        implements ConnectorTransactionHandle
{
    static {
//        ThriftSerializationRegistry.registerSerializer(RemoteTransactionHandle.class, RemoteTransactionHandle::serialize);
        ThriftSerializationRegistry.registerDeserializer(RemoteTransactionHandle.class, ThriftRemoteTransactionHandle.class, RemoteTransactionHandle::deserialize, null);
    }

    @JsonCreator
    public RemoteTransactionHandle()
    {
    }

    @JsonProperty
    public String getDummyValue()
    {
        // Necessary for Jackson serialization
        return null;
    }

    public RemoteTransactionHandle(ThriftRemoteTransactionHandle thriftHandle)
    {
        this();
    }

    @Override
    public ThriftConnectorTransactionHandle toThriftInterface()
    {
        try {
            TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
            ThriftConnectorTransactionHandle thriftHandle = new ThriftConnectorTransactionHandle();
            thriftHandle.setType(getImplementationType());
            thriftHandle.setSerializedConnectorTransactionHandle(serializer.serialize(this.toThrift()));
            return thriftHandle;
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ThriftRemoteTransactionHandle toThrift()
    {
        return new ThriftRemoteTransactionHandle();
    }

    public static RemoteTransactionHandle deserialize(byte[] bytes)
    {
        return new RemoteTransactionHandle();
    }
}
