package com.facebook.presto.hive.util;

import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.auto_gen.ThriftPrimitiveTypeInfo;
import com.facebook.presto.common.experimental.auto_gen.ThriftTypeInfo;
import com.facebook.presto.common.experimental.auto_gen.ThriftVarcharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

public class TypeInfoRegister
{
    static {
        ThriftSerializationRegistry.registerSerializer(PrimitiveTypeInfo.class, TypeInfoRegister::toThriftPrimitiveTypeInfo, null);
        ThriftSerializationRegistry.registerDeserializer(PrimitiveTypeInfo.class, ThriftPrimitiveTypeInfo.class, TypeInfoRegister::deserializePrimitiveTypeInfo, TypeInfoRegister::fromThriftPrimitiveTypeInfo);

        ThriftSerializationRegistry.registerSerializer(VarcharTypeInfo.class, TypeInfoRegister::toThriftVarcharTypeInfo, null);
        ThriftSerializationRegistry.registerDeserializer(VarcharTypeInfo.class, ThriftVarcharTypeInfo.class, TypeInfoRegister::deserializeVarcharTypeInfo, TypeInfoRegister::fromThriftVarcharTypeInfo);
    }

    public static ThriftTypeInfo toThriftInterface(TypeInfo typeInfo)
    {
        throw new RuntimeException("ThriftTypeInfo toThriftInterface not implemented");
    }

    public static ThriftPrimitiveTypeInfo toThriftPrimitiveTypeInfo(PrimitiveTypeInfo typeInfo)
    {
        return new ThriftPrimitiveTypeInfo(typeInfo.getTypeName());
    }

    public static PrimitiveTypeInfo deserializePrimitiveTypeInfo(byte[] bytes)
    {
        try {
            ThriftPrimitiveTypeInfo thriftHandle = new ThriftPrimitiveTypeInfo();
            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            deserializer.deserialize(thriftHandle, bytes);
            return fromThriftPrimitiveTypeInfo(thriftHandle);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static PrimitiveTypeInfo fromThriftPrimitiveTypeInfo(ThriftPrimitiveTypeInfo thriftTypeInfo)
    {
        PrimitiveTypeInfo typeInfo = new PrimitiveTypeInfo();
        typeInfo.setTypeName(thriftTypeInfo.getTypeName());
        return typeInfo;
    }

    public static ThriftVarcharTypeInfo toThriftVarcharTypeInfo(VarcharTypeInfo typeInfo)
    {
        return new ThriftVarcharTypeInfo();
    }

    public static VarcharTypeInfo fromThriftVarcharTypeInfo(ThriftVarcharTypeInfo thriftTypeInfo)
    {
        return new VarcharTypeInfo(thriftTypeInfo.getLength());
    }

    public static VarcharTypeInfo deserializeVarcharTypeInfo(byte[] bytes)
    {
        try {
            ThriftVarcharTypeInfo thriftHandle = new ThriftVarcharTypeInfo();
            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            deserializer.deserialize(thriftHandle, bytes);
            return fromThriftVarcharTypeInfo(thriftHandle);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
