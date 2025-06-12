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

import com.facebook.drift.TException;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.protocol.TField;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.drift.protocol.TStruct;
import com.facebook.drift.protocol.TType;
import com.facebook.presto.spi.PrestoException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractTypedThriftModule<T>
{
    private static final String TYPE_PROPERTY = "type";

    private final Class<T> baseClass;
    private final Function<T, String> nameResolver;
    private final Function<String, Class<? extends T>> classResolver;
    private final ThriftCodecManager thriftCodecManager;

    protected AbstractTypedThriftModule(Class<T> baseClass, Function<T, String> nameResolver, Function<String, Class<? extends T>> classResolver, ThriftCodecManager thriftCodecManager)
    {
        this.baseClass = requireNonNull(baseClass, "baseClass is null");
        this.nameResolver = requireNonNull(nameResolver, "nameResolver is null");
        this.classResolver = requireNonNull(classResolver, "classResolver is null");
        this.thriftCodecManager = requireNonNull(thriftCodecManager, "thriftCodecManager is null");
    }

    public ThriftSerializer<T> getSerializer()
    {
        return new
    }

    public interface ThriftSerializer<T>
    {
        void serialize(T value, TProtocolWriter writer);
    }

    public interface ThriftDeserializer<T>
    {
        T deserialize(TProtocolReader reader);
    }

    private static class InternalThriftSerializer<T>
            implements ThriftSerializer<T>
    {
        private final Class<T> baseClass;
        private final Function<T, String> nameResolver;
        private final ThriftCodecManager thriftCodecManager;
        private final Cache<Class<?>, ThriftCodec<?>> codecCache = CacheBuilder.newBuilder().build();

        public InternalThriftSerializer(Class<T> baseClass, Function<T, String> nameResolver, ThriftCodecManager thriftCodecManager)
        {
            this.baseClass = baseClass;
            this.nameResolver = nameResolver;
            this.thriftCodecManager = thriftCodecManager;
        }

        @Override
        public void serialize(T value, TProtocolWriter writer)
        {
            if (value == null) {
                return;
            }
            try {
                writer.writeStructBegin(new TStruct(baseClass.getSimpleName()));

                writer.writeFieldBegin(new TField(TYPE_PROPERTY, TType.STRING, (short) 1));
                String typeId = nameResolver.apply(value);
                requireNonNull(typeId, "typeId is null");
                writer.writeString(typeId);
                writer.writeFieldEnd();

                writer.writeFieldBegin(new TField("value", TType.STRUCT, (short) 2));
                Class<?> type = value.getClass();
                ThriftCodec<?> codec = codecCache.get(type, () ->  thriftCodecManager.getCodec(type));
                @SuppressWarnings("unchecked")
                ThriftCodec<T> typedCodec = (ThriftCodec<T>) codec;
                typedCodec.write(value, writer);
                writer.writeFieldEnd();

                writer.writeFieldEnd();
                writer.writeStructEnd();
            }
            catch (Exception e) {
                throw new IllegalArgumentException(format("Unexpected type found: %s", value.getClass()));
            }
        }
    }

    private static class InternalThriftDeserializer<T>
            implements ThriftDeserializer<T>
    {
        private final Class<T> baseClass;
        private final Function<String, Class<? extends T>> classResolver;
        private final ThriftCodecManager thriftCodecManager;

        public InternalThriftDeserializer(Class<T> baseClass, Function<String, Class<? extends T>> classResolver, ThriftCodecManager thriftCodecManager)
        {
            this.baseClass = baseClass;
            this.classResolver = classResolver;
            this.thriftCodecManager = thriftCodecManager;
        }

        @Override
        public T deserialize(TProtocolReader reader)
        {
            String typeId = null;
            T value = null;

            try {
                reader.readStructBegin();
                while (true) {
                    TField field = reader.readFieldBegin();
                    if (field.getType() == TType.STOP)
                    {
                        break;
                    }

                    switch (field.getId())
                    {
                        case 1:
                            typeId = reader.readString();
                            break;
                        case 2:
                            requireNonNull(typeId, "typeId is null");

                            Class<? extends T> type = classResolver.apply(typeId);
                            requireNonNull(type, "type is null");

                            ThriftCodec<?> codec = thriftCodecManager.getCodec(type);
                            @SuppressWarnings("unchecked")
                            ThriftCodec<T> typedCodec = (ThriftCodec<T>) codec;
                            value = typedCodec.read(reader);
                            break;
                    }

                    reader.readFieldEnd();
                }
                reader.readStructEnd();

                return value;
            }
            catch (Exception e) {
                throw new IllegalArgumentException("Can not deserialize: ", e);
            }
        }
    }
}
