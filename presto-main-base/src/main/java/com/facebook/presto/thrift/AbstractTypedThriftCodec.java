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
import com.facebook.drift.annotations.ThriftField.Requiredness;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.metadata.DefaultThriftTypeReference;
import com.facebook.drift.codec.metadata.FieldKind;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftFieldMetadata;
import com.facebook.drift.codec.metadata.ThriftMethodInjection;
import com.facebook.drift.codec.metadata.ThriftStructMetadata;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TField;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import com.facebook.drift.protocol.TStruct;
import com.facebook.drift.protocol.TType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractTypedThriftCodec<T>
        implements ThriftCodec<T>
{
    private static final Set<String> THRIFT_ENABLED_CONNECTORS = new HashSet<>(Arrays.asList("prism"));
    private static final Set<String> THRIFT_ENABLED_BASECLASSES = new HashSet<>(Arrays.asList("ConnectorSplit"));
    private static final String TYPE_PROPERTY = "type";
    private static final String THRIFT_VALUE_PROPERTY = "thrift";
    private static final String JSON_VALUE_PROPERTY = "json";
    private static final short TYPE_FIELD_ID = 1;
    private static final short THRIFT_FIELD_ID = 2;
    private static final short JSON_FIELD_ID = 3;

    private final Class<T> baseClass;
    private final JsonCodec<T> jsonCodec;
    private final Function<T, String> nameResolver;
    private final Function<String, Class<? extends T>> classResolver;
    private final Provider<ThriftCodecManager> thriftCodecManagerProvider;

    protected AbstractTypedThriftCodec(Class<T> baseClass,
            JsonCodec<T> jsonCodec,
            Function<T, String> nameResolver,
            Function<String, Class<? extends T>> classResolver,
            GlobalThriftCodecManager globalThriftCodecManager)
    {
        this.baseClass = requireNonNull(baseClass, "baseClass is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.nameResolver = requireNonNull(nameResolver, "nameResolver is null");
        this.classResolver = requireNonNull(classResolver, "classResolver is null");

        this.thriftCodecManagerProvider = requireNonNull(globalThriftCodecManager, "globalThriftCodecManager is null").getThriftCodecManagerProvider();
    }

    @Override
    public abstract ThriftType getType();

    protected static ThriftType createThriftType(Class<?> baseClass)
    {
        List<ThriftFieldMetadata> fields = new ArrayList<>();
        try {
            fields.add(new ThriftFieldMetadata(
                    TYPE_FIELD_ID,
                    false, false, Requiredness.NONE, ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.STRING),
                    TYPE_PROPERTY,
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(AbstractTypedThriftCodec.class.getDeclaredMethod("getTypeField"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));
            fields.add(new ThriftFieldMetadata(
                    THRIFT_FIELD_ID,
                    false, false, Requiredness.OPTIONAL, ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.struct(new ThriftStructMetadata(
                            baseClass.getSimpleName(),
                            ImmutableMap.of(), Object.class, null, ThriftStructMetadata.MetadataType.STRUCT,
                            Optional.empty(), ImmutableList.of(), ImmutableList.of(), Optional.empty(), ImmutableList.of()))),
                    THRIFT_VALUE_PROPERTY,
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(AbstractTypedThriftCodec.class.getDeclaredMethod("getThriftField"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));

            fields.add(new ThriftFieldMetadata(
                    JSON_FIELD_ID,
                    false, false, Requiredness.OPTIONAL, ImmutableMap.of(),
                    new DefaultThriftTypeReference(ThriftType.STRING),
                    JSON_VALUE_PROPERTY,
                    FieldKind.THRIFT_FIELD,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(new ThriftMethodInjection(AbstractTypedThriftCodec.class.getDeclaredMethod("getJsonField"), ImmutableList.of())),
                    Optional.empty(),
                    Optional.empty()));
        }
        catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Failed to create ThriftFieldMetadata", e);
        }

        return ThriftType.struct(new ThriftStructMetadata(
                baseClass.getSimpleName() + "Wrapper",
                ImmutableMap.of(), baseClass, null, ThriftStructMetadata.MetadataType.STRUCT,
                Optional.empty(), ImmutableList.of(), fields, Optional.empty(), ImmutableList.of()));
    }

    @Override
    public T read(TProtocolReader reader)
            throws Exception
    {
        String typeId = null;
        T value = null;
        String jsonValue = null;

        reader.readStructBegin();
        while (true) {
            TField field = reader.readFieldBegin();
            if (field.getType() == TType.STOP) {
                break;
            }
            switch (field.getId()) {
                case TYPE_FIELD_ID:
                    typeId = reader.readString();
                    break;
                case THRIFT_FIELD_ID:
                    requireNonNull(typeId, "typeId is null");
                    Class<? extends T> concreteClass = classResolver.apply(typeId);
                    requireNonNull(concreteClass, "concreteClass is null");

                    ThriftCodec<? extends T> codec = thriftCodecManagerProvider.get().getCodec(concreteClass);
                    value = codec.read(reader);
                    break;
                case JSON_FIELD_ID:
                    jsonValue = reader.readString();
                    break;
                default:
                    throw new IllegalArgumentException(format("Unexpected field id found: %s", field.getId()));
            }
            reader.readFieldEnd();
        }
        reader.readStructEnd();

        if (value != null) {
            return value;
        }
        if (jsonValue != null) {
            return jsonCodec.fromJson(jsonValue);
        }
        throw new IllegalStateException("Neither thrift nor json value was present");
    }

    @Override
    public void write(T value, TProtocolWriter writer)
            throws Exception
    {
        if (value == null) {
            return;
        }

        writer.writeStructBegin(new TStruct(baseClass.getSimpleName()));

        writer.writeFieldBegin(new TField(TYPE_PROPERTY, TType.STRING, TYPE_FIELD_ID));
        String typeId = nameResolver.apply(value);
        requireNonNull(typeId, "typeId is null");

        writer.writeString(typeId);
        writer.writeFieldEnd();

        if (isThriftEnabled(typeId, baseClass)) {
            System.out.println(format("==========> type id: %s, base class: %s, concrete class: %s", typeId, baseClass.getSimpleName(), value.getClass().getSimpleName()));

            writer.writeFieldBegin(new TField(THRIFT_VALUE_PROPERTY, TType.STRUCT, THRIFT_FIELD_ID));
            Class<?> concreteType = value.getClass();
            System.out.println("==========> AbstractTypedThriftCodec thriftcodecmanager  " + System.identityHashCode(thriftCodecManagerProvider.get()));
            ThriftCatalog thriftCatalog = thriftCodecManagerProvider.get().getCatalog();
            System.out.println("==========> AbstractTypedThriftCodec thriftcatalog  " + System.identityHashCode(thriftCatalog));
            if (concreteType.getSimpleName().equals("HiveSplit")) {
                System.out.println("==========> thriftcatalog " + Joiner.on(", ").withKeyValueSeparator("=").join(thriftCatalog.getManualTypes()));
            }
            ThriftCodec concreteCodec = thriftCodecManagerProvider.get().getCodec(concreteType);
            concreteCodec.write(value, writer);
            writer.writeFieldEnd();
        }
        else {
            writer.writeFieldBegin(new TField(JSON_VALUE_PROPERTY, TType.STRING, JSON_FIELD_ID));
            writer.writeString(jsonCodec.toJson(value));
            writer.writeFieldEnd();
        }

        writer.writeFieldStop();
        writer.writeStructEnd();
    }

    private boolean isThriftEnabled(String typeId, Class<?> baseClass)
    {
        return THRIFT_ENABLED_CONNECTORS.contains(typeId) && THRIFT_ENABLED_BASECLASSES.contains(baseClass.getSimpleName());
    }

    private String getTypeField()
    {
        return "getTypeField";
    }

    private String getThriftField()
    {
        return "getThriftField";
    }

    private String getJsonField()
    {
        return "getJsonField";
    }
}
