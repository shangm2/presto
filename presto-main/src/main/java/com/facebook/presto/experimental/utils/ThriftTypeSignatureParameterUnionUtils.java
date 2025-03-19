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
package com.facebook.presto.experimental.utils;

import com.facebook.presto.common.type.TypeSignatureParameterUnion;
import com.facebook.presto.experimental.auto_gen.ThriftTypeSignatureParameterUnion;

import static com.facebook.presto.experimental.utils.ThriftDistinctTypeInfoUtils.fromDistinctTypeInfo;
import static com.facebook.presto.experimental.utils.ThriftDistinctTypeInfoUtils.toDistinctTypeInfo;
import static com.facebook.presto.experimental.utils.ThriftLongEnumMapUtils.fromLongEnumMap;
import static com.facebook.presto.experimental.utils.ThriftLongEnumMapUtils.toLongEnumMap;
import static com.facebook.presto.experimental.utils.ThriftNamedTypeSignatureUtils.fromNamedTypeSignature;
import static com.facebook.presto.experimental.utils.ThriftNamedTypeSignatureUtils.toNamedTypeSignature;
import static com.facebook.presto.experimental.utils.ThriftTypeSignatureUtils.fromTypeSignature;
import static com.facebook.presto.experimental.utils.ThriftTypeSignatureUtils.toTypeSignature;
import static com.facebook.presto.experimental.utils.ThriftVarcharEnumMapUtils.fromVarcharEnumMap;
import static com.facebook.presto.experimental.utils.ThriftVarcharEnumMapUtils.toVarcharEnumMap;

public class ThriftTypeSignatureParameterUnionUtils
{
    private ThriftTypeSignatureParameterUnionUtils() {}

    public static ThriftTypeSignatureParameterUnion fromTypeSignatureParameterUnion(TypeSignatureParameterUnion typeSignatureParameterUnion)
    {
        if (typeSignatureParameterUnion == null) {
            return null;
        }
        ThriftTypeSignatureParameterUnion thriftTypeSignatureParameterUnion = new ThriftTypeSignatureParameterUnion();

        if (typeSignatureParameterUnion.getTypeSignature() != null) {
            thriftTypeSignatureParameterUnion.setTypeSignature(fromTypeSignature(typeSignatureParameterUnion.getTypeSignature()));
        }
        else if (typeSignatureParameterUnion.getLongLiteral() != null) {
            thriftTypeSignatureParameterUnion.setLongLiteral(typeSignatureParameterUnion.getLongLiteral());
        }
        else if (typeSignatureParameterUnion.getNamedTypeSignature() != null) {
            thriftTypeSignatureParameterUnion.setNamedTypeSignature(fromNamedTypeSignature(typeSignatureParameterUnion.getNamedTypeSignature()));
        }
        else if (typeSignatureParameterUnion.getVariable() != null) {
            thriftTypeSignatureParameterUnion.setVariable(typeSignatureParameterUnion.getVariable());
        }
        else if (typeSignatureParameterUnion.getLongEnumMap() != null) {
            thriftTypeSignatureParameterUnion.setLongEnumMap(fromLongEnumMap(typeSignatureParameterUnion.getLongEnumMap()));
        }
        else if (typeSignatureParameterUnion.getVarcharEnumMap() != null) {
            thriftTypeSignatureParameterUnion.setVarcharEnumMap(fromVarcharEnumMap(typeSignatureParameterUnion.getVarcharEnumMap()));
        }
        else if (typeSignatureParameterUnion.getDistinctTypeInfo() != null) {
            thriftTypeSignatureParameterUnion.setDistinctTypeInfo(fromDistinctTypeInfo(typeSignatureParameterUnion.getDistinctTypeInfo()));
        }
        else {
            throw new RuntimeException("Unsupported type.");
        }

        return thriftTypeSignatureParameterUnion;
    }

    public static TypeSignatureParameterUnion toTypeSignatureParameterUnion(ThriftTypeSignatureParameterUnion thriftTypeSignatureParameterUnion)
    {
        if (thriftTypeSignatureParameterUnion == null) {
            return null;
        }
        TypeSignatureParameterUnion typeSignatureParameterUnion = null;
        if (thriftTypeSignatureParameterUnion != null) {
            if (thriftTypeSignatureParameterUnion.getTypeSignature() != null) {
                typeSignatureParameterUnion = new TypeSignatureParameterUnion(toTypeSignature(thriftTypeSignatureParameterUnion.getTypeSignature()));
            }
            else if (thriftTypeSignatureParameterUnion.isSetLongLiteral()) {
                typeSignatureParameterUnion = new TypeSignatureParameterUnion(thriftTypeSignatureParameterUnion.getLongLiteral());
            }
            else if (thriftTypeSignatureParameterUnion.getNamedTypeSignature() != null) {
                typeSignatureParameterUnion = new TypeSignatureParameterUnion(toNamedTypeSignature(thriftTypeSignatureParameterUnion.getNamedTypeSignature()));
            }
            else if (thriftTypeSignatureParameterUnion.getVariable() != null) {
                typeSignatureParameterUnion = new TypeSignatureParameterUnion(thriftTypeSignatureParameterUnion.getVariable());
            }
            else if (thriftTypeSignatureParameterUnion.getLongEnumMap() != null) {
                typeSignatureParameterUnion = new TypeSignatureParameterUnion(toLongEnumMap(thriftTypeSignatureParameterUnion.getLongEnumMap()));
            }
            else if (thriftTypeSignatureParameterUnion.getVarcharEnumMap() != null) {
                typeSignatureParameterUnion = new TypeSignatureParameterUnion(toVarcharEnumMap(thriftTypeSignatureParameterUnion.getVarcharEnumMap()));
            }
            else if (thriftTypeSignatureParameterUnion.getDistinctTypeInfo() != null) {
                typeSignatureParameterUnion = new TypeSignatureParameterUnion(toDistinctTypeInfo(thriftTypeSignatureParameterUnion.getDistinctTypeInfo()));
            }
            else {
                throw new RuntimeException("Unsupported type.");
            }
        }

        return typeSignatureParameterUnion;
    }
}
