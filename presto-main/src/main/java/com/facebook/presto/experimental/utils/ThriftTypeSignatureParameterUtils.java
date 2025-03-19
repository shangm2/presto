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

import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.experimental.auto_gen.ThriftTypeSignatureParameter;

import static com.facebook.presto.experimental.utils.ThriftParameterKindUtils.fromParameterKind;
import static com.facebook.presto.experimental.utils.ThriftParameterKindUtils.toParameterKind;
import static com.facebook.presto.experimental.utils.ThriftTypeSignatureParameterUnionUtils.fromTypeSignatureParameterUnion;
import static com.facebook.presto.experimental.utils.ThriftTypeSignatureParameterUnionUtils.toTypeSignatureParameterUnion;

public class ThriftTypeSignatureParameterUtils
{
    private ThriftTypeSignatureParameterUtils() {}

    public static TypeSignatureParameter toTypeSignatureParameter(ThriftTypeSignatureParameter thriftTypeSignatureParameter)
    {
        if (thriftTypeSignatureParameter == null) {
            return null;
        }
        return new TypeSignatureParameter(
                toParameterKind(thriftTypeSignatureParameter.getKind()),
                toTypeSignatureParameterUnion(thriftTypeSignatureParameter.getThriftTypeSignatureParameterUnion()));
    }

    public static ThriftTypeSignatureParameter fromTypeSignatureParameter(TypeSignatureParameter typeSignatureParameter)
    {
        if (typeSignatureParameter == null) {
            return null;
        }
        return new ThriftTypeSignatureParameter(
                fromParameterKind(typeSignatureParameter.getKind()),
                fromTypeSignatureParameterUnion(typeSignatureParameter.getValue()));
    }
}
