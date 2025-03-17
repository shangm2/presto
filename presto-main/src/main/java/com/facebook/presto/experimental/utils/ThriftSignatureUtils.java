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

import com.facebook.presto.experimental.auto_gen.ThriftSignature;
import com.facebook.presto.spi.function.Signature;

import java.util.stream.Collectors;

import static com.facebook.presto.experimental.utils.ThriftFunctionKindUtils.fromFunctionKind;
import static com.facebook.presto.experimental.utils.ThriftFunctionKindUtils.toFunctionKind;
import static com.facebook.presto.experimental.utils.ThriftQualifiedObjectNameUtils.fromQualifiedObjectName;
import static com.facebook.presto.experimental.utils.ThriftQualifiedObjectNameUtils.toQualifiedObjectName;
import static com.facebook.presto.experimental.utils.ThriftTypeSignatureUtils.fromTypeSignature;
import static com.facebook.presto.experimental.utils.ThriftTypeSignatureUtils.toTypeSignature;

public class ThriftSignatureUtils
{
    private ThriftSignatureUtils() {}

    public static Signature toSignature(ThriftSignature thriftSignature)
    {
        return new Signature(
                toQualifiedObjectName(thriftSignature.getName()),
                toFunctionKind(thriftSignature.getKind()),
                thriftSignature.getTypeVariableConstraints().stream().map(ThriftTypeVariableConstraintUtils::toTypeVariableConstraint).collect(Collectors.toList()),
                thriftSignature.getLongVariableConstraints().stream().map(ThriftLongVariableConstraintUtils::toLongVariableConstraint).collect(Collectors.toList()),
                toTypeSignature(thriftSignature.getReturnType()),
                thriftSignature.getArgumentTypes().stream().map(ThriftTypeSignatureUtils::toTypeSignature).collect(Collectors.toList()),
                thriftSignature.isVariableArity());
    }

    public static ThriftSignature fromSignature(Signature signature)
    {
        return new ThriftSignature(
                fromQualifiedObjectName(signature.getName()),
                fromFunctionKind(signature.getKind()),
                signature.getTypeVariableConstraints().stream().map(ThriftTypeVariableConstraintUtils::fromTypeVariableConstraint).collect(Collectors.toList()),
                signature.getLongVariableConstraints().stream().map(ThriftLongVariableConstraintUtils::fromLongVariableConstraint).collect(Collectors.toList()),
                fromTypeSignature(signature.getReturnType()),
                signature.getArgumentTypes().stream().map(ThriftTypeSignatureUtils::fromTypeSignature).collect(Collectors.toList()),
                signature.isVariableArity());
    }
}
