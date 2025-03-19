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

import com.facebook.presto.common.type.DistinctTypeInfo;
import com.facebook.presto.experimental.auto_gen.ThriftDistinctTypeInfo;

import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.experimental.utils.ThriftQualifiedObjectNameUtils.fromQualifiedObjectName;
import static com.facebook.presto.experimental.utils.ThriftQualifiedObjectNameUtils.toQualifiedObjectName;
import static com.facebook.presto.experimental.utils.ThriftTypeSignatureUtils.fromTypeSignature;
import static com.facebook.presto.experimental.utils.ThriftTypeSignatureUtils.toTypeSignature;

public class ThriftDistinctTypeInfoUtils
{
    private ThriftDistinctTypeInfoUtils() {}

    public static ThriftDistinctTypeInfo fromDistinctTypeInfo(DistinctTypeInfo distinctTypeInfo)
    {
        if (distinctTypeInfo == null) {
            return null;
        }
        return new ThriftDistinctTypeInfo(
                fromQualifiedObjectName(distinctTypeInfo.getName()),
                fromTypeSignature(distinctTypeInfo.getBaseType()),
                distinctTypeInfo.isOrderable(),
                distinctTypeInfo.getTopMostAncestor().map(ThriftQualifiedObjectNameUtils::fromQualifiedObjectName).orElse(null),
                distinctTypeInfo.getOtherAncestors().stream().map(ThriftQualifiedObjectNameUtils::fromQualifiedObjectName).collect(Collectors.toList()));
    }

    public static DistinctTypeInfo toDistinctTypeInfo(ThriftDistinctTypeInfo thriftDistinctTypeInfo)
    {
        if (thriftDistinctTypeInfo == null) {
            return null;
        }
        return new DistinctTypeInfo(
                toQualifiedObjectName(thriftDistinctTypeInfo.getName()),
                toTypeSignature(thriftDistinctTypeInfo.getBaseType()),
                Optional.of(toQualifiedObjectName(thriftDistinctTypeInfo.getTopMostAncestor())),
                thriftDistinctTypeInfo.getOtherAncestors().stream().map(ThriftQualifiedObjectNameUtils::toQualifiedObjectName).collect(Collectors.toList()),
                thriftDistinctTypeInfo.isOrderable);
    }
}
