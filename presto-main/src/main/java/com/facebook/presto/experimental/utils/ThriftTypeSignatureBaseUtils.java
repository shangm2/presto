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

import com.facebook.presto.common.type.TypeSignatureBase;
import com.facebook.presto.experimental.auto_gen.ThriftTypeSignatureBase;

import static com.facebook.presto.experimental.utils.ThriftQualifiedObjectNameUtils.fromQualifiedObjectName;
import static com.facebook.presto.experimental.utils.ThriftQualifiedObjectNameUtils.toQualifiedObjectName;

public class ThriftTypeSignatureBaseUtils
{
    private ThriftTypeSignatureBaseUtils() {}

    public static TypeSignatureBase toTypeSignatureBase(ThriftTypeSignatureBase thriftTypeSignatureBase)
    {
        return new TypeSignatureBase(
                thriftTypeSignatureBase.getStandardTypeBase(),
                thriftTypeSignatureBase.getTypeName().map(ThriftQualifiedObjectNameUtils::toQualifiedObjectName));
    }

    public static ThriftTypeSignatureBase fromTypeSignatureBase(TypeSignatureBase typeSignatureBase)
    {
        ThriftTypeSignatureBase thriftTypeSignatureBase = new ThriftTypeSignatureBase();
        typeSignatureBase.getOptionalStandardTypeBase().ifPresent(thriftTypeSignatureBase::setStandardTypeBase);
        typeSignatureBase.getOptionalQualifiedObjectName().map(ThriftQualifiedObjectNameUtils::fromQualifiedObjectName).ifPresent(thriftTypeSignatureBase::setTypeName);
        return thriftTypeSignatureBase;
    }
}
