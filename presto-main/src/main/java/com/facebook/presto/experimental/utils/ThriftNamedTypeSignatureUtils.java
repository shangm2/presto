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

import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.experimental.auto_gen.ThriftNamedTypeSignature;

import static com.facebook.presto.experimental.utils.ThriftRowFieldNameUtils.fromRowFieldName;
import static com.facebook.presto.experimental.utils.ThriftTypeSignatureUtils.fromTypeSignature;
import static com.facebook.presto.experimental.utils.ThriftTypeSignatureUtils.toTypeSignature;

public class ThriftNamedTypeSignatureUtils
{
    private ThriftNamedTypeSignatureUtils() {}

    public static NamedTypeSignature toNamedTypeSignature(ThriftNamedTypeSignature thriftNamedTypeSignature)
    {
        if (thriftNamedTypeSignature == null) {
            return null;
        }
        return new NamedTypeSignature(
                thriftNamedTypeSignature.getRowFieldName().map(ThriftRowFieldNameUtils::toRowFieldName),
                toTypeSignature(thriftNamedTypeSignature.getTypeSignature()));
    }

    public static ThriftNamedTypeSignature fromNamedTypeSignature(NamedTypeSignature namedTypeSignature)
    {
        if (namedTypeSignature == null) {
            return null;
        }
        ThriftNamedTypeSignature thriftNamedTypeSignature = new ThriftNamedTypeSignature(fromTypeSignature(namedTypeSignature.getTypeSignature()));
        namedTypeSignature.getFieldName().map(fieldName -> thriftNamedTypeSignature.setRowFieldName(fromRowFieldName(fieldName)));

        return thriftNamedTypeSignature;
    }
}
