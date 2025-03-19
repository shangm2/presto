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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.experimental.auto_gen.ThriftQualifiedObjectName;

public class ThriftQualifiedObjectNameUtils
{
    private ThriftQualifiedObjectNameUtils() {}

    public static QualifiedObjectName toQualifiedObjectName(ThriftQualifiedObjectName thriftObjectName)
    {
        if (thriftObjectName == null) {
            return null;
        }
        return new QualifiedObjectName(thriftObjectName.getCatalogName(), thriftObjectName.getSchemaName(), thriftObjectName.getObjectName());
    }

    public static ThriftQualifiedObjectName fromQualifiedObjectName(QualifiedObjectName qualifiedObjectName)
    {
        if (qualifiedObjectName == null) {
            return null;
        }
        return new ThriftQualifiedObjectName(qualifiedObjectName.getCatalogName(), qualifiedObjectName.getSchemaName(), qualifiedObjectName.getObjectName());
    }
}
