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

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.experimental.auto_gen.ThriftSqlFunctionId;
import com.facebook.presto.experimental.auto_gen.ThriftTypeSignature;
import com.facebook.presto.spi.function.SqlFunctionId;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.experimental.utils.ThriftQualifiedObjectNameUtils.fromQualifiedObjectName;
import static com.facebook.presto.experimental.utils.ThriftQualifiedObjectNameUtils.toQualifiedObjectName;

public class ThriftSqlFunctionUtils
{
    private ThriftSqlFunctionUtils() {}

    public ThriftSqlFunctionId fromSqlFunctionId(SqlFunctionId functionId)
    {
        if (functionId == null) {
            return null;
        }
        List<ThriftTypeSignature> thriftTypeSignatures = functionId.getArgumentTypes().stream().map(ThriftTypeSignatureUtils::fromTypeSignature).collect(Collectors.toList());
        return new ThriftSqlFunctionId(fromQualifiedObjectName(functionId.getFunctionName()), thriftTypeSignatures);
    }

    public SqlFunctionId toSqlFunctionId(ThriftSqlFunctionId thriftSqlFunctionId)
    {
        if (thriftSqlFunctionId == null) {
            return null;
        }
        List<TypeSignature> typeSignatures = thriftSqlFunctionId.getArgumentTypes().stream().map(ThriftTypeSignatureUtils::toTypeSignature).collect(Collectors.toList());
        return new SqlFunctionId(toQualifiedObjectName(thriftSqlFunctionId.getFunctionName()), typeSignatures);
    }
}
