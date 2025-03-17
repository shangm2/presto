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

import com.facebook.presto.experimental.auto_gen.ThriftSqlFunctionId;
import com.facebook.presto.spi.function.SqlFunctionId;

import java.util.stream.Collectors;

import static com.facebook.presto.experimental.utils.ThriftQualifiedObjectNameUtils.fromQualifiedObjectName;
import static com.facebook.presto.experimental.utils.ThriftQualifiedObjectNameUtils.toQualifiedObjectName;

public class ThriftSqlFunctionIdUtils
{
    private ThriftSqlFunctionIdUtils() {}

    public static SqlFunctionId toSqlFunctionId(ThriftSqlFunctionId thriftSqlFunctionId)
    {
        return new SqlFunctionId(
                toQualifiedObjectName(thriftSqlFunctionId.getFunctionName()),
                thriftSqlFunctionId.getArgumentTypes().stream().map(ThriftTypeSignatureUtils::toTypeSignature).collect(Collectors.toList()));
    }

    public static ThriftSqlFunctionId fromSqlFunctionId(SqlFunctionId sqlFunctionId)
    {
        return new ThriftSqlFunctionId(
                fromQualifiedObjectName(sqlFunctionId.getFunctionName()),
                sqlFunctionId.getArgumentTypes().stream().map(ThriftTypeSignatureUtils::fromTypeSignature).collect(Collectors.toList()));
    }
}
