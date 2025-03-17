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

import com.facebook.presto.experimental.auto_gen.ThriftSqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionHandle;

import static com.facebook.presto.experimental.utils.ThriftSqlFunctionIdUtils.fromSqlFunctionId;
import static com.facebook.presto.experimental.utils.ThriftSqlFunctionIdUtils.toSqlFunctionId;

public class ThriftSqlFunctionHandleUtils
{
    private ThriftSqlFunctionHandleUtils() {}

    public static SqlFunctionHandle toSqlFunctionHandle(ThriftSqlFunctionHandle thriftSqlFunctionHandle)
    {
        return new SqlFunctionHandle(
                toSqlFunctionId(thriftSqlFunctionHandle.getFunctionId()),
                thriftSqlFunctionHandle.getVersion());
    }

    public static ThriftSqlFunctionHandle fromSqlFunctionHandle(SqlFunctionHandle sqlFunctionHandle)
    {
        return new ThriftSqlFunctionHandle(
                fromSqlFunctionId(sqlFunctionHandle.getFunctionId()),
                sqlFunctionHandle.getVersion());
    }
}
