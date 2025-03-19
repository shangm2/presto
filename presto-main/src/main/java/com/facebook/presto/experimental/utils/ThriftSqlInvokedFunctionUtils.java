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

import com.facebook.presto.experimental.auto_gen.ThriftSqlInvokedFunction;
import com.facebook.presto.spi.function.SqlInvokedFunction;

import java.util.stream.Collectors;

import static com.facebook.presto.experimental.utils.ThriftFunctionVersionUtils.fromFunctionVersion;
import static com.facebook.presto.experimental.utils.ThriftRoutineCharacteristicsUtils.fromRoutineCharacteristics;
import static com.facebook.presto.experimental.utils.ThriftRoutineCharacteristicsUtils.toRoutineCharacteristics;
import static com.facebook.presto.experimental.utils.ThriftSignatureUtils.fromSignature;
import static com.facebook.presto.experimental.utils.ThriftSignatureUtils.toSignature;
import static com.facebook.presto.experimental.utils.ThriftSqlFunctionIdUtils.fromSqlFunctionId;
import static com.facebook.presto.experimental.utils.ThriftSqlFunctionIdUtils.toSqlFunctionId;

public class ThriftSqlInvokedFunctionUtils
{
    private ThriftSqlInvokedFunctionUtils() {}

    // TODO: add more constructor
    public static SqlInvokedFunction toSqlInvokedFunction(ThriftSqlInvokedFunction thriftSqlInvokedFunction)
    {
        if (thriftSqlInvokedFunction == null) {
            return null;
        }
        return new SqlInvokedFunction(
                thriftSqlInvokedFunction.getParameters().stream().map(ThriftParameterUtils::toParameter).collect(Collectors.toList()),
                thriftSqlInvokedFunction.getDescription(),
                toRoutineCharacteristics(thriftSqlInvokedFunction.getRoutineCharacteristics()),
                thriftSqlInvokedFunction.getBody(),
                thriftSqlInvokedFunction.isVariableArity(),
                toSignature(thriftSqlInvokedFunction.getSignature()),
                toSqlFunctionId(thriftSqlInvokedFunction.getFunctionId()));
    }

    public static ThriftSqlInvokedFunction fromSqlInvokedFunction(SqlInvokedFunction sqlInvokedFunction)
    {
        if (sqlInvokedFunction == null) {
            return null;
        }
        ThriftSqlInvokedFunction thriftSqlInvokedFunction = new ThriftSqlInvokedFunction(
                sqlInvokedFunction.getParameters().stream().map(ThriftParameterUtils::fromParameter).collect(Collectors.toList()),
                sqlInvokedFunction.getDescription(),
                fromRoutineCharacteristics(sqlInvokedFunction.getRoutineCharacteristics()),
                sqlInvokedFunction.getBody(),
                sqlInvokedFunction.getVariableArity(),
                fromSignature(sqlInvokedFunction.getSignature()),
                fromSqlFunctionId(sqlInvokedFunction.getFunctionId()),
                fromFunctionVersion(sqlInvokedFunction.getVersion()));
        sqlInvokedFunction.getFunctionHandle().map(ThriftSqlFunctionHandleUtils::fromSqlFunctionHandle)
                .map(thriftSqlInvokedFunction::setFunctionHandle);
        sqlInvokedFunction.getAggregationMetadata().map(ThriftAggregationFunctionMetadataUtils::fromAggregationFunctionMetadata)
                .map(thriftSqlInvokedFunction::setAggregationMetadata);

        return thriftSqlInvokedFunction;
    }
}
