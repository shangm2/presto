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

import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.experimental.auto_gen.ThriftSelectedRole;
import com.facebook.presto.experimental.auto_gen.ThriftSessionRepresentation;
import com.facebook.presto.experimental.auto_gen.ThriftSqlFunctionId;
import com.facebook.presto.experimental.auto_gen.ThriftSqlInvokedFunction;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.security.SelectedRole;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.experimental.utils.ThriftResourceEstimatesUtils.fromResourceEstimates;
import static com.facebook.presto.experimental.utils.ThriftResourceEstimatesUtils.toResourceEstimates;
import static com.facebook.presto.experimental.utils.ThriftSelectedRoleUtils.fromSelectedRole;
import static com.facebook.presto.experimental.utils.ThriftSelectedRoleUtils.toSelectedRole;
import static com.facebook.presto.experimental.utils.ThriftSqlFunctionIdUtils.fromSqlFunctionId;
import static com.facebook.presto.experimental.utils.ThriftSqlFunctionIdUtils.toSqlFunctionId;
import static com.facebook.presto.experimental.utils.ThriftSqlInvokedFunctionUtils.fromSqlInvokedFunction;
import static com.facebook.presto.experimental.utils.ThriftSqlInvokedFunctionUtils.toSqlInvokedFunction;
import static com.facebook.presto.experimental.utils.ThriftTimeZoneKeyUtils.fromTimeZoneKey;
import static com.facebook.presto.experimental.utils.ThriftTimeZoneKeyUtils.toTimeZoneKey;
import static com.facebook.presto.experimental.utils.ThriftTransactionIdUtils.fromTransactionId;
import static com.facebook.presto.experimental.utils.ThriftTransactionIdUtils.toTransactionId;

public class ThriftSessionRepresentationUtils
{
    private ThriftSessionRepresentationUtils() {}

    public static SessionRepresentation toSessionRepresentation(ThriftSessionRepresentation thriftSession)
    {
        Map<ConnectorId, Map<String, String>> catalogProperties = thriftSession.getCatalogProperties().entrySet().stream().collect(Collectors.toMap(
                entry -> new ConnectorId(entry.getKey()),
                Map.Entry::getValue));
        Map<String, SelectedRole> roles = thriftSession.getRoles().entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> toSelectedRole(entry.getValue())));

        Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions = thriftSession.getSessionFunctions().entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> toSqlFunctionId(entry.getKey()),
                        entry -> toSqlInvokedFunction(entry.getValue())));
        return new SessionRepresentation(
                thriftSession.getQueryId(),
                Optional.ofNullable(toTransactionId(thriftSession.getTransactionId())),
                thriftSession.isClientTransactionSupport(),
                thriftSession.getUser(),
                thriftSession.getPrincipal(),
                thriftSession.getSource(),
                thriftSession.getCatalog(),
                thriftSession.getSchema(),
                thriftSession.getTraceToken(),
                toTimeZoneKey(thriftSession.getTimeZoneKey()),
                Locale.forLanguageTag(thriftSession.getLocale().replace('_', '-')),
                thriftSession.getRemoteUserAddress(),
                thriftSession.getUserAgent(),
                thriftSession.getClientInfo(),
                thriftSession.getClientTags(),
                toResourceEstimates(thriftSession.getResourceEstimates()),
                thriftSession.getStartTime(),
                thriftSession.getSystemProperties(),
                catalogProperties,
                thriftSession.getUnprocessedCatalogProperties(),
                roles,
                thriftSession.getPreparedStatements(),
                sessionFunctions);
    }

    public static ThriftSessionRepresentation fromSessionRepresentation(SessionRepresentation sessionRepresentation)
    {
        Map<String, Map<String, String>> catalogProperties = sessionRepresentation.getCatalogProperties().entrySet().stream().collect(Collectors.toMap(
                Object::toString,
                Map.Entry::getValue));
        Map<String, ThriftSelectedRole> roles = sessionRepresentation.getRoles().entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> fromSelectedRole(entry.getValue())));
        Map<ThriftSqlFunctionId, ThriftSqlInvokedFunction> sessionFunctions = sessionRepresentation.getSessionFunctions().entrySet()
                .stream().collect(Collectors.toMap(
                        entry -> fromSqlFunctionId(entry.getKey()),
                        entry -> fromSqlInvokedFunction(entry.getValue())));

        ThriftSessionRepresentation thriftSessionRepresentation = new ThriftSessionRepresentation(
                sessionRepresentation.getQueryId(),
                sessionRepresentation.isClientTransactionSupport(),
                sessionRepresentation.getUser(),
                fromTimeZoneKey(sessionRepresentation.getTimeZoneKey()),
                sessionRepresentation.getLocale().toString(),
                sessionRepresentation.getClientTags(),
                sessionRepresentation.getStartTime(),
                fromResourceEstimates(sessionRepresentation.getResourceEstimates()),
                sessionRepresentation.getSystemProperties(),
                catalogProperties,
                sessionRepresentation.getUnprocessedCatalogProperties(),
                roles,
                sessionRepresentation.getPreparedStatements(),
                sessionFunctions);
        sessionRepresentation.getTransactionId().map(id -> thriftSessionRepresentation.setTransactionId(fromTransactionId(id)));
        sessionRepresentation.getPrincipal().map(thriftSessionRepresentation::setPrincipal);
        sessionRepresentation.getSource().map(thriftSessionRepresentation::setSource);
        sessionRepresentation.getCatalog().map(thriftSessionRepresentation::setCatalog);
        sessionRepresentation.getSchema().map(thriftSessionRepresentation::setSchema);
        sessionRepresentation.getTraceToken().map(thriftSessionRepresentation::setTraceToken);
        sessionRepresentation.getRemoteUserAddress().map(thriftSessionRepresentation::setRemoteUserAddress);
        sessionRepresentation.getUserAgent().map(thriftSessionRepresentation::setUserAgent);
        sessionRepresentation.getClientInfo().map(thriftSessionRepresentation::setClientInfo);

        return thriftSessionRepresentation;
    }
}
