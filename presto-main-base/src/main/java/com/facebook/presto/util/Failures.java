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
package com.facebook.presto.util;

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.client.ErrorLocation;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.InvalidTypeDefinitionException;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.RuntimeUnit;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.Failure;
import com.facebook.presto.spi.ErrorCause;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoTransportException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.tree.NodeLocation;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SliceTooLargeException;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.ErrorCause.UNKNOWN;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TYPE_DEFINITION;
import static com.facebook.presto.spi.StandardErrorCode.SLICE_TOO_LARGE;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Failures
{
    private static final String NODE_CRASHED_ERROR = "The node may have crashed or be under too much load. " +
            "This is probably a transient issue, so please retry your query in a few minutes.";

    public static final String WORKER_NODE_ERROR = "Encountered too many errors talking to a worker node. " + NODE_CRASHED_ERROR;

    public static final String REMOTE_TASK_MISMATCH_ERROR = "Could not communicate with the remote task. " + NODE_CRASHED_ERROR;

    // no limitation by default
    private static final int defaultExecutionFailureInfoDepth = Integer.MAX_VALUE;
    private static final int defaultStackTraceElementCount = Integer.MAX_VALUE;
    private static final int defaultSuppressedExceptionsCount = Integer.MAX_VALUE;

    private Failures() {}

    public static ExecutionFailureInfo toFailure(Throwable failure)
    {
        return toFailure(failure, newIdentityHashSet());
    }

    public static ExecutionFailureInfo toFailureWithLimitedTrace(Throwable failure, int maxExecutionFailureInfoDepth, int maxStackTraceDepth, int maxSuppressedExceptions, RuntimeStats runtimeStats)
    {
        return toFailureWithLimitedTrace(failure, newIdentityHashSet(), 0, maxExecutionFailureInfoDepth, maxStackTraceDepth, maxSuppressedExceptions, runtimeStats);
    }

    public static void checkArgument(boolean expression, String errorMessage)
    {
        if (!expression) {
            throw new PrestoException(StandardErrorCode.INVALID_ARGUMENTS, errorMessage);
        }
    }

    public static void checkArgument(boolean expression, String errorMessageTemplate, Object... errorMessageArgs)
    {
        if (!expression) {
            throw new PrestoException(StandardErrorCode.INVALID_ARGUMENTS, String.format(errorMessageTemplate, errorMessageArgs));
        }
    }

    public static void checkCondition(boolean condition, ErrorCodeSupplier errorCode, String formatString, Object... args)
    {
        if (!condition) {
            throw new PrestoException(errorCode, format(formatString, args));
        }
    }

    public static List<ExecutionFailureInfo> toFailures(Collection<? extends Throwable> failures)
    {
        return failures.stream()
                .map(Failures::toFailure)
                .collect(toImmutableList());
    }

    private static ExecutionFailureInfo toFailure(Throwable throwable, Set<Throwable> seenFailures)
    {
        return toFailureWithLimitedTrace(throwable, seenFailures, 0, defaultExecutionFailureInfoDepth, defaultStackTraceElementCount, defaultSuppressedExceptionsCount, new RuntimeStats());
    }

    private static ExecutionFailureInfo toFailureWithLimitedTrace(Throwable throwable, Set<Throwable> seenFailures,
            int currentDepth, int maxExecutionFailureInfoDepth,
            int maxStackTraceElementCount, int maxSuppressedExceptionsCount,
            RuntimeStats runtimeStats)
    {
        runtimeStats.addMetricValue("executionFailureInfoDepth", RuntimeUnit.NONE, currentDepth);
        if (throwable == null || currentDepth >= maxExecutionFailureInfoDepth) {
            return null;
        }

        String type;
        HostAddress remoteHost = null;
        if (throwable instanceof Failure) {
            type = ((Failure) throwable).getType();
        }
        else {
            Class<?> clazz = throwable.getClass();
            type = firstNonNull(clazz.getCanonicalName(), clazz.getName());
        }
        if (throwable instanceof PrestoTransportException) {
            remoteHost = ((PrestoTransportException) throwable).getRemoteHost();
        }

        if (seenFailures.contains(throwable)) {
            return new ExecutionFailureInfo(type, "[cyclic] " + throwable.getMessage(), null, ImmutableList.of(), ImmutableList.of(), null, GENERIC_INTERNAL_ERROR.toErrorCode(), remoteHost, UNKNOWN);
        }
        seenFailures.add(throwable);

        ExecutionFailureInfo cause = toFailureWithLimitedTrace(throwable.getCause(), seenFailures, currentDepth + 1, maxExecutionFailureInfoDepth, maxStackTraceElementCount, maxSuppressedExceptionsCount, runtimeStats);
        ErrorCode errorCode = toErrorCode(throwable);
        if (errorCode == null) {
            if (cause == null) {
                errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
            }
            else {
                errorCode = cause.getErrorCode();
            }
        }
        Throwable[] throwables = throwable.getSuppressed();
        runtimeStats.addMetricValue("suppressedExceptionsCount", RuntimeUnit.NONE, throwables.length);
        StackTraceElement[] stackTraceElements = throwable.getStackTrace();
        runtimeStats.addMetricValue("stackTraceElementsCount", RuntimeUnit.NONE, stackTraceElements.length);

        return new ExecutionFailureInfo(
                type,
                throwable.getMessage(),
                cause,
                Arrays.stream(throwables)
                        .limit(maxSuppressedExceptionsCount)
                        .map(failure -> toFailureWithLimitedTrace(failure, seenFailures, currentDepth + 1, maxExecutionFailureInfoDepth, maxStackTraceElementCount, maxSuppressedExceptionsCount, runtimeStats))
                        .collect(toImmutableList()),
                Arrays.stream(stackTraceElements).limit(maxStackTraceElementCount).map(Object::toString).collect(Collectors.toList()),
                getErrorLocation(throwable),
                errorCode,
                remoteHost,
                toErrorCause(throwable));
    }

    @Nullable
    private static ErrorLocation getErrorLocation(Throwable throwable)
    {
        // TODO: this is a big hack
        if (throwable instanceof ParsingException) {
            ParsingException e = (ParsingException) throwable;
            return new ErrorLocation(e.getLineNumber(), e.getColumnNumber());
        }
        else if (throwable instanceof SemanticException) {
            SemanticException e = (SemanticException) throwable;
            if (e.getLocation().isPresent()) {
                NodeLocation nodeLocation = e.getLocation().get();
                return new ErrorLocation(nodeLocation.getLineNumber(), nodeLocation.getColumnNumber());
            }
        }
        return null;
    }

    @Nullable
    private static ErrorCode toErrorCode(Throwable throwable)
    {
        requireNonNull(throwable);

        if (throwable instanceof SliceTooLargeException) {
            return SLICE_TOO_LARGE.toErrorCode();
        }

        if (throwable instanceof InvalidTypeDefinitionException) {
            return INVALID_TYPE_DEFINITION.toErrorCode();
        }

        if (throwable instanceof PrestoException) {
            return ((PrestoException) throwable).getErrorCode();
        }
        if (throwable instanceof Failure && ((Failure) throwable).getErrorCode() != null) {
            return ((Failure) throwable).getErrorCode();
        }
        if (throwable instanceof ParsingException || throwable instanceof SemanticException) {
            return SYNTAX_ERROR.toErrorCode();
        }
        return null;
    }

    private static ErrorCause toErrorCause(Throwable throwable)
    {
        requireNonNull(throwable);

        if (throwable instanceof ExceededMemoryLimitException) {
            return ((ExceededMemoryLimitException) throwable).getErrorCause();
        }
        return UNKNOWN;
    }

    public static PrestoException internalError(Throwable t)
    {
        throwIfInstanceOf(t, Error.class);
        throwIfInstanceOf(t, PrestoException.class);
        return new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, t);
    }
}
