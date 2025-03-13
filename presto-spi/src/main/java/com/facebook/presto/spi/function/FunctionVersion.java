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
package com.facebook.presto.spi.function;

import com.facebook.presto.common.experimental.auto_gen.ThriftFunctionVersion;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FunctionVersion
{
    private static final FunctionVersion NOT_VERSIONED = new FunctionVersion(Optional.empty());
    private final Optional<String> version;

    public FunctionVersion(ThriftFunctionVersion thriftVersion)
    {
        this(thriftVersion.getVersion());
    }

    public ThriftFunctionVersion toThrift()
    {
        ThriftFunctionVersion thriftVersion = new ThriftFunctionVersion();
        version.ifPresent(thriftVersion::setVersion);
        return thriftVersion;
    }

    public static FunctionVersion notVersioned()
    {
        return NOT_VERSIONED;
    }

    public static FunctionVersion withVersion(String version)
    {
        return new FunctionVersion(Optional.of(version));
    }

    @JsonCreator
    public FunctionVersion(@JsonProperty("version") Optional<String> version)
    {
        this.version = requireNonNull(version, "version is null");
    }

    @JsonProperty("version")
    Optional<String> getVersion()
    {
        return version;
    }

    public boolean hasVersion()
    {
        return version.isPresent();
    }

    @Override
    public String toString()
    {
        return version.orElse("");
    }
}
