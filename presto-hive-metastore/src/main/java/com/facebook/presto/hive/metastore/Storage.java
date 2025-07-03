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
package com.facebook.presto.hive.metastore;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.hive.HiveBucketProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
@ThriftStruct
public class Storage
{
    private final StorageFormat storageFormat;
    private final String location;
    private final Optional<HiveBucketProperty> bucketProperty;
    private final boolean skewed;
    private final Map<String, String> serdeParameters;
    private final Map<String, String> parameters;

    @JsonCreator
    @ThriftConstructor
    public Storage(
            @JsonProperty("storageFormat") StorageFormat storageFormat,
            @JsonProperty("location") String location,
            @JsonProperty("bucketProperty") Optional<HiveBucketProperty> bucketProperty,
            @JsonProperty("skewed") boolean skewed,
            @JsonProperty("serdeParameters") Map<String, String> serdeParameters,
            @JsonProperty("parameters") Map<String, String> parameters)
    {
        this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
        this.location = requireNonNull(location, "location is null");
        this.bucketProperty = requireNonNull(bucketProperty, "bucketProperty is null");
        this.skewed = skewed;
        this.serdeParameters = ImmutableMap.copyOf(requireNonNull(serdeParameters, "serdeParameters is null"));
        this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));
    }

    @JsonProperty
    @ThriftField(1)
    public StorageFormat getStorageFormat()
    {
        return storageFormat;
    }

    @JsonProperty
    @ThriftField(2)
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    @ThriftField(3)
    public Optional<HiveBucketProperty> getBucketProperty()
    {
        return bucketProperty;
    }

    @JsonProperty
    @ThriftField(4)
    public boolean isSkewed()
    {
        return skewed;
    }

    @JsonProperty
    @ThriftField(5)
    public Map<String, String> getSerdeParameters()
    {
        return serdeParameters;
    }

    @JsonProperty
    @ThriftField(6)
    public Map<String, String> getParameters()
    {
        return parameters;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("skewed", skewed)
                .add("storageFormat", storageFormat)
                .add("location", location)
                .add("bucketProperty", bucketProperty)
                .add("serdeParameters", serdeParameters)
                .add("parameters", parameters)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Storage storage = (Storage) o;
        return skewed == storage.skewed &&
                Objects.equals(storageFormat, storage.storageFormat) &&
                Objects.equals(location, storage.location) &&
                Objects.equals(bucketProperty, storage.bucketProperty) &&
                Objects.equals(serdeParameters, storage.serdeParameters) &&
                Objects.equals(parameters, storage.parameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(skewed, storageFormat, location, bucketProperty, serdeParameters, parameters);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(Storage storage)
    {
        return new Builder(storage);
    }

    public static class Builder
    {
        private StorageFormat storageFormat;
        private String location;
        private Optional<HiveBucketProperty> bucketProperty = Optional.empty();
        private boolean skewed;
        private Map<String, String> serdeParameters = ImmutableMap.of();
        private Map<String, String> parameters = ImmutableMap.of();

        private Builder()
        {
        }

        private Builder(Storage storage)
        {
            this.storageFormat = storage.storageFormat;
            this.location = storage.location;
            this.bucketProperty = storage.bucketProperty;
            this.skewed = storage.skewed;
            this.serdeParameters = storage.serdeParameters;
            this.parameters = storage.parameters;
        }

        public Builder setStorageFormat(StorageFormat storageFormat)
        {
            this.storageFormat = storageFormat;
            return this;
        }

        public Builder setLocation(String location)
        {
            this.location = location;
            return this;
        }

        public Builder setBucketProperty(Optional<HiveBucketProperty> bucketProperty)
        {
            this.bucketProperty = bucketProperty;
            return this;
        }

        public Builder setSkewed(boolean skewed)
        {
            this.skewed = skewed;
            return this;
        }

        public Builder setSerdeParameters(Map<String, String> serdeParameters)
        {
            this.serdeParameters = serdeParameters;
            return this;
        }

        public Builder setParameters(Map<String, String> parameters)
        {
            this.parameters = parameters;
            return this;
        }

        public Storage build()
        {
            return new Storage(storageFormat, location, bucketProperty, skewed, serdeParameters, parameters);
        }
    }
}
