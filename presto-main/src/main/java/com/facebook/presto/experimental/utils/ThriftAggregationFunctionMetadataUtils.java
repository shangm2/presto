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

import com.facebook.presto.experimental.auto_gen.ThriftAggregationFunctionMetadata;
import com.facebook.presto.spi.function.AggregationFunctionMetadata;

import static com.facebook.presto.experimental.utils.ThriftTypeSignatureUtils.fromTypeSignature;
import static com.facebook.presto.experimental.utils.ThriftTypeSignatureUtils.toTypeSignature;

public class ThriftAggregationFunctionMetadataUtils
{
    private ThriftAggregationFunctionMetadataUtils() {}

    public static AggregationFunctionMetadata toAggregationFunctionMetadata(ThriftAggregationFunctionMetadata thriftMetadata)
    {
        if (thriftMetadata == null) {
            return null;
        }
        return new AggregationFunctionMetadata(
                toTypeSignature(thriftMetadata.getIntermediateType()),
                thriftMetadata.isOrderSensitive);
    }

    public static ThriftAggregationFunctionMetadata fromAggregationFunctionMetadata(AggregationFunctionMetadata metadata)
    {
        if (metadata == null) {
            return null;
        }
        return new ThriftAggregationFunctionMetadata(
                fromTypeSignature(metadata.getIntermediateType()),
                metadata.isOrderSensitive());
    }
}
