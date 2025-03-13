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
package com.facebook.presto.hive;

import com.facebook.presto.common.experimental.ColumnHandleAdapter;
import com.facebook.presto.common.experimental.auto_gen.ThriftHivePartition;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.predicate.NullableValue.createNullableValue;
import static java.util.Objects.requireNonNull;

public class HivePartition
{
    public static final PartitionNameWithVersion UNPARTITIONED_ID = new PartitionNameWithVersion("<UNPARTITIONED>", Optional.empty());

    private final SchemaTableName tableName;
    private final PartitionNameWithVersion partitionId;
    private final Map<ColumnHandle, NullableValue> keys;

    public HivePartition(ThriftHivePartition thriftHivePartition)
    {
        this(new SchemaTableName(thriftHivePartition.getTableName()),
                new PartitionNameWithVersion(thriftHivePartition.getPartitionId()),
                thriftHivePartition.getKeys().entrySet().stream().collect(Collectors.toMap(
                        entry -> (ColumnHandle) ColumnHandleAdapter.fromThrift(entry.getKey()),
                        entry -> createNullableValue(entry.getValue()))));
    }

    public HivePartition(SchemaTableName tableName)
    {
        this(tableName, UNPARTITIONED_ID, ImmutableMap.of());
    }

    public HivePartition(
            SchemaTableName tableName,
            PartitionNameWithVersion partitionId,
            Map<ColumnHandle, NullableValue> keys)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.partitionId = requireNonNull(partitionId, "partitionId is null");
        this.keys = ImmutableMap.copyOf(requireNonNull(keys, "keys is null"));
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    public PartitionNameWithVersion getPartitionId()
    {
        return partitionId;
    }

    public Map<ColumnHandle, NullableValue> getKeys()
    {
        return keys;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        HivePartition other = (HivePartition) obj;
        return Objects.equals(this.partitionId, other.partitionId);
    }

    @Override
    public String toString()
    {
        return tableName + ":" + partitionId;
    }
}
