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

import com.facebook.presto.common.experimental.auto_gen.ThriftHivePageSinkMetadata;
import com.facebook.presto.common.experimental.auto_gen.ThriftListOfStringAsKey;
import com.facebook.presto.common.experimental.auto_gen.ThriftOptionalPartition;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class HivePageSinkMetadata
{
    private final SchemaTableName schemaTableName;
    private final Optional<Table> table;
    private final Map<List<String>, Optional<Partition>> modifiedPartitions;

    public static HivePageSinkMetadata createHivePageSinkMetadata(ThriftHivePageSinkMetadata thriftMetadata)
    {
        Map<List<String>, Optional<Partition>> modifiedPartitions =
                thriftMetadata.getModifiedPartitions().entrySet().stream().collect(Collectors.toMap(
                        entry -> entry.getKey().getKey(),
                        entry -> fromThriftValue(entry.getValue())));

        return new HivePageSinkMetadata(
                new SchemaTableName(thriftMetadata.getSchemaTableName()),
                thriftMetadata.getTable().map(Table::new), modifiedPartitions);
    }

    private static Optional<Partition> fromThriftValue(ThriftOptionalPartition partition)
    {
        if (!partition.isIsPresent()) {
            return Optional.empty();
        }
        return Optional.of(new Partition(partition.getPartition()));
    }

    private static ThriftOptionalPartition toThriftValue(Optional<Partition> partition)
    {
        if (!partition.isPresent()) {
            return new ThriftOptionalPartition(false, null);
        }
        return new ThriftOptionalPartition(true, partition.get().toThrift());
    }

    public ThriftHivePageSinkMetadata toThrift()
    {
        Map<ThriftListOfStringAsKey, ThriftOptionalPartition> thriftPartitions = modifiedPartitions.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> new ThriftListOfStringAsKey(entry.getKey()),
                        entry -> toThriftValue(entry.getValue())));
        ThriftHivePageSinkMetadata thriftMetadata = new ThriftHivePageSinkMetadata(
                schemaTableName.toThrift(),
                thriftPartitions);
        table.ifPresent(t -> thriftMetadata.setTable(t.toThrift()));
        return thriftMetadata;
    }

    public HivePageSinkMetadata(
            SchemaTableName schemaTableName,
            Optional<Table> table,
            Map<List<String>, Optional<Partition>> modifiedPartitions)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.table = requireNonNull(table, "table is null");
        this.modifiedPartitions = requireNonNull(modifiedPartitions, "modifiedPartitions is null");
        checkArgument(table.isPresent() && !table.get().getPartitionColumns().isEmpty() || modifiedPartitions.isEmpty());
    }

    @JsonCreator
    public static HivePageSinkMetadata deserialize(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("table") Optional<Table> table,
            @JsonProperty("modifiedPartitions") List<JsonSerializableEntry<List<String>, Optional<Partition>>> modifiedPartitions)
    {
        requireNonNull(modifiedPartitions, "modifiedPartitions is null");
        return new HivePageSinkMetadata(schemaTableName, table, JsonSerializableEntry.toMap(modifiedPartitions));
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    /**
     * This method returns empty when the table has not been created yet (i.e. for CREATE TABLE AS SELECT queries)
     */
    @JsonProperty
    public Optional<Table> getTable()
    {
        return table;
    }

    @JsonProperty("modifiedPartitions")
    public List<JsonSerializableEntry<List<String>, Optional<Partition>>> getJsonSerializableModifiedPartitions()
    {
        return JsonSerializableEntry.fromMap(modifiedPartitions);
    }

    public Map<List<String>, Optional<Partition>> getModifiedPartitions()
    {
        return modifiedPartitions;
    }

    public static class JsonSerializableEntry<K, V>
    {
        private final K key;
        private final V value;

        @JsonCreator
        public JsonSerializableEntry(@JsonProperty("key") K key, @JsonProperty("value") V value)
        {
            this.key = key;
            this.value = value;
        }

        @JsonProperty
        public K getKey()
        {
            return key;
        }

        @JsonProperty
        public V getValue()
        {
            return value;
        }

        public static <K, V> List<JsonSerializableEntry<K, V>> fromMap(Map<K, V> map)
        {
            return map.entrySet().stream()
                    .map(entry -> new JsonSerializableEntry<>(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList());
        }

        public static <K, V> Map<K, V> toMap(List<JsonSerializableEntry<K, V>> list)
        {
            return list.stream()
                    .collect(Collectors.toMap(JsonSerializableEntry::getKey, JsonSerializableEntry::getValue));
        }
    }
}
