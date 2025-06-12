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
package com.facebook.presto.thrift;

import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.FunctionHandle;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.drift.codec.guice.ThriftCodecBinder.thriftCodecBinder;

public class HandleThriftModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        thriftCodecBinder(binder).bindCustomThriftCodec(TableHandleThriftCodec.class);
        thriftCodecBinder(binder).bindCustomThriftCodec(TableLayoutHandleThriftCodec.class);
        thriftCodecBinder(binder).bindCustomThriftCodec(ColumnHandleThriftCodec.class);
        thriftCodecBinder(binder).bindCustomThriftCodec(SplitThriftCodec.class);
        thriftCodecBinder(binder).bindCustomThriftCodec(OutputTableHandleThriftCodec.class);
        thriftCodecBinder(binder).bindCustomThriftCodec(InsertTableHandleThriftCodec.class);
        thriftCodecBinder(binder).bindCustomThriftCodec(DeleteTableHandleThriftCodec.class);
        thriftCodecBinder(binder).bindCustomThriftCodec(IndexHandleThriftCodec.class);
        thriftCodecBinder(binder).bindCustomThriftCodec(TransactionHandleThriftCodec.class);
        thriftCodecBinder(binder).bindCustomThriftCodec(PartitioningHandleThriftCodec.class);
        thriftCodecBinder(binder).bindCustomThriftCodec(FunctionHandleThriftCodec.class);

        jsonCodecBinder(binder).bindJsonCodec(ConnectorTableHandle.class);
        jsonCodecBinder(binder).bindJsonCodec(ConnectorTableLayoutHandle.class);
        jsonCodecBinder(binder).bindJsonCodec(ColumnHandle.class);
        jsonCodecBinder(binder).bindJsonCodec(ConnectorSplit.class);
        jsonCodecBinder(binder).bindJsonCodec(ConnectorOutputTableHandle.class);
        jsonCodecBinder(binder).bindJsonCodec(ConnectorInsertTableHandle.class);
        jsonCodecBinder(binder).bindJsonCodec(ConnectorDeleteTableHandle.class);
        jsonCodecBinder(binder).bindJsonCodec(ConnectorIndexHandle.class);
        jsonCodecBinder(binder).bindJsonCodec(ConnectorTransactionHandle.class);
        jsonCodecBinder(binder).bindJsonCodec(ConnectorPartitioningHandle.class);
        jsonCodecBinder(binder).bindJsonCodec(FunctionHandle.class);

        binder.bind(HandleResolver.class).in(Scopes.SINGLETON);
    }
}
