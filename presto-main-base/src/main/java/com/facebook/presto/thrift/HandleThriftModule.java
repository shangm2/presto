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
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.drift.codec.guice.ThriftCodecBinder.thriftCodecBinder;

public class HandleThriftModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        thriftCodecBinder(binder).addModuleBinding().to(TableHandleThriftModule.class);
        thriftCodecBinder(binder).addModuleBinding().to(TableLayoutHandleThriftModule.class);
        thriftCodecBinder(binder).addModuleBinding().to(ColumnHandleThriftModule.class);
        thriftCodecBinder(binder).addModuleBinding().to(SplitThriftModule.class);
        thriftCodecBinder(binder).addModuleBinding().to(OutputTableHandleThriftModule.class);
        thriftCodecBinder(binder).addModuleBinding().to(InsertTableHandleThriftModule.class);
        thriftCodecBinder(binder).addModuleBinding().to(DeleteTableHandleThriftModule.class);
        thriftCodecBinder(binder).addModuleBinding().to(IndexHandleThriftModule.class);
        thriftCodecBinder(binder).addModuleBinding().to(TransactionHandleThriftModule.class);
        thriftCodecBinder(binder).addModuleBinding().to(PartitioningHandleThriftModule.class);
        thriftCodecBinder(binder).addModuleBinding().to(FunctionHandleThriftModule.class);

        binder.bind(HandleResolver.class).in(Scopes.SINGLETON);
    }
}
