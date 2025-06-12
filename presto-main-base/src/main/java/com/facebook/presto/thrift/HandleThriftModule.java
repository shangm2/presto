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

import com.facebook.presto.index.IndexHandleThriftModule;
import com.facebook.presto.metadata.ColumnHandleThriftModule;
import com.facebook.presto.metadata.DeleteTableHandleThriftModule;
import com.facebook.presto.metadata.FunctionHandleThriftModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InsertTableHandleThriftModule;
import com.facebook.presto.metadata.MetadataUpdateThriftModule;
import com.facebook.presto.metadata.OutputTableHandleThriftModule;
import com.facebook.presto.metadata.PartitioningHandleThriftModule;
import com.facebook.presto.metadata.SplitThriftModule;
import com.facebook.presto.metadata.TableHandleThriftModule;
import com.facebook.presto.metadata.TableLayoutHandleThriftModule;
import com.facebook.presto.metadata.TransactionHandleThriftModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.json.JsonBinder.jsonBinder;

public class HandleThriftModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        jsonBinder(binder).addModuleBinding().to(TableHandleThriftModule.class);
        jsonBinder(binder).addModuleBinding().to(TableLayoutHandleThriftModule.class);
        jsonBinder(binder).addModuleBinding().to(ColumnHandleThriftModule.class);
        jsonBinder(binder).addModuleBinding().to(SplitThriftModule.class);
        jsonBinder(binder).addModuleBinding().to(OutputTableHandleThriftModule.class);
        jsonBinder(binder).addModuleBinding().to(InsertTableHandleThriftModule.class);
        jsonBinder(binder).addModuleBinding().to(DeleteTableHandleThriftModule.class);
        jsonBinder(binder).addModuleBinding().to(IndexHandleThriftModule.class);
        jsonBinder(binder).addModuleBinding().to(TransactionHandleThriftModule.class);
        jsonBinder(binder).addModuleBinding().to(PartitioningHandleThriftModule.class);
        jsonBinder(binder).addModuleBinding().to(FunctionHandleThriftModule.class);
        jsonBinder(binder).addModuleBinding().to(MetadataUpdateThriftModule.class);

        binder.bind(HandleResolver.class).in(Scopes.SINGLETON);
    }
}
