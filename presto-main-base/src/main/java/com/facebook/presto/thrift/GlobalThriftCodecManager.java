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

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.thrift.ThriftCodecProvider;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;

import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

@Singleton
public class GlobalThriftCodecManager
{
    private final Provider<ThriftCodecManager> thriftCodecManagerProvider;
    private final Set<Class<?>> registeredCodecs = ConcurrentHashMap.newKeySet();

    @Inject
    public GlobalThriftCodecManager(Provider<ThriftCodecManager> thriftCodecManagerProvider)
    {
        this.thriftCodecManagerProvider = requireNonNull(thriftCodecManagerProvider, "thriftCodecManager is null");
    }

    public void registerCodecsFromProvider(ThriftCodecProvider provider, ClassLoader pluginClassLoader)
    {
        requireNonNull(provider, "provider is null");
        requireNonNull(pluginClassLoader, "pluginClassLoader is null");

        for (ThriftCodec<?> thriftCodecClass : provider.getThriftCodecClasses()) {
            Class<?> codecClass = thriftCodecClass.getClass();
            if (registeredCodecs.add(codecClass)) {
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
                    thriftCodecManagerProvider.get().addCodec(thriftCodecClass);
                }
            }
        }
    }

    private Object createCodecInstance(Class<?> thriftCodecClass)
    {
        try {
            return thriftCodecClass.getConstructor().newInstance();
        }
        catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public Provider<ThriftCodecManager> getThriftCodecManagerProvider()
    {
        return thriftCodecManagerProvider;
    }
}
