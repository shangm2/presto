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
package com.facebook.presto;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Locale;

@ThriftStruct
public class Everything
{
    @ThriftField(1)
    public Duration duration;

    @ThriftField(2)
    public DataSize dataSize;

    @ThriftField(3)
    public Locale locale;

    @ThriftConstructor
    public Everything(Duration duration, DataSize dataSize, Locale locale)
    {
        this.duration = duration;
        this.dataSize = dataSize;
        this.locale = locale;
    }
}
