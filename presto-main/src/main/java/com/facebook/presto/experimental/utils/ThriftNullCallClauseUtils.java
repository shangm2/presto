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

import com.facebook.presto.experimental.auto_gen.ThriftNullCallClause;
import com.facebook.presto.spi.function.RoutineCharacteristics;

public class ThriftNullCallClauseUtils
{
    private ThriftNullCallClauseUtils() {}

    public static ThriftNullCallClause fromNullCallClause(RoutineCharacteristics.NullCallClause nullCallClause)
    {
        return ThriftNullCallClause.valueOf(nullCallClause.name());
    }

    public static RoutineCharacteristics.NullCallClause toNullCallClause(ThriftNullCallClause thriftNullCallClause)
    {
        return RoutineCharacteristics.NullCallClause.valueOf(thriftNullCallClause.name());
    }
}
