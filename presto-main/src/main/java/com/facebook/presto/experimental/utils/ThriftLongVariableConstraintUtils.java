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

import com.facebook.presto.experimental.auto_gen.ThriftLongVariableConstraint;
import com.facebook.presto.spi.function.LongVariableConstraint;

public class ThriftLongVariableConstraintUtils
{
    private ThriftLongVariableConstraintUtils() {}

    public static LongVariableConstraint toLongVariableConstraint(ThriftLongVariableConstraint constraint)
    {
        if (constraint == null) {
            return null;
        }
        return new LongVariableConstraint(constraint.getName(), constraint.getExpression());
    }

    public static ThriftLongVariableConstraint fromLongVariableConstraint(LongVariableConstraint constraint)
    {
        if (constraint == null) {
            return null;
        }
        return new ThriftLongVariableConstraint(constraint.getName(), constraint.getExpression());
    }
}
