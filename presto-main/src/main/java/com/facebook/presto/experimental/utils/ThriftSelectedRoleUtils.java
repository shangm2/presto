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

import com.facebook.presto.experimental.auto_gen.ThriftSelectedRole;
import com.facebook.presto.experimental.auto_gen.ThriftSelectedRoleType;
import com.facebook.presto.spi.security.SelectedRole;

public class ThriftSelectedRoleUtils
{
    private ThriftSelectedRoleUtils() {}

    public static SelectedRole.Type toSelectedRoleType(ThriftSelectedRoleType thriftType)
    {
        return SelectedRole.Type.valueOf(thriftType.name());
    }

    public static ThriftSelectedRoleType fromSelectedRoleType(SelectedRole.Type selectedRoleType)
    {
        return ThriftSelectedRoleType.valueOf(selectedRoleType.name());
    }

    public static SelectedRole toSelectedRole(ThriftSelectedRole thriftSelectedRole)
    {
        return new SelectedRole(toSelectedRoleType(thriftSelectedRole.getType()), thriftSelectedRole.getRole());
    }

    public static ThriftSelectedRole fromSelectedRole(SelectedRole selectedRole)
    {
        ThriftSelectedRole thriftSelectedRole = new ThriftSelectedRole(fromSelectedRoleType(selectedRole.getType()));
        selectedRole.getRole().ifPresent(thriftSelectedRole::setRole);
        return thriftSelectedRole;
    }
}
