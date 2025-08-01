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
package com.facebook.presto.sql.tree;

import com.facebook.presto.spi.analyzer.UpdateInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class RenameView
        extends Statement
{
    private final QualifiedName source;
    private final QualifiedName target;

    private final boolean exists;

    public RenameView(QualifiedName source, QualifiedName target, boolean exists)
    {
        this(Optional.empty(), source, target, exists);
    }

    public RenameView(NodeLocation location, QualifiedName source, QualifiedName target, boolean exists)
    {
        this(Optional.of(location), source, target, exists);
    }

    private RenameView(Optional<NodeLocation> location, QualifiedName source, QualifiedName target, boolean exists)
    {
        super(location);
        this.source = requireNonNull(source, "source name is null");
        this.target = requireNonNull(target, "target name is null");
        this.exists = exists;
    }

    public QualifiedName getSource()
    {
        return source;
    }

    public QualifiedName getTarget()
    {
        return target;
    }

    public boolean isExists()
    {
        return exists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRenameView(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public UpdateInfo getUpdateInfo()
    {
        return new UpdateInfo("RENAME VIEW", source.toString());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(source, target, exists);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        RenameView o = (RenameView) obj;
        return Objects.equals(source, o.source) &&
                Objects.equals(target, o.target) &&
                Objects.equals(exists, o.exists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("source", source)
                .add("target", target)
                .add("exists", exists)
                .toString();
    }
}
