/* Copyright 2019 Norconex Inc.
 *
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
package com.norconex.committer.neo4j;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Describe how a relationship must be built between nodes in Neo4j.
 *
 * @author Sylvain Roussy
 */
public class Relationship implements Serializable{

    private static final long serialVersionUID = 1L;

    /**
     * Direction of the relationship
     * <ul>
     * <li>OUTGOING: from source to target</li>
     * <li>INCOMING: from target to source</li>
     * <li>BOTH: two relationships, from target to source and to source from target</li>
     * <li>NODE: unique relationship between two nodes (direction has no importance)</li>
     * </ul>
     */
    public enum Direction {
        OUTGOING,
        INCOMING,
        BOTH,
        NONE
    }

    /**
     * Neo4j Cypher keyword used to retrieve a node (usually applied to the target node)
     * <ul>
     * <li>MATCH: find the target node and create the relationship only if the target node exists </li>
     * <li>MERGE: find or create the target node and create the relationship</li
     * </ul>
     */
    public enum FindSyntax {
        MATCH,
        MERGE
    }

    private String type;
    private String sourcePropertyKey;
    private String targetPropertyKey;
    private Direction direction;
    private FindSyntax targetFindSyntax = FindSyntax.MERGE;

    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }

    public String getTargetPropertyKey() {
        return targetPropertyKey;
    }
    public void setTargetPropertyKey(String parentPropertyKey) {
        this.targetPropertyKey = parentPropertyKey;
    }

    public String getSourcePropertyKey() {
        return sourcePropertyKey;
    }
    public void setSourcePropertyKey(String childPropertyKey) {
        this.sourcePropertyKey = childPropertyKey;
    }

    public Direction getDirection() {
        return direction;
    }
    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public FindSyntax getTargetFindSyntax() {
        return targetFindSyntax;
    }
    public void setTargetFindSyntax(FindSyntax targetFindSyntax) {
        this.targetFindSyntax = targetFindSyntax;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof Relationship)) {
            return false;
        }
        Relationship castOther = (Relationship) other;
        return new EqualsBuilder()
                .append(direction, castOther.direction)
                .append(targetFindSyntax, castOther.targetFindSyntax)
                .append(type, castOther.type)
                .append(sourcePropertyKey, castOther.sourcePropertyKey)
                .append(targetPropertyKey, castOther.targetPropertyKey)
                .isEquals();
    }
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(direction)
                .append(targetFindSyntax)
                .append(type)
                .append(sourcePropertyKey)
                .append(targetPropertyKey)
                .toHashCode();
    }
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("direction", direction)
                .append("targetFindSyntax", targetFindSyntax)
                .append("type", type)
                .append("sourcePropertyKey", sourcePropertyKey)
                .append("targetPropertyKey", targetPropertyKey)
                .toString();
    }
}