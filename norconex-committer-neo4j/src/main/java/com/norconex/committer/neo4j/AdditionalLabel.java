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
 * Additional label allow to transform a metadata field in a node label.
 * With the <i>keep</i>, you can keep the metadata property or remove it
 * after the label was stored.
 *
 * @author Sylvain Roussy
 */
public class AdditionalLabel implements Serializable{

    private static final long serialVersionUID = 1L;

    private String sourceField;
    private boolean keep;

    /**
     * Default constructor.
     */
    public AdditionalLabel(){
        super();
    }

    /**
     * Constructor with the following arguments:
     * @param sourceField the name of the targeted metadata field
     * @param keep if you want to keep the matadata field or not
     */
    public AdditionalLabel(String sourceField, boolean keep) {
        super();
        this.keep = keep;
        this.sourceField = sourceField;
    }


    public boolean isKeep() {
        return keep;
    }
    public void setKeep(boolean keep) {
        this.keep = keep;
    }
    public String getSourceField() {
        return sourceField;
    }
    public void setSourceField(String sourceField) {
        this.sourceField = sourceField;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof AdditionalLabel)) {
            return false;
        }
        AdditionalLabel castOther = (AdditionalLabel) other;
        return new EqualsBuilder()
                .append(sourceField, castOther.sourceField)
                .append(keep, castOther.keep)
                .isEquals();
    }
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(sourceField)
                .append(keep)
                .toHashCode();
    }
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("sourceField", sourceField)
                .append("keep", keep)
                .toString();
    }
}