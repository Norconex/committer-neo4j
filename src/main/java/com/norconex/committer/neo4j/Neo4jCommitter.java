/* Copyright 2019-2021 Norconex Inc.
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

import java.util.Iterator;
import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.EqualsExclude;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.HashCodeExclude;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringExclude;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.neo4j.driver.internal.value.NullValue;

import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.ICommitterRequest;
import com.norconex.committer.core3.batch.AbstractBatchCommitter;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>
 * Commit documents/fields to a Neo4j graph database.
 * </p>
 *
 * <h3>Authentication</h3>
 * <p>
 * Basic authentication is supported for password-protected
 * Solr installations.
 * </p>
 *
 * {@nx.include com.norconex.commons.lang.security.Credentials#doc}
 *
 * {@nx.include com.norconex.committer.core3.AbstractCommitter#restrictTo}
 *
 * {@nx.include com.norconex.committer.core3.AbstractCommitter#fieldMappings}
 *
 * {@nx.xml.usage
 * <committer class="com.norconex.committer.neo4j.Neo4jCommitter">
 *
 *   <!-- Mandatory settings --->
 *   <uri>
 *     (Required connection URI. E.g., "bolt://localhost:7687".)
 *   </uri>
 *   <upsertCypher>
 *     (Cypher query for adding relationships to Neo4j. Typically,
 *     you want to use MERGE and the "nodeIdProperty" value to update
 *     existing entries matching the ID. In order to delete all nodes
 *     related to an entry, make sure to add the ID property on all
 *     appropriate nodes.
 *     The query parameters correspond the document fields.)
 *   </upsertCypher>
 *   <deleteCypher>
 *     (Cypher query for adding relationships to Neo4j. Typically,
 *     you want to use "nodeIdProperty" value to delete nodes having
 *     a matching property value. The query parameters correspond the
 *     document fields.)
 *   </deleteCypher>
 *
 *   <!-- Optional settings --->
 *   <database>
 *     (A database name when using one other than the default one.)
 *   </database>
 *   <credentials>
 *     {@nx.include com.norconex.commons.lang.security.Credentials@nx.xml.usage}
 *   </credentials>
 *   <multiValuesJoiner>
 *     (One or more characters to join multi-value fields. Default is "|".)
 *   </multiValuesJoiner>
 *   <nodeIdProperty>
 *     (Optional property name where to store the document reference
 *     in Neo4j graph entries.  Use it as a cypher parameter to uniquely
 *     identify your graph entries in your configured "upsertCypher" and
 *     "deleteCypher" queries. Default is "id".)
 *   </nodeIdProperty>
 *   <nodeContentProperty>
 *     (Optional property name where to store the document content
 *     in Neo4j graph entries.  Use it as a cypher parameter
 *     in your configured "upsertCypher" query. Default is "content".)
 *   </nodeContentProperty>
 *   <optionalParameters>
 *     (Comma-separated list of parameter names that can be missing when
 *     creating the query. They will be set to {@link NullValue}) to avoid
 *     client exception for missing parameters.)
 *   <optionalParameters>
 *
 *   {@nx.include com.norconex.committer.core3.batch.AbstractBatchCommitter#options}
 *
 * </committer>
 * }
 *
 *
 * {@nx.xml.example
 * <committer class="com.norconex.committer.neo4j.Neo4jCommitter">
 *   <uri>bolt://localhost:7687</uri>
 *   <upsertCypher>
 *     MERGE (a:Document { docId: $id })
 *     SET a += { title: $title }
 *     SET a += { author: $author }
 *   </upsertCypher>
 *   <deleteCypher>
 *     MATCH (a:Document { docId: $id })
 *     DETACH DELETE a
 *   </deleteCypher>
 *   <credentials>
 *     <username>neo4j</username>
 *     <password>AcwFJPHITfk6LrRp7HW7Ag6hvDZotXcvWt2WvDMcGIo=</password>
 *     <passwordKey>
 *       <value>key.txt</value>
 *       <source>file</source>
 *     </passwordKey>
 *   </credentials>
 *   <multiValuesJoiner>_</multiValuesJoiner>
 * </committer>
 * }
 *
 * <p>
 * The above example creates a graph of collected documents.
 * </p>
 *
 * @author Sylvain Roussy
 * @author Pascal Essiembre
 */
@SuppressWarnings("javadoc")
public class Neo4jCommitter extends AbstractBatchCommitter {

    private final Neo4jCommitterConfig config;

    @ToStringExclude
    @HashCodeExclude
    @EqualsExclude
    private Neo4jClient client;

    public Neo4jCommitter() {
        this(new Neo4jCommitterConfig());
    }
    public Neo4jCommitter(Neo4jCommitterConfig config) {
        this.config = Objects.requireNonNull(
                config, "'config' must not be null.");
    }

    @Override
    protected void initBatchCommitter() throws CommitterException {
        client = new Neo4jClient(config);
    }

    @Override
    protected void commitBatch(Iterator<ICommitterRequest> it)
            throws CommitterException {
        client.post(it);
    }

    @Override
    protected void closeBatchCommitter() throws CommitterException {
        if (client != null) {
            client.close();
        }
    }

    public Neo4jCommitterConfig getConfig() {
        return config;
    }

    @Override
    protected void loadBatchCommitterFromXML(XML xml) {
        config.loadFromXML(xml);
    }
    @Override
    protected void saveBatchCommitterToXML(XML xml) {
        config.saveToXML(xml);
    }

    @Override
    public boolean equals(final Object other) {
        return EqualsBuilder.reflectionEquals(this, other);
    }
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
    @Override
    public String toString() {
        return new ReflectionToStringBuilder(
                this, ToStringStyle.SHORT_PREFIX_STYLE).toString();
    }
}