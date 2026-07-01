/* Copyright 2021 Norconex Inc.
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.norconex.commons.lang.collection.CollectionUtil;
import com.norconex.commons.lang.security.Credentials;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>
 * Neo4j Committer configuration.
 * </p>
 * @author Sylvain Roussy
 * @author Pascal Essiembre
 */
public class Neo4jCommitterConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Default property name used to store the document ID. */
    public static final String DEFAULT_NEO4J_ID_PROPERTY = "id";
    /** Default property name used to store the document content. */
    public static final String DEFAULT_NEO4J_CONTENT_PROPERTY = "content";

    /** Neo4j connection URI. */
    private String uri;
    /** Target database name, or {@code null} for the default. */
    private String database;
    /** Authentication credentials. */
    private final Credentials credentials = new Credentials();
    /** Separator used to join multi-valued fields into a single string. */
    private String multiValuesJoiner;
    /** Neo4j property name where the document ID is stored. */
    private String nodeIdProperty = DEFAULT_NEO4J_ID_PROPERTY;
    /** Neo4j property name where the document content is stored. */
    private String nodeContentProperty = DEFAULT_NEO4J_CONTENT_PROPERTY;
    /** Cypher statement used for upsert operations. */
    private String upsertCypher;
    /** Cypher statement used for delete operations. */
    private String deleteCypher;
    /** Query parameter names that may be absent without causing an error. */
    private final Set<String> optionalParameters = new HashSet<>();

    /**
     * Gets the target database name.
     * @return database name, or {@code null} for the default
     */
    public String getDatabase() {
        return database;
    }
    /**
     * Sets the target database name.
     * @param database database name, or {@code null} for the default
     */
    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * Gets the authentication credentials.
     * @return credentials
     */
    public Credentials getCredentials() {
        return credentials;
    }
    /**
     * Sets the authentication credentials.
     * @param credentials the credentials
     */
    public void setCredentials(Credentials credentials) {
        this.credentials.copyFrom(credentials);
    }

    /**
     * Gets the Neo4j connection URI.
     * @return connection URI
     */
    public String getUri() {
        return uri;
    }
    /**
     * Sets the Neo4j connection URI (e.g., {@code bolt://localhost:7687}).
     * @param uri connection URI
     */
    public void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * Gets the separator used to join multi-valued fields into a single string.
     * @return multi-values joiner string
     */
    public String getMultiValuesJoiner() {
        return multiValuesJoiner;
    }
    /**
     * Sets the separator used to join multi-valued fields. Default is "|".
     * @param multiValuesJoiner separator string
     */
    public void setMultiValuesJoiner(String multiValuesJoiner) {
        this.multiValuesJoiner = multiValuesJoiner;
    }

    /**
     * Gets the Neo4j property name where the document ID is stored.
     * @return node ID property name
     */
    public String getNodeIdProperty() {
        return nodeIdProperty;
    }
    /**
     * Sets the Neo4j property name where the document ID is stored.
     * Default is {@value #DEFAULT_NEO4J_ID_PROPERTY}.
     * @param nodeIdProperty node ID property name
     */
    public void setNodeIdProperty(String nodeIdProperty) {
        this.nodeIdProperty = nodeIdProperty;
    }

    /**
     * Gets the Neo4j property name where the document content is stored.
     * @return node content property name
     */
    public String getNodeContentProperty() {
        return nodeContentProperty;
    }
    /**
     * Sets the Neo4j property name where the document content is stored.
     * Default is {@value #DEFAULT_NEO4J_CONTENT_PROPERTY}.
     * @param nodeContentProperty node content property name
     */
    public void setNodeContentProperty(String nodeContentProperty) {
        this.nodeContentProperty = nodeContentProperty;
    }

    /**
     * Gets the Cypher query used for upsert operations.
     * @return upsert Cypher query
     */
    public String getUpsertCypher() {
        return upsertCypher;
    }
    /**
     * Sets the Cypher query used for upsert operations.
     * @param upsertCypher upsert Cypher query
     */
    public void setUpsertCypher(String upsertCypher) {
        this.upsertCypher = upsertCypher;
    }

    /**
     * Gets the Cypher query used for delete operations.
     * @return delete Cypher query
     */
    public String getDeleteCypher() {
        return deleteCypher;
    }
    /**
     * Sets the Cypher query used for delete operations.
     * @param deleteCypher delete Cypher query
     */
    public void setDeleteCypher(String deleteCypher) {
        this.deleteCypher = deleteCypher;
    }

    /**
     * Gets the names of optional query parameters (those that may be absent
     * without causing a client exception).
     * @return unmodifiable set of optional parameter names
     */
    public Set<String> getOptionalParameters() {
        return Collections.unmodifiableSet(optionalParameters);
    }
    /**
     * Sets the names of optional query parameters.
     * @param optionalParameters optional parameter names
     */
    public void setOptionalParameters(Set<String> optionalParameters) {
        CollectionUtil.setAll(this.optionalParameters, optionalParameters);
    }
    /**
     * Adds a parameter name to the set of optional query parameters.
     * @param optionalParameter optional parameter name to add
     */
    public void addOptionalParameter(String optionalParameter) {
        this.optionalParameters.add(optionalParameter);
    }

    void saveToXML(XML xml) {
        xml.addElement("uri", getUri());
        xml.addElement("database", getDatabase());
        credentials.saveToXML(xml.addElement("credentials"));
        xml.addElement("multiValuesJoiner", getMultiValuesJoiner());
        xml.addElement("nodeIdProperty", getNodeIdProperty());
        xml.addElement("nodeContentProperty", getNodeContentProperty());
        xml.addElement("upsertCypher", getUpsertCypher());
        xml.addElement("deleteCypher", getDeleteCypher());
        xml.addDelimitedElementList(
                "optionalParameters", new ArrayList<>(optionalParameters));
    }
    void loadFromXML(XML xml) {
        setUri(xml.getString("uri", getUri()));
        setDatabase(xml.getString("database", getDatabase()));
        xml.ifXML("credentials", x -> x.populate(credentials));
        setMultiValuesJoiner(xml.getString(
                "multiValuesJoiner", getMultiValuesJoiner()));
        setNodeIdProperty(xml.getString("nodeIdProperty", getNodeIdProperty()));
        setNodeContentProperty(
                xml.getString("nodeContentProperty", getNodeContentProperty()));
        setUpsertCypher(xml.getString("upsertCypher", getUpsertCypher()));
        setDeleteCypher(xml.getString("deleteCypher", getDeleteCypher()));
        List<String> params =
                xml.getDelimitedStringList("optionalParameters",
                        (List<String>) null);
        if (params != null) {
            setOptionalParameters(new HashSet<>(params));
        }
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
