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
package com.norconex.committer.neo4j.topologies;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;

import com.norconex.committer.core.CommitterException;
import com.norconex.committer.neo4j.AdditionalLabel;
import com.norconex.committer.neo4j.GraphEntry;
import com.norconex.committer.neo4j.Neo4jCommitter;
import com.norconex.committer.neo4j.Relationship;
import com.norconex.commons.lang.map.Properties;

/**
 * Based node topology.
 *
 * @author Sylvain Roussy
 */
public abstract class NodeTopology {

    public static final String NEO4J_PARAM_CONTENT = "source_content";
    public static final String NEO4J_PARAM_METADATA = "source_metadata";
    public static final String NEO4J_PARAM_SOURCE = "source_id";
    public static final String NEO4J_PARAM_SUB_ENTRIES = "subentries";

    private final Neo4jCommitter committer;

    public NodeTopology(Neo4jCommitter committer) {
        this.committer = committer;
    }

    protected final Neo4jCommitter getCommitter() {
        return committer;
    }

    // parent property values
    public List<String> getTargetPropertyValues(Properties p, Relationship r) {
        return p.getStrings(r.getSourcePropertyKey());
    }

    protected String extractContent(InputStream stream) throws IOException {
        String s = "";
        if (stream != null) {
            s = IOUtils.toString(stream, StandardCharsets.UTF_8);
        }
        return s;
    }

    protected Map<String, Object> extractMetadata(Properties p) {
        final Map<String, Object> map = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : p.entrySet()) {

            final String key = entry.getKey();

            final List<String> values = entry.getValue();
            final String value = values.stream().collect(
                    Collectors.joining(committer.getMultiValuesJoiner()));
            map.put(key, value);
        }
        return map;
    }

    protected abstract String buildStoreQuery(final GraphEntry entry);

    protected abstract String buildDeleteQuery();

    protected String buildParentQueryPart() {
        StringBuilder sb = new StringBuilder();
        if (committer.getRelationships() != null) {
            final String primaryLabel = committer.getPrimaryLabel();

            int i = 0;

            for (Relationship rel : committer.getRelationships()) {
                sb.append(" WITH a,$").append(NEO4J_PARAM_SUB_ENTRIES)
                        .append("_").append(i)
                        .append(" AS subs UNWIND subs AS sub ")
                        .append(" WITH a, sub").append(" WHERE a.`")
                        .append(rel.getTargetPropertyKey()).append("` <> sub ")
                        .append(rel.getTargetFindSyntax().name())
                        .append(" (s:`").append(primaryLabel).append("`{`")
                        .append(rel.getTargetPropertyKey()).append("`:sub})");

                switch (rel.getDirection()) {
                case OUTGOING:
                    sb.append(" MERGE (s)<-[:").append(rel.getType())
                            .append("]-(a)");
                    break;
                case INCOMING:
                    sb.append(" MERGE (s)-[:").append(rel.getType())
                            .append("]->(a)");
                    break;
                case BOTH:
                    sb.append(" MERGE (s)-[:").append(rel.getType())
                            .append("]->(a)");
                    sb.append(" MERGE (s)<-[:").append(rel.getType())
                            .append("]-(a)");
                    break;
                case NONE:
                    sb.append(" MERGE (s)-[:").append(rel.getType())
                            .append("]-(a)");
                }
                i++;
            }
        }
        return sb.toString();
    }

    public synchronized void storeEntry(final GraphEntry entry) {
        try (Session session = committer.getNeo4jDriver().session()) {
            session.writeTransaction(new TransactionWork<Void>() {
                @Override
                public Void execute(Transaction tx) {
                    tx.run(buildStoreQuery(entry), buildNeo4jParameters(entry));
                    return null;
                }
            });
        }
    }

    protected Value buildNeo4jParameters(final GraphEntry entry) {
        Value values = null;
        try {
            final String content = extractContent(entry.getContent());
            final Map<String, Object> meta =
                    extractMetadata(entry.getMetaData());

            final Map<String, Object> neo4jParameters = new HashMap<>();
            neo4jParameters.put(NEO4J_PARAM_SOURCE, entry.getId());
            neo4jParameters.put(NEO4J_PARAM_METADATA, meta);
            neo4jParameters.put(NEO4J_PARAM_CONTENT, content);

            int i = 0;
            for (Relationship rel : committer.getRelationships()) {
                neo4jParameters.put(NEO4J_PARAM_SUB_ENTRIES + "_" + i,
                        getTargetPropertyValues(entry.getMetaData(), rel));
                i++;
            }
            values = Values.value(neo4jParameters);
        } catch (IOException e) {
            throw new CommitterException(e);
        }
        return values;
    }

    public synchronized void deleteEntry(final String id) {
        try (Session session = committer.getNeo4jDriver().session()) {
            session.writeTransaction(new TransactionWork<Void>() {
                @Override
                public Void execute(Transaction tx) {
                    tx.run(buildDeleteQuery(),
                            Values.parameters(NEO4J_PARAM_SOURCE, id));
                    return null;
                }
            });
        }
    }

    protected String buildAdditionalLabelsQueryPart(
            String prefix, GraphEntry entry) {
        StringBuilder nodeLabels = new StringBuilder();
        if (!committer.getAdditionalLabels().isEmpty()) {
            for (AdditionalLabel addlabel : committer.getAdditionalLabels()) {
                String sourceField = addlabel.getSourceField();

                String label = entry.getMetaData().getString(sourceField);
                if (label != null) {
                    if (nodeLabels.length() > 0) {
                        nodeLabels.append(',');
                    }
                    nodeLabels.append("a:`").append(label).append("`");
                }
                if (!addlabel.isKeep()) {
                    entry.getMetaData().remove(sourceField);
                }
            }
        }
        if (nodeLabels.length() > 0) {
            nodeLabels.insert(0, prefix);
        }
        return nodeLabels.toString() + " ";
    }
}
