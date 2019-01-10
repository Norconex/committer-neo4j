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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.helpers.collection.MapUtil;

import com.norconex.committer.neo4j.Relationship.Direction;
import com.norconex.committer.neo4j.topologies.NoContentTopology;
import com.norconex.committer.neo4j.topologies.NodeTopology;
import com.norconex.committer.neo4j.topologies.OneNodeTopology;
import com.norconex.committer.neo4j.topologies.SplittedTopology;
import com.norconex.commons.lang.map.Properties;

public class TopologyTest {

    @Rule
    public Neo4jRule neo4jRule = new Neo4jRule();

    private Neo4jCommitter committer;

    @Before
    public void loadDriverWithBasicAuth() {
        this.committer = buildNeo4jCommitter();
    }

    @After
    public void tearDown() {
        this.committer.close();
    }

    @Test
    public void oneNodeTopologyTest() {

        final OneNodeTopology topology = this.buildOneNodeTopology();

        this.storeData(topology);

        final String id = this.storeData(topology);

        try (Transaction tx = neo4jRule.getGraphDatabaseService().beginTx()) {

            final Map<String, Object> parameters = MapUtil.map("sourceId", id);

            final List<Map<String, Object>> results = this
                    .query("MATCH (n:`" + committer.getPrimaryLabel() + "`{"
                            + committer.getTargetReferenceField()
                            + ":$sourceId}) RETURN n", parameters);
            Assert.assertEquals("Bad size of resultCursor", 1, results.size());
            final Map<String, Object> result = results.iterator().next();
            final Node node = (Node) result.get("n");
            Assert.assertEquals("Value is not the same", "test-meta-value",
                    node.getProperty("test-meta"));

        }

        this.deleteData(topology);

        try (Transaction tx = neo4jRule.getGraphDatabaseService().beginTx()) {

            final Map<String, Object> parameters = MapUtil.map("sourceId", id);

            final List<Map<String, Object>> results = this
                    .query("MATCH (n:`" + committer.getPrimaryLabel() + "`{"
                            + committer.getTargetReferenceField()
                            + ":$sourceId}) RETURN n", parameters);
            Assert.assertEquals("Bad size of resultCursor", 0, results.size());

        }

    }

    @Test
    public void noContentTopologyTest() {

        final NoContentTopology topology = this.buildNoContentTopology();

        this.storeData(topology);

        String id = this.storeData(topology);

        try (Transaction tx = neo4jRule.getGraphDatabaseService().beginTx()) {

            final Map<String, Object> parameters = MapUtil.map("sourceId", id);

            final List<Map<String, Object>> results = this
                    .query("MATCH (n:`" + committer.getPrimaryLabel() + "`{"
                            + committer.getTargetReferenceField()
                            + ":$sourceId}) RETURN n", parameters);
            Assert.assertEquals("Bad size of resultCursor", 1, results.size());
            final Map<String, Object> result = results.iterator().next();
            final Node node = (Node) result.get("n");
            Assert.assertEquals("Value is not the same", "test-meta-value",
                    node.getProperty("test-meta"));

        }

        this.storeData(topology);

        id = this.storeData(topology);

        try (Transaction tx = neo4jRule.getGraphDatabaseService().beginTx()) {

            final Map<String, Object> parameters = MapUtil.map("sourceId", id);

            final List<Map<String, Object>> results = this
                    .query("MATCH (n:`" + committer.getPrimaryLabel() + "`{"
                            + committer.getTargetReferenceField()
                            + ":$sourceId}) RETURN n", parameters);
            Assert.assertEquals("Bad size of resultCursor", 1, results.size());
            final Map<String, Object> result = results.iterator().next();
            final Node node = (Node) result.get("n");
            Assert.assertEquals("Value is not the same", "test-meta-value",
                    node.getProperty("test-meta"));

        }

    }

    private String storeData(NodeTopology topology) {
        final Properties meta = new Properties();
        meta.put("test-meta", Arrays.asList("test-meta-value"));
        meta.put("reference", Arrays.asList("reference-value"));
        meta.put("sourceId", Arrays.asList("source-id"));

        final GraphEntry entry = new GraphEntry(meta.getString("sourceId"),
                meta, null);

        topology.storeEntry(entry);
        return meta.getString("sourceId");
    }

    private void deleteData(NodeTopology topology) {

        final String id = "source-id";
        topology.deleteEntry(id);

    }

    @Test
    public void splittedTopologyTest() {
        final SplittedTopology topology = this.buildSplittedTopology();

        final String id = this.storeData(topology);

        try (Transaction tx = neo4jRule.getGraphDatabaseService().beginTx()) {

            final Map<String, Object> parameters = MapUtil.map("sourceId", id);

            final List<Map<String, Object>> results = this.query("MATCH (n:`"
                    + committer.getPrimaryLabel() + "`{"
                    + committer.getTargetReferenceField() + ":$sourceId})  "
                    + "MATCH (n)-[:WITH_META]->(meta) "
                    + "MATCH (n)-[:WITH_CONTENT]->(content) "
                    + "RETURN n,meta,content", parameters);
            Assert.assertEquals("Bad size of resultCursor", 1, results.size());
            final Map<String, Object> result = results.iterator().next();
            final Node metaNode = (Node) result.get("meta");
            Assert.assertEquals("Value is not the same", "test-meta-value",
                    metaNode.getProperty("test-meta"));

        }

        this.deleteData(topology);

        try (Transaction tx = neo4jRule.getGraphDatabaseService().beginTx()) {

            final Map<String, Object> parameters = MapUtil.map("sourceId", id);

            final List<Map<String, Object>> results = this.query("MATCH (n:`"
                    + committer.getPrimaryLabel() + "`{"
                    + committer.getTargetReferenceField() + ":$sourceId})  "
                    + "OPTIONAL MATCH (n)-[:WITH_META]->(meta) "
                    + "OPTIONAL MATCH (n)-[:WITH_CONTENT]->(content) "
                    + "RETURN n,meta,content", parameters);
            Assert.assertEquals("Bad size of resultCursor", 0, results.size());

        }
    }

    private List<Map<String, Object>> query(String query,
            Map<String, Object> parameters) {
        Result resultCursor = neo4jRule.getGraphDatabaseService().execute(query,
                parameters);
        final List<Map<String, Object>> results = this
                .extractResults(resultCursor);
        return results;
    }

    private List<Map<String, Object>> extractResults(Result resultCursor) {
        final List<Map<String, Object>> results = new ArrayList<>();
        while (resultCursor.hasNext()) {
            final Map<String, Object> result = resultCursor.next();
            results.add(result);
        }
        return results;
    }

    private OneNodeTopology buildOneNodeTopology() {
        return new OneNodeTopology(committer);
    }

    private NoContentTopology buildNoContentTopology() {
        return new NoContentTopology(committer);
    }

    private Neo4jCommitter buildNeo4jCommitter() {
        Neo4jCommitter comm = new Neo4jCommitter();

        comm.setPrimaryLabel("Primary-Label");
        comm.setSourceReferenceField("source.Id");
        comm.setTargetReferenceField(Neo4jCommitter.DEFAULT_NEO4J_ID_FIELD);
        comm.setTargetContentField(Neo4jCommitter.DEFAULT_NEO4J_CONTENT_FIELD);
        comm.addAdditionalLabel(new AdditionalLabel("source.Id", true));
        comm.setMultiValuesJoiner(";");

        comm.setUri(neo4jRule.boltURI().toString());
        comm.setUser("neo4j");
        comm.setPassword("neo4j");

        Relationship rel = new Relationship();
        rel.setDirection(Direction.INCOMING);
        rel.setSourcePropertyKey("source.Id");
        rel.setTargetPropertyKey("source.Id");
        rel.setType("RELATED_TO");

        comm.addRelationship(rel);
        return comm;
    }

    private SplittedTopology buildSplittedTopology() {
        return new SplittedTopology(committer);
    }
}
