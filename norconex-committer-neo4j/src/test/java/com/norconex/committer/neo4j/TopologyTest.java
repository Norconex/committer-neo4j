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
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.helpers.collection.MapUtil;

import com.norconex.committer.neo4j.topologies.NodeTopology;
import com.norconex.committer.neo4j.GraphConfiguration.AdditionalLabel;
import com.norconex.committer.neo4j.GraphConfiguration.Relationship;
import com.norconex.committer.neo4j.GraphConfiguration.Relationship.DIRECTION;
import com.norconex.committer.neo4j.topologies.NoContentTopology;
import com.norconex.committer.neo4j.topologies.OneNodeTopology;
import com.norconex.committer.neo4j.topologies.SplittedTopology;
import com.norconex.commons.lang.map.Properties;

public class TopologyTest {

	@Rule
	public Neo4jRule neo4jRule = new Neo4jRule();
	
	 
	Driver neo4jDriver;
	
	
	@Before
	public void loadDriverWithBasicAuth(){
		this.neo4jDriver = GraphDatabase.driver(neo4jRule.boltURI(), AuthTokens.basic("neo4j", "neo4j"));
	}
	
	@After
	public void tearDown (){
		this.neo4jDriver.close();
	}
	
	
	@Test
	public void oneNodeTopologyTest(){
		

		final OneNodeTopology topology = this.buildOneNodeTopology();
		
		this.storeData(topology);
		
		final String id = this.storeData(topology);
		
		try(Transaction tx = neo4jRule.getGraphDatabaseService().beginTx()){
			
			final GraphConfiguration conf = topology.getGraphConfiguration();
			final Map<String,Object> parameters = MapUtil.map("sourceId",id);
			
			
			final List<Map<String,Object>> results = this.query("MATCH (n:`"+conf.getPrimaryLabel()
						+"`{"+conf.getTargetReferenceField()+":$sourceId}) RETURN n", parameters );
			Assert.assertEquals("Bad size of resultCursor",1,results.size());
			final Map <String, Object>  result = results.iterator().next();
			final Node node = (Node) result.get("n");
			Assert.assertEquals("Value is not the same","test-meta-value", node.getProperty("test-meta"));			
			
			
		}
		
		this.deleteData(topology);
		
		try(Transaction tx = neo4jRule.getGraphDatabaseService().beginTx()){
			
			final GraphConfiguration conf = topology.getGraphConfiguration();
			final Map<String,Object> parameters = MapUtil.map("sourceId",id);
			
			
			final List<Map<String,Object>> results = this.query("MATCH (n:`"+conf.getPrimaryLabel()
						+"`{"+conf.getTargetReferenceField()+":$sourceId}) RETURN n", parameters );
			Assert.assertEquals("Bad size of resultCursor",0,results.size());
						
			
			
		}
		
	}
	
	@Test
	public void noContentTopologyTest(){
		

		final NoContentTopology topology = this.buildNoContentTopology();
		
		this.storeData(topology);
		
		 String id = this.storeData(topology);
		
		try(Transaction tx = neo4jRule.getGraphDatabaseService().beginTx()){
			
			final GraphConfiguration conf = topology.getGraphConfiguration();
			final Map<String,Object> parameters = MapUtil.map("sourceId",id);
			
			
			final List<Map<String,Object>> results = this.query("MATCH (n:`"+conf.getPrimaryLabel()
						+"`{"+conf.getTargetReferenceField()+":$sourceId}) RETURN n", parameters );
			Assert.assertEquals("Bad size of resultCursor",1,results.size());
			final Map <String, Object>  result = results.iterator().next();
			final Node node = (Node) result.get("n");
			Assert.assertEquals("Value is not the same","test-meta-value", node.getProperty("test-meta"));			
			
		}
		
		this.storeData(topology);
		
		id = this.storeData(topology);
		
		try(Transaction tx = neo4jRule.getGraphDatabaseService().beginTx()){
			
			final GraphConfiguration conf = topology.getGraphConfiguration();
			final Map<String,Object> parameters = MapUtil.map("sourceId",id);
			
			
			final List<Map<String,Object>> results = this.query("MATCH (n:`"+conf.getPrimaryLabel()
						+"`{"+conf.getTargetReferenceField()+":$sourceId}) RETURN n", parameters );
			Assert.assertEquals("Bad size of resultCursor",1,results.size());
			final Map <String, Object>  result = results.iterator().next();
			final Node node = (Node) result.get("n");
			Assert.assertEquals("Value is not the same","test-meta-value", node.getProperty("test-meta"));			
			
		}
		
	}
	
	private String storeData (NodeTopology topology){
		final Properties meta = new Properties();		
		meta.put("test-meta", Arrays.asList("test-meta-value"));
		meta.put("reference", Arrays.asList("reference-value"));
		meta.put("sourceId", Arrays.asList("source-id"));
		
		
		final GraphEntry entry = new GraphEntry(meta.getString("sourceId"), meta, null);
		
		topology.storeEntry(entry);
		return meta.getString("sourceId");
	}
	
	private void deleteData (NodeTopology topology){
		
		final String id = "source-id";	
		topology.deleteEntry(id);
		
	}
	
	@Test
	public void splittedTopologyTest(){
		final SplittedTopology topology = this.buildSplittedTopology();
		
		final String id = this.storeData(topology);
		
		try(Transaction tx = neo4jRule.getGraphDatabaseService().beginTx()){
			
			final Map<String,Object> parameters = MapUtil.map("sourceId",id);
			final GraphConfiguration conf = topology.getGraphConfiguration();
			
			
			final List<Map<String,Object>> results = this.query("MATCH (n:`"+conf.getPrimaryLabel()
																+"`{"+conf.getTargetReferenceField()+":$sourceId})  "
																+ "MATCH (n)-[:WITH_META]->(meta) "
																+ "MATCH (n)-[:WITH_CONTENT]->(content) "
																+ "RETURN n,meta,content", parameters );
			Assert.assertEquals("Bad size of resultCursor",1,results.size());
			final Map <String, Object>  result = results.iterator().next();
			final Node metaNode = (Node) result.get("meta");
			Assert.assertEquals("Value is not the same","test-meta-value", metaNode.getProperty("test-meta"));			
			
		}
		
		this.deleteData(topology);
		
		try(Transaction tx = neo4jRule.getGraphDatabaseService().beginTx()){
			
			final Map<String,Object> parameters = MapUtil.map("sourceId",id);
			final GraphConfiguration conf = topology.getGraphConfiguration();
			
			
			final List<Map<String,Object>> results = this.query("MATCH (n:`"+conf.getPrimaryLabel()
																+"`{"+conf.getTargetReferenceField()+":$sourceId})  "
																+ "OPTIONAL MATCH (n)-[:WITH_META]->(meta) "
																+ "OPTIONAL MATCH (n)-[:WITH_CONTENT]->(content) "
																+ "RETURN n,meta,content", parameters );
			Assert.assertEquals("Bad size of resultCursor",0,results.size());
						
			
		}
	}
	
	private List<Map<String,Object>> query (String query, Map<String,Object> parameters){
		Result resultCursor = neo4jRule.getGraphDatabaseService().execute(query, parameters);
		final List<Map<String,Object>> results =this.extractResults(resultCursor);
		return results;
	}
	
	private List<Map<String,Object>> extractResults(Result resultCursor){
		final List<Map<String,Object>> results = new ArrayList<>();
		while(resultCursor.hasNext()){
			final Map <String, Object>  result = resultCursor.next();
			results.add(result);
		}
		return results;
	}
	
	private OneNodeTopology buildOneNodeTopology(){
				
		final OneNodeTopology topology = new OneNodeTopology(neo4jDriver, buildGraphTopologyConfiguration());
		return topology;
	}
	
	private NoContentTopology buildNoContentTopology(){
			
		final NoContentTopology topology = new NoContentTopology(neo4jDriver, buildGraphTopologyConfiguration());
		return topology;
	}
	
	private GraphConfiguration buildGraphTopologyConfiguration(){
		final GraphConfiguration conf = new GraphConfiguration();
		
		
		conf.setPrimaryLabel("Primary-Label");
		conf.setSourceReferenceField("source.Id");
		conf.setTargetReferenceField(Neo4jCommitter.DEFAULT_NEO4J_ID_FIELD);
		conf.setTargetContentField(Neo4jCommitter.DEFAULT_NEO4J_CONTENT_FIELD);
		conf.addAdditionalLabel(new AdditionalLabel("source.Id",true));
		conf.setMultiValuesJoiner(";");
		
		Relationship r1 = new Relationship();
		r1.setDirection(DIRECTION.INCOMING);
		r1.setSourcePropertyKey("source.Id");
		r1.setTargetPropertyKey("source.Id");
		r1.setType("RELATED_TO");
		
		conf.getRelationships().addRelationship(r1);
		
		
		return conf;

	}
	
	private SplittedTopology buildSplittedTopology(){
				
		final SplittedTopology topology = new SplittedTopology(neo4jDriver, buildGraphTopologyConfiguration());
		return topology;
	}
	
	
}
