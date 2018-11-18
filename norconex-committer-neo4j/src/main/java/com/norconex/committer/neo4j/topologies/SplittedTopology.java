package com.norconex.committer.neo4j.topologies;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.neo4j.driver.v1.Driver;

import com.norconex.committer.neo4j.GraphConfiguration;
import com.norconex.committer.neo4j.GraphEntry;

public class SplittedTopology extends NodeTopology{

	private static final Logger LOGGER = LogManager.getLogger(SplittedTopology.class);
	
	public SplittedTopology(Driver neo4jDriver,GraphConfiguration graphTypologyConfiguration) {
		super(neo4jDriver,graphTypologyConfiguration);
	
	}	

	@Override
	protected String buildStoreQuery(GraphEntry entry) {
		final String primaryLabel = this.graphConfiguration.getPrimaryLabel();
		final String targetReferenceField = this.graphConfiguration.getTargetReferenceField();
		final String targetContentField = this.graphConfiguration.getTargetContentField();
		String addLabels = !this.getAdditionalLabelsQueryPart(entry).isEmpty() ? " SET "+this.getAdditionalLabelsQueryPart(entry).substring(1):"";		
		final StringBuilder sb = new StringBuilder ()
				.append("MERGE (a:`").append(primaryLabel).append("`{").append(targetReferenceField).append(":$").append(NEO4J_PARAM_SOURCE).append("})")
				.append(addLabels)
				.append(" MERGE (a)-[:WITH_CONTENT]->(").append("content:`").append(primaryLabel).append("_content`{source:$").append(NEO4J_PARAM_SOURCE).append("})")
				.append (" SET content.").append(targetContentField).append("=$").append(NEO4J_PARAM_CONTENT)
				.append(" MERGE (a)-[:WITH_META]->(metadata:`").append(primaryLabel).append("_metadata` {source:$").append(NEO4J_PARAM_SOURCE).append("})")
				.append(" SET metadata+=$").append(NEO4J_PARAM_METADATA);
		
		this.addParentQueryPart(sb);
				
    	
    	if (LOGGER.isDebugEnabled()) LOGGER.debug("buildStoreQuery() "+sb);
    	
    	return sb.toString();
	}

	@Override
	protected String buildDeleteQuery() {
		final String primaryLabel = this.graphConfiguration.getPrimaryLabel();
		
		final String targetReferenceField = this.graphConfiguration.getTargetReferenceField();
		
		final StringBuilder sb = new StringBuilder ()
				.append("MATCH (a:`").append(primaryLabel).append("`{").append(targetReferenceField).append(":$").append(NEO4J_PARAM_SOURCE).append("})")
				.append(" MATCH (a)-[:WITH_CONTENT]->(content)")
				.append(" MATCH (a)-[:WITH_META]->(metadata)")
				.append(" DETACH DELETE a,content,metadata");
		
		if (LOGGER.isDebugEnabled()) LOGGER.debug("buildDeleteQuery() "+sb);
		return sb.toString();
	}

}
