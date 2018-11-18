package com.norconex.committer.neo4j.topologies;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.neo4j.driver.v1.Driver;

import com.norconex.committer.neo4j.GraphConfiguration;
import com.norconex.committer.neo4j.GraphEntry;

/**
 * This topology makes one node per entry (content + metadata) 
 * @author sroussy
 *
 */
public class OneNodeTopology extends NodeTopology{

	private static final Logger LOGGER = LogManager.getLogger(OneNodeTopology.class);
	
	public OneNodeTopology(Driver neo4jDriver,GraphConfiguration graphTypologyConfiguration) {
		super(neo4jDriver,graphTypologyConfiguration);		
	}	

	@Override
	protected String buildStoreQuery(final GraphEntry entry) {
						
    	final String targetReferenceField = graphConfiguration.getTargetReferenceField();
    	
    	final StringBuilder sb = new StringBuilder()
    			.append("MERGE (a:`").append(this.graphConfiguration.getPrimaryLabel()).append("`{").append(targetReferenceField).append(":$").append(NEO4J_PARAM_SOURCE).append("})")    			
    			.append(" SET a+=$").append(NEO4J_PARAM_METADATA)
    			.append(", a.").append(graphConfiguration.getTargetContentField()).append("=$").append(NEO4J_PARAM_CONTENT)
    			.append(this.getAdditionalLabelsQueryPart(entry));
    			
    	    	
    	this.addParentQueryPart(sb);
    	
    	
    	if (LOGGER.isDebugEnabled()) LOGGER.debug("buildStoreQuery() "+sb);
    	return sb.toString();
	}

	@Override
	protected String buildDeleteQuery() {
		final String primaryLabel = graphConfiguration.getPrimaryLabel();
		final String targetReferenceField = graphConfiguration.getTargetReferenceField();
		final StringBuilder sb = new StringBuilder()
    			.append("MATCH (a:`").append(primaryLabel).append("`{").append(targetReferenceField).append(":$").append(NEO4J_PARAM_SOURCE).append("})")
    			.append(" DETACH DELETE a");
		if (LOGGER.isDebugEnabled()) LOGGER.debug("buildDeleteQuery() "+sb);
		return sb.toString();
    			
	}
	
	

}
