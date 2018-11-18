package com.norconex.committer.neo4j.topologies;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.neo4j.driver.v1.Driver;

import com.norconex.committer.neo4j.GraphConfiguration;
import com.norconex.committer.neo4j.GraphEntry;

/**
 * Topology with one node bringing metadata only (no content)
 * @author sroussy
 *
 */
public class NoContentTopology extends OneNodeTopology{

	private static final Logger LOGGER = LogManager.getLogger(NoContentTopology.class);
	
	public NoContentTopology(Driver neo4jDriver, GraphConfiguration graphTypologyConfiguration) {
		super(neo4jDriver, graphTypologyConfiguration);
		
	}

	@Override
	protected String buildStoreQuery(GraphEntry entry) {
						
    	final String targetReferenceField = graphConfiguration.getTargetReferenceField();
    	entry.getMetaData().remove("content");
    	final StringBuilder sb = new StringBuilder()
    			.append("MERGE (a:`").append(this.graphConfiguration.getPrimaryLabel()).append("`{").append(targetReferenceField).append(":$").append(NEO4J_PARAM_SOURCE).append("})")    			
    			.append(" SET a+=$").append(NEO4J_PARAM_METADATA)
    			.append(this.getAdditionalLabelsQueryPart(entry));
    	    	
    	this.addParentQueryPart(sb);
    	
    	
    	if (LOGGER.isDebugEnabled()) LOGGER.debug("buildStoreQuery() "+sb);
    	return sb.toString();
	}

}
