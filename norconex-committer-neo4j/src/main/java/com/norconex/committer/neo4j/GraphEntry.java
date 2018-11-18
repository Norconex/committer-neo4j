package com.norconex.committer.neo4j;

import java.io.InputStream;

import com.norconex.commons.lang.map.Properties;

/**
 * GraphEntry represents an immutable entry needs to be stored in Neo4j
 * @author sroussy
 *
 */
public  class GraphEntry {

	
	private final String id;
	private final Properties metaData;
	private final InputStream content;
	
	public GraphEntry(String id, Properties metaData, InputStream content) {
		super();
		this.id = id;
		this.metaData = metaData;
		this.content = content;
		
		
	}
	
	public String getId() {
		return id;
	}

	public Properties getMetaData() {
		return metaData;
	}

	public InputStream getContent() {
		return content;
	}
	
	
}
