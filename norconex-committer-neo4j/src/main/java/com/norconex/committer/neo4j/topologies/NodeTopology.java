package com.norconex.committer.neo4j.topologies;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;

import com.norconex.committer.core.CommitterException;
import com.norconex.committer.neo4j.GraphConfiguration;
import com.norconex.committer.neo4j.GraphEntry;
import com.norconex.committer.neo4j.GraphConfiguration.AdditionalLabel;
import com.norconex.committer.neo4j.GraphConfiguration.Relationship;
import com.norconex.commons.lang.map.Properties;

public abstract class NodeTopology {
	public static final String NEO4J_PARAM_CONTENT = "source_content";
	public static final String NEO4J_PARAM_METADATA = "source_metadata";
	public static final String NEO4J_PARAM_SOURCE = "source_id";
	public static final String NEO4J_PARAM_SUB_ENTRIES = "subentries";
	
	protected final Driver neo4jDriver;
	protected final GraphConfiguration graphConfiguration;
	
	public NodeTopology (Driver neo4jDriver, GraphConfiguration graphConfiguration){
		this.neo4jDriver = neo4jDriver;
		this.graphConfiguration = graphConfiguration;
	}

	public Driver getNeo4jDriver() {
		return neo4jDriver;
	}	

	public GraphConfiguration getGraphConfiguration() {
		return graphConfiguration;
	}	
	
	public List<String> getTargetPropertyValues (Properties p,Relationship r){		
		List<String> parentPropertyValues = p.getStrings(r.getSourcePropertyKey());
		return parentPropertyValues;
	}
	
	protected String extractContent (InputStream stream) throws IOException{
		String s= "";
		if (stream != null){			
			s = IOUtils.toString(stream,StandardCharsets.UTF_8.name());
		}
		return s;
	
	}	
	
	protected Map<String,Object> extractMetadata (Properties p){		
		final Map<String,Object> map = new HashMap<>();
		for (Map.Entry<String, List<String>> entry : p.entrySet()){
			
			final String key = entry.getKey();
			
			final List<String> values = entry.getValue();
			final String value = values.stream().collect(Collectors.joining(this.graphConfiguration.getMultiValuesJoiner()));
			map.put(key,value );
		}		
		return map;
	}
	
	protected abstract String buildStoreQuery (final GraphEntry entry);
	protected abstract String buildDeleteQuery ();
	
	protected void addParentQueryPart(StringBuilder sb){
		
		if ( this.graphConfiguration.getRelationships() != null){
			final String primaryLabel = this.graphConfiguration.getPrimaryLabel();		
			
			int i = 0;
			
			for (Relationship rel : this.graphConfiguration.getRelationships().getRelationships() ) {
				sb.append(" WITH a,$").append(NEO4J_PARAM_SUB_ENTRIES).append("_").append(i).append(" AS subs UNWIND subs AS sub ")
				.append(" WITH a, sub")
				.append(" WHERE a.`").append(rel.getTargetPropertyKey()).append("` <> sub")				
				.append(" MERGE (s:`").append(primaryLabel).append("`{`").append(rel.getTargetPropertyKey()).append("`:sub})");
				
				switch (rel.getDirection()){
					case OUTGOING :sb.append(" MERGE (s)<-[:").append(rel.getType()).append("]-(a)");break;
					case INCOMING :sb.append(" MERGE (s)-[:").append(rel.getType()).append("]->(a)");break;
					case BOTH :sb.append(" MERGE (s)-[:").append(rel.getType()).append("]->(a)");sb.append(" MERGE (s)<-[:").append(rel.getType()).append("]-(a)");break;
					case NONE :sb.append(" MERGE (s)-[:").append(rel.getType()).append("]-(a)");
				}
				i++;
			}
		}
		
	}	
	
	
	public synchronized void  storeEntry(final GraphEntry entry) throws CommitterException {
		
		try ( Session session = neo4jDriver.session() )
        {
        	session.writeTransaction( new TransactionWork<Void>() {
             
                @Override
				public Void execute( Transaction tx ){
                    tx.run(NodeTopology.this.buildStoreQuery(entry),NodeTopology.this.buildNeo4jParameters(entry));
                    return null;
                }
            } );	
        }
	}
	
	protected Value buildNeo4jParameters(final GraphEntry entry){
		Value values = null;
		try{
			final String content = this.extractContent(entry.getContent());
			final Map<String,Object> meta = this.extractMetadata(entry.getMetaData());
			
			final Map<String,Object> neo4jParameters = new HashMap<>();
			neo4jParameters.put(NEO4J_PARAM_SOURCE, entry.getId());
			neo4jParameters.put(NEO4J_PARAM_METADATA, meta);
			neo4jParameters.put(NEO4J_PARAM_CONTENT, content);
			
			int i = 0;
			for (Relationship rel : this.graphConfiguration.getRelationships().getRelationships()) {
				neo4jParameters.put(NEO4J_PARAM_SUB_ENTRIES+"_"+i, this.getTargetPropertyValues(entry.getMetaData(), rel));
				i++;
			}
			values = Values.value(neo4jParameters);
		}
		catch (IOException e){
			throw new CommitterException(e);
		}
		return values;
	}
	
	public synchronized void deleteEntry(final String id) throws CommitterException {
		
		try ( Session session = neo4jDriver.session() )
        {
        	session.writeTransaction( new TransactionWork<Void>() {
             
                @Override
				public Void execute( Transaction tx ){
                    tx.run(NodeTopology.this.buildDeleteQuery(),Values.parameters(NEO4J_PARAM_SOURCE, id));
                    return null;
                }
            } );	
        }
		
	}
	
	protected String getAdditionalLabelsQueryPart (GraphEntry entry){
		String nodeLabels =  "";
		if (!this.graphConfiguration.getAdditionalLabels().isEmpty()){
			for (AdditionalLabel addlabel : this.graphConfiguration.getAdditionalLabels()) {
				String label = entry.getMetaData().getString(addlabel.getSourceField());
				if (label != null) nodeLabels += ",a:`"+label+"`";
				if (!addlabel.isKeep()){
					entry.getMetaData().remove(addlabel.getSourceField());
				}
			}
		}
		return nodeLabels;
	}
	
	
}
