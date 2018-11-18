package com.norconex.committer.neo4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
/**
 * General configuration for the Graph building.
 * 
 * @author sroussy
 *
 */
@SuppressWarnings ("serial")
public class GraphConfiguration implements Serializable{

	private String primaryLabel;
	
	private String sourceReferenceField;
	private String targetReferenceField;
	private String sourceContentField;
	private String targetContentField;
	private String multiValuesJoiner = "|";
	private List<AdditionalLabel> additionalLabels = new ArrayList<>();
	private Relationships relationships = new Relationships();

	/**
	 * Returns the label used for all nodes imported into Neo4j with the committer, aka the primary label
	 * @return primaryLabel
	 */
	public String getPrimaryLabel() {
		return primaryLabel;
	}

	/**
	 * Sets  the label used for all nodes imported into Neo4j with the committer
	 * @param primaryLabel
	 */
	public void setPrimaryLabel(String primaryLabel) {
		this.primaryLabel = primaryLabel;
	}

	

	public String getSourceReferenceField() {
		return sourceReferenceField;
	}

	public void setSourceReferenceField(String sourceReferenceField) {
		this.sourceReferenceField = sourceReferenceField;
	}

	public String getTargetReferenceField() {
		return targetReferenceField;
	}

	public void setTargetReferenceField(String targetReferenceField) {
		this.targetReferenceField = targetReferenceField;
	}

	public String getSourceContentField() {
		return sourceContentField;
	}

	public void setSourceContentField(String sourceContentField) {
		this.sourceContentField = sourceContentField;
	}

	public String getTargetContentField() {
		return targetContentField;
	}

	public void setTargetContentField(String targetContentField) {
		this.targetContentField = targetContentField;
	}

	public String getMultiValuesJoiner() {
		return multiValuesJoiner;
	}

	public void setMultiValuesJoiner(String multiValuesJoiner) {
		this.multiValuesJoiner = multiValuesJoiner;
	}
	
	/**
	 * Additional labels are a mapping between meta property values and labels to add on a node
	 * @return additional labels
	 */
	public List<AdditionalLabel> getAdditionalLabels() {
		return additionalLabels;
	}

	/**
	 * Additional labels are a mapping between meta property values and labels to add on a node
	 * Sets additional labels
	 * @param additionalLabels
	 */
	public void setAdditionalLabels(List<AdditionalLabel> additionalLabels) {
		this.additionalLabels = additionalLabels;
	}
	
	/**
	 * Additional labels are a mapping between meta property values and labels to add on a node
	 * Adds additional label
	 * @param additionalLabel
	 */
	public void addAdditionalLabel(AdditionalLabel additionalLabel) {
		this.additionalLabels.add(additionalLabel);
	}
	

	public Relationships getRelationships() {
		return relationships;
	}

	public void setRelationships(Relationships relationships) {
		this.relationships = relationships;
	}	

	/**
	 * Additional label allow to transform a metadata field in a node label.
	 * With the <i>keep</i>, you can keep the metadata property or remove it after the label was stored.  
	 * 
	 * @author sroussy
	 *
	 */
	public static class AdditionalLabel implements Serializable{
				
		private String sourceField;
		private boolean keep = false;
		
		/**
		 * Default constructor.
		 */
		public AdditionalLabel(){
			super();
		}
		
		/**
		 * Constructor with the following arguments:
		 * @param sourceField: the name of the targeted metadata field
		 * @param keep: if you want to keep the matadata field or not
		 */
		public AdditionalLabel(String sourceField,boolean keep) {
			super();
			this.keep = keep;
			this.sourceField = sourceField;
		}
		
		
		public boolean isKeep() {
			return keep;
		}
		public void setKeep(boolean keep) {
			this.keep = keep;
		}
		public String getSourceField() {
			return sourceField;
		}
		public void setSourceField(String sourceField) {
			this.sourceField = sourceField;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((sourceField == null) ? 0 : sourceField.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			AdditionalLabel other = (AdditionalLabel) obj;
			if (sourceField == null) {
				if (other.sourceField != null)
					return false;
			} else if (!sourceField.equals(other.sourceField))
				return false;
			return true;
		}
	}
	
	
	/**
	 * Brings the description of many relationships.
	 * 
	 * @author sroussy
	 *
	 */
	public static class Relationships implements Serializable{
		private List<Relationship> relationships = new ArrayList<>();
		private List<RelationshipExclusion> exclusions = new ArrayList<>();
		
		public List<Relationship> getRelationships() {
			return relationships;
		}
		public void setRelationships(List<Relationship> relationships) {
			this.relationships = relationships;
		}
		
		public void addRelationship(Relationship relationship) {
			this.relationships.add(relationship);
		}
		
		public List<RelationshipExclusion> getExclusions() {
			return exclusions;
		}
		public void setExclusions(List<RelationshipExclusion> exclusions) {
			this.exclusions = exclusions;
		}
		
		public void addExclusion (RelationshipExclusion exclusion){
			this.exclusions.add(exclusion);
		}
		
	}
	
	public static class RelationshipExclusion {
		
		private String sourceField;
		
		public String getSourceField() {
			return sourceField;
		}
		public void setSourceField(String sourceField) {
			this.sourceField = sourceField;
		}
	}
	
	/**
	 * Describe how a relationship must be built between nodes in Neo4j. 
	 * 
	 * @author sroussy
	 *
	 */
	public static class Relationship implements Serializable{
		
		public static enum DIRECTION{
			OUTGOING,
			INCOMING,
			BOTH,
			NONE
		}
		
		private String type;
		private String sourcePropertyKey;
		private String targetPropertyKey;
		private DIRECTION direction;
		
		
		
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
		
		public void setTargetPropertyKey(String parentPropertyKey) {
			this.targetPropertyKey = parentPropertyKey;
		}
		
		public void setSourcePropertyKey(String childPropertyKey) {
			this.sourcePropertyKey = childPropertyKey;
		}
		public DIRECTION getDirection() {
			return direction;
		}
		public void setDirection(DIRECTION direction) {
			this.direction = direction;
		}
		public String getSourcePropertyKey() {
			return sourcePropertyKey;
		}
		public String getTargetPropertyKey() {
			return targetPropertyKey;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((direction == null) ? 0 : direction.hashCode());
			result = prime * result + ((sourcePropertyKey == null) ? 0 : sourcePropertyKey.hashCode());
			result = prime * result + ((targetPropertyKey == null) ? 0 : targetPropertyKey.hashCode());
			result = prime * result + ((type == null) ? 0 : type.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Relationship other = (Relationship) obj;
			if (direction != other.direction)
				return false;
			if (sourcePropertyKey == null) {
				if (other.sourcePropertyKey != null)
					return false;
			} else if (!sourcePropertyKey.equals(other.sourcePropertyKey))
				return false;
			if (targetPropertyKey == null) {
				if (other.targetPropertyKey != null)
					return false;
			} else if (!targetPropertyKey.equals(other.targetPropertyKey))
				return false;
			if (type == null) {
				if (other.type != null)
					return false;
			} else if (!type.equals(other.type))
				return false;
			return true;
		}
		
		
		
	}
}
