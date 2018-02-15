package com.datamelt.neo4j.csv;

import java.util.ArrayList;
import java.util.HashMap;

public class Relation
{
	private Node startNode;
	private Node endNode;
	private String relationType;
	private HashMap<Integer,Integer> csvColumnToAttributesMap = new HashMap<>();
	
	private Attributes metadataAttributes = new Attributes();
	private Attributes attributes = new Attributes();
	
	public Relation(Node startNode, Node endNode, String relationType)
	{
		this.startNode = startNode;
		this.endNode = endNode;
		this.relationType = relationType;
	}
	
	public void addAttribute(Attribute attribute)
	{
		attributes.getAttributes().add(attribute);
	}

	public void addMetadataAttribute(Attribute attribute)
	{
		metadataAttributes.getAttributes().add(attribute);
	}
	
	public Attributes getAttributes()
	{
		return attributes;
	}
	
	public Attributes getMetadataAttributes()
	{
		return metadataAttributes;
	}
	
	public ArrayList<Attribute> getAllAttributes()
	{
		ArrayList<Attribute> allAttributes = new ArrayList<>();
		allAttributes.addAll(metadataAttributes.getAttributes());
		allAttributes.addAll(attributes.getAttributes());
		return allAttributes;
	}
	
	public String getRelationCreateStatement(String nodeStartVariable, String nodeEndVariable)
	{
		String matchStatementStartNode="match(" + nodeStartVariable + " :" + startNode.getLabel() + ") ";
		String matchStatementEndNode="match(" + nodeEndVariable + " :" + endNode.getLabel() + ") ";
		
		String createStatement1 = "create (" + nodeStartVariable + ")-[:" + relationType ;

		ArrayList<Attribute> allAttributes = getAllAttributes();
		StringBuffer attributesBuffer = new StringBuffer();
		if(allAttributes.size()>0)
		{
			attributesBuffer.append(" {");
			for(int i=0;i<allAttributes.size();i++)
			{
				Attribute attribute = allAttributes.get(i);
				attributesBuffer.append(attribute.getKey() + ":" + "\"" + attribute.getValue());
				if(attribute.getJavaType()!=null)
				{
					attributesBuffer.append(":" +attribute.getJavaType());
				}
				attributesBuffer.append("\"");
				if(i<allAttributes.size()-1)
				{
					attributesBuffer.append(", ");
				}
			}
			attributesBuffer.append("}");
		}
		
		String createStatement2 = createStatement1 + attributesBuffer.toString() + "]->(" + nodeEndVariable + ")";
		String completeStatement = matchStatementStartNode + matchStatementEndNode + createStatement2 + ";";
		return completeStatement;
	}

	public Node getStartNode()
	{
		return startNode;
	}
	
	public void setStartNode(Node startNode)
	{
		this.startNode = startNode;
	}
	public Node getEndNode()
	{
		return endNode;
	}
	public void setEndNode(Node endNode)
	{
		this.endNode = endNode;
	}
	public String getRelationType()
	{
		return relationType;
	}
	public void setRelationType(String relationType)
	{
		this.relationType = relationType;
	}

	public boolean equals(Relation relation)
	{
		if(this.relationType.equals(relation.relationType) && this.startNode.getLabel().equals(relation.startNode.getLabel()) && this.endNode.getLabel().equals(relation.endNode.getLabel()))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	public HashMap<Integer, Integer> getCsvColumnToAttributesMap()
	{
		return csvColumnToAttributesMap;
	}
	
	public void setCsvColumnToAttributesMap(HashMap<Integer, Integer> csvColumnToAttributesMap)
	{
		this.csvColumnToAttributesMap = csvColumnToAttributesMap;
	}
}
