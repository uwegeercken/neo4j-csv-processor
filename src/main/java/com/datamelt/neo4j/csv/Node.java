package com.datamelt.neo4j.csv;

import java.util.ArrayList;

public class Node
{
	private String label;
	private Attributes metadataAttributes = new Attributes();
	private Attributes attributes = new Attributes();
	
	public String getLabel()
	{
		return label;
	}

	public void setLabel(String label)
	{
		this.label = label;
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
	
	public Attribute getKeyAttribute()
	{
		String idFieldKey = getIdFieldKey();
		return attributes.getAttributeByKey(idFieldKey);
	}
	
	/**
	 * returns the name of the field which is the key for
	 * the relevant node
	 * 
	 */
	public String getIdFieldKey()
	{
		int found =-1;
		
		for(int i=0;i<metadataAttributes.getAttributes().size();i++)
		{
			if(metadataAttributes.getAttributes().get(i).isIdField())
			{
				found = i;
				break;
			}
		}
		if(found>-1)
		{
			return metadataAttributes.getAttributes().get(found).getValue();
		}
		else
		{
			return null;
		}
	}
	
	public String getNodeCreateStatement(String nodeVariable, String csvDefinitionLabel)
	{
		ArrayList<Attribute> allAttributes = getAllAttributes();
		String createStatement="create (" + nodeVariable + ":" + getLabel() + ":" + csvDefinitionLabel + " {";
		StringBuffer attributesBuffer = new StringBuffer();
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
		String completeStatement = createStatement + attributesBuffer.toString() + "});";
		return completeStatement;
	}
	
	public void addAttribute(Attribute attribute)
	{
		attributes.getAttributes().add(attribute);
	}

	public void addMetadataAttribute(Attribute attribute)
	{
		metadataAttributes.getAttributes().add(attribute);
	}

	public boolean equals(Node node)
	{
		return this.getLabel().equals(node.getLabel());
	}
}
