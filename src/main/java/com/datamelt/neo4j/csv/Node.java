package com.datamelt.neo4j.csv;

import java.util.ArrayList;
import com.datamelt.neo4j.csv.util.MetadataAttribute;

public class Node
{
	private String label;
	private ArrayList<Attribute> metadataAttributes = new ArrayList<Attribute>();
	private ArrayList<Attribute> attributes = new ArrayList<Attribute>();
	
	public String getLabel()
	{
		return label;
	}

	public void setLabel(String label)
	{
		this.label = label;
	}

	public ArrayList<Attribute> getAttributes()
	{
		return attributes;
	}
	
	public ArrayList<Attribute> getMetadataAttributes()
	{
		return metadataAttributes;
	}

	public Attribute getMetadataAttribute(String attributeKey)
	{
		Attribute attribute = null;
		for(int i=0;i< metadataAttributes.size();i++)
		{
			if(attribute.getKey().equals(attributeKey))
			{
				attribute = metadataAttributes.get(i);
			}
		}
		return attribute;
	}
	
	public ArrayList<String> getAttributeKeys()
	{
		ArrayList<String> attributeKeys = new ArrayList<>();
		for(int i=0;i<attributes.size();i++)
		{
			Attribute attribute = attributes.get(i);
			attributeKeys.add(attribute.getKey());
		}
		return attributeKeys;
	}

	/**
	 * returns the name of the field which is the key for
	 * the relevant node
	 * 
	 */
	public String getIdFieldName()
	{
		int found =-1;
		for(int i=0;i<attributes.size();i++)
		{
			if(attributes.get(i).isIdField())
			{
				found = i;
				break;
			}
		}
		if(found>-1)
		{
			return attributes.get(found).getValue();
		}
		else
		{
			return null;
		}
	}
	
	public Attribute getAttributeByKey(String key)
	{
		Attribute attribute = null;
		for(int i=0;i<attributes.size();i++)
		{
			if(attributes.get(i).getKey().equals(key))
			{
				attribute = attributes.get(i);
			}
		}
		return attribute;
	}
	
	public Attribute getAttributeByValue(String value)
	{
		Attribute attribute = null;
		for(int i=0;i<attributes.size();i++)
		{
			if(!attributes.get(i).getKey().equals(MetadataAttribute.ID_FIELD.key()) &&attributes.get(i).getValue().equals(value))
			{
				attribute = attributes.get(i);
			}
		}
		return attribute;
	}
	
	public void addAttribute(Attribute attribute)
	{
		attributes.add(attribute);
	}

	public void addMetadataAttribute(Attribute attribute)
	{
		metadataAttributes.add(attribute);
	}

	public boolean equals(Node node)
	{
		return this.getLabel().equals(node.getLabel());
	}
}
