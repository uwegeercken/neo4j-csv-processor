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

	public ArrayList<Attribute> getAllAttributes()
	{
		ArrayList<Attribute> allAttributes = new ArrayList<>();
		allAttributes.addAll(metadataAttributes);
		allAttributes.addAll(attributes);
		return allAttributes;
	}
	
	public Attribute getMetadataAttributeByKey(String attributeKey)
	{
		Attribute attribute = null;
		for(int i=0;i< metadataAttributes.size();i++)
		{
			Attribute metadataAttribute = metadataAttributes.get(i);
			if(metadataAttribute.getKey().equals(attributeKey))
			{
				attribute = metadataAttribute;
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

	public Attribute getKeyAttribute()
	{
		String idFieldKey = getIdFieldKey();
		return getAttributeByKey(idFieldKey);
	}
	
	/**
	 * returns the name of the field which is the key for
	 * the relevant node
	 * 
	 */
	public String getIdFieldKey()
	{
		int found =-1;
		for(int i=0;i<metadataAttributes.size();i++)
		{
			if(metadataAttributes.get(i).isIdField())
			{
				found = i;
				break;
			}
		}
		if(found>-1)
		{
			return metadataAttributes.get(found).getValue();
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
