package com.datamelt.neo4j.csv;

import java.util.ArrayList;

public class Attributes
{
	private ArrayList<Attribute> attributes = new ArrayList<Attribute>();
	
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
	
	public Attribute getIdAttribute()
	{
		Attribute attribute = null;
		for(int i=0;i<attributes.size();i++)
		{
			if(attributes.get(i).isIdField())
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
			if(attributes.get(i).getValue().equals(value))
			{
				attribute = attributes.get(i);
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
	
	public ArrayList<String> getAttributeValues()
	{
		ArrayList<String> attributeValues = new ArrayList<>();
		for(int i=0;i<attributes.size();i++)
		{
			Attribute attribute = attributes.get(i);
			attributeValues.add(attribute.getValue());
		}
		return attributeValues;
	}
	
	public ArrayList<Attribute> getAttributes()
	{
		return attributes;
	}
	
	public Attribute getAttribute(int index)
	{
		return attributes.get(index);
	}
	
	public int size()
	{
		return attributes.size();
	}
}
