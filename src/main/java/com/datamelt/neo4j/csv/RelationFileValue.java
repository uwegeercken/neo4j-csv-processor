package com.datamelt.neo4j.csv;

import java.util.ArrayList;

public class RelationFileValue
{
	private String startNodeValue;
	private String endNodeValue;
	ArrayList<Object> attributeValues = new ArrayList<>();
	
	public RelationFileValue(String startNodeValue, String endNodeValue,ArrayList<Object> attributeValues)
	{
		this.startNodeValue = startNodeValue;
		this.endNodeValue = endNodeValue;
		this.attributeValues = attributeValues;
	}

	public String getStartNodeValue()
	{
		return startNodeValue;
	}

	public String getEndNodeValue()
	{
		return endNodeValue;
	}
	
	public ArrayList<Object> getAttributeValues()
	{
		return attributeValues;
	}
	
	@Override
	public boolean equals(Object relationFileValue)
	{
		if (!(relationFileValue instanceof RelationFileValue))
		{
	        return false;
	    }

		RelationFileValue value = (RelationFileValue) relationFileValue;
		return startNodeValue.equals(value.startNodeValue) && endNodeValue.equals(value.endNodeValue);
	}
}
