package com.datamelt.neo4j.csv;

public class RelationFileValue
{
	private String startNodeValue;
	private String endNodeValue;
	
	public RelationFileValue(String startNodeValue, String endNodeValue)
	{
		this.startNodeValue = startNodeValue;
		this.endNodeValue = endNodeValue;
	}

	public String getStartNodeValue()
	{
		return startNodeValue;
	}

	public String getEndNodeValue()
	{
		return endNodeValue;
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
