package com.datamelt.neo4j.csv.util;

public enum MetadataAttributeValue
{
	ID("ID"), TYPE_INT("int"), TYPE_FLOAT("float");
	
	private String key;
	
	MetadataAttributeValue(String key)
	{
		this.key = key;
	}
	
	public String key()
	{
		return key;
	}
}
