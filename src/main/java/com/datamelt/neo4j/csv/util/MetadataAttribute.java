package com.datamelt.neo4j.csv.util;

public enum MetadataAttribute
{
	NAMESPACE("namespace");
	
	private String key;
	
	MetadataAttribute(String key)
	{
		this.key = key;
	}
	
	public String key()
	{
		return key;
	}
}
