package com.datamelt.neo4j.csv.util;

public enum MetadataAttribute
{
	ID_FIELD("id_field"), NAMESPACE("namespace");
	
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
