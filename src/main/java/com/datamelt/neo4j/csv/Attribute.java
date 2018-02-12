package com.datamelt.neo4j.csv;

import com.datamelt.neo4j.csv.util.MetadataAttribute;

public class Attribute
{
	private String key;
	private String value;
	private boolean isIdField=false;
	
	public String getKey()
	{
		return key;
	}
	
	public void setKey(String key)
	{
		this.key = key;
	}
	
	public String getValue()
	{
		return value;
	}
	
	public void setValue(String value)
	{
		this.value = value;
	}

	public boolean isIdField()
	{
		return isIdField;
	}

	public void setIsIdField(boolean isIdField)
	{
		this.isIdField = isIdField;
	}
	
	public static boolean isMetadataAttribute(String attributeKey)
	{
    	boolean isMetadataAttribute=false;
    	for(MetadataAttribute metaDataAttribute : MetadataAttribute.values())
    	{
    		if(metaDataAttribute.key().equals(attributeKey))
    		{
    			isMetadataAttribute = true;
    			break;
    		}
    	}
    	return isMetadataAttribute;
	}
}
