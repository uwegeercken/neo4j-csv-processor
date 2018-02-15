package com.datamelt.neo4j.csv;

import java.util.ArrayList;
import java.util.HashMap;

public class RelationFile
{
	private static final String CSV_FILE_EXTENSION 	= ".csv";
	private static final String CSV_FILE_PREFIX		= "relation_";
	
	private static final String START_ID 	= ":START_ID";
	private static final String END_ID 		= ":END_ID";
	private static final String TYPE	 	= ":TYPE";
	
	private String relationType;
	private String startNodeLabel;
	private String endNodeLabel;
	private ArrayList<Attribute> metadataAttributes;
	private ArrayList<Attribute> attributes;
	private HashMap<String,RelationFileValue> values = new HashMap<>(1000);
	public RelationFile(String startNodeLabel, String endNodeLabel, String relationType, ArrayList<Attribute> attributes, ArrayList<Attribute> metadataAttributes)
	{
		this.startNodeLabel = startNodeLabel;
		this.endNodeLabel = endNodeLabel;
		this.relationType = relationType;
		this.attributes = attributes;
		this.metadataAttributes = metadataAttributes;
	}
	
	public void addValue(String startNodeValue, String endNodeValue,ArrayList<String> attributeValues)
	{
		RelationFileValue fileValue = new RelationFileValue(startNodeValue, endNodeValue, attributeValues);
		// only add value for the relation if it does not exist already
		//if(!values.contains(fileValue))
		//{
			values.put(startNodeValue+attributeValues.toString()+endNodeValue,fileValue);
		//}
	}

	public String getHeader(String delimiter)
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append(START_ID);
		buffer.append(delimiter);
		for(int i=0;i<attributes.size();i++)
		{
			Attribute attribute = attributes.get(i);
			String key = attribute.getKey();
			buffer.append(key);
			if(attribute.getJavaType()!=null)
			{
				buffer.append(":" + attribute.getJavaType());
			}
			buffer.append(delimiter);
		}
		buffer.append(END_ID + delimiter + TYPE);
		return buffer.toString();
	}

	public String getRelationFileName()
	{
		return CSV_FILE_PREFIX + startNodeLabel + "_" + endNodeLabel + CSV_FILE_EXTENSION;
	}
	
	public String getRelationType()
	{
		return relationType;
	}

	public void setRelationType(String relationType)
	{
		this.relationType = relationType;
	}

	public String getStartNodeLabel()
	{
		return startNodeLabel;
	}

	public String getEndNodeLabel()
	{
		return endNodeLabel;
	}

	public HashMap<String,RelationFileValue> getValues()
	{
		return values;
	}
}
