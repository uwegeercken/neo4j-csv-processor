package com.datamelt.neo4j.csv;

import java.util.ArrayList;
import java.util.HashMap;

import com.datamelt.neo4j.csv.util.MetadataAttribute;

public class NodeFile
{
	private static final String CSV_FILE_EXTENSION 	= ".csv";
	
	private static final String ID 					= ":ID";
	private static final String LABEL 				= ":LABEL";
	
	private String nodeLabel;
	private String idFieldName;
	private ArrayList<Attribute> metadataAttributes;
	private ArrayList<Attribute> attributes;
	private HashMap<String,ArrayList<String>> values = new HashMap<>(1000);
	
	public NodeFile(String nodeLabel, String idFieldName, ArrayList<Attribute> attributes,ArrayList<Attribute> metadataAttributes)
	{
		this.nodeLabel = nodeLabel;
		this.idFieldName = idFieldName;
		this.attributes = attributes;
		this.metadataAttributes = metadataAttributes;
	}
	
	public void addValue(String key,ArrayList<String> values)
	{
		this.values.put(key,values);
	}

	public String getNodeLabel()
	{
		return nodeLabel;
	}

	public String getHeader(String delimiter)
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append(idFieldName).append(ID);
		buffer.append(delimiter);
		for(int i=0;i<attributes.size();i++)
		{
			Attribute attribute = attributes.get(i);
			String key = attribute.getKey();
			if(!key.equals(MetadataAttribute.ID_FIELD.key()) && !key.equals(idFieldName))
			{
				buffer.append(key);
				if(attribute.getJavaType()!=null)
				{
					buffer.append(":" + attribute.getJavaType());
				}
				buffer.append(delimiter);
			}
		}
		buffer.append(LABEL);
		return buffer.toString();
	}
	
	public String getNodeFileName()
	{
		return getNodeLabel() + CSV_FILE_EXTENSION;
	}

	public HashMap<String,ArrayList<String>> getValues()
	{
		return values;
	}
}
