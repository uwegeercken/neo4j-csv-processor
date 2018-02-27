package com.datamelt.neo4j.csv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class NodeFile
{
	private static final String CSV_FILE_EXTENSION 	= ".csv";
	
	private static final String ID 					= ":ID";
	private static final String LABEL 				= ":LABEL";
	
	private String nodeLabel;
	private String nameSpace;
	private String idFieldName;
	private ArrayList<Attribute> metadataAttributes;
	private ArrayList<Attribute> attributes;
	private HashMap<String,ArrayList<String>> values = new HashMap<>(1000);
	private HashSet<String> keys = new HashSet<>(100);
	
	public NodeFile(String nodeLabel, String nameSpace, String idFieldName, ArrayList<Attribute> attributes,ArrayList<Attribute> metadataAttributes)
	{
		this.nodeLabel = nodeLabel;
		this.nameSpace = nameSpace;
		this.idFieldName = idFieldName;
		this.attributes = attributes;
		this.metadataAttributes = metadataAttributes;
	}
	
	public void addValue(String key,ArrayList<String> values)
	{
		this.values.put(key,values);
	}
	
	public void addKey(String key)
	{
		this.keys.add(key);
	}

	public String getNodeLabel()
	{
		return nodeLabel;
	}

	public String getHeader(String delimiter)
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append(idFieldName).append(ID);
		if(nameSpace != null)
		{
			buffer.append("(").append(nameSpace).append(")");
		}
		buffer.append(delimiter);
		for(int i=0;i<attributes.size();i++)
		{
			Attribute attribute = attributes.get(i);
			String key = attribute.getKey();
			if(!key.equals(idFieldName))
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
