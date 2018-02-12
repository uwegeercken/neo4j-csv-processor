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
	private ArrayList<String> attributeKeys = new ArrayList<>();
	private HashMap<String,ArrayList<Object>> values = new HashMap<>();
	
	public NodeFile(String nodeLabel, String idFieldName, ArrayList<String> attributeKeys)
	{
		this.nodeLabel = nodeLabel;
		this.idFieldName = idFieldName;
		this.attributeKeys = attributeKeys;
	}
	
	public void addValue(String key,ArrayList<Object> values)
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
		buffer.append(idFieldName).append(ID).append(delimiter);
		for(int i=0;i<attributeKeys.size();i++)
		{
			String key = attributeKeys.get(i);
			if(!key.equals(MetadataAttribute.ID_FIELD.key()) &&!key.equals(idFieldName))
			{
				buffer.append(key);
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

	public HashMap<String,ArrayList<Object>> getValues()
	{
		return values;
	}
	
	public ArrayList<String> getAttributeKeys()
	{
		return attributeKeys;
	}
}
