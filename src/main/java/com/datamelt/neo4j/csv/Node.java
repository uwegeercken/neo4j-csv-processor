package com.datamelt.neo4j.csv;

import java.util.ArrayList;
import java.util.HashMap;

import com.datamelt.neo4j.csv.util.MetadataAttribute;

public class Node
{
	private String label;
	private int keyAttributeIndex;
	private Attributes metadataAttributes = new Attributes();
	private Attributes attributes = new Attributes();
	private HashMap<Integer,Integer> attributesToCsvColumnMap = new HashMap<>();
	
	public String getLabel()
	{
		return label;
	}

	public void setLabel(String label)
	{
		this.label = label;
	}

	public Attributes getAttributes()
	{
		return attributes;
	}

	public ArrayList<String> getAttributesKeys()
	{
		return attributes.getAttributeKeys();
	}

	public ArrayList<String> getAttributesValues()
	{
		return attributes.getAttributeValues();
	}

	public Attributes getMetadataAttributes()
	{
		return metadataAttributes;
	}
	
	public String getNamespace()
	{
		String nameSpace = null;
		for(int j=0;j<metadataAttributes.size();j++)
		{
			Attribute attribute = metadataAttributes.getAttribute(j);
			if(attribute.getKey().equals(MetadataAttribute.NAMESPACE.key()))
			{
				nameSpace = attribute.getValue();
				break;
			}
		}
		return nameSpace;
	}
	
	public ArrayList<Attribute> getAllAttributes()
	{
		ArrayList<Attribute> allAttributes = new ArrayList<>();
		allAttributes.addAll(metadataAttributes.getAttributes());
		allAttributes.addAll(attributes.getAttributes());
		return allAttributes;
	}
	
	public Attribute getKeyAttribute()
	{
		return attributes.getAttribute(keyAttributeIndex);
	}
	
	public String getNodeCreateStatement(String nodeVariable, String csvDefinitionLabel)
	{
		ArrayList<Attribute> allAttributes = getAllAttributes();
		String createStatement="create (" + nodeVariable + ":" + getLabel() + ":" + csvDefinitionLabel + " {";
		StringBuffer attributesBuffer = new StringBuffer();
		for(int i=0;i<allAttributes.size();i++)
		{
			Attribute attribute = allAttributes.get(i);
			attributesBuffer.append(attribute.getKey() + ":" + "\"" + attribute.getValue());
			if(attribute.getJavaType()!=null)
			{
				attributesBuffer.append(":" +attribute.getJavaType());
			}
			attributesBuffer.append("\"");
			if(i<allAttributes.size()-1)
			{
				attributesBuffer.append(", ");
			}
		}
		String completeStatement = createStatement + attributesBuffer.toString() + "});";
		return completeStatement;
	}
	
	public void addAttribute(Attribute attribute)
	{
		attributes.getAttributes().add(attribute);
	}

	public void addMetadataAttribute(Attribute attribute)
	{
		metadataAttributes.getAttributes().add(attribute);
	}

	public boolean equals(Node node)
	{
		return this.getLabel().equals(node.getLabel());
	}

	public HashMap<Integer, Integer> getAttributesToCsvColumnMap()
	{
		return attributesToCsvColumnMap;
	}

	public void setAttributesToCsvColumnMap(HashMap<Integer, Integer> attributesToCsvColumnMap)
	{
		this.attributesToCsvColumnMap = attributesToCsvColumnMap;
	}

	public int getKeyAttributeIndex()
	{
		return keyAttributeIndex;
	}

	public void setKeyAttributeIndex(int keyAttributeIndex)
	{
		this.keyAttributeIndex = keyAttributeIndex;
	}
}
