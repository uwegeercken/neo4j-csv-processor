package com.datamelt.neo4j.csv;

import java.util.ArrayList;

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
	
	private ArrayList<RelationFileValue> values = new ArrayList<>();
	
	public RelationFile(String startNodeLabel, String endNodeLabel, String relationType)
	{
		this.startNodeLabel = startNodeLabel;
		this.endNodeLabel = endNodeLabel;
		this.relationType = relationType;
	}
	
	public void addValue(String startNodeValue, String endNodeValue)
	{
		RelationFileValue fileValue = new RelationFileValue(startNodeValue, endNodeValue);
		// only add value for the relation if it does not exist already
		if(!values.contains(fileValue))
		{
			values.add(fileValue);
		}
	}

	public String getHeader(String delimiter)
	{
		return START_ID +delimiter + END_ID + delimiter + TYPE;
	}

	public String getNodeFileName()
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

	public ArrayList<RelationFileValue> getValues()
	{
		return values;
	}
}
