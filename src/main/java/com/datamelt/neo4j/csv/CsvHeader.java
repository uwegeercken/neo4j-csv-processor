package com.datamelt.neo4j.csv;

import java.util.HashMap;

public class CsvHeader
{
	private String[] columns;
	private HashMap<String,Integer> positions = new HashMap<>();
	
	
	public CsvHeader(String headerLine,String delimiter)
	{
		columns = headerLine.split(delimiter);
		for(int i=0;i<columns.length;i++)
		{
			positions.put(columns[i], i);
		}
	}
	
	public String[] getColumns()
	{
		return columns;
	}
	
	public int getColumnNumber(String columnName)
	{
		int position = -1;
		if(positions.containsKey(columnName))
		{
			position = positions.get(columnName);
		}
		return position;
	}
	
	
}
