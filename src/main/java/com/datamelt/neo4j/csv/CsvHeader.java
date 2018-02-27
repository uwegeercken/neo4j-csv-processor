package com.datamelt.neo4j.csv;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class CsvHeader
{
	private String[] columns;
	private String delimiter;
	private HashMap<String,Integer> positions = new HashMap<>();
	
	public CsvHeader(String headerLine, String delimiter)
	{
		this.delimiter = delimiter;
		columns = headerLine.split(delimiter);
		for(int i=0;i<columns.length;i++)
		{
			positions.put(columns[i], i);
		}
	}
	
	public void removeColumns(HashSet<Integer> requiredColumns)
	{
		HashMap<String,Integer> requiredPositions = new HashMap<>();
		for(Map.Entry<String, Integer> entry : positions.entrySet())
		{
			int position = entry.getValue();
			if(!requiredColumns.contains(position))
			{
				requiredPositions.put(entry.getKey(), entry.getValue());
			}
		}
		positions = requiredPositions;
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

	public String getDelimiter()
	{
		return delimiter;
	}

	public HashMap<String, Integer> getPositions()
	{
		return positions;
	}
}
