package com.datamelt.neo4j.csv;

import java.util.HashMap;
import java.util.HashSet;

public class NodeToCsvMapper
{
	public static HashSet<Integer> mapAttributesToColumns(Node node, CsvHeader header, HashSet<Integer> requiredColumns)
	{
		HashMap<Integer,Integer> map = new HashMap<>(header.getPositions().size());
		for(int f=0;f<node.getAttributes().size();f++)
		{
			Attribute attribute = node.getAttributes().getAttribute(f);
			if(header.getPositions().containsKey(attribute.getValue()))
			{
				int position = header.getPositions().get(attribute.getValue());
   				map.put(f, position);
   				requiredColumns.add(position);
   				if(attribute.isIdField())
   				{
   					node.setKeyAttributeIndex(position);
   				}
			}
		}
		node.setAttributesToCsvColumnMap(map);
		return requiredColumns;
	}
	
	public static HashSet<Integer> mapAttributesToColumns(Relation relation, CsvHeader header, HashSet<Integer> requiredColumns)
	{
		HashMap<Integer,Integer> map = new HashMap<>(header.getPositions().size());
		for(int f=0;f<relation.getAttributes().size();f++)
		{
			Attribute attribute = relation.getAttributes().getAttribute(f);
			if(header.getPositions().containsKey(attribute.getValue()))
			{
				int position = header.getPositions().get(attribute.getValue());
   				map.put(f, position);
   				requiredColumns.add(position);
			}
		}
		relation.setAttributesToCsvColumnMap(map);
		return requiredColumns;
	}
}
