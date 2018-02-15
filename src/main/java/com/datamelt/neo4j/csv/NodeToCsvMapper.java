package com.datamelt.neo4j.csv;

import java.util.HashMap;

public class NodeToCsvMapper
{
	public static void mapColumnsToAttributes(Node node, CsvHeader header)
	{
		String idFieldKey = node.getIdFieldKey();
		
		HashMap<Integer,Integer> map = new HashMap<>(header.getPositions().size());
		for(int f=0;f<node.getAttributes().size();f++)
		{
			Attribute attribute = node.getAttributes().getAttribute(f);
			if(header.getPositions().containsKey(attribute.getValue()))
			{
				int position = header.getPositions().get(attribute.getValue());
   				map.put(f, position);
   				if(attribute.getValue().equals(idFieldKey))
   				{
   					node.setKeyAttributeIndex(position);
   				}
			}
		}
		node.setAttributesToCsvColumnMap(map);
	}
	
	public static void mapColumnsToAttributes(Relation relation, CsvHeader header)
	{
		HashMap<Integer,Integer> map = new HashMap<>(header.getPositions().size());
		for(int f=0;f<relation.getAttributes().size();f++)
		{
			Attribute attribute = relation.getAttributes().getAttribute(f);
			if(header.getPositions().containsKey(attribute.getValue()))
			{
				int position = header.getPositions().get(attribute.getValue());
   				map.put(f, position);
			}
		}
		relation.setAttributesToCsvColumnMap(map);
	}
}
