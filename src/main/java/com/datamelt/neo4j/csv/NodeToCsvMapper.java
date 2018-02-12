package com.datamelt.neo4j.csv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class NodeToCsvMapper
{
	public static HashMap<String,Integer> nodeAttributesToCsvPositions(NodesCollection nodes,CsvHeader header)
	{
		ArrayList<Attribute> attributes = nodes.getNodeAttributes();
	    HashMap<String,Integer> attributePositions = new HashMap<>();
	    HashSet<String> missingFields = new HashSet<>();
	    for(int f=0;f<attributes.size();f++)
		{
			Attribute attribute = attributes.get(f);
			int position = header.getColumnNumber(attribute.getValue());
			if(position>-1)
			{
				attributePositions.put(attribute.getValue(), position);
			}
			else
			{
    			missingFields.add(attribute.getValue());
			}
		}
	    if(missingFields.size()>0)
	    {
	    	System.out.println("column not found in CSV file header: " + missingFields.toString());
	    }
	    return attributePositions;
	}
}
