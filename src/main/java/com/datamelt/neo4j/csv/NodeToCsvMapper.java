package com.datamelt.neo4j.csv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.datamelt.util.MessageUtility;

public class NodeToCsvMapper
{
	public static HashMap<String,Integer> attributesToCsvPositions(NodesCollection nodes,RelationsCollection relations, CsvHeader header)
	{
		ArrayList<Attribute> attributes = nodes.getAllNodesAttributes();
		attributes.addAll(relations.getAllRelationsAttributes());
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
	    	System.out.println(MessageUtility.getFormattedMessage("column not found in CSV file header: " + missingFields.toString()));
	    }
	    return attributePositions;
	}
}
