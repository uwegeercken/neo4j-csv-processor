package com.datamelt.neo4j.csv;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

import com.datamelt.util.MessageUtility;

public class RelationFileCollection
{
	private ArrayList<RelationFile> relationFiles = new ArrayList<>();
	
	public RelationFileCollection(RelationsCollection relations)
	{
		collectRelationFiles(relations);
	}
	
	public RelationFile get(int index)
	{
		return relationFiles.get(index);
	}
	
	public RelationFile get(Node startNode,Node endNode,String relationType)
	{
		int found = -1;
		for(int i=0;i<relationFiles.size();i++)
		{
			RelationFile relationFile = relationFiles.get(i);
			if(relationFile.getStartNodeLabel().equals(startNode.getLabel()) && relationFile.getEndNodeLabel().equals(endNode.getLabel()) && relationFile.getRelationType().equals(relationType))
			{
				found = i;
				break;
			}
		}
		if(found>-1)
		{
			return relationFiles.get(found);
		}
		else
		{
			return null;
		}
	}
	
	private void collectRelationFiles(RelationsCollection relations)
	{
		for(int i=0;i<relations.size();i++)
		{
			Relation relation = relations.getRelation(i);
			RelationFile file = new RelationFile(relation.getStartNode().getLabel(),relation.getStartNode().getNamespace(),relation.getEndNode().getLabel(),relation.getEndNode().getNamespace(), relation.getRelationType(),relation.getAttributes().getAttributes(),relation.getMetadataAttributes().getAttributes());
			relationFiles.add(file);
		}
	}
	
	public int size()
	{
		return relationFiles.size();
	}
	
	public void writeRelationFiles(String outputFolder,String delimiter) throws Exception
	{
		File folder = new File(outputFolder);
	    folder.mkdirs();
	    
		for(int i=0;i<relationFiles.size();i++)
	    {
	    	RelationFile relationFile = relationFiles.get(i);
	    	String fileName = outputFolder + "/" + relationFile.getRelationFileName(); 
	    	System.out.println(MessageUtility.getFormattedMessage("writing file for relation: " + relationFile.getStartNodeLabel() + " to " + relationFile.getEndNodeLabel()));
	    	FileWriter fileWriter = new FileWriter(fileName);
	        PrintWriter printWriter = new PrintWriter(fileWriter);
	        printWriter.println(relationFile.getHeader(delimiter));

	        HashMap<String,RelationFileValue> values = relationFile.getValues();
	        for (RelationFileValue value : values.values()) 
	        {
	        	StringBuffer buffer = new StringBuffer();
	        	buffer.append(value.getStartNodeValue()).append(delimiter);
	        		
        		ArrayList<String> attributeValues = value.getAttributeValues();
    	        for (int k=0;k<attributeValues.size();k++) 
    	        {
    	        	String attributeValue = attributeValues.get(k).toString();
	            	buffer.append(attributeValue);
            		buffer.append(delimiter);
    	        }	
        	
    	        buffer.append(value.getEndNodeValue())
        			.append(delimiter)
        			.append(relationFile.getRelationType());
	        	printWriter.println(buffer.toString());
	        }
 	        printWriter.flush();
	        printWriter.close();
	        fileWriter.close();
	    }
	}
}
