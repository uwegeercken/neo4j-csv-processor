package com.datamelt.neo4j.csv;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;

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
			RelationFile file = new RelationFile(relation.getStartNode().getLabel(),relation.getEndNode().getLabel(),relation.getRelationType());
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
	    	String fileName = outputFolder + "/" + relationFile.getNodeFileName(); 
	    	System.out.println(MessageUtility.getFormattedMessage("writing file for relation: " + relationFile.getStartNodeLabel() + " to " + relationFile.getEndNodeLabel()));
	    	FileWriter fileWriter = new FileWriter(fileName);
	        PrintWriter printWriter = new PrintWriter(fileWriter);
	        printWriter.println(relationFile.getHeader(delimiter));

	        ArrayList<RelationFileValue> values = relationFile.getValues();
	        for (int j=0;j<values.size();j++) 
	        {
	        	StringBuffer buffer = new StringBuffer();
	        	RelationFileValue value = values.get(j);
	        	buffer.append(value.getStartNodeValue())
	        		.append(delimiter)
	        		.append(value.getEndNodeValue())
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
