package com.datamelt.neo4j.csv;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import com.datamelt.util.FileUtility;
import com.datamelt.util.MessageUtility;

public class NodeFileCollection
{
	private ArrayList<NodeFile> nodeFiles = new ArrayList<>();
	
	public NodeFileCollection(NodesCollection nodes)
	{
		collectNodeFiles(nodes);
	}
	
	public NodeFile get(int index)
	{
		return nodeFiles.get(index);
	}
	
	private void collectNodeFiles(NodesCollection nodes)
	{
		for(int i=0;i<nodes.size();i++)
		{
			Node node = nodes.getNode(i);
			String idFieldName = node.getIdFieldKey();
			Attribute attribute = node.getAttributes().getAttributeByValue(idFieldName);

			NodeFile file = new NodeFile(node.getLabel(),attribute.getKey(),node.getAttributes().getAttributes(),node.getMetadataAttributes().getAttributes());
			nodeFiles.add(file);
		}
	}
	
	public NodeFile getNodeFile(String nodeLabel)
	{
		int found = -1;
		for(int i=0;i<nodeFiles.size();i++)
		{
			NodeFile file = nodeFiles.get(i);
			if(file.getNodeLabel().equals(nodeLabel))
			{
				found = i;
				break;
			}
		}
		return nodeFiles.get(found);
	}
	
	public int size()
	{
		return nodeFiles.size();
	}
	
	public void writeNodeFiles(String outputFolder,String delimiter) throws Exception
	{
		File folder = new File(outputFolder);
	    folder.mkdirs();
	    
		for(int i=0;i<nodeFiles.size();i++)
	    {
	    	NodeFile nodeFile = nodeFiles.get(i);
	    	String fileName = outputFolder + "/" + nodeFile.getNodeFileName(); 
	    	System.out.println(MessageUtility.getFormattedMessage("writing file for node: " + nodeFile.getNodeLabel()));
	    	FileWriter fileWriter = new FileWriter(fileName);
	        PrintWriter printWriter = new PrintWriter(fileWriter);
	        printWriter.println(nodeFile.getHeader(delimiter));

	        HashMap<String, ArrayList<Object>> map = nodeFile.getValues();
	        for (Entry<String, ArrayList<Object>> entry : map.entrySet()) 
	        {
	        	StringBuffer buffer = new StringBuffer();
	        	String key = entry.getKey();
	        	ArrayList<Object> value = entry.getValue();
	            buffer.append(key).append(delimiter);
	            for(int j=0;j<value.size();j++)
	            {
	            	buffer.append(value.get(j));
            		buffer.append(delimiter);
	            }
	        	printWriter.println(buffer.toString() + nodeFile.getNodeLabel());
	        }
 
	        printWriter.flush();
	        printWriter.close();
	        fileWriter.close();
	    }
	}
	
	public static void writeOriginalSchemaFile(String outputFolder, String fileName, ArrayList<String> createNodesStatements, ArrayList<String> createRelationsStatements) throws Exception
	{
		File folder = new File(outputFolder);
	    folder.mkdirs();
	    
    	String fullFileName = outputFolder + "/" + fileName; 
    	System.out.println(MessageUtility.getFormattedMessage("writing file for original schema: " + fileName));
    	
    	FileUtility.backupFile(outputFolder, fileName);
    	
    	FileWriter fileWriter = new FileWriter(fullFileName);
        PrintWriter printWriter = new PrintWriter(fileWriter);
        for(int i=0;i<createNodesStatements.size();i++)
        {
        	printWriter.println(createNodesStatements.get(i));
        }
        for(int i=0;i<createRelationsStatements.size();i++)
        {
        	printWriter.println(createRelationsStatements.get(i));
        }

        printWriter.flush();
        printWriter.close();
        fileWriter.close();
	}
}
