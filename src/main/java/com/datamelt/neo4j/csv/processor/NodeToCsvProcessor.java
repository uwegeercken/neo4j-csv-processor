package com.datamelt.neo4j.csv.processor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.datamelt.neo4j.csv.NodesCollector;
import com.datamelt.util.MessageUtility;

public class NodeToCsvProcessor
{
	public static void main(String[] args) throws Exception
	{
		String hostname = "localhost";
		String username = "neo4j";
		String password = "fasthans";
		String outputFolder = "/opt/neo4j-community-3.3.2/import/epx";
	    String csvFilename = "/opt/neo4j-community-3.3.2/import/epx/epx_201801_neo4j.csv";
	    String delimiter = ";";
		
		System.out.println(MessageUtility.getFormattedMessage("start of processing..."));
		System.out.println(MessageUtility.getFormattedMessage("writing CSV files to folder: " + outputFolder));

		NodesCollector collector = new NodesCollector(hostname,username,password, outputFolder);
		collector.writeSchemaAsCypherStatement();
		
		System.out.println(MessageUtility.getFormattedMessage("number of nodes found: " + collector.getNumberOfNodes()));
		
		String line;		
	    try(BufferedReader br = new BufferedReader(new FileReader(csvFilename)))
	    {
	    	long lineCounter = 0;
	    	
	    	String headerLine = br.readLine();
		    if(headerLine!=null)
		    {
		    	lineCounter++;
		    	collector.setCsvHeader(headerLine, delimiter);
		    }
		
		    System.out.println(MessageUtility.getFormattedMessage("processing CSV file: " + csvFilename));
	    	while ((line = br.readLine()) != null && !line.trim().equals("") && !line.startsWith("#")) 
	        {
	    		lineCounter++;
	    		collector.processLine(line,lineCounter,delimiter);
	        }
	    }
	    catch (IOException e)
	    {
            e.printStackTrace();
        }
	    
	    System.out.println(MessageUtility.getFormattedMessage("writing CSV files for nodes..."));
	    collector.writeNodeFiles(delimiter);
	    
	    System.out.println(MessageUtility.getFormattedMessage("writing CSV files for relations..."));
	    collector.writeRelationFiles(delimiter);
	    
	    System.out.println(MessageUtility.getFormattedMessage("processing complete."));
	    System.out.println();
	}
}
