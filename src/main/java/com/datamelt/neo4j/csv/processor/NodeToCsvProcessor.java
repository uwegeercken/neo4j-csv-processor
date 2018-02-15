/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datamelt.neo4j.csv.processor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import com.datamelt.neo4j.csv.CsvHeader;
import com.datamelt.neo4j.csv.NodesCollector;
import com.datamelt.util.MessageUtility;

public class NodeToCsvProcessor
{
	// the version of the business rule engine
	private static final String VERSION = "0.2";
	private static final String REVISION = "1";
	private static final String LAST_UPDATE = "2018-02-13";
	
	private static String hostname;
	private static String username;
	private static String password;
	private static String outputFolder;
	private static String csvFilename;
	private static String delimiter = ";";
    
	public static void main(String[] args) throws Exception
	{
		if (args.length==0 || args.length<5 || args.length>6)
        {
        	help();
        }
        else
        {
        	for(int i=0;i<args.length;i++)
	    	{
	    		if (args[i].startsWith("-h="))
	    		{
	    			hostname = args[i].substring(3);
	    		}
	    		else if (args[i].startsWith("-u="))
	    		{
	    			username = args[i].substring(3);
	    		}
	    		else if (args[i].startsWith("-p="))
	    		{
	    			password = args[i].substring(3);
	    		}
	    		else if (args[i].startsWith("-o="))
	    		{
	    			outputFolder = args[i].substring(3);
	    		}
	    		else if (args[i].startsWith("-c="))
	    		{
	    			csvFilename = args[i].substring(3);
	    		}
	    		else if (args[i].startsWith("-d="))
	    		{
	    			delimiter = args[i].substring(3);
	    		}
	    	}
        	
        	boolean error= false;
        	
    		if(hostname==null || hostname.trim().equals(""))
	    	{
	    		System.out.println(" error - hostname must be specified");
	    		error = true;
	    	}
    		if(username==null || username.trim().equals(""))
	    	{
    			System.out.println(" error - username must be specified");
    			error = true;
	    	}
    		if(password==null || password.trim().equals(""))
	    	{
    			System.out.println(" error - password must be specified");
    			error = true;
	    	}
    		if(outputFolder==null || outputFolder.trim().equals(""))
	    	{
    			System.out.println(" error - name of the output folder must be specified");
    			error = true;
	    	}
    		if(csvFilename==null || csvFilename.trim().equals(""))
	    	{
    			System.out.println(" error - name of the CSV file (including path) must be specified");
    			error = true;
	    	}
    		else
    		{
    			boolean csvFileOk = checkCsvFileAccessible(csvFilename);
    			if(!csvFileOk)
    			{
    				System.out.println(" error - CSV file is not existing or can not be read");
    				error = true;
    			}
    		}
    		if(!checkDatabaseAccess())
    		{
    			System.out.println(" error - can not access neo4j host. check if the host is available and credentials are correct.");
    			error = true;
    		}
    		
    		if(!error)
    		{
		    	Calendar start = Calendar.getInstance();
	
	        	System.out.println(MessageUtility.getFormattedMessage("start of processing..."));
	    		System.out.println(MessageUtility.getFormattedMessage("writing CSV files to folder: " + outputFolder));
	
	    		String line;		
	    	    File csvFile = new File(csvFilename);
	    		try(BufferedReader br = new BufferedReader(new FileReader(csvFile)))
	    	    {
	    	    	long lineCounter = 0;
	    	    	
	    	    	String headerLine = br.readLine();
	    	    	CsvHeader header = null;
	    		    if(headerLine!=null)
	    		    {
	    		    	lineCounter++;
	    		    	header = new CsvHeader(headerLine, delimiter);
	    		    }
	    		
		    		NodesCollector collector = new NodesCollector(hostname,username,password, outputFolder,header);
		    		collector.writeSchemaAsCypherStatement();
		    		
		    		System.out.println(MessageUtility.getFormattedMessage("number of nodes found: " + collector.getNumberOfNodes()));
		    		System.out.println(MessageUtility.getFormattedMessage("number of relations found: " + collector.getNumberOfRelations()));
		    		
	    		    System.out.println(MessageUtility.getFormattedMessage("processing CSV file: " + csvFilename));
	    		    long filesize =csvFile.length();
	    		    double bytesProcessed=headerLine.length();
	    		    double percentage = 0;
	    		    int percentageIncrement= 5;
	    		    long percentageLimit=percentageIncrement;
	    		    while ((line = br.readLine()) != null && !line.trim().equals("") && !line.startsWith("#")) 
	    	        {
	    		    	lineCounter++;
	    		    	bytesProcessed = bytesProcessed + line.length();
	    		    	percentage = bytesProcessed/filesize * 100;
	    		    	if(percentage>= percentageLimit)
	    		    	{
	    		    		percentageLimit = percentageLimit + percentageIncrement;
	    		    		System.out.println(MessageUtility.getFormattedMessage("percent complete: " + (long)percentage) + " - rows: " + lineCounter);
	    		    	}
	    		    	int startPosition=0;
	    		    	int position=-1;
	    		    	ArrayList<String> columns = new ArrayList<>();
	    		    	do
	    		    	{
	    		    		position = line.indexOf(delimiter,startPosition);
	    		    		if(position>-1)
	    		    		{
	    		    			columns.add(line.substring(startPosition, position));
	    		    			startPosition = position+1;
	    		    		}
	    		    		else
	    		    		{
	    		    			if(line.substring(startPosition)!=null)
	    		    			{
	    		    				columns.add(line.substring(startPosition));
	    		    			}
	    		    		}
	    		    	} while (position>=0);
	    		    	
	    		    	collector.processLine(columns,lineCounter);
	    	        }
	    		    System.out.println(MessageUtility.getFormattedMessage("processed rows: " + lineCounter));
	    		    
	    		    System.out.println(MessageUtility.getFormattedMessage("writing CSV files for nodes..."));
		    	    collector.writeNodeFiles(delimiter);
		    	    
		    	    System.out.println(MessageUtility.getFormattedMessage("writing CSV files for relations..."));
		    	    collector.writeRelationFiles(delimiter);
	    	    }
	    	    catch (IOException e)
	    	    {
	                e.printStackTrace();
	            }
	    	    
	    	    Calendar end = Calendar.getInstance();
	            long elapsed = end.getTimeInMillis() - start.getTimeInMillis();
	            long elapsedSeconds = (elapsed/1000);
	
	    	    System.out.println(MessageUtility.getFormattedMessage("elapsed time: " + elapsedSeconds + " second(s)"));
	    	    System.out.println(MessageUtility.getFormattedMessage("end of process."));
	    	    System.out.println();
    		}
        }
	}
	
	private static boolean checkDatabaseAccess()
	{
		boolean accessOk = false;
		try
		{
			Driver driver = GraphDatabase.driver(NodesCollector.DEFAULT_PROTOCOL +"://" + hostname, AuthTokens.basic(username,password));
			Session session = driver.session();
			session.close();
			accessOk = true;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return accessOk;

	}
	
	private static boolean checkCsvFileAccessible(String filename)
	{
		boolean fileOk = false;
		File csvFile = new File(filename);
		if(csvFile.exists() && csvFile.isFile() && csvFile.canRead())
		{
			fileOk = true;
		}	
		return fileOk;
	}
	
	/** 
     * returns the version and revision of the program
     * 
     * @return		the version and revision of the program
     */
    public static String getVersion()
    {
    	return VERSION + "-R" + REVISION;
    }
    
    /** 
     * returns the last date of an update of the program
     * 
     * @return		the last updated date
     */
    public static String getLastUpdateDate()
    {
    	return LAST_UPDATE;
    }
    
	public static void help()
    {
    	System.out.println("NodeToCsvProcessor. program to process a CSV file using a given schema with metadata.");
    	System.out.println();
    	System.out.println("For further functionality consult the API documentation and the handbook.");
    	System.out.println();
    	System.out.println("NodeToCsvProcessor -h=[hostname]|-u=[username] -p=[password] -o= [outputfolder] -c=[csv file name] -d=[delimiter]");
    	System.out.println("where [hostname]     : required. hostname or IP of the neo4j server.");
    	System.out.println("      [username]     : required. the name of the neo4j user.");
    	System.out.println("      [password]     : required. password of the neo4j user.");
    	System.out.println("      [outputfolder] : required. path of the folder where to generate the files.");
    	System.out.println("      [csv file name]: required. name of the csv file - containing the data - to use");
    	System.out.println("      [delimiter]    : optional. default:\";\".field delimiter to use for the generates files");
    	System.out.println();
    	System.out.println("example: NodeToCsvProcessor -h=localhost -u=user1 -p=mypassword -o=/home/user1 -c=/home/user1/testfile.csv -d=;");
    	System.out.println();
    	System.out.println("published as open source under the Apache License. read the licence notice");
    	System.out.println("all code by uwe geercken, 2018. uwe.geercken@web.de");
    	System.out.println();
    }
}
