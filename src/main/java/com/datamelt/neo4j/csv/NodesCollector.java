package com.datamelt.neo4j.csv;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import com.datamelt.util.FileUtility;
import com.datamelt.util.MessageUtility;
import com.datamelt.neo4j.csv.util.MetadataAttributeValue;

public class NodesCollector
{
	private static final String LABELS_LABEL 						= "labels";
	private static final String KEYS_LABEL 							= "keys";
	private static final String RELATIONS_LABEL						= "relation";
	private static final String NODE_VARIABLE 						= "node";
	private static final String RELATION_VARIABLE					= "relation";
	private static final String NODE_START	 						= "startnode";
	private static final String NODE_END 							= "endnode";
	
	public static final String DEFAULT_PROTOCOL 					= "bolt";
	private static final String DEFAULT_ORIGINAL_SCHEMA_FILENAME 	= "original_schema.cyp"; 
	
	private Session session;
	private NodesCollection nodes = new NodesCollection();
	private RelationsCollection relations = new RelationsCollection();
	private NodeFileCollection nodeFiles;
	private RelationFileCollection relationFiles;
	private String outputFolder;
	private String metaLabel;
	private CsvHeader header;
	HashMap<String,Integer> attributePositions;
	HashSet<Integer> requiredColumns = new HashSet<>();
	
	public NodesCollector(Session session, String outputFolder, String metaLabel,CsvHeader header)
	{
		this.outputFolder = outputFolder;
		this.metaLabel = metaLabel;
		this.header = header;
		
		this.session = session;

		collectNodes();
		
		//header.removeColumns(requiredColumns);
		
		for (int i=0;i<nodes.size();i++)
		{
			collectRelations(nodes.getNode(i));
		}
		session.close();
		
		nodeFiles = new NodeFileCollection(nodes);
		relationFiles = new RelationFileCollection(relations);
	}
	
	private void collectNodes()
	{
		final String STATEMENT_MATCH_NODES = "MATCH (a :"+ metaLabel + ") RETURN labels(a) as " + LABELS_LABEL + ", keys(a) as " + KEYS_LABEL;
		
		System.out.println(MessageUtility.getFormattedMessage("collecting information on nodes and attributes..."));
		StatementResult result = session.run(STATEMENT_MATCH_NODES);
		
		while (result.hasNext())
	    {
	        Record record = result.next();
	        List<Object> labels = record.get(LABELS_LABEL).asList();
	        String nodeLabel = null;
	        for(Object label : labels)
	        {
	        	if(!label.toString().equals(metaLabel))
	        	{
	        		nodeLabel = label.toString();
	        	}
	        }
	        
	        Node node = new Node();
	        node.setLabel(nodeLabel);
	        
	        List<Object> keys = record.get(KEYS_LABEL).asList();
	        for(Object key : keys)
	        {
	        	Attribute attribute= new Attribute();
	        	attribute.setKey(key.toString());
	        	boolean isMetadataAttribute = Attribute.isMetadataAttribute(key.toString());
	        	if(isMetadataAttribute)
	        	{
	        		node.addMetadataAttribute(attribute);
	        	}
	        	else
	        	{
	        		node.addAttribute(attribute);
	        	}
	        }
	        if(keys.size()>0)
	        {
	        	collectAttributeValues(node);
	        	requiredColumns = NodeToCsvMapper.mapAttributesToColumns(node,header,requiredColumns);
	        }
	        nodes.addNode(node);
	    }
	}
	
	public int getNumberOfNodes()
	{
		return nodes.size();
	}
	
	public int getNumberOfRelations()
	{
		return relations.size();
	}
	
	public void processKeys(ArrayList<String> columns, long counter)
	{
		for(int i=0;i<nodes.size();i++)
    	{
    		Node node = nodes.getNode(i);
    		NodeFile nodeFile = nodeFiles.getNodeFile(node.getLabel());
    		
    		HashMap<Integer, Integer> nodesMap = node.getAttributesToCsvColumnMap();
    		ArrayList<String> values = new ArrayList<>(nodesMap.size());
    		for(Map.Entry<Integer, Integer>entry : nodesMap.entrySet())
    		{
    			if(entry.getValue()== node.getKeyAttributeIndex())
    			{
    				String value = columns.get(entry.getValue());
    				values.add(value);
    			}
    		}
    		nodeFile.addKey(columns.get(node.getKeyAttributeIndex()));
   			
    	}
	}
	
	public void processLine(HashMap<Integer,String> columns, long counter)
	{
		for(int i=0;i<nodes.size();i++)
    	{
    		Node node = nodes.getNode(i);
    		NodeFile nodeFile = nodeFiles.getNodeFile(node.getLabel());
    		
    		HashMap<Integer, Integer> nodesMap = node.getAttributesToCsvColumnMap();
    		ArrayList<String> values = new ArrayList<>(nodesMap.size());
    		for(Map.Entry<Integer, Integer>entry : nodesMap.entrySet())
    		{
    			if(entry.getValue()!= node.getKeyAttributeIndex())
    			{
    				String value = columns.get(entry.getValue());
    				values.add(value);
    			}
    		}
   			nodeFile.addValue(columns.get(node.getKeyAttributeIndex()), values);
   			
    		ArrayList<Relation> startNodeRelations = relations.getRelations(node);
    		for(int k=0;k<startNodeRelations.size();k++)
    		{
    			Relation relation = startNodeRelations.get(k);
    			Node endNode = relation.getEndNode();
    			RelationFile relationFile = relationFiles.get(node, endNode, relation.getRelationType());
    			
    			HashMap<Integer, Integer> relationsMap = relation.getAttributesToCsvColumnMap();
    			ArrayList<String> relationValues = new ArrayList<>(relationsMap.size());
    			for(Map.Entry<Integer, Integer>entry : relationsMap.entrySet())
        		{
        			String value = columns.get(entry.getValue());
        			relationValues.add(value);
        		}
   			
   				relationFile.addValue(columns.get(node.getKeyAttributeIndex()), columns.get(endNode.getKeyAttributeIndex()),relationValues);

    		}
    	}
	}
	
	public void writeNodeFiles(String delimiter) throws Exception
	{
		nodeFiles.writeNodeFiles(outputFolder,delimiter);
	}
	
	public void writeRelationFiles(String delimiter) throws Exception
	{
		relationFiles.writeRelationFiles(outputFolder,delimiter);
	}

	public void writeSchemaAsCypherStatement() throws Exception
	{
		File folder = new File(outputFolder);
	    if(folder.exists() && folder.isDirectory())
	    {
	    	FileUtility.backupFile(outputFolder, DEFAULT_ORIGINAL_SCHEMA_FILENAME);	
	    }
	    else
	    {
	    	folder.mkdirs();
	    }
	    
		String fullFileName = outputFolder + "/" + DEFAULT_ORIGINAL_SCHEMA_FILENAME;
    	
    	System.out.println(MessageUtility.getFormattedMessage("writing file for original schema: " + DEFAULT_ORIGINAL_SCHEMA_FILENAME));
    	
    	FileWriter fileWriter = new FileWriter(fullFileName);
        PrintWriter printWriter = new PrintWriter(fileWriter);

		for(int i=0;i<nodes.size();i++)
		{
			Node node = nodes.getNode(i);
			printWriter.println(node.getNodeCreateStatement(NODE_VARIABLE, metaLabel));
		}
		
		for(int i=0;i<relations.size();i++)
		{
			Relation relation = relations.getRelation(i); 
			printWriter.println(relation.getRelationCreateStatement(NODE_START,NODE_END));
		}
		
        printWriter.flush();
        printWriter.close();
        fileWriter.close();
	}
	
	private void collectAttributeValues(Node node)
	{
		StringBuffer buffer = new StringBuffer();
    	for(int f=0;f<node.getAllAttributes().size();f++)
    	{
    		Attribute attribute = node.getAllAttributes().get(f);
    		buffer.append(NODE_VARIABLE + "." + attribute.getKey());
    		if(f<node.getAllAttributes().size()-1)
    		{
    			buffer.append(", ");
    		}
    	}
    	
    	String statement_node_attributes = "MATCH (" + NODE_VARIABLE + " :" + node.getLabel() + ") RETURN " + buffer.toString();
    	StatementResult result = session.run(statement_node_attributes);

	    while (result.hasNext())
	    {
	    	Record record = result.next();
	    	for(int j=0;j<node.getAllAttributes().size();j++)
	    	{
	    		Attribute attribute = node.getAllAttributes().get(j);
	    		String value = record.get(NODE_VARIABLE + "." + attribute.getKey()).asString();
	    		if(value!=null && !value.trim().equals(""))
	    		{
		    		String values[] = value.split(":");
		    		if(values.length>0)
		    		{
		    			for(int k=0;k<values.length;k++)
		    			{
		    				if(values[k].equals(MetadataAttributeValue.ID.key()))
		    				{
		    					attribute.setIsIdField(true);
		    				}
		    				else if(values[k].equals(MetadataAttributeValue.TYPE_INT.key())||values[k].equals(MetadataAttributeValue.TYPE_FLOAT.key()))
		    				{
		    					attribute.setJavaType(values[k]);
		    				}
		    				else
		    				{
		    					attribute.setValue(values[k]);
		    				}
		    			}
		    		}
	    		}
	    	}
		}
	}
	
	private void collectAttributeValues(Relation relation)
	{
		StringBuffer buffer = new StringBuffer();
    	for(int f=0;f<relation.getAllAttributes().size();f++)
    	{
    		Attribute attribute = relation.getAllAttributes().get(f);
    		buffer.append(RELATION_VARIABLE + "." + attribute.getKey());
    		if(f<relation.getAllAttributes().size()-1)
    		{
    			buffer.append(", ");
    		}
    	}
    	
    	String statement_relation_attributes = "";
    	if(relation.getAllAttributes().size()>0)
    	{
    		statement_relation_attributes= "MATCH (" + NODE_START + " :" + relation.getStartNode().getLabel() + ")-[" + RELATION_VARIABLE + "]-(" + NODE_END + " :" + relation.getEndNode().getLabel() + ") RETURN " + buffer.toString();
    		StatementResult result = session.run(statement_relation_attributes);
    		while (result.hasNext())
    	    {
    	    	Record record = result.next();
    	    	for(int j=0;j<relation.getAllAttributes().size();j++)
    	    	{
    	    		Attribute attribute = relation.getAllAttributes().get(j);
    	    		String value = record.get(RELATION_VARIABLE + "." + attribute.getKey()).asString();
    	    		if(value!=null && !value.trim().equals(""))
    	    		{
    		    		String values[] = value.split(":");
    		    		if(values.length>0)
    		    		{
    		    			for(int k=0;k<values.length;k++)
    		    			{
    		    				if(values[k].equals(MetadataAttributeValue.TYPE_INT.key())||values[k].equals(MetadataAttributeValue.TYPE_FLOAT.key()))
    		    				{
    		    					attribute.setJavaType(values[k]);
    		    				}
    		    				else
    		    				{
    		    					attribute.setValue(values[k]);
    		    				}
    		    			}
    		    		}
    	    		}
    	    	}
    		}
    	}
	}
	
	private void collectRelations(Node node)
	{
		for(int i=0;i<nodes.size();i++)
		{
			Node relatedNode = nodes.getNode(i);
			
			String statement_nodes_relations = "MATCH (" + NODE_START + " :" + node.getLabel() + ")-[relation]->(" + NODE_END + " :" + relatedNode.getLabel() + ") RETURN type(relation) as " + RELATIONS_LABEL + ", keys(relation) as " + KEYS_LABEL;			
			StatementResult result = session.run(statement_nodes_relations);
	
		    while (result.hasNext())
		    {
		    	Record record = result.next();
		    	
		    	String relationType = record.get(RELATIONS_LABEL).asString();
		    	Relation relation = new Relation(node,relatedNode,relationType);
		    	
		    	List<Object> keys = record.get(KEYS_LABEL).asList();
		        for(Object key : keys)
		        {
		        	Attribute attribute= new Attribute();
		        	attribute.setKey(key.toString());
		        	boolean isMetadataAttribute = Attribute.isMetadataAttribute(key.toString());
		        	if(isMetadataAttribute)
		        	{
		        		relation.addMetadataAttribute(attribute);
		        	}
		        	else
		        	{
		        		relation.addAttribute(attribute);
		        	}
		        }
		        
		        if(keys.size()>0)
		        {
		        	collectAttributeValues(relation);
		        	requiredColumns = NodeToCsvMapper.mapAttributesToColumns(relation,header,requiredColumns);
		        }
		    	
		    	relations.addRelation(relation);
		    	
		    	
			}
		}
	}

	public HashSet<Integer> getRequiredColumns()
	{
		return requiredColumns;
	}

	public NodesCollection getNodes()
	{
		return nodes;
	}

	public RelationsCollection getRelations()
	{
		return relations;
	}
}
