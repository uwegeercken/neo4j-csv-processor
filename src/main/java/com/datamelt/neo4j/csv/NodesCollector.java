package com.datamelt.neo4j.csv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import com.datamelt.util.MessageUtility;
import com.datamelt.neo4j.csv.util.MetadataAttribute;

public class NodesCollector
{
	private static final String DEFINITION_LABEL 					= "csvdef";
	private static final String LABELS_LABEL 						= "labels";
	private static final String KEYS_LABEL 							= "keys";
	private static final String RELATIONS_LABEL						= "relation";
	private static final String NODE_VARIABLE 						= "node";
	private static final String RELATION_VARIABLE					= "relation";
	private static final String NODE_START	 						= "startnode";
	private static final String NODE_END 							= "endnode";
	
	public static final String DEFAULT_PROTOCOL 					= "bolt";
	private static final String DEFAULT_ORIGINAL_SCHEMA_FILENAME 	= "original_schema.cyp"; 
	
	private static final String STATEMENT_DEFINITION_NODES = "MATCH (a :"+ DEFINITION_LABEL + ") RETURN labels(a) as " + LABELS_LABEL + ", keys(a) as " + KEYS_LABEL;
	
	private Session session;
	private NodesCollection nodes = new NodesCollection();
	private RelationsCollection relations = new RelationsCollection();
	private NodeFileCollection nodeFiles;
	private RelationFileCollection relationFiles;
	private String outputFolder;
	CsvHeader header;
	HashMap<String,Integer> attributePositions;
	
	public NodesCollector(String hostname, String username, String password, String outputFolder)
	{
		this.outputFolder = outputFolder;
		
		Driver driver = GraphDatabase.driver(DEFAULT_PROTOCOL +"://" + hostname, AuthTokens.basic(username,password));
		this.session = driver.session();

		collectNodes();
		
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
		System.out.println(MessageUtility.getFormattedMessage("collecting information on nodes and attributes..."));
		StatementResult result = session.run(STATEMENT_DEFINITION_NODES);
		
		while (result.hasNext())
	    {
	        Record record = result.next();
	        List<Object> labels = record.get(LABELS_LABEL).asList();
	        String nodeLabel = null;
	        for(Object label : labels)
	        {
	        	if(!label.toString().equals(DEFINITION_LABEL))
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
	
	public void setCsvHeader(String line, String delimiter)
	{
		header = new CsvHeader(line, delimiter);
		mapNodeAttributes();
	}
	
	private void mapNodeAttributes() 
	{
		attributePositions = NodeToCsvMapper.attributesToCsvPositions(nodes, relations, header);
	}
	
	public void processLine(String line, long counter,String delimiter)
	{
	    String[] columns = line.split(delimiter);
	    int numberOfColumns = columns.length;
	    HashSet<String> missingFields = new HashSet<>();
		for(int i=0;i<nodes.size();i++)
    	{
    		Node node = nodes.getNode(i);
    		NodeFile nodeFile = nodeFiles.getNodeFile(node.getLabel());
    		ArrayList<Object> values = new ArrayList<>();
    		Attribute idAttribute = node.getMetadataAttributes().getAttributeByKey(MetadataAttribute.ID_FIELD.key());
    		String keyValue = null;
    		for(int f=0;f<node.getAttributes().size();f++)
    		{
    			Attribute attribute = node.getAttributes().getAttribute(f);
				if(attributePositions.containsKey(attribute.getValue()))
				{
    				int position = attributePositions.get(attribute.getValue());
	    			if(position < numberOfColumns)
	    			{
	    				String value = columns[position];
		    			if(!attribute.getValue().equals(idAttribute.getValue()))
		    			{
		    				values.add(value);
		    			}
		    			else
		    			{
		    				keyValue = value;
		    			}
	    			}
	    			else
	    			{
	    				missingFields.add(attribute.getValue());
	    			}
				}
    		}
    		if(keyValue!=null)
    		{
    			nodeFile.addValue(keyValue, values);
    		}
    		
    		ArrayList<Relation> startNodeRelations = relations.getRelations(node);
    		for(int k=0;k<startNodeRelations.size();k++)
    		{
    			Relation relation = startNodeRelations.get(k);
    			Node endNode = relation.getEndNode();
    			RelationFile relationFile = relationFiles.get(node, endNode, relation.getRelationType());
    			
    			String endNodeIdFieldKey = endNode.getIdFieldKey();
    			String endNodeIdFieldValue = null;
    			if(attributePositions.containsKey(endNodeIdFieldKey))
				{
    				int position = attributePositions.get(endNodeIdFieldKey);
	    			if(position < numberOfColumns)
	    			{
	    				endNodeIdFieldValue = columns[position];
	    			}
	    		}
    			else
    			{
    				missingFields.add(endNodeIdFieldKey);
    			}
    			
    			ArrayList<Object> relationValues = new ArrayList<>();
    			for(int f=0;f<relation.getAttributes().size();f++)
        		{
        			Attribute attribute = relation.getAttributes().getAttribute(f);
    				if(attributePositions.containsKey(attribute.getValue()))
    				{
        				int position = attributePositions.get(attribute.getValue());
    	    			if(position < numberOfColumns)
    	    			{
    	    				String value = columns[position];
    	    				relationValues.add(value);
    	    			}
    	    			else
    	    			{
    	    				missingFields.add(attribute.getValue());
    	    			}
    				}
        		}
    			if(endNodeIdFieldValue!=null)
        		{
    				relationFile.addValue(keyValue, endNodeIdFieldValue,relationValues);
        		}
    		}
    	}
		if(missingFields.size()>0)
		{
			System.out.println(MessageUtility.getFormattedMessage("line: [" + counter + "]: columns not found: " + missingFields.toString()));
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
		ArrayList<String> createNodesStatements = new ArrayList<>();
		ArrayList<String> createRelationsStatements = new ArrayList<>();
		
		for(int i=0;i<nodes.size();i++)
		{
			Node node = nodes.getNode(i);
			createNodesStatements.add(node.getNodeCreateStatement(NODE_VARIABLE, DEFINITION_LABEL));
		}
		
		for(int i=0;i<relations.size();i++)
		{
			Relation relation = relations.getRelation(i); 
			createRelationsStatements.add(relation.getRelationCreateStatement(NODE_START,NODE_END));
		}
		
		NodeFileCollection.writeOriginalSchemaFile(outputFolder, DEFAULT_ORIGINAL_SCHEMA_FILENAME, createNodesStatements, createRelationsStatements);

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
	    		String values[] = value.split(":");
	    		attribute.setValue(values[0]);
	    		if(values.length>1)
	    		{
	    			attribute.setJavaType(values[1]);
	    		}
	    		if(attribute.getKey().equals(MetadataAttribute.ID_FIELD.key()))
	    		{
	    			attribute.setIsIdField(true);
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
    	    		String values[] = value.split(":");
    	    		attribute.setValue(values[0]);
    	    		if(values.length>1)
    	    		{
    	    			attribute.setJavaType(values[1]);
    	    		}
    	    		attribute.setValue(value);
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
		        }
		    	
		    	relations.addRelation(relation);
		    	
		    	
			}
		}
	}
}
