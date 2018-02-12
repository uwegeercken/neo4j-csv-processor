package com.datamelt.neo4j.csv;

public class Relation
{
	private Node startNode;
	private Node endNode;
	private String relationType;
	
	public Relation(Node startNode, Node endNode, String relationType)
	{
		this.startNode = startNode;
		this.endNode = endNode;
		this.relationType = relationType;
	}
	
	public Node getStartNode()
	{
		return startNode;
	}
	public void setStartNode(Node startNode)
	{
		this.startNode = startNode;
	}
	public Node getEndNode()
	{
		return endNode;
	}
	public void setEndNode(Node endNode)
	{
		this.endNode = endNode;
	}
	public String getRelationType()
	{
		return relationType;
	}
	public void setRelationType(String relationType)
	{
		this.relationType = relationType;
	}

	public boolean equals(Relation relation)
	{
		if(this.relationType.equals(relation.relationType) && this.startNode.getLabel().equals(relation.startNode.getLabel()) && this.endNode.getLabel().equals(relation.endNode.getLabel()))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
}
