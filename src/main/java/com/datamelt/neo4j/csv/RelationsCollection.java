package com.datamelt.neo4j.csv;

import java.util.ArrayList;

public class RelationsCollection
{
	ArrayList<Relation> relations = new ArrayList<>();
	
	public void addRelation(Relation relation)
	{
		relations.add(relation);
	}

	public Relation getRelation(int index)
	{
		return relations.get(index);
	}
	
	public ArrayList<Relation> getRelations(Node startNode)
	{
		ArrayList<Relation> startNodeRelations = new ArrayList<>();
		for(int i=0;i<relations.size();i++)
		{
			Relation relation = relations.get(i);
			if(relation.getStartNode().equals(startNode))
			{
				startNodeRelations.add(relation);
			}
		}
		return startNodeRelations;
	}

	public ArrayList<Node> getEndNode(Node startNode)
	{
		ArrayList<Node> endNodes = new ArrayList<>();
		for(int i=0;i<relations.size();i++)
		{
			Relation relation = relations.get(i);
			
			if(relation.getStartNode().equals(startNode))
			{
				endNodes.add(relation.getEndNode());
			}
		}
		return endNodes;
	}
	
	
	public ArrayList<Relation> getRelations()
	{
		return relations;
	}
	
	public void setRelations(ArrayList<Relation> relations)
	{
		this.relations = relations;
	}
	
	public int size()
	{
		return relations.size();
	}
	
}
