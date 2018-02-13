package com.datamelt.neo4j.csv;

import java.util.ArrayList;

public class NodesCollection
{
	ArrayList<Node> nodes = new ArrayList<>();
	
	public void addNode(Node node)
	{
		nodes.add(node);
	}

	public Node getNode(int index)
	{
		return nodes.get(index);
	}

	public ArrayList<Node> getNodes()
	{
		return nodes;
	}
	
	public void setNodes(ArrayList<Node> nodes)
	{
		this.nodes = nodes;
	}
	
	public int size()
	{
		return nodes.size();
	}
	
	public Node getNodeByLabel(String label)
	{
		int found=-1;
		for(int i=0;i<nodes.size();i++)
		{
			Node node = nodes.get(i);
			if(node.getLabel().equals(label))
			{
				found=i;
				break;
			}
		}
		if(found>-1)
		{
			return nodes.get(found);
		}
		else
		{
			return null;
		}
	}
	
	public ArrayList<String> getNodeLabels()
	{
		ArrayList<String> labels = new ArrayList<>();
		for(int i=0;i<nodes.size();i++)
		{
			Node node = nodes.get(i);
			labels.add(node.getLabel());
		}
		return labels;
	}
	
	public ArrayList<Attribute> getAllNodesAttributes()
	{
		ArrayList<Attribute> attributes = new ArrayList<>();
		for(int i=0;i<nodes.size();i++)
		{
			Node node = nodes.get(i);
			attributes.addAll(node.getAttributes().getAttributes());
		}
		return attributes;
	}
}
