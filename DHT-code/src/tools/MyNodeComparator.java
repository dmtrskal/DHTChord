package tools;

import java.util.Comparator;

import servers.Node;

public class MyNodeComparator implements Comparator<Node>{
 
	 @Override
	 public int compare(Node a, Node b) {
		 return a.getNodeId().compareToIgnoreCase(b.getNodeId());
	 }

}