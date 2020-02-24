package structures;

import java.io.Serializable;

import servers.Node;

public class MessageJoin extends Message implements Serializable {

	private Node newNode;

	public Node getNewNode() {
		return newNode;
	}

	public void setNewNode(Node newNode) {
		this.newNode = newNode;
	}

}
