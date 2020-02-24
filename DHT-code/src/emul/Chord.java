package emul;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;

import servers.Node;
import tools.MyNodeComparator;

public class Chord {

	private ArrayList<Node> chordList; /* we prefer ArrayList instead of LinkedList because of fast random read access */
	private Node bootstrapped = null; /*We keep a separate reference to bootstrapped node for easy access.*/

	/* Constructor */
	public Chord() {
		chordList = new ArrayList<Node>();
	}

	public Node createNode(int serialId, boolean linear, int repNumber) throws NoSuchAlgorithmException {
		Node node = new Node(serialId, linear, repNumber);
		chordList.add(node);
		node.initiate();
		node.start();

		//if first node created in our emulator, consider it bootstrapped
		if (bootstrapped == null) {
			bootstrapped = node;
		}

		return node;
	}

	public void setNeighbors() {
		Collections.sort(chordList, new MyNodeComparator());
		// set neighbors for first and last node

		chordList.get(chordList.size() - 1).setNext(chordList.get(0));

		for (Node curr : chordList) {
			if (curr.equals(chordList.get(0))) {
				// first node
				curr.setPrevious((chordList.get(chordList.size() - 1)));
				curr.setNext(chordList.get(chordList.indexOf(curr) + 1));
			} else if (curr.equals(chordList.get(chordList.size() - 1))) {
				// last node
				curr.setPrevious(chordList.get(chordList.indexOf(curr) - 1));
				curr.setNext(chordList.get(0));
			} else {
				// other nodes
				curr.setPrevious((chordList.get(chordList.indexOf(curr) - 1)));
				curr.setNext(chordList.get(chordList.indexOf(curr) + 1));
			}
		}
	}

	public Node getNode(int i) {
		return chordList.get(i);
	}

	public void terminateSimulation() {
		System.out.println("Emulator shutting down...");
		for (Node e : chordList) {
			System.out.println("Node" + e.getSerialId() + " shutting down.");
			e.terminate();
			e.interrupt();
//			try {
//				e.join();
//			} catch (InterruptedException e1) {
//				e1.printStackTrace();
//			}
		}
		System.out.println("Simulation completed. Goodbye!");
		System.exit(1);
	}

	public Node getBootstrapped() {
		return bootstrapped;
	}

	public void setBootstrapped(Node bootstrapped) {
		this.bootstrapped = bootstrapped;
	}
	
	public ArrayList<Node> getChordList() {
		return chordList;
	}

	public void setChordList(ArrayList<Node> chordList) {
		this.chordList = chordList;
	}
}
