package servers;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import structures.Data;
import structures.Message;
import structures.MessageType;
import tools.SHA1Hash;

public class Node extends Thread implements Serializable {
	private static int defaultPort = 64000;

	private int serialId; /* serial id of node */
	private String nodeId; /* hash id of node */
	private int port; /*port that server listens*/

	private Node previous;
	private Node next;

	private int clientPort; /*port to reply to client*/

	private Hashtable<String, String> hashTable;
	private Hashtable<String, String> replHashTable;
	private Listener listener;

	private boolean running;
	
	//for Linearizability. (using chained replication)
	private int replFactor;
	private boolean linear = false;
	private Lock lock;

	public Node(int serialId, boolean isLinear, int replNumber) throws NoSuchAlgorithmException {
		this.serialId = serialId;
		nodeId = SHA1Hash.hash(Integer.toString(serialId));
		port = defaultPort + serialId;
		listener = new Listener(port, this);
		hashTable = new Hashtable<>();
		replHashTable = new Hashtable<>();
		
		linear = isLinear;
		lock = new ReentrantLock();
		replFactor = replNumber;
	}

	public boolean insert(String notHashedKey, String value) {
		try {
			String hashedKey = SHA1Hash.hash(notHashedKey);
			
			//update or insert the key to the hash.
			hashTable.put(hashedKey, value);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean insertR(String notHashedKey, String value) {
		try {
			String hashedKey = SHA1Hash.hash(notHashedKey);
			
			//update or insert the to the hash.
			replHashTable.put(hashedKey, value);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean delete(String notHashedkey) throws NoSuchAlgorithmException {
		String hashedKey = SHA1Hash.hash(notHashedkey);
		
		if (hashTable.remove(hashedKey) != null) {
			//entry removed
			return true;
		} else {
			//no such entry
			return false;
		}
	}

	public boolean deleteR(String notHashedkey) throws NoSuchAlgorithmException {
		String hashedKey = SHA1Hash.hash(notHashedkey);
		
		if (replHashTable.remove(hashedKey) != null) {
			//entry removed
			return true;
		} else {
			//no such entry
			return false;
		}
	}

	public String query(String key) {
		//System.out.println("[NODE" + serialId + "] quering.");
		String res = hashTable.get(key);
		//System.out.println("[NODE" + serialId + "] queried " + res);
		return res;
	}

	public String queryR(String key) {
		//System.out.println("[NODE" + serialId + "] quering.");
		String res = replHashTable.get(key);
		//System.out.println("[NODE" + serialId + "] queried " + res);
		return res;
	}

	public String specialQuery() {
		String returnString = "// Node " + this.getSerialId() + ": " + hashTable.toString();
		returnString += " /Replicas: " + replHashTable.toString();
		return returnString;
	}

	public boolean joinNode(Node newNode) {
		//System.out.println("[NODE" + serialId + "] in join.");
		if (newNode == null) {
			return false;
		}
		newNode.setNext(this);
		newNode.setPrevious(previous);

		previous.setNext(newNode);

		previous = newNode;

		Set<String> keySet = hashTable.keySet();
		ArrayList<Message> mesList = new ArrayList<>();

		//Start new thread
		newNode.initiate();
		newNode.start();

		for (String string : keySet) {
			if (string.compareTo(newNode.getNodeId()) <= 0) {
				Message mes = new Message();
				mes.setFrom(port);
				mes.setTo(newNode.getPort());
				mes.setReplyTo(-1);
				mes.setType(MessageType.INSERT);
				mes.setData(new Data(string, hashTable.get(string)));

				mesList.add(mes);

				hashTable.remove(string);
			}
		}
		boolean res = false;
		try {
			res = sendList(mesList, newNode.getPort());
			//System.out.println("joinNode returning " + res);
			return res;
		} catch (IOException e) {
			System.out.println("joinNode returning false!!!!!");
			return false;
		}
	}

	public boolean joinR(int ttl, Node newNode) {

		Set<String> keySet = hashTable.keySet();
		ArrayList<Message> mesList = new ArrayList<>();

		for (String string : keySet) {
			if (string.compareTo(newNode.getNodeId()) <= 0) {
				Message mes = new Message();
				mes.setFrom(port);
				mes.setTo(newNode.getPort());
				mes.setReplyTo(-1);
				mes.setType(MessageType.INSERTR);
				mes.setData(new Data(string, hashTable.get(string)));

				mesList.add(mes);

				hashTable.remove(string);
			}
		}
		boolean res = false;
		try {
			res = sendList(mesList, newNode.getPort());
			//System.out.println("joinNode returning " + res);
			return res;
		} catch (IOException e) {
			//System.out.println("joinNode returning false!!!!!");
			return false;
		}
	}

	/**
	 * This method sends hashTable to next node and updates the next and previous fields of it's
	 * neighbors.
	 *
	 * @return true if sendList() succeeded, else false.
	 */
	public boolean depart() {
		previous.setNext(next);
		next.setPrevious(previous);

		Set<String> keySet = hashTable.keySet();
		ArrayList<Message> mesList = new ArrayList<>();
		for (String string : keySet) {
			Message mes = new Message();
			mes.setFrom(port);
			mes.setTo(next.getPort());
			mes.setReplyTo(-1);
			mes.setType(MessageType.INSERT);
			mes.setData(new Data(string, hashTable.get(string)));

			mesList.add(mes);
		}

		boolean res = false;
		try {
			res = sendList(mesList, next.getPort());
			return res;
		} catch (IOException e) {
			return false;
		}
	}

	public boolean departR(int ttl) {
		Set<String> keySet = replHashTable.keySet();
		ArrayList<Message> mesList = new ArrayList<>();
		for (String string : keySet) {
			Message mes = new Message();
			mes.setFrom(port);
			mes.setTo(next.getPort());
			mes.setReplFactor(ttl);
			mes.setReplyTo(-1);
			mes.setType(MessageType.INSERTR);
			mes.setData(new Data(string, replHashTable.get(string)));

			mesList.add(mes);
		}

		boolean res = false;
		try {
			res = sendList(mesList, next.getPort());
			return res;
		} catch (IOException e) {
			return false;
		}
	}

	private void send(Message mes, int dst) throws IOException {
		Socket sock = new Socket("127.0.0.1", dst);

		// Send the message to the server
		ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
		out.writeObject(mes);
		out.flush();
		sock.close();
	}

	private boolean sendList(ArrayList<Message> mesList, int dst) throws IOException {
		//System.out.println("[NODE" + serialId + "] in sendList.");
		for (Message m : mesList) {
			send(m, dst);
		}
		return true;
	}

	public Node getPrevious() {
		return previous;
	}

	public void setPrevious(Node previous) {
		this.previous = previous;
	}

	public Node getNext() {
		return next;
	}

	public void setNext(Node next) {
		this.next = next;
	}

	public int getSerialId() {
		return serialId;
	}

	public void setSerialId(int serialId) {
		this.serialId = serialId;
	}

	public Hashtable<String, String> getReplHashTable() {
		return replHashTable;
	}

	public void setReplHashTable(Hashtable<String, String> replHashTable) {
		this.replHashTable = replHashTable;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public Hashtable<String, String> getHashTable() {
		return hashTable;
	}

	public void setHashTable(Hashtable<String, String> hashTable) {
		this.hashTable = hashTable;
	}

	public Listener getListener() {
		return listener;
	}

	public void setListener(Listener listener) {
		this.listener = listener;
	}

	public int getClientPort() {
		return clientPort;
	}

	public void setClientPort(int clientPort) {
		this.clientPort = clientPort;
	}

	public void initiate() {
		running = true;
	}

	public void terminate() {
		running = false;
	}

	public boolean getRunning() {
		return running;
	}
	
	/*
	 *@return returns true if the lock is available at the time of invocation.
	 *Otherwise returns false. 
	 */
	public boolean getLock(){
		return lock.tryLock();
	}
	
	public void releaseLock(){
		//lock.unlock();
		lock =  new ReentrantLock();
		
	}

	public boolean isLinear(){
		return linear;
	}
	
	public void setLinear(boolean x){
		linear = x;
	}
	
	
	public int getReplFactor() {
		return replFactor;
	}

	public void setReplFactor(int replFactor) {
		this.replFactor = replFactor;
	}

	@Override
	public void run() {
		listener.listen();
		//System.out.println("[NODE" + serialId + "] exiting run.");
	}

	@Override
	public String toString() {
		return "[Node]";
	}

}
