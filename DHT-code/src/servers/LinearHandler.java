package servers;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;

import structures.Data;
import structures.Message;
import structures.MessageType;
import tools.SHA1Hash;

public class LinearHandler extends Thread {

	private Node master;
	private Message mes;

	public LinearHandler(Node m, Message msg) {
		master = m;
		mes = msg;
	}

	/**
	 * This is called whenever the handler thread starts. It checks various parameters (eg. type,
	 * range of key) and decides what actions should be taken.
	 */
	@Override
	public void run() {

		//System.out.println("Linear Handler of node" + master.getSerialId() + " started.");
		//check '*'
		if (isSpecialQuery()) {
			handleAllQuery();
			return;
		}

		//handle OK/FAIL messages.
		if ( (mes.getType().equals(MessageType.OK)) 
			|| (mes.getType().equals(MessageType.FAIL)) ) {
			handleOkFail();
			return;
		}
		
		
		//handle queryR
		if(mes.getType().equals(MessageType.QUERYR)){
			try {
				handleQueryR();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			return;
		}
		
		//handle insertR/DeleteR
		if ( (mes.getType().equals(MessageType.INSERTR)) 
			|| (mes.getType().equals(MessageType.DELETER)) ) {
			try {
				handleInsertDeleteR();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			return;
		}
		
		//handle unlock messages
		if (mes.getType().equals(MessageType.UNLOCK)) {
			//System.out.println("Node" + master.getSerialId() + " is unlocking");
			releaseLock();
			mes.setReplFactor(mes.getReplFactor() - 1);
			if (mes.getReplFactor() >= 1) {
				forward();
			}
			return;
		}

		//get key from message
		String key = null;
		if (mes.isHashed()) {
			key = mes.getData().getKey();
		} else {
			try {
				key = SHA1Hash.hash(mes.getData().getKey());
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		//if this node is responsible call corresponding method,
		//else forward to next node.
		if (isResponsible(key)) {
			switch (mes.getType()) {
			case DELETE: {
				try {
					handleDelete();
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				break;
			}
			case DEPART: {
				handleDepart();
				break;
			}
			case INSERT: {
				handleInsert();
				break;
			}
			case JOIN: {
				handleJoin();
				break;
			}
			case QUERY: {
				handleSimpleQuery();
				break;
			}
			default: {
				System.out.println("Unknown message type received, at node" + master.getSerialId());
				break;
			}
			}
		} else {
				forward();
		}
	}

	/**
	 * This is called if a replication message is found. It justs calls the corresponding
	 * replication method of master. Key must be already hashed!!!!!
	 * @throws NoSuchAlgorithmException 
	 */
	private boolean handleRepls() throws NoSuchAlgorithmException {
		//System.out.println("REPLICATION MESSAGE: " + mes);
		boolean res = false;
		if (mes.getType().equals(MessageType.INSERTR)) {
			res = master.insertR(mes.getData().getKey(), mes.getData().getVal());
		} else if (mes.getType().equals(MessageType.DELETER)) {
			res = master.deleteR(mes.getData().getKey());
		}

		//System.out.println("Changed repl hast of master.");
		return res;
	}
	
	private void handleQueryR() throws NoSuchAlgorithmException{
		
		getLock();
		
		if (mes.getReplFactor() > 1){
			//should forward
			mes.setInitialPort(mes.getInitialPort());
			mes.setFrom(master.getPort());
			mes.setTo(master.getNext().getPort());
			mes.setReplFactor(mes.getReplFactor() - 1);
			try {
				send(mes, mes.getTo());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else{
			//query to be completed.
			String res = master.queryR(SHA1Hash.hash(mes.getData().getKey()));
			Message reply = new Message();
			reply.setInitialPort(mes.getInitialPort());
			reply.setFrom(mes.getFrom());
			reply.setTo(mes.getInitialPort());
			reply.setReplyTo(mes.getReplyTo());
			reply.setData(new Data(mes.getData().getKey(), res));
			if (res == null || res.equals("")){
				reply.setType(MessageType.FAIL);
			}
			else{
				reply.setType(MessageType.OK);
			}
			
			//send reply
			try {
				send(reply, reply.getTo());
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
		//unlock
		releaseLock();
		
	}

	/**
	 * This is called if a message is of type INSERT and master node is responsible for this key.
	 * Calls master's insert method, and sends OK/FAILED back.
	 */
	private void handleInsert() {
		boolean res = false;
		
		//System.out.println("Insert message found. " + mes);
		//System.out.println("Insert until last in chain!");
		
		//Lock master
		getLock();
		
		
		//do operation
		if (mes.isHashed()) {
			res = master.insert(mes.getData().getKey(), mes.getData().getVal());
		} else {
			res = master.insert(mes.getData().getKey(), mes.getData().getVal());
		}

		
		if (mes.getReplyTo() != -1) {
			//always is 
			if ( (mes.getReplFactor() > 1) ) {
				//replicate in the next k-1 nodes
				//System.out.println(" I should repl.");
				//needs to check the value of replFact!!!!!!!!!!!!!!!!!!! ---> DONE
				mes.setResponsiblePort(master.getPort());
				mes.setReplFactor(mes.getReplFactor() - 1);
				mes.setType(MessageType.INSERTR);
				forward();
				
			}
			else {
				// insert only in this node, so send reply
				//System.err.println("Node " + master.getSerialId() + "  is about to send ok");
				Message reply = new Message();
				reply.setInitialPort(mes.getInitialPort());
				reply.setFrom(master.getPort());
				reply.setData(mes.getData());
				reply.setTo(mes.getInitialPort());
				reply.setReplyTo(mes.getReplyTo());
				if (res) {
					reply.setType(MessageType.OK);
				} else {
					reply.setType(MessageType.FAIL);
				}

				try {
					send(reply, reply.getTo());
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				//unlock only when I reply OK/FAIL
				releaseLock();
			}
		}	
		
		
		

	}
	

	/**
	 * This is called if a message is of type DELETE and master node is responsible for this key.
	 * Calls master's delete method, and sends OK/FAILED back.
	 * @throws NoSuchAlgorithmException 
	 */
	private void handleDelete() throws NoSuchAlgorithmException {
		//System.out.println("Delete message found.");
		boolean res = false;
		//do operation
		if (mes.isHashed()) {
			res = master.delete(mes.getData().getKey());
		} else {
			try {
				res = master.delete(mes.getData().getKey());
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		if (mes.getReplyTo() != -1) {
			//always is 
			if ( (mes.getReplFactor() > 1) ) {
				//replicate in the next k-1 nodes
				//System.out.println(" I should repl.");
				//needs to check the value of replFact!!!!!!!!!!!!!!!!!!! ---> DONE
				mes.setResponsiblePort(master.getPort());
				mes.setReplFactor(mes.getReplFactor() - 1);
				mes.setType(MessageType.DELETER);
				forward();
				
			}
			else {
				// insert only in this node, so send reply
				//System.err.println("Node " + master.getSerialId() + "  is about to send ok");
				Message reply = new Message();
				reply.setInitialPort(mes.getInitialPort());
				reply.setFrom(master.getPort());
				reply.setData(mes.getData());
				reply.setTo(mes.getInitialPort());
				reply.setReplyTo(mes.getReplyTo());
				if (res) {
					reply.setType(MessageType.OK);
				} else {
					reply.setType(MessageType.FAIL);
				}

				try {
					send(reply, reply.getTo());
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				//unlock only when I reply OK/FAIL
				releaseLock();
			}
		}	
		
	}

	private void handleInsertDeleteR() throws NoSuchAlgorithmException{
		
		getLock();
		
		// insert in my replication hashtable
		boolean res = handleRepls();
		
		if (mes.getReplFactor() > 1){
			//should forward
			mes.setInitialPort(mes.getInitialPort());
			mes.setReplFactor(mes.getReplFactor() - 1);
			forward();
		}
		else{
			// 1) send reply(ok/fail) to initial port
			Message reply = new Message();
			reply.setInitialPort(mes.getInitialPort());
			reply.setFrom(master.getPort());
			reply.setTo(mes.getInitialPort());
			reply.setReplyTo(mes.getReplyTo());
			reply.setData(mes.getData());
			if (res == false){
				reply.setType(MessageType.FAIL);
			}
			else{
				reply.setType(MessageType.OK);
			}
			
			//send reply
			try {
				send(reply, reply.getTo());
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			// 2) send unlock message to responsibleNode
			Message reply2 = new Message();
			reply2.setInitialPort(mes.getInitialPort());
			reply2.setFrom(master.getPort());
			reply2.setTo(mes.getResponsiblePort());
			reply2.setReplyTo(mes.getReplyTo());
			reply2.setReplFactor(master.getReplFactor());
			reply2.setType(MessageType.UNLOCK);
			
			//send unlock message
			try {
				send(reply2, reply2.getTo());
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
	}
	
	/**
	 * This is called if a message is of type QUERY and master node is responsible for this key.
	 * Calls master's query method, and sends OK/FAILED and response back.
	 */
	private void handleSimpleQuery() {
		//System.out.println("Query message found. " + mes);
		//System.out.println("Asking last in chain!");
		String res = null;
		
		//Lock master
		getLock();

		if (mes.getReplyTo() != -1) {
			//always is.
			if ( mes.getReplFactor() > 1) {
				// node with last replication is responsible to answer, so send QueryR
				Message reply = new Message();
				reply.setInitialPort(mes.getInitialPort());
				reply.setFrom(master.getPort());
				reply.setTo(master.getNext().getPort());
				reply.setReplyTo(mes.getReplyTo());
				//needs to check the value of replFact!!!!!!!!!!!!!!!!!!! ---> DONE
				reply.setReplFactor(mes.getReplFactor() - 1);
				reply.setData(mes.getData());
				reply.setType(MessageType.QUERYR);
	
				try {
					send(reply, reply.getTo());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			else {
				// only this node has replica, so send reply
				
				//do operation
				if (mes.isHashed()) {
					//never used.
					res = master.query(mes.getData().getKey());
				} else {
					try {
						res = master.query(SHA1Hash.hash(mes.getData().getKey()));
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}
				}
				
				Message reply = new Message();
				reply.setInitialPort(mes.getInitialPort());
				reply.setFrom(master.getPort());
				reply.setTo(mes.getInitialPort());
				reply.setReplyTo(mes.getReplyTo());
				reply.setData(new Data(mes.getData().getKey(), res));
				if (res == null || res.isEmpty()) {
					reply.setType(MessageType.FAIL);
				} else {
					reply.setType(MessageType.OK);
				}

				try {
					send(reply, reply.getTo());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		//unlock
		releaseLock();
	}

	/**
	 * This is called if a message is of type QUERY *. Checks if should append and forward, or if
	 * should return to user. If from!=master.port then first arrival => process and forward, else
	 * message get through all nodes send reply back to user.
	 */
	private void handleAllQuery() {
		Message reply = null;
		int dst = -1;
		if (mes.getFrom() != master.getPort()) {
			String res = master.specialQuery();
			Data temp = mes.getData();
			temp.setVal(temp.getVal() + " " + res);
			reply = new Message();
			if (mes.getFrom() == mes.getReplyTo()) {
				reply.setFrom(master.getPort());
			} else {
				reply.setFrom(mes.getFrom());
			}
			reply.setInitialPort(mes.getInitialPort());
			reply.setData(temp);
			reply.setReplyTo(mes.getReplyTo());
			reply.setTo(master.getNext().getPort());
			reply.setType(MessageType.QUERY);

			dst = reply.getTo();
		} else {
			if (master.getPort() == mes.getInitialPort() ) {
				//full circle, send back to user.
				reply = mes;
				dst = mes.getReplyTo();
			}
		}

		try {
			send(reply, dst);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * The node will be created here, and the chord emulator will then take a copy from the chord
	 * with
	 */
	private void handleJoin() {
		//System.out.println("Join message found.");

		Message reply = new Message();
		boolean res = false;
		Node newNode = null;
		String id = null;

		try {
			id = SHA1Hash.hash(mes.getData().getKey());
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		reply.setInitialPort(mes.getInitialPort());
		reply.setFrom(master.getPort());
		reply.setData(mes.getData());
		reply.setReplyTo(mes.getReplyTo());
		reply.setTo(mes.getInitialPort());

		if (id.compareTo(master.getNodeId()) != 0) {
			try {
				newNode = new Node(Integer.parseInt(mes.getData().getKey()), master.isLinear(), master.getReplFactor());
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			res = master.joinNode(newNode);
			if (res) {
				reply.setType(MessageType.OK);
				reply.setTo(mes.getReplyTo());
				
				//send repls. Method is not correct!!!!!
				if (mes.getReplFactor() > 1) {
					boolean res2 = master.joinR(mes.getReplFactor(), newNode);
					res = res && res2;
				}
			} else {
				reply.setType(MessageType.FAIL);
			}
		} else {
			System.out.println("Node already exists. Request failed.");
		}

		
		if (mes.getReplyTo() != -1) {
			try {
				//System.out.println("[HANDLER]: reply created" + reply);
				send(reply, reply.getTo());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void handleDepart() {
		//System.out.println("Depart message found.");
		String id = null;
		boolean res = false;
		if (mes.isHashed()) {
			id = mes.getData().getKey();
		} else {
			try {
				id = SHA1Hash.hash(mes.getData().getKey());
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		Message reply = new Message();
		reply.setInitialPort(mes.getInitialPort());
		reply.setData(mes.getData());
		reply.setFrom(master.getPort());
		reply.setTo(mes.getInitialPort());
		reply.setReplyTo(mes.getReplyTo());

		if (!id.equals(master.getNodeId())) {
			System.out.println("The requested node doesn't exist.");
			res = false;
		} else {
			res = master.depart();
			if (mes.getReplFactor() > 1) {
				boolean res2 = master.departR(mes.getReplFactor());
				res = res && res2;
			}
		}

		if (res) {
			reply.setType(MessageType.OK);
		} else {
			reply.setType(MessageType.FAIL);
		}

		try {
			if (mes.getReplyTo() != -1) {
				send(reply, reply.getTo());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * this is called if a message is of type OK/FAILED. In order to receive such a message master
	 * node has previously forwarded that message, and now received response from responsible node.
	 * So it just has to forward this answer to the client that made the request.
	 */
	private void handleOkFail() {
		
		if (mes.getReplyTo() != -1) {
			mes.setFrom(master.getPort());
			mes.setTo(mes.getReplyTo());
			try {
				send(mes, mes.getReplyTo());
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			//System.out.println("[NODE" + master.getSerialId() + "]: no reply generated for mes:" + mes);
		}
	}
	
	private void send(Message reply, int dst) throws IOException {
		System.out.println("Reply is: " + reply);
		Socket sock = new Socket("127.0.0.1", dst);

		// Send the message to the server
		ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
		out.writeObject(reply);
		out.flush();
		sock.close();
	}


	private boolean isResponsible(String key) {
		if (master.getPrevious().getNodeId().compareTo(master.getNodeId()) > 0) {
			/* check between last and first node */
			if ( (key.compareTo(master.getPrevious().getNodeId()) > 0)
				|| (key.compareTo(master.getNodeId()) <= 0) ) {
				return true;
			}
			else
				return false;
		}
		else {
			/* check between middle nodes */
			if ( (key.compareTo(master.getPrevious().getNodeId()) > 0)
					&& (key.compareTo(master.getNodeId()) <= 0) ) {
					return true;
			}
			else
				return false;
		}
	}
	
	private boolean isSpecialQuery() {
		if (mes == null || mes.getData() == null) {
			return false;
		} else {
			return mes.getData().getKey().equals("*");
			//return (mes.getData().getKey().equals("*")) && (mes.getType().equals(MessageType.QUERY));
		}
	}

	/**
	 * This is called if a handler receives a message with an out of range key. It changes "from" to
	 * show to its listening port, changes "to" to show to its next's node port and sends message to
	 * next node's port.
	 */
	private void forward() {
		//System.out.println("NODE" + master.getSerialId() + " not responsible. Forwarding...");
		//mes.setFrom(master.getPort());
		mes.setTo(master.getNext().getPort());
		mes.setFrom(master.getPort());
		try {
			send(mes, mes.getTo());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	private void getLock(){
		//System.out.println("[HANDLER"+master.getSerialId()+"] trying to acquire lock.");
		boolean res = false;
		while(!res){
			res = master.getLock();
			if(!res){
				System.out.println("Fail to acquire lock. Trying again...");
			}
		}
		System.out.println("Lock of node" + master.getSerialId()+" acquired!");
	}
	
	private void releaseLock(){
		System.out.println("[HANDLER" + master.getSerialId() +"] releasing node of master.");
		master.releaseLock();
	}
}

