package emul;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import servers.Node;
import structures.Data;
import structures.Message;
import structures.MessageJoin;
import structures.MessageType;

/*
 * Client class. Created from emulator and emulates (:P) a user that connects to
 * a server of our chord and makes a request.
 */
public class Client implements Runnable {
	
	private ServerSocket serverSock;
	private int myPort;
	private int serverPort;
	private int serverSId;

	private MessageType type;
	private String key;
	private String value = "";
	private int replFactor;
	
	private Message reply;

	private Chord emulator;

	private String prefix = "[CLIENT]:";

	public Client(Chord emul) {
		emulator = emul;
	}

	@Override
	public void run() {
		//System.out.println("Client started.");

		try {
			sendRequest();
		} catch (Exception error) {
			System.err.println(error);
			System.exit(-1);
		}

		//System.out.println("Request made. Waiting for reply...");
		getReply();

		//System.out.println("Reply received. Processing...");
		processReply();

		//System.out.println("Client terminating...");
	}

	private Message createMessage() throws Exception {
		//System.out.println(prefix + "Creating message.");
		Message request = new Message();
		request.setInitialPort(serverPort); /* initialPort is always the port of node in chord */
		request.setFrom(myPort); 
		request.setTo(serverPort);
		request.setReplyTo(myPort);
		request.setReplFactor(replFactor);
		switch (type) {
		case DELETE: {
			request.setType(MessageType.DELETE);
			request.setData(new Data(key, value));
			break;
		}
		case DEPART: {
			System.out.println(
					"Bootstraped is " + emulator.getBootstrapped().getSerialId() + ". Departing is " + key);
			if (emulator.getBootstrapped().getSerialId() == Integer.parseInt(key)) {
				System.err.println("Cannot remove bootstrapped node! Request failed");
				Exception error = new Exception("Please select a node except node 0 -> bootstramp");
				throw error; /* throw error and exit() */
			} else {
				request.setInitialPort(emulator.getBootstrapped().getPort()); /* initialPort is always the port of node in chord */
				request.setTo(emulator.getBootstrapped().getPort());
				request.setData(new Data(key, value));
				request.setType(MessageType.DEPART);
			}
			break;
		}
		case FAIL: {
			System.out.println("internal error FAIL");
			return null;
		}
		case INSERT: {
			request.setType(MessageType.INSERT);
			request.setData(new Data(key, value));
			break;
		}
		case JOIN: {
			request.setInitialPort(emulator.getBootstrapped().getPort()); /* initialPort is always the port of node in chord */
			request.setTo(emulator.getBootstrapped().getPort());
			request.setData(new Data(key, value));
			request.setType(MessageType.JOIN);
			break;
		}
		case OK: {
			System.out.println("internal error OK");
			return null;
		}
		case QUERY: {
			request.setType(MessageType.QUERY);
			request.setData(new Data(key, value));
			break;
		}
		default: {
			System.out.println("Unknown option. Returning...");
			return null;
		}

		}

		System.out.println(prefix + "Message created:");
		System.out.println(request);
		return request;
	}

	private void sendRequest() throws Exception {
		Socket reqSock;
		try {
			reqSock = new Socket("127.0.0.1", serverPort);

			//System.out.print("Client connected to server " + serverSId);
			//System.out.println(" at port " + serverPort + ". Making request...");

			//create message
			Message request = createMessage();

			//Send the message to the server
			ObjectOutputStream out;
			out = new ObjectOutputStream(reqSock.getOutputStream());
			out.writeObject(request);
			out.flush();
			reqSock.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void getReply() {

		Socket incoming;
		try {
			serverSock = new ServerSocket(myPort);
			incoming = serverSock.accept();
			ObjectInputStream input = new ObjectInputStream(incoming.getInputStream());
			Message mes = (Message) input.readObject();

			reply = mes;

			serverSock.close();
			incoming.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void processReply() {
		if (reply.getType().equals(MessageType.FAIL)) {
			System.out.println("Request Failed.");
		} else {
			System.out.println("Request completed.");
			if (type.equals(MessageType.QUERY)) {
				System.out.println("[CLIENT]: ----->Value of " + key + ", is " + reply.getData().getVal());
			} else if (type.equals(MessageType.INSERT)) {
				System.out.println("[CLIENT]: ----->[" + key + ", " + value + "] was added.");
			} else if (type.equals(MessageType.DELETE)) {
				System.out.println("[" + key + "] was deleted.");
			} else if (type.equals(MessageType.JOIN)) {
				joinReply();
			} else if (type.equals(MessageType.DEPART)) {
				System.out.println("[NODE" + key + "] has departed.");
				departReply();
			}
		}
	}

	private void joinReply() {
		Node temp = null;
		temp = cheat();
		System.out.println("Join completed. Added node " + temp.getSerialId());
	}

	private Node cheat() {
		Node temp = null;
		for (Node e : emulator.getChordList()) {
			if (e.getPort() == reply.getFrom()) {
				temp = e.getPrevious();
				break;
			}
		}
		emulator.getChordList().add(emulator.getChordList().indexOf(temp.getNext()), temp);
		return temp;
	}

	/**
	 * on successful return from depart. Removes node from chordList.
	 */
	private void departReply() {
		System.out.println("Removing from emulator");
		for (Node dead : emulator.getChordList()) {
			if (dead.getSerialId() == Integer.parseInt(key)) {
				emulator.getChordList().remove(dead);
				return;
			}
		}
		System.out.println("DepartReply: INTERNAL ERROR!!!!!!!!");
	}

	public int getMyPort() {
		return myPort;
	}

	public void setMyPort(int myPort) {
		this.myPort = myPort;
	}

	public int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	public int getServerSId() {
		return serverSId;
	}

	public void setServerSId(int serverSId) {
		this.serverSId = serverSId;
	}

	public MessageType getType() {
		return type;
	}

	public void setType(MessageType type) {
		this.type = type;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public void setReply(Message reply) {
		this.reply = reply;
	}

	public ServerSocket getServerSock() {
		return serverSock;
	}

	public void setServerSock(ServerSocket serverSock) {
		this.serverSock = serverSock;
	}

	public int getReplFactor() {
		return replFactor;
	}

	public void setReplFactor(int replFactor) {
		this.replFactor = replFactor;
	}

	
	
}