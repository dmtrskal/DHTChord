package servers;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;

import structures.Message;

public class Listener implements Serializable{

	private ServerSocket serverSock;
	private int listeningPort;
	private Node master;
	String prefix = "[NODE]:";

	public Listener(int listeningPort, Node master) {
		this.listeningPort = listeningPort;
		this.master = master;
	}

	public void listen() {
		//System.out.println(prefix + "Listener of node" + master.getSerialId() + " started.");
		serverSock = null;
		Socket incoming = null;
		try {
			serverSock = new ServerSocket(listeningPort);
			while (master.getRunning()) {
				incoming = serverSock.accept();
				//System.out.print(prefix + "Incoming connection, at node" + master.getSerialId());
				//System.out.println(", at port " + master.getPort());
				ObjectInputStream input = new ObjectInputStream(incoming.getInputStream());
				Message mes = (Message) input.readObject();

				Thread t;
				if (master.isLinear()){
					t = new LinearHandler(master, mes);
				}
				else{
					t = new Handler(master, mes);
				}
				t.start();

				incoming.close();
			}
			stopListen();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void stopListen() throws IOException  {
		this.serverSock.close();	
		//System.out.println("[NODE" + master.getSerialId() + "] listener ending.");
	}
	
	public int getListeningPort() {
		return listeningPort;
	}

	public void setListeningPort(int listeningPort) {
		this.listeningPort = listeningPort;
	}

	public Node getMaster() {
		return master;
	}

	public void setMaster(Node master) {
		this.master = master;
	}

	public ServerSocket getServerSock() {
		return serverSock;
	}

	public void setServerSock(ServerSocket serverSock) {
		this.serverSock = serverSock;
	}
	
	
}
