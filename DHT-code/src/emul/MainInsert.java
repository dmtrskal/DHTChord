package emul;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.Scanner;

import servers.Node;
import structures.MessageType;

public class MainInsert {

	final static int nodes = 10;
	final static int nodeStartPort = 64000;
	static int clientStartPort = 64700;
	final static int delay = 10;
	final static boolean linear = false;	//set to true if we want to implement chained replication.
	private static int replFactor = 1;

	public static void main(String[] args) throws NoSuchAlgorithmException, IOException, InterruptedException {
		
		Chord chordEmulator = new Chord();

		for (int i = 0; i < nodes; i++) {
			Node node = chordEmulator.createNode(i, linear, replFactor);

			System.out.println("Node =  " + node.getSerialId() + " with HASH: " + node.getNodeId());
		}
		
		/* create links with neighbors for each node */
		chordEmulator.setNeighbors();
		
		System.out.println("AFTER SORTING");
		for (int i = 0; i < nodes; i++) {

			Node node = chordEmulator.getNode(i);
			System.out.println("Node =  " + node.getSerialId() + " with HASH: " + node.getNodeId()
					+ "  // previous= " + node.getPrevious().getNodeId() +
					" // next = " + node.getNext().getNodeId());
		}
		
		//check if data from file should be inserted
		System.out.print("Should data be read from file? (y/n)");
		Scanner reader = new Scanner(System.in);
		String input = reader.nextLine();
		if (input.compareTo("y") == 0){
			System.out.print("\nPlease provide path to file input:");
			input = reader.nextLine();
			readDataFromFile(input, chordEmulator);
		}
		else {
			while (input.compareTo("q") != 0){
				System.out.println("**********************************");
				System.out.println("Please give a valid action:");
				System.out.println("insert, <key>, <value>");
				System.out.println("query, <key>" + " ( \"query, '*' \" returns all data) ");
				System.out.println("delete, <key>");
				System.out.println("join, <nodeId>");
				System.out.println("depart, <nodeId>");
				System.out.println("or 'q' to quit...");
				System.out.println("**********************************");
				input = reader.nextLine();
				// process action
				if (!input.equals("q")) 
					processAction(input.split(", "), chordEmulator);
			}
		}
		reader.close();
		
		//End simulation
		chordEmulator.terminateSimulation();
	}

	
	private static void processAction(String[] split, Chord emulator) throws IOException, InterruptedException {	
		/* random ports for node and client */
		int nodePort = randInt(0, nodes) + nodeStartPort;
		int nodeSId = nodePort - nodeStartPort; 
		//System.err.println(nodePort);
		//int clientPort = clientStartPort + randInt(0, nodes);
		int clientPort = clientStartPort++;
		//System.err.println(clientPort);
		
		if (split[0].equals("insert")){
			Client myClient = new Client(emulator);
			myClient.setMyPort(clientPort);
			myClient.setServerPort(nodePort);
			myClient.setReplFactor(replFactor);
			
			myClient.setKey(split[1]);
			myClient.setValue(split[2]);				
			myClient.setServerSId(nodeSId);
			myClient.setType(MessageType.INSERT);

			Thread t = new Thread(myClient);
			t.start();

			t.join();
			/* make a delay to print messages in correct order */
			//delay(myClient);
			
		}
		else if (split[0].equals("query")) {
			Client myClient = new Client(emulator);
			myClient.setMyPort(clientPort);
			myClient.setServerPort(nodePort);
			myClient.setReplFactor(replFactor);
			
			myClient.setKey(split[1]);			
			myClient.setServerSId(nodeSId);
			myClient.setType(MessageType.QUERY);

			Thread t = new Thread(myClient);
			t.start();

			t.join();
			/* make a delay to print messages in correct order */
			//delay(myClient);
		}
		else if (split[0].equals("delete")) {	
			Client myClient = new Client(emulator);
			myClient.setMyPort(clientPort);
			myClient.setServerPort(nodePort);
			myClient.setReplFactor(replFactor);
			
			myClient.setKey(split[1]);			
			myClient.setServerSId(nodeSId);
			myClient.setType(MessageType.DELETE);

			Thread t = new Thread(myClient);
			t.start();

			t.join();
			/* make a delay to print messages in correct order */
			//delay(myClient);
		}
		else if (split[0].equals("join")) {
			Client myClient = new Client(emulator);
			myClient.setMyPort(clientPort);
			myClient.setServerPort(nodePort);
			myClient.setReplFactor(replFactor);
			
			myClient.setKey(split[1]);
			myClient.setValue("");				
			myClient.setServerSId(nodeSId);
			myClient.setType(MessageType.JOIN);

			Thread t = new Thread(myClient);
			t.start();

			t.join();
			/* make a delay to print messages in correct order */
			//delay(myClient);
		}
		else if (split[0].equals("depart")) {
			Client myClient = new Client(emulator);
			myClient.setMyPort(clientPort);
			myClient.setServerPort(nodePort);
			myClient.setReplFactor(replFactor);
			
			myClient.setKey(split[1]);
			myClient.setValue(null);				
			myClient.setServerSId(nodeSId);
			myClient.setType(MessageType.DEPART);

			Thread t = new Thread(myClient);
			t.start();
			
			t.join();
			/* make a delay to print messages in correct order */
			//delay(myClient);
		}
		else {
			System.err.println("Unknown Action!");
		}
	}
	
	
	private static void readDataFromFile(String filePath, Chord emulator) throws IOException, InterruptedException {
		
		BufferedReader br = null;
		
		try {
			// Read from file
			br = new BufferedReader(new FileReader(new File(filePath)));
			
			long startTime = System.currentTimeMillis();
			
			String line;
			while((line = br.readLine()) != null) {
				if (filePath.contains("insert")) {
					line = "insert, " + line;
					processAction(line.split(", "), emulator);
				}
				else if (filePath.contains("query")) {
					line = "query, " + line;
					processAction(line.split(", "), emulator);
				}
				else {
					// requests
					processAction(line.split(", "), emulator);
				}
				Thread.sleep(delay);
			}
			long endTime = System.currentTimeMillis();
			long miliseconds = (endTime - startTime) - 500 * delay;
			System.out.println("----------------------------------------");
			System.out.println("Time in miliseconds: " + miliseconds);
			System.out.println("Throughput(inserts/sec): " + 500*1000 / miliseconds);
			System.out.println("----------------------------------------");
			
		} finally {
			br.close();
		}
	}
	
	private static void delay(Client client) throws IOException {
		try {
			Thread.sleep(delay);
			client.getServerSock().close();
			//t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}	
	
	/**
	 * Generate random integers within a specific range [min,max]
	 **/
	public static int randInt(int min, int max) {
		Random rand = new Random();
		int randomNum = rand.nextInt((max - min)) + min;

		return randomNum;
	}
}