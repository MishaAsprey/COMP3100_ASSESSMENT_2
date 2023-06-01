import java.io.*;
import java.net.*;

public class Client {

	final static String username = System.getProperty("user.name");
	final static int port = 50000;
	private static Socket socket;
	private static DataOutputStream output;
	private static BufferedReader input;
	private static int globalCounter = 0;

	private enum ASCII {
		ZERO(48),
		NINE(57),
		SPACE(32);

		private final int value;

		ASCII(int value) {
			this.value = value;
		}

		public int getValue() { return value; }
	}

	private static String receiveMsg() {
		try {
			String receivedMsg = input.readLine();
			System.out.println("RCVD <<< " + receivedMsg);
			return receivedMsg;
		} catch (Exception e) {
			System.out.println(e);
		}
		return null;
	}

	private static void sendMsg(String msg) {
		try {
			System.out.println("SENT >>> " + msg);
			output.write(msg.getBytes());
			output.flush();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	private static String handshake() {
		sendMsg("HELO\n");
		receiveMsg();
		sendMsg("AUTH " + Client.username + "\n");
		receiveMsg();
		sendMsg("REDY\n");
		String temp = receiveMsg();
		return temp;
	}

	private static String defineJob(String job) {
		String jobData = "";
		int counter = 0;

		for (int i = ("JOBN").length()+1; i < job.length(); i++) {
			if (job.charAt(i) == ASCII.SPACE.getValue()) {
				counter++;
				i++;
				if (counter >= 3) {
					jobData += " ";
				}
			}
			if (counter >=  3) {
				jobData += job.charAt(i);
			}
		}
		System.out.println(jobData);
		return jobData;
	}

	private static int findJobID(String job) {
		String jobID = "";
		int counter = 0;

		for (int i = ("JOBN").length()+1; i < job.length(); i++) {
			if (job.charAt(i) == ASCII.SPACE.getValue()) {
				counter++;
				i++;
			}
			if (counter == 1) {
				jobID += job.charAt(i);
			} else if (counter >= 2) {
				return Integer.parseInt(jobID);
			}
		}
		return -1;
		
	}

	private static String getServerName(String server) {
		String serverName = "";
		for (int i = 0; i < server.length(); i++) {
			if (server.charAt(i) != ASCII.SPACE.getValue()) {
				serverName += server.charAt(i);
			} else {
				return serverName;
			}
		}
		return "ERROR";
	}

	public static int getServerID(String server) {
		String serverID = "";
		int counter = 0;

		for (int i = 0; i < server.length(); i++) {
			if (server.charAt(i) == ASCII.SPACE.getValue()) {
				counter++;
				i++;
			}
			if (counter == 1) {
				serverID += server.charAt(i);
			} else if (counter >= 2) {
				return Integer.parseInt(serverID);
			}
		}
		return -1;
	}


	private static String getAvail(String dataMsg) {
		String num = "";
		int counter = 0;
		
		

		for (int i = 0; i < dataMsg.length(); i++) {
			if (dataMsg.charAt(i) >= ASCII.ZERO.getValue() && dataMsg.charAt(i) <= ASCII.NINE.getValue()) {
				num += dataMsg.charAt(i);
			} else if (dataMsg.charAt(i) == ASCII.SPACE.getValue()) {
				counter++;
				if (counter >= 2) { break; }
			}
		}
		int numOfServers = Integer.parseInt(num);
		
		String firstServer = receiveMsg();

		for (int i = 0; i < numOfServers-1; i++) {
			receiveMsg();
		}
		sendMsg("OK\n");
		receiveMsg();

		return firstServer;
	}

	private static Boolean serverFitness(String server, String job) {
		String serverCores = "";
		String serverRAM = "";
		String serverDISK = "";
		int startRecording = 0;
		for (int i = 0; i < server.length(); i++) {
			if (server.charAt(i) == ASCII.SPACE.getValue()) {
				startRecording++;
				i++;			
			}
			if (startRecording == 4) {
				serverCores += server.charAt(i);
			}
			else if (startRecording == 5) {
				serverRAM += server.charAt(i);
			}
			else if (startRecording == 6) {
				serverDISK += server.charAt(i);
			}
			else if (startRecording > 6) {
				break;
			}
		}

		String jobCores = "";
		String jobRAM = "";
		String jobDISK = "";
		startRecording = 0;
		for (int i = 0; i < job.length(); i++) {
			if (job.charAt(i) == ASCII.SPACE.getValue()) {
				startRecording++;
				i++;
			}
			if (startRecording == 4) {
				jobCores += job.charAt(i);
			}
			else if (startRecording == 5) {
				jobRAM += job.charAt(i);
			}
			else if (startRecording == 6) {
				jobDISK += job.charAt(i);
			}
			else if (startRecording > 6) {
				break;
			}
		}

		System.out.println("<<<<<<<<   " + serverCores + serverRAM + serverDISK +  "    >>>>>>>>\n");
		System.out.println("<<<<<<<<   " + jobCores + jobRAM + jobDISK + "   >>>>>>>>\n");
		
		int scores = Integer.parseInt(serverCores);
		int sram = Integer.parseInt(serverRAM);
		int sdisk = Integer.parseInt(serverDISK);

		int jcores = Integer.parseInt(jobCores);
		int jram = Integer.parseInt(jobRAM);
		int jdisk = Integer.parseInt(jobDISK);

		if (scores >= jcores && sram >= jram && sdisk >= jdisk) {
			return true;
		}
		
		return false;
	}

	private static String getCapable(String dataMsg, String job) {
		String num = "";
		int counter = 0;

		for (int i = 0; i < dataMsg.length(); i++) {
			if (dataMsg.charAt(i) >= ASCII.ZERO.getValue() && dataMsg.charAt(i) <= ASCII.NINE.getValue()) {
				num += dataMsg.charAt(i);
			} else if (dataMsg.charAt(i) == ASCII.SPACE.getValue()) {
				counter++;
				if (counter >= 2) { break; }
			}
		}
		int numOfServers = Integer.parseInt(num);
		
		globalCounter++;
		if (globalCounter >= numOfServers) {
			globalCounter = 0;
		}

		String idle = "";
		int counter2 = 0;
		

		String firstServer = receiveMsg();

		for (int i = 0; i < numOfServers - 1; i++) {
			idle = receiveMsg();
			counter2++;

			if (i == globalCounter) {
				firstServer = idle;
			}

			if (idle.contains("booting")) { continue; }
			if (idle.contains("inactive")) { continue; }
			if (idle.contains("idle")) {
				for (int j = 0; j < numOfServers -counter2 - 1; j++) {
					receiveMsg();
				}
				sendMsg("OK\n");
				receiveMsg();
				return idle;
			}
			if (serverFitness(idle, job)) {
				for (int j = 0; j < numOfServers - counter2 - 1; j++) {
					receiveMsg();
				}
				sendMsg("OK\n");
				receiveMsg();
				return idle;
			}
		}

		for (int i = 0; i < numOfServers - counter2 - 1; i++) {
			receiveMsg();
		}
		sendMsg("OK\n");
		receiveMsg();
		return firstServer;
	}
		

	public static void main(String args[]) {
		try {
			System.out.println("Hello, World!");
			
			socket = new Socket("localhost", Client.port);
			output = new DataOutputStream(socket.getOutputStream());
			input = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			String temp = handshake();
			sendMsg("GETS Avail" + defineJob(temp) + "\n");
			String dataMsg = receiveMsg();
			sendMsg("OK\n");

			String firstServer;
			firstServer = getAvail(dataMsg);

			sendMsg("SCHD " + findJobID(temp) + " " + getServerName(firstServer) + " " + getServerID(firstServer) + "\n");
			receiveMsg();

			while (true) {
				sendMsg("REDY\n");
				temp = receiveMsg();
				if (temp.toLowerCase().contains(("JCPL").toLowerCase())) { continue; }
				if (temp.toLowerCase().contains(("NONE").toLowerCase())) { break; }
				sendMsg("GETS Avail" + defineJob(temp) + "\n");
				dataMsg = receiveMsg();
				System.out.println("------------------------------------\n");
				sendMsg("OK\n");
				firstServer = getAvail(dataMsg);
				if (firstServer.contains(".")) { break; }
				sendMsg("SCHD " + findJobID(temp) + " " + getServerName(firstServer) + " " + getServerID(firstServer) + "\n");
				receiveMsg();
			}

			receiveMsg();
			
			while (true) {
				sendMsg("REDY\n");
				temp = receiveMsg();
				if (temp.toLowerCase().contains(("JCPL").toLowerCase())) { continue; }
				if (temp.toLowerCase().contains(("NONE").toLowerCase())) { break; }
				sendMsg("GETS Capable" + defineJob(temp) + "\n");
				dataMsg = receiveMsg();
				sendMsg("OK\n");
				firstServer = getCapable(dataMsg, temp);
				if (firstServer.contains(".")) { break; }
				sendMsg("SCHD " + findJobID(temp) + " " + getServerName(firstServer) + " " + getServerID(firstServer) + "\n");
				receiveMsg();
			}

			sendMsg("QUIT\n");
		} catch (Exception e) {
			System.out.println(e);
		}

	}
}
