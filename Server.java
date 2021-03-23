import java.util.*;
import java.net.*;
import java.io.*;

public class Server {
	private Socket socket = null;
	private ServerSocket server = null;
	private DataInputStream input = null;
	private DataOutputStream output = null;
	private static int partitionPower;
	private static ArrayList<String> disks;
	//the keys are the names of the files, and the values are the hashes of the files
	private static HashMap<String, String> hashes = new HashMap<String, String>();
	//the keys are the disks, and the values are the files stored on the disks
	private static HashMap<String, ArrayList<String>> table1 = new HashMap<String, ArrayList<String>>();
	private static HashMap<String, ArrayList<String>> table2 = new HashMap<String, ArrayList<String>>();

	public Server(int partitionPower, String[] ipAddresses) {
		this.partitionPower = partitionPower;
		disks = new ArrayList<String>(Arrays.asList(ipAddresses));
		for (String ipAddress : ipAddresses) {
			table1.put(ipAddress, new ArrayList<String>());
			table2.put(ipAddress, new ArrayList<String>());
		}
		for (String disk1 : disks) {
			String[] commandOutput = runCommand("ssh " + disk1 + " ls /tmp/achen2/").split("\n");
			ArrayList<String> lines = new ArrayList<String>(Arrays.asList(commandOutput));
			ArrayList<String> files = new ArrayList<String>();
			for (String line : lines) {
				if ((line.length() > 4) && (line.substring(0, 4).equals("hash"))) {
					String hash = runCommand("ssh " + disk1 + " cat /tmp/achen2/" + line);
					hashes.put(line.substring(4), hash.replace("\n", ""));
					files.add(line.substring(4));
				}
			}
			table1.get(disk1).addAll(files);
			int index = (disks.indexOf(disk1) + 1) % disks.size();
			String disk2 = disks.get(index);
			table2.get(disk2).addAll(files);
		}
		try {
			server = new ServerSocket(0);
			System.out.println(server.getLocalPort());
			while (true) {
				try {
					socket = server.accept();
					String string = socket.getRemoteSocketAddress().toString();
					string = string.substring(1, string.indexOf(":"));
					input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
					output = new DataOutputStream(socket.getOutputStream());
					Thread thread = new ClientHandler(socket, input, output);
					thread.start();
				}
				catch (IOException i) {
					System.out.println(i);
				}
			}
		}
		catch (IOException i) {
			System.out.println(i);
		}
	}
	
	/*
	run shell commands
	@param command the shell command that is run
	@return the output of the shell command
	*/
	public static String runCommand(String command) {
		System.out.println(command);
		try {
			Process process = Runtime.getRuntime().exec(command);
			BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String lines = "";
			String line;
			while ((line = reader.readLine()) != null) {
				lines += line + "\n";
			}
			return lines;
		}
		catch (IOException i) {
			System.out.println(i);
		}
		return "";
	}
	
	/*
	run md5sum
	@param file the file that is hashed
	@return the hash of the file
	*/
	public static String hashFunction(String file) {
		int bits = (128 - partitionPower) / 4;
		String hash = runCommand("md5sum " + file).split(" ")[0];
		return hash.substring(hash.length() - bits);
	}

	/*
	display the content of the file
	@param file the file that is downloaded
	@return the content of the file
	*/
	public static String download(String file) {
		String hash = hashes.get(file);
		String disk1 = "";
		for (Map.Entry<String, ArrayList<String>> entry : table1.entrySet()) {
			if (entry.getValue().contains(file)) {
				disk1 = entry.getKey();
			}
		}
		String disk2 = "";
		for (Map.Entry<String, ArrayList<String>> entry : table2.entrySet()) {
			if (entry.getValue().contains(file)) {
				disk2 = entry.getKey();
			}
		}
		if ((runCommand("ssh " + disk1 + " ls /tmp/achen2/" + file) == "") && (runCommand("ssh " + disk2 + " ls /tmp/achen2/" + file) == "")) {
			return "error: file not found";
		}
		else {
			if (runCommand("ssh " + disk1 + " ls /tmp/achen2/" + file) != "") {
				runCommand("scp " + disk1 + ":/tmp/achen2/" + file + " ./");
				if (hashFunction(file).equals(hash)) {
					return "original file downloaded\n" + runCommand("cat " + file);
				}
			}
			if (runCommand("ssh " + disk2 + " ls /tmp/achen2/" + file) != "") {
				runCommand("scp " + disk2 + ":/tmp/achen2/" + file + " ./");
				runCommand("scp " + disk2 + ":/tmp/achen2/" + file + " " + disk1 + ":/tmp/achen2/" + file);
				if (hashFunction(file).equals(hash)) {
					return "backup file downloaded\n" + runCommand("cat " + file);
				}
			}
			return "files corrupted";
		}
	}

	/*
	display the user's files
	@param user the user that is listed
	@return the user's files
	*/
	public static String list(String user) {
		String commandOutput = "";
		ArrayList<String> files = new ArrayList<String>();
		for (Map.Entry<String, String> entry : hashes.entrySet()) {
			String file = entry.getKey();
			System.out.println(file);
			int index = file.indexOf("_", file.indexOf("_") + 1);
			if (file.substring(1, index).equals(user)) {
				files.add(file);
			}
		}
		if (files.size() == 0) {
			return "user not found";
		}
		else {
			for (String file : files) {
				for (Map.Entry<String, ArrayList<String>> entry : table1.entrySet()) {
					if (entry.getValue().contains(file)) {
						String disk = entry.getKey();
						commandOutput += runCommand("ssh " + disk + " ls -lrt /tmp/achen2/" + file);
					}
				}
			}
		}
		return commandOutput;
	}

	/*
	upload the file
	@param file the file that is uploaded
	*/
	public static void uploadFile(String disk, String file) {
		runCommand("ssh " + disk + " cd /tmp && mkdir -p achen2 && chmod 777 achen2");
		runCommand("scp " + file + " " + disk + ":/tmp/achen2/" + file);
		runCommand("ssh " + disk + " cd /tmp/achen2/ && chmod 666 " + file);
	}

	/*
	delete the file
	@param file the file that is deleted
	*/
	public static void deleteFile(String disk, String file) {
		runCommand("ssh " + disk + " rm /tmp/achen2/" + file);
	}

	/*
	move the file
	@param disk1 the old disk
	@param disk2 the new disk
	@param original true if the original file is moved, false if the backup file is moved
	*/
	public static void moveFile(String disk1, String disk2, String file, boolean original) {
		runCommand("ssh " + disk2 + " cd /tmp && mkdir -p achen2 && chmod 777 achen2");
		runCommand("scp " + disk1 + ":/tmp/achen2/" + file + " " + disk2 + ":/tmp/achen2/" + file);
		runCommand("ssh " + disk2 + " cd /tmp/achen2/ && chmod 666 " + file);
		if (original == true) {
			runCommand("scp " + disk1 + ":/tmp/achen2/hash" + file + " " + disk2 + ":/tmp/achen2/hash" + file);
			runCommand("ssh " + disk1 + " rm /tmp/achen2/hash" + file);
		}
	}

	/*
	upload the file and display the disks that store the file
	@param file the file that is uploaded
	@return the disks that store the file
	*/
	public static String upload(String file) {
		String disk1 = table1.entrySet().iterator().next().getKey();
		for (Map.Entry<String, ArrayList<String>> entry : table1.entrySet()) {
			if (entry.getValue().size() < table1.get(disk1).size()) {
				disk1 = entry.getKey();
			}
		}
		String hash = hashFunction(file);
		hashes.put(file, hash);
		table1.get(disk1).add(file);
		uploadFile(disk1, file);
		runCommand("ssh " + disk1 + " echo " + hash + " > /tmp/achen2/hash" + file);
		System.out.println("file uploaded");
		int index = (disks.indexOf(disk1) + 1) % disks.size();
		String disk2 = disks.get(index);
		table2.get(disk2).add(file);
		uploadFile(disk2, file);
		System.out.println("file uploaded");
		return disk1 + " " + disk2;
	}

	/*
	delete the file and display a confirmation or error message
	@param file the file that is deleted
	@return a confirmation or error message
	*/
	public static String delete(String file) {
		boolean found = false;
		for (Map.Entry<String, ArrayList<String>> entry : table1.entrySet()) {
			while (entry.getValue().contains(file)) {
				String disk = entry.getKey();
				deleteFile(disk, file);
				deleteFile(disk, "hash" + file);
				if (runCommand("ssh " + disk + " cd /tmp/achen2/ && ls " + file) == "") {
					found = true;
				}
				table1.get(entry.getKey()).remove(file);
			}
		}
		for (Map.Entry<String, ArrayList<String>> entry : table2.entrySet()) {
			while (entry.getValue().contains(file)) {
				String disk = entry.getKey();
				deleteFile(disk, file);
				if (runCommand("ssh " + disk + " cd /tmp/achen2/ && ls " + file) == "") {
					found = true;
				}
				table2.get(entry.getKey()).remove(file);
			}
		}
		if (found == false) {
			return "error: file not found";
		}
		else {
			return "file deleted";
		}
	}

	/*
	sort disks by the number of files stored on the disk
	@param disks the unsorted disks
	@return the sorted disks
	*/
	public static ArrayList<String> sortDisks(HashMap<String, ArrayList<String>> disks) {
		HashMap<String, Integer> sizes = new HashMap<String, Integer>();
		for (Map.Entry<String, ArrayList<String>> entry : disks.entrySet()) {
			sizes.put(entry.getKey(), entry.getValue().size());
		}
		HashMap<String, Integer> sortedSizes = new HashMap<String, Integer>();
		ArrayList<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(sizes.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});
		ArrayList<String> sortedDisks = new ArrayList<String>();
		for (Map.Entry<String, Integer> entry : list) {
			sortedDisks.add(entry.getKey());
		}
		return sortedDisks;
	}

	/*
	add the disk and display the new partitions
	@param disk the disk that is added
	@return the new partitions
	*/
	public static String add(String disk) {
		if (disks.contains(disk)) {
			return "error: disk found";
		}
		else {
			int size1 = Integer.MIN_VALUE;
			int size2 = Integer.MAX_VALUE;
			for (Map.Entry<String, ArrayList<String>> entry : table1.entrySet()) {
				if (entry.getValue().size() > size1) {
					size2 = size1;
					size1 = entry.getValue().size();
				}
				else if (entry.getValue().size() > size2) {
					size2 = entry.getValue().size();
				}
			}
			if (size1 != 0) {
				ArrayList<String> sortedDisks = sortDisks(table1);
				ArrayList<String> files = new ArrayList<String>();
				int i = 0;
				while (files.size() < size2) {
					String file = table1.get(sortedDisks.get(i)).remove(0);
					moveFile(sortedDisks.get(i), disk, file, true);
					System.out.println("move original " + file + " from " + sortedDisks.get(i) + " to " + disk);
					files.add(file);
					int index = (disks.indexOf(sortedDisks.get(i)) + 1) % disks.size();
					deleteFile(disks.get(index), file);
					table2.get(disks.get(index)).remove(file);
					if (i == sortedDisks.size() - 1) {
						i = 0;
					}
					else {
						i++;
					}
				}
				table1.put(disk, files);
				table2.put(disk, new ArrayList<String>());
				disks.add(disk);
				for (String file : table2.get(disks.get(0))) {
					moveFile(disks.get(0), disks.get(disks.size() - 1), file, false);
					System.out.println("move backup " + file + " from " + disks.get(0) + " to " + disks.get(disks.size() - 1));
				}
				table2.get(disks.get(disks.size() - 1)).addAll(table2.get(disks.get(0)));
				for (String file : table2.get(disks.get(0))) {
					deleteFile(disks.get(0), file);
				}
				table2.get(disks.get(0)).clear();
				for (String file : files) {
					moveFile(disk, disks.get(0), file, false);
					System.out.println("move backup " + file + " from " + disk + " to " + disks.get(0));
				}
				table2.get(disks.get(0)).addAll(files);
			}
			else {
				table1.put(disk, new ArrayList<String>());
				table2.put(disk, new ArrayList<String>());
				disks.add(disk);
			}
			return "disk added";
		}
	}

	/*
	remove the disk and display the new partitions
	@param disk the disk that is removed
	@return the new partitions
	*/
	public static String remove(String disk) {
		if (!disks.contains(disk)) {
			return "error: disk not found";
		}
		else {
			ArrayList<String> files1 = table1.get(disk);
			ArrayList<String> files2 = table2.get(disk);
			table1.remove(disk);
			table2.remove(disk);
			int temp = (disks.indexOf(disk) + 1) % disks.size();
			table2.get(disks.get(temp)).clear();
			disks.remove(disk);
			if (temp > 0) {
				temp--;
			}
			if (files1.size() > 0) {
				ArrayList<String> sortedDisks = sortDisks(table1);
				Collections.reverse(sortedDisks);
				int i = 0;
				for (String file : files1) {
					table1.get(sortedDisks.get(i)).add(file);
					moveFile(disk, sortedDisks.get(i), file, true);
					System.out.println("move original " + file + " from " + disk + " to " + sortedDisks.get(i));
					int index = (disks.indexOf(sortedDisks.get(i)) + 1) % disks.size();
					moveFile(disk, disks.get(index), file, false);
					System.out.println("move backup " + file + " from " + disk + " to " + disks.get(index));
					table2.get(disks.get(index)).add(file);
					if (i == sortedDisks.size() - 1) {
						i = 0;
					}
					else {
						i++;
					}
				}
				for (String file : files2) {
					moveFile(disk, disks.get(temp), file, false);
					System.out.println("move backup " + file + " from " + disk + " to " + disks.get(temp));
				}
				table2.get(disks.get(temp)).addAll(files2);
				for (String file : files1) {
					deleteFile(disk, file);
				}
				for (String file : files2) {
					deleteFile(disk, file);
				}
			}
			else {
				table1.remove(disk);
				table2.remove(disk);
				disks.remove(disk);
			}
			return "disk removed";
		}
	}

	public static void main(String[] args) {
		int partitionPower = Integer.parseInt(args[0]);
		String[] ipAddresses = Arrays.copyOfRange(args, 1, args.length);
		Server server = new Server(partitionPower, ipAddresses);
	}
}

class ClientHandler extends Thread {
	private Socket socket = null;
	private DataInputStream input = null;
	private DataOutputStream output = null;

	public ClientHandler(Socket socket, DataInputStream input, DataOutputStream output) {
		this.socket = socket;
		this.input = input;
		this.output = output;
	}

	public void run() {
		while (true) {
			try {
				String line = "";
				line = input.readUTF();
				System.out.println(line);
				String commandOutput = "";
				switch (line.split(" ")[0]) {
					case "download":
						commandOutput = Server.download(line.split(" ")[1]);
						break;
					case "list":
						commandOutput = Server.list(line.split(" ")[1]);
						break;
					case "upload":
						commandOutput = Server.upload(line.split(" ")[1]);
						break;
					case "delete":
						commandOutput = Server.delete(line.split(" ")[1]);
						break;
					case "add":
						commandOutput = Server.add(line.split(" ")[1]);
						break;
					case "remove":
						commandOutput = Server.remove(line.split(" ")[1]);
						break;
				}
				System.out.println(commandOutput);
				output.writeUTF(commandOutput);
			}
			catch (IOException e) {
				try {
					socket.close();
					input.close();
					output.close();
				}
				catch (IOException i) {
					System.out.println(i);
				}
			}
		}
	}
}