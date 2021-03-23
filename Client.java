import java.net.*;
import java.io.*;

public class Client {
	private Socket socket = null;
	private BufferedReader buffer = null;
	private DataInputStream input = null;
	private DataOutputStream output = null;

	public Client(String address, int port) {
		try {
			socket = new Socket(address, port);
			buffer = new BufferedReader(new InputStreamReader(System.in));
			input = new DataInputStream(socket.getInputStream());
			output = new DataOutputStream(socket.getOutputStream());
		}
		catch (UnknownHostException u) {
			System.out.println(u);
		}
		catch (IOException i) {
			System.out.println(i);
		}
		try {
			System.out.print("> ");
			String line = "";
			while (true) {
				line = buffer.readLine();
				if (line.split(" ")[0].equals("upload")) {
					String file = line.split(" ")[1];
					if (runCommand("ls " + file) == "") {
						System.out.println("error: file not found");
						line = "error: file not found";
					}
					else if ((file.indexOf("_") == -1) || (file.indexOf("_", file.indexOf("_")) == -1)) {
						System.out.println("error: file name not in the form of _user_object");
						line = "error: file name not in the form of _user_object";
					}
					else {
						System.out.println("file uploaded");
					}
				}
				output.writeUTF(line);
				System.out.println(input.readUTF());
				System.out.print("> ");
			}
		}
		catch (IOException i) {
			System.out.println(i);
		}
		try {
			buffer.close();
			output.close();
			socket.close();
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

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("wrong number of arguments");
		}
		else {
			String ipAddress = args[0];
			int portNumber = Integer.parseInt(args[1]);
			Client client = new Client(ipAddress, portNumber);
		}
	}
}