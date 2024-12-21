import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static Map<String, String> config = new HashMap<String, String>();
    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--dir")) {
                config.put("dir", args[++i]);
            }
            if (arg.equals("--dbfilename")) {
                config.put("dbfilename", args[++i]);
            }
        }

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            // Wait for connection from client.
            while (true) {

                clientSocket = serverSocket.accept();

                Thread t1 = new Thread(new Client(clientSocket));
                t1.start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}