import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
   private static final ConcurrentHashMap<String, String> dataStore = new ConcurrentHashMap<>();
   private static int expiryTime = 0;
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");
    // Uncomment this block to pass the first stage 
    ServerSocket serverSocket = null;
    int port = 6379;
    
    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connections from clients.
      while (true) {
        Socket clientSocket = serverSocket.accept();
        new Thread(() -> handleClient(clientSocket)).start();
        
      }
      
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }

  private static void handleClient(Socket clientSocket) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
         BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))) {

        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println("::" + line);
            if(expiryTime != 0) {
              expiryTime--;
            }
            if (line.startsWith("*")) {
                // Multi-bulk request (e.g., SET key value)
                int numArgs = Integer.parseInt(line.substring(1));
                String[] args = new String[numArgs];
                for (int i = 0; i < numArgs; i++) {
                    reader.readLine(); // Read length line (e.g., $3)
                    args[i] = reader.readLine(); // Read actual argument
                }

                processCommand(args, writer);
            } else {
                writer.write("-ERR Unknown command\r\n");
                writer.flush();
            }
        }
    } catch (IOException e) {
        System.out.println("Client disconnected: " + e.getMessage());
    }
  }

  private static void processCommand(String[] args, BufferedWriter writer) throws IOException {
      if (args.length < 1) {
          writer.write("-ERR Missing command\r\n");
          writer.flush();
          return;
      }

      String command = args[0].toUpperCase();
      switch (command) {
          case "ECHO" :
            if(args.length != 2) {
              writer.write("-ERR Wrong number of arguments for ECHO\r\n");
            } else {
              writer.write("+" + args[1] + "\r\n");
            }
            break;
          case "PING":
            if(args.length != 1) {
              writer.write("-ERR Wrong number of arguments for PING");
            } else {
              writer.write("+PONG\r\n");
            }
            break;
          case "SET":
              if (args.length != 3 && args.length != 5) {
                  writer.write("-ERR Wrong number of arguments for SET\r\n");
              } else if (args.length == 3) {
                  dataStore.put(args[1], args[2]);
                  writer.write("+OK\r\n");
              } else if (args.length == 5) {
                dataStore.put(args[1], args[2]);
                expiryTime = Integer.parseInt(args[4]);
                writer.write("+OK\r\n");
              }
              break;

          case "GET":
              if (args.length != 2) {
                  writer.write("-ERR Wrong number of arguments for GET\r\n");
              } else {
                  String value = dataStore.get(args[1]);
                  if (value != null && expiryTime != 0) {
                      writer.write("$" + value.length() + "\r\n" + value + "\r\n");
                  } else {
                      writer.write("$-1\r\n");
                  }
              }
              break;

          default:
              writer.write("-ERR Unknown command\r\n");
      }
      writer.flush();
  }
}

