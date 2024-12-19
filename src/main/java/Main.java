import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");
    // Uncomment this block to pass the first stage 
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 6379;
    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connections from clients.
      while (clientSocket.isConnected()) {
        clientSocket = serverSocket.accept();
        new Thread(() -> {
          try {
            pingPong(clientSocket);
          } catch(Exception e) {
            System.out.println("Exception: " + e.getMessage());
          }
        }
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

  private static void pingPong(Socket clientSocket) {
    try(BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));) {
      String content;
      while((content = reader.readLine()) != null) {
        System.out.println("::" + content);
        if("ping".equalsIgnoreCase(content)) {
          writer.write("+PONG\r\n");
          writer.flush();
        } else if ("eof".equalsIgnoreCase(content)) {
          System.out.println("eof");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
