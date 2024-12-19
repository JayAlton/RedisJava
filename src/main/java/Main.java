import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
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
      setterGetter[] setterArr = new setterGetter[]{};
      int count = 0;
      while (true) {
        Socket clientSocket = serverSocket.accept();
        String[] strArray = {"", ""};
        System.out.println(set(clientSocket));
        strArray = set(clientSocket); 
        setterArr[count] = new setterGetter(strArray[0], strArray[1]);
        get(clientSocket, setterArr);
        new Thread(() -> {
          try {
            process(clientSocket);
          } catch(Exception e) {
            System.out.println("Exception: " + e.getMessage());
          }
        }).start();
        count++;
      }
      
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }

  private static void get(Socket clientSocket, setterGetter[] setterArr) {
    try(BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));) {
      String content;
      while((content = reader.readLine()) != null) {
        System.out.println("::" + content);
       if ("GET".equalsIgnoreCase(content)) {
          content = reader.readLine();
          for(int i = 0; i < setterArr.length; i++) {
            if(content.equalsIgnoreCase(setterArr[i].getter)) {
              writer.write("$3\r\n" + setterArr[i].setString + "\r\n");
              writer.flush();
            }
          }  
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  private static String[] set(Socket clientSocket) {
    try(BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));) {
      String content;
      String setter;
      String str;
      while((content = reader.readLine()) != null) {
        System.out.println("::" + content);
       if ("SET".equalsIgnoreCase(content)) {
          reader.readLine();
          setter = reader.readLine();
          reader.readLine();
          str = reader.readLine();
          writer.write("+OK\r\n");
          writer.flush();
          return new String[]{setter, str};
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new String[]{"", ""};
  }
  private static void process(Socket clientSocket) {
    try(BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));) {
      String content;
      while((content = reader.readLine()) != null) {
        System.out.println("::" + content);
        if("ping".equalsIgnoreCase(content)) {
          writer.write("+PONG\r\n");
          writer.flush();
        } else if("ECHO".equalsIgnoreCase(content)) {
          for(int i = 0; i < 2; i++) {
            content = reader.readLine();
          }
          writer.write("+" + content + "\r\n");
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

